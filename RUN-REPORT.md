# RUN-REPORT

Phase-by-phase log of what was built and the real command output that proves it. Hardware for every run unless stated otherwise: author's laptop, Windows 11 Home 10.0.26300, 12 logical CPUs, 16 GB RAM, JDK 22.0.1 (compiling `--release 21`), Docker Desktop 29.1.2 (WSL2 VM: 12 CPUs, 7.6 GiB).

---

## Phase 0: Foundation

Built: git repo, `.gitignore`, `.gitattributes`, Maven wrapper (Maven 3.9.11), multi-module build (`broker`, `client`, `bench`), Error Prone, Spotless (google-java-format), JaCoCo with an 80% line gate on `dev.logforge.log` and `dev.logforge.replication`, GitHub Actions workflow, `AGENTS.md`, `CLAUDE.md`, `docs/PRD.md`, `docs/ARCHITECTURE.md`, `docs/API.md`, `docs/PROTOCOL.md`, `docs/DECISIONS.md`.

### Verified
- `./mvnw -B -ntp verify` on the empty skeleton:
  ```
  [INFO] LogForge ........................................... SUCCESS [  5.974 s]
  [INFO] LogForge Broker .................................... SUCCESS [ 13.553 s]
  [INFO] LogForge Client .................................... SUCCESS [  0.577 s]
  [INFO] LogForge Bench ..................................... SUCCESS [  7.605 s]
  [INFO] BUILD SUCCESS
  ```
- Error Prone is active: a planted `new RuntimeException("x");` statement failed compilation with
  `[DeadException] Exception created but not thrown`. The probe file was then deleted.

### Not verified / assumed
- **CI green on GitHub Actions is not verified.** Nothing has been pushed (the author's rules require approval before any push). The workflow runs the same `./mvnw -B -ntp verify` that passed locally, on Temurin 21 instead of JDK 22.
- `PROTOCOL.md` completeness for a third-party client is a judgement; it is exercised in Phase 3 by a test that builds a Produce request from raw bytes following the document only.

---

## Phase 1: Record batch format and segment files

### Verified
- `./mvnw -B -ntp -pl broker test`: `RecordBatchTest` 13/13, `OffsetIndexTest` 6/6, `LogSegmentTest` 7/7.
- Corruption detection proven: CRC mismatch, wrong magic, bad length, truncated final record (`RecordBatchTest`); recovery truncates a torn tail and stops at the first corrupt batch (`LogSegmentTest`).
- Offset lookup proven for first, middle and last offsets of a 1000-batch segment (`LogSegmentTest.indexLookupResolvesFirstMiddleAndLastOffsets`).

## Phase 2: Log, segment roll, retention and recovery

### Verified
- `./mvnw -B -ntp -pl broker verify`: 45 tests, 0 failures, `All coverage checks have been met.`, `BUILD SUCCESS`.
- 100k appends across forced rolls read back exactly, before and after reopen (`LogTest`).
- Gate, SIGKILL recovery 10 consecutive runs (each run = 10 kill cycles of a child JVM via `destroyForcibly`), command:
  `for i in 1..10: ./mvnw -B -ntp -o -q -pl broker surefire:test -Dtest=LogCrashRecoveryTest`
  ```
  run 1 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 387007
  run 2 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 503807
  run 3 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 556031
  run 4 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 495679
  run 5 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 461055
  run 6 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 524287
  run 7 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 704511
  run 8 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 720895
  run 9 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 696319
  run 10 exit=0 SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 860159
  ```

### Not verified / assumed
- These runs were on Windows (`TerminateProcess`). No kill landed inside a write there (0 torn tails), so torn-tail truncation is proven only by the deterministic truncation tests. The same test still needs a run on Linux (real `SIGKILL`, where a kill can interrupt a multi-page write).
- Fixed during Phase 2: on Windows, resizing a sealed segment's index on reopen failed with "user-mapped section open". Sealed indexes are now mapped at their existing size, and resizes retry briefly.

---

## Phase 3: Network layer and single-broker produce and fetch

Built: `NioServer` (1 acceptor, N IO threads with own selectors, length-prefixed frames checked before allocation, per-connection in-order pipelining, backpressure by muting reads), `BrokerApis` dispatch, `ReplicaManager`/`Partition`/`IsrManager`/`DelayedOperations` (acks 0/1/all, long-poll fetch), `Controller` (registration by heartbeat, topic create/delete, AlterIsr validation, atomic `metadata.json`), `Node`, `Main`; Java `Producer` (batching, linger, retries, idempotence, bounded buffer), `Consumer` (manual assignment, parallel long-poll per leader), `Admin`.

### Verified
- Broker tests, `./mvnw -B -ntp test -pl broker,client -Dtest=NioServerTest,SingleBrokerTest,ThirdPartyProtocolTest,PartitionerTest,EndToEndTest,MillionRecordsTest`:
  ```
  Tests run: 4, Failures: 0 -- NioServerTest      (pipelining 50 requests in order; 5 malformed frame kinds each close only their
                                                   connection; slow reader muted while another client stays under 5 s; bad version)
  Tests run: 8, Failures: 0 -- SingleBrokerTest   (acks 0/1/all, long poll, typed errors, list offsets, restart, delete)
  Tests run: 1, Failures: 0 -- ThirdPartyProtocolTest (client written only from PROTOCOL.md: Produce and Fetch by hand)
  Tests run: 3, Failures: 0 -- EndToEndTest       (real socket produce then consume; same key same partition; acks all/none)
  Tests run: 3, Failures: 0 -- PartitionerTest
  ```
- **Gate, 1 million records at acks=1, zero loss, zero duplicates** (`MillionRecordsTest`, 4 partitions, 100-byte values, batch 64 KB, linger 5 ms, rf 1, fsync never, in-process broker on the laptop):
  ```
  GATE 1M: produced 1000000 in 1753 ms, consumed 1000000 in 923 ms, distinct=1000000, duplicates=0
  ```
  These timings are test-harness timings in one JVM (producer, broker and consumer share the process), not benchmark results; Phase 8 produces the benchmark numbers.
- Partitioner matches Kafka's murmur2 on 20,000 random keys (`bench/Murmur2CompatibilityTest`, against `org.apache.kafka.common.utils.Utils.murmur2`).
- Phase 0 gate evidence: `ThirdPartyProtocolTest` encodes a record batch, CRC, framing and Produce/Fetch purely from `PROTOCOL.md` and round-trips through a real broker.
- Code review (built-in, high effort) on the Phase 3 diff: 9 findings, 8 fixed (stray-directory deletion could wipe data, heartbeat starving the scheduler, controller-call backlog, unbounded client wait times, executor stuck on `Error`, fetch budget overrun, consumer channel address change, admin polling), 1 accepted in `DECISIONS.md` B18.

### Not verified / assumed
- `./mvnw verify` fails the coverage gate for now: `dev.logforge.replication` is at 0.64 line coverage (log package passes). The follower, ISR shrink/expand and election paths are exercised by Phase 4's multi-broker tests.
- Backpressure is shown by the slow-client test (muted count >= 1 and a fast client unaffected); maximum broker memory under many slow clients was not measured.

---

## Phase 4: Replication and ISR

Built: `ReplicaFetcher`/`ReplicaFetcherManager` (followers pull; OffsetForLeaderEpoch truncation before fetching), controller elections (from the live ISR on broker death or restart; offline partitions keep their last ISR; unclean election only behind `LOGFORGE_UNCLEAN_ELECTION=true`), ISR shrink by lag time and expand at HW and epoch start offset, AlterIsr round trip.

### Verified
- `ReplicationTest` (in-process, 1 controller + 3 brokers), 8/8, and 3 further consecutive runs 8/8: followers pull and HW advances; consumers see only records below HW; leader halt elects an ISR member with all 100 acked records; paused follower leaves the ISR while acks=all keeps completing, then rejoins; acks=1 loss measured (`acks=1 leader kill: 3 of 3 acknowledged records lost`); the returning old leader truncates its divergent tail and matches the new leader; unclean election off by default (partition offline) and, when enabled, elects an out-of-sync replica and loses the acked record; stale and future epochs fenced.
- **Gate, zero acknowledged-record loss at acks=all across 10 leader-kill cycles** (`LeaderKillGateTest`: 4 JVM processes, rf=3, min.insync.replicas=2, idempotent producer, leader killed with `destroyForcibly`, restarted, ISR waited back to 3 each cycle):
  - Windows (TerminateProcess): `GATE leader-kill: cycles=10 acked=1388601 failedSends=0 consumed=1388601 missingAcked=0 duplicates=0`; after the review fixes: `acked=1544001 ... missingAcked=0 duplicates=0`.
  - Linux (Docker `maven:3.9-eclipse-temurin-21`, real SIGKILL): `GATE leader-kill: cycles=10 acked=1428201 failedSends=0 consumed=1428201 missingAcked=0 duplicates=0`. Election time per cycle 1963 to 2145 ms (session timeout 2000 ms), ISR whole again 2681 to 3051 ms.
- `FollowerPauseProcessTest` on Linux (real SIGSTOP/SIGCONT): `SIGSTOP follower 2: 50 acks=all writes completed in 2610 ms, ISR now [1, 3]` then `SIGCONT follower 2: back in ISR after 106 ms`.
- Phase 2 gate re-run on Linux with real SIGKILL, 10 consecutive runs (`mvn -pl broker surefire:test -Dtest=LogCrashRecoveryTest` x10 in Docker):
  ```
  linux run 1: SIGKILL recovery: 10 cycles, 1 torn tails truncated (49546 bytes), last ack 544319
  linux run 2: SIGKILL recovery: 10 cycles, 2 torn tails truncated (89187 bytes), last ack 417087
  linux run 3: SIGKILL recovery: 10 cycles, 1 torn tails truncated (36101 bytes), last ack 284543
  linux run 4: SIGKILL recovery: 10 cycles, 1 torn tails truncated (56196 bytes), last ack 356351
  linux run 5: SIGKILL recovery: 10 cycles, 1 torn tails truncated (50973 bytes), last ack 281983
  linux run 6: SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 201855
  linux run 7: SIGKILL recovery: 10 cycles, 1 torn tails truncated (58148 bytes), last ack 312191
  linux run 8: SIGKILL recovery: 10 cycles, 1 torn tails truncated (55830 bytes), last ack 371199
  linux run 9: SIGKILL recovery: 10 cycles, 2 torn tails truncated (126037 bytes), last ack 239103
  linux run 10: SIGKILL recovery: 10 cycles, 0 torn tails truncated (0 bytes), last ack 266239
  ```
  100 kills; 11 landed inside a write and left a torn tail; every one was truncated and every acknowledged record survived.
- `./mvnw -B -ntp verify`: BUILD SUCCESS, 66 broker tests, `All coverage checks have been met.` Line coverage (`broker/target/site/jacoco/jacoco.csv`): `dev.logforge.log` 0.847, `dev.logforge.replication` 0.886, controller 0.879, network 0.871.
- Code review on the Phase 4 commit: 6 findings, 4 fixed (ISR expansion also requires the leader epoch start offset; live broker for InitProducerId; fetch state updated in place; offset gaps re-validated), 2 accepted as test-hook limitations (DECISIONS.md B19).

### Not verified / assumed
- In-process `halt()` closes files cleanly and `freeze()` keeps serving requests; those tests check replication logic, not crash recovery. Crash claims rest on the process-level tests above.
- Election time is dominated by the configured session timeout; not tuned further.
- `FollowerPauseProcessTest` is skipped on Windows (no POSIX signals) and was run on Linux only.

---

## Phase 5: Consumer groups and compaction

Built: `GroupCoordinator` (join barrier, heartbeat-driven rebalance, session expiry, leave, range and round-robin assignment on the coordinator, generation-fenced offset commits stored in the compacted `__consumer_offsets` topic and reloaded on leadership change), client `GroupMembership` (coordinator discovery, background heartbeats, commit-before-rejoin), `Compactor` (newest record per key below the high watermark, offsets preserved, tombstones kept for `delete.retention.ms`, atomic `.cleaned` swap). The idempotent producer was built in Phases 2 and 3; this phase proves it end to end.

### Verified
- Broker tests: `AssignorsTest` 4/4, `GroupCoordinatorTest` 5/5 (barrier, session expiry, leave, inconsistent assignor, generation-fenced commits that survive a broker restart), `CompactorTest` 3/3, `IdempotentFailoverTest` 1/1 (a retry sent to the new leader after the old one died returns the original offset; nothing appended).
- **Gate, exactly-once per partition under a forced retry** (`ProducerRetryDedupTest`: a proxy delivers the first Produce, waits for the broker to append, then cuts the connection before the response). Three runs, identical output each time:
  ```
  forced retry (idempotent=true): produce requests delivered=2, copies in log=1
  forced retry (idempotent=false): produce requests delivered=2, copies in log=2
  ```
  The control run proves the retry really reached the broker twice.
- `ConsumerGroupTest` (3 members, 6 partitions, 3,000 records; one member stops heartbeating without leaving after committing part of a partition), 5 consecutive passes, e.g.:
  ```
  partition 2 (495 records): victim read to 49 and committed 40; survivors resumed at 40
  partition 3 (475 records): victim read to -1 and committed 0; survivors resumed at 0
  ```
  Every offset of every partition was consumed (no gap). Offsets 40 to 49 were delivered twice: the victim read them but died before committing, which is the documented at-least-once behaviour.
- `./mvnw -B -ntp verify` (before the final formatting fix, which changed only line endings): broker 79 tests, `All coverage checks have been met.`; client 12 tests (1 skipped on Windows: SIGSTOP). Coverage: log 0.856, replication 0.876, coordinator 0.862, controller 0.892.

### Not verified / assumed
- Two tests were wrong and were corrected; the reasons are in commit `45ddfc0`: the non-idempotent control expected offset 0 instead of 1, and the group test raced a fixed sleep and counted pre-rebalance reads as the takeover.
- Group membership is in memory: after a coordinator failover members rejoin (UNKNOWN_MEMBER_ID). Measured only in the single-node tests above, not under coordinator failover.
- Compaction drops batches that end up empty, so the leader-epoch history can lose entries for fully compacted ranges. A follower's truncation is then more conservative (it re-fetches more), never less safe. Not exercised by a test.

---

## Phase 6: Observability and admin API

Built: `Metrics` (Prometheus text format: messages in per topic, 1-minute EWMA messages/s, bytes in/out, produce and fetch latency histograms, ISR size, under-replicated count, log end offset, high watermark, consumer group lag), `AdminHttpServer` (health, metrics, cluster view, topic detail, groups, segments, topic create/delete; bearer tokens, constant-time compare, read token cannot mutate, per-IP and per-token rate limits with 429 and Retry-After, exact CORS allowlist, 64 KiB body cap, security headers), JSON logs with correlation ids (`JsonLog`, `<connection>-<correlationId>` per request).

### Verified
- `MetricsTest` 3/3, `AdminHttpServerTest` 7/7 (including S13: read token gets 403 on POST and DELETE; S18: refuses to start without tokens, with a short one, or with equal ones, and the message never contains the value), `JsonLogTest` 3/3.
- `./mvnw -B -ntp verify`: BUILD SUCCESS. Broker 92 tests, client 12 (1 skipped on Windows), bench 1. `All coverage checks have been met.` Line coverage:
```
  dev.logforge                     0.656 (440/671)
  dev.logforge.admin               0.886 (492/555)
  dev.logforge.controller          0.912 (354/388)
  dev.logforge.coordinator         0.855 (360/421)
  dev.logforge.log                 0.855 (1165/1363)
  dev.logforge.network             0.871 (297/341)
  dev.logforge.network.protocol    0.979 (663/677)
  dev.logforge.replication         0.877 (837/954)
  dev.logforge.util                0.846 (236/279)
```
- All earlier gates still pass inside this build: `GATE leader-kill: cycles=10 acked=1309401 ... missingAcked=0 duplicates=0`, `GATE 1M: ... distinct=1000000, duplicates=0`, forced retry 1 copy (idempotent) and 2 copies (control).
- Test fix: `AdminHttpServerTest` did not compile on first run (a wildcard `Map<?, ?>` cast); the cast was corrected, no assertion changed.

### Not verified / assumed
- **Gate item "CI green; push to GitHub" is not done.** Pushing needs the author's approval and a GitHub remote; nothing has been pushed. CI has only been reproduced locally with the same `./mvnw -B -ntp verify` command.
- Prometheus has not scraped `/metrics` for real; the format is checked by string assertions against the 0.0.4 text format.

---

## Phase 7: Security hardening

### Verified
- `/security-audit` run; report committed as `SECURITY-AUDIT.md` (findings F1 to F8). S-checklist in `SECURITY.md`.
- gitleaks v8.21.2 over full history before the first push: `46 commits scanned ... no leaks found`.
- OWASP dependency-check 10.0.4 over all jars: only `kafka-clients-3.9.0` (bench-only) flagged, four CVEs triaged in `SECURITY.md`. `pnpm audit`: four postcss advisories fixed by override, then `No known vulnerabilities found`; CI runs `pnpm audit` on every push.

### Not verified / assumed
- **Gate not complete:** findings F1 to F8 await the owner's approval before fixes (one concern per commit). F1 (unauthenticated binary protocol) is mitigated only by network isolation; on Render port 9092 is never routed.

## Phase 8: Benchmark and chaos harness

### Verified
- `bash bench/run.sh` (WARMUP=5 MEASURE=20), LogForge and Apache Kafka 3.9.0 on one i7-1255U laptop (Docker Desktop, WSL 2), keyed records: 22 results in `bench/results`, 0 send errors, `replication-check.txt`: 0 of 41 partitions with a follower behind. Kafka led at acks=all (1.6x), 1 partition (1.8x), 10 KB (1.5x); identical configs measured minutes apart differed by up to 1.9x (noise). Two earlier unfair runs discarded (DECISIONS W1).
- `bash chaos/run-all.sh`: leader-kill, follower-pause, disk-full, network-partition, restart-under-load all PASS, 0 acknowledged records missing, 0 duplicates; recovery 5.23 s, 3.03 s, 0.52 s, 10.75 s, 0.94 s. follower-pause repeated 5 times, all PASS; run 1 moved leadership unexpectedly, unexplained (`chaos/results/follower-pause-repeats/README.md`).

### Not verified / assumed
- One benchmark run per configuration; no error bars. Kubernetes in Docker Desktop was running during both systems' runs.

## Phase 9: Showcase website

### Verified
- `DEMO-REPORT.md`: nine widths with no horizontal overflow, every cold-start state, reduced motion, 33-stop Tab walk with visible focus, contrast, console.
- Lighthouse mobile via PageSpeed Insights on production (2026-10-27 02:36): Performance 98, Accessibility 100, Best Practices 100, SEO 100. A first run scored Accessibility 97 (pending replay events dimmed by opacity); fixed in `e16b2f3`.

### Not verified / assumed
- web-design-guidelines and animation reviews not yet run. Chromium only.

## Phase 10: Deploy

### Verified
- GitHub `rachitkumar2105/logforge`, branch `build/v1` only; CI (broker and web jobs) green.
- Render Blueprint `logforge-broker` (free, 512 MB): `/health` 200, `/api/v1/cluster` 401 without a token; tokens generated by Render. Local stand-in at 512 MB / 0.1 CPU: 333,331 records at 500/s for 10 min, 0 failed, peak 55 MiB (`bench/results/hosted-512mb.txt`).
- Vercel `https://logforge-five.vercel.app`: security headers and per-request CSP nonce; `/api/cluster` source `live`; browser: checking, live at 2.6 s, layout shift 0.0000, console clean on every page.
- Render service deliberately suspended: broker 503, site painted every section with the recorded snapshot, pill checking, waking 5.2 s, asleep 93.2 s, 0 cluster polls, console clean (`docs/demo/prod-suspended-*.jpg`). Resumed: back in about 10 s, topic re-seeded with 600 records (no persistent disk, as expected).

### Not verified / assumed
- Not launch-ready: no custom domain; privacy and terms are drafts, not legal advice. Vercel variables are set for Production only (not Preview). Uptime workflow needs the repository variable `BROKER_URL` and runs only from the default branch.

---

## Phase 7 (completed): audit findings F1 to F8 fixed

The owner approved fixing F1 to F8, one concern per commit. Status table: `SECURITY-AUDIT.md`.

### Verified
- F1 cluster token on the binary protocol (`ClusterAuthTest` 7/7, including the review follow-ups: a third wrong token closes the connection, the token must differ from the HTTP tokens). Docker Compose leader-kill rerun with authentication: PASS, 64,294 acked, 0 missing, 0 duplicates, recovery 5.22 s, no refused authentication in any node log.
- F2 connection caps (`ConnectionLimitsTest` 2/2), F3 status cap (`ControllerStatusLimitTest` 1/1), F5 minimal `/health` with HEAD and F6 HSTS (`AdminHttpServerTest` 9/9), F6 site refuses non-HTTPS admin URLs (`adminUrl.test.mjs`), F8 four methods removed. F4 and F7 documented as accepted risks in `SECURITY.md`.
- Production after the fixes: Render rebuilt with a Render-generated `LOGFORGE_CLUSTER_TOKEN` in about 60 s; `curl https://logforge-broker.onrender.com/health` gives `{"status":"ok"}`; `curl -I` gives 200 with `strict-transport-security: max-age=63072000; includeSubDomains`; the site's `/api/cluster` reports `source live` with `demo-orders` re-seeded by the authenticated seeder.
- `./mvnw -B -ntp verify` after all fixes and review fixes: BUILD SUCCESS, broker 112, client 12 (1 skipped, the SIGSTOP test that needs Linux), bench 1, `All coverage checks have been met.`
- gitleaks v8.21.2 over full history: `68 commits scanned ... no leaks found`; no `.env` tracked.
- Found while fixing: `Node.close` did not wait for background threads, so a late metadata update could touch a log after close and leave a file locked on Windows (AdminHttpServerTest temp-dir cleanup failed in 2 of 3 runs; 0 of 3 after the fix).

### Tests changed, and why (AGENTS.md rule 3)
No assertion was weakened. Each change is in its commit message too.
- Test helpers (`TestNodes`, `TestClient`, `EmbeddedNode`, `ProcessCluster`, `HostedModeTest`, the client tests' `Admin` calls): F1 made the cluster token a required, real part of the API, so nodes start with a test token, and `TestClient` authenticates before topic administration and follower fetches, as the cluster's own tools do. `TestClient.anonymous` keeps an unauthenticated client for the refusal tests.
- `ReplicationTest.staleOrFutureLeaderEpochsAreFenced`: after F1 the follower sometimes had not yet learned the new partition and answered UNKNOWN_TOPIC_OR_PARTITION instead of NOT_LEADER_OR_FOLLOWER; the test now waits until the follower holds the partition before probing it.
- `SingleBrokerTest.deleteTopicRemovesItsData`: the broker drops a deleted partition from its map, then deletes the files; under load the test looked at the directory in between (passed 5 of 5 alone). It now waits up to 10 s for the directory to go, then asserts as before.
- `ClusterAuthTest` (new): waited for leadership before producing, as every other test does.

### Not verified / assumed
- The cluster token travels in clear text on the cluster network; it is authentication, not encryption. Port 9092 must stay private.
- No request-rate limit on the binary protocol beyond the connection caps.

## Reviews (web interface guidelines, motion, code)

### Verified
- Web interface guidelines (vercel-labs `command.md`, fetched 2026-10-27): fixed the hero tape's missing pause control (WCAG 2.2.2), input attributes, heading balance, tap handling, an ASCII ellipsis, a hardcoded number format and brand translation. Kept on purpose: sentence-case headings (DESIGN.md) and tab state not mirrored in the URL.
- Motion review: every animated property is transform, opacity or SVG pathLength after removing a border-color transition; architecture diagrams' entrance shortened from 1.0 s to 0.7 s. Chromium frame pacing over 3 s: idle p50 16.7 ms, p95 17.0 ms; while scrolling p50 16.7 ms, p95 16.9 ms; 0 frames over 33 ms. Tape toggle pauses the drift (`animation-play-state: paused`, `aria-pressed="true"`).
- Code review (`/code-review high`, range `14227d1..HEAD`): 10 findings; 9 fixed (connection-slot race and leak, auth-failure throttling, distinct cluster token, stale data labelled Live after the broker sleeps, thrown poll errors not restarting, hosted-only wording, metadata refresh authenticating needlessly, this test-edit record), 1 kept (the cross-fade height lock leaves blank space on phones; it is what keeps the swap free of layout jumps).
- The stale-Live fix checked in Chromium by stopping the broker after the page went live: restart at 25.8 s, recorded view 8 ms later, asleep at 115.8 s, console clean.

### Not verified / assumed
- Only Chromium; no real phone.

