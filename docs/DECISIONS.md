# Decisions

Every choice the spec left open, every deviation from it, and every dependency. Newest last.

## Process

| # | Decision | Reason |
|---|---|---|
| P1 | Work happens on branch `build/v1`, not `main`. | The spec says "work on main locally"; the author's global rules say never work directly on main. The global rule is stricter and costs nothing; `main` is fast-forwarded when the author approves. |
| P2 | Hooks disabled for this repo via `.claude/settings.json` (`disableAllHooks: true`). | The spec says Impeccable hooks are globally active and must be disabled for this repo. |
| P3 | `design-taste-frontend` and `review-animations` skills are not installed on this machine. The design direction comes from the spec's Phase 9 text, written down in `docs/DESIGN.md`; motion is reviewed with the installed `animate`, `animation-performance` and `motion-react` skills. | Dependency unavailable; spec text is specific enough to substitute. |
| P4 | Machine has JDK 22, not 21. Code is compiled with `--release 21`, so it only uses the Java 21 API and class-file version 65. The Docker image runs on a Java 21 JRE. | Spec target is Java 21. |

## Build and dependencies

| # | Dependency | Version | Scope | Reason |
|---|---|---|---|---|
| D1 | JUnit Jupiter | 5.10.3 | test, all modules | Spec. |
| D2 | AssertJ | 3.27.7 | test, all modules | Spec. |
| D3 | Error Prone | 2.36.0 | compiler plugin | Spec (static analysis). |
| D4 | Spotless + google-java-format | 2.43.0 / 1.24.0 | build plugin | Spec ("Spotless or Checkstyle"). |
| D5 | JaCoCo | 0.8.12 | build plugin | Spec coverage gate. 0.8.12 supports JDK 22 runtimes. |
| D6 | kafka-clients | 3.9.0 | `bench/` only | Spec baseline. Never in `broker/`. |
| D7 | Maven wrapper 3.3.2, Maven 3.9.11 | | build | One global `~/.m2`, nothing vendored. |
| D8 | Testcontainers: **not added**. | | | Multi-broker durability tests spawn real broker JVMs from JUnit and kill them (real `SIGKILL` on Linux CI). Container-level chaos (SIGSTOP via `docker pause`, network partition, disk full) uses Docker Compose scripts in `chaos/`. Testcontainers would duplicate the Compose path and add a Docker requirement to unit CI. |

## Broker design

| # | Decision | Reason |
|---|---|---|
| B1 | Batch header order is `length, baseOffset, partitionLeaderEpoch, magic, crc, ...`; the spec lists "length prefix, magic byte, crc32c over the payload, base offset, ...". All the spec's fields are present; the leader-stamped fields (`baseOffset`, `partitionLeaderEpoch`) sit before the CRC so the producer's checksum survives stamping and the broker never recomputes it. | Same reason Kafka's v2 format does it. |
| B2 | Records carry `timestampDelta` and `offsetDelta` in addition to the spec's key, value and headers. | Compaction removes records from inside a batch; offset deltas keep the remaining offsets correct. |
| B3 | Index entries map batch base offsets (relative) to batch positions. | Batches are the unit of IO; a record-level index would add nothing because a batch is read whole anyway. |
| B4 | Default fsync policy: `ms:1000` (fsync at most once a second, plus on segment roll and clean shutdown). | Bounded loss window on power failure (at most about 1 s of acknowledged writes on a single broker) without paying an fsync per request. A process crash (`SIGKILL`) loses nothing acknowledged under any policy, because the page cache belongs to the kernel, not the process. Documented in README. |
| B5 | Null-key partitioning: round-robin from a random start. | Even spread; ordering is only promised per key. |
| B6 | The controller is a role on a fixed node. In Compose it runs as its own container next to three brokers. | Spec non-goal: no consensus. A dedicated controller means killing any broker exercises real failover; killing the controller is the documented limitation. |
| B7 | Metadata propagates by heartbeat pull (snapshot in the heartbeat response when the broker's version is stale) instead of controller push. | One code path, self-healing after missed updates, no controller-to-broker connections. |
| B8 | ISR shrink is applied by the leader only after the controller accepts it; expansion is counted in the high watermark immediately. | Shrinking locally first could advance the HW past what a replica the controller still considers in sync has, which could lose committed data on the next election. Counting an expanding replica early only makes HW more conservative. |
| B9 | Follower truncation uses leader epochs (OffsetForLeaderEpoch), not "truncate to HW". | Truncate-to-HW can lose committed data or leave replicas divergent in double-failure cases (the problem Kafka's KIP-101 fixed). |
| B10 | Group assignment is computed by the coordinator (no SyncGroup API). | Fewer round trips and one fewer API; switching range or round-robin is a request field. Trade-off: custom client-side assignors are not possible. |
| B11 | Idempotent producer keeps at most one in-flight batch per partition in the client; the broker keeps the last 5 batch sequences per producer per partition. | A retried batch is recognised by sequence; one in flight keeps ordering trivially correct. |
| B12 | Producer state and the leader-epoch history are rebuilt on startup by scanning batch headers (61 bytes per batch, data skipped) rather than checkpointed. | Always consistent with the log after truncation; cost is a header scan per partition at startup. |
| B13 | Structured logging is a 100-line hand-written JSON logger, not SLF4J/Logback. Admin JSON is a hand-written writer/parser. | Keeps the broker at zero runtime dependencies. |
| B14 | No zero-copy `sendfile` on fetch: batches are copied through a heap buffer. | Simpler, and it is a measured, explained part of the Kafka gap. |
| B15 | Topic names allow `-` (`[A-Za-z0-9._-]{1,200}`), as in Kafka. | Directory names are parsed on the last `-`, which is always followed by the partition number, so dashes in names are unambiguous. The first draft of PROTOCOL.md was stricter than needed. |
| B16 | A broker never deletes partition directories that no snapshot assigned to it; it logs them. Only partitions it was serving are deleted when their topic disappears from metadata. | Found in the Phase 3 code review: deleting unassigned directories would let a controller that lost `metadata.json` wipe every broker. Cost: a topic deleted while a broker was down leaves a stray directory on that broker until removed by hand. |
| B17 | Clients cannot park a request for more than 30 s: Fetch `maxWaitMs` and Produce `timeoutMs` are capped server-side. | Code review: client-controlled waits could pin purgatory entries and connections for days. |
| B18 | Requests pipelined after one whose body fails to parse may still execute before the connection closes. | Bodies are parsed on the handler thread, after later frames have already been dispatched. Accepted: a malformed client is already broken; idempotent producers are unaffected. |
| B19 | In-process test hooks (`Node.halt`, `Node.freeze`) are approximations: `halt` closes files cleanly and `freeze` keeps serving requests. | They exist to drive replication logic quickly and to count toward coverage. Every durability claim instead rests on real process kills: `LogCrashRecoveryTest` (child JVM), `LeaderKillGateTest` and `FollowerPauseProcessTest` (separate JVMs, SIGKILL/SIGSTOP), and the Docker chaos scripts. |
| B20 | A follower rejoins the ISR only when its fetch offset reaches both the high watermark and the start offset of the leader's current epoch. | Code review: a new leader's HW can lag what the old leader committed; the epoch start offset is a safe bound (same rule as Kafka). |
| B21 | Known Phase 5 limitations, accepted: (a) tombstone expiry uses the record's timestamp, so a producer that writes an old timestamp shortens its tombstone's life; (b) every compaction pass rescans the whole partition and holds every key in memory (no dirty-range tracking or bounded offset map); (c) offsets-partition reload holds one coordinator-wide lock; (d) a group does not rebalance when a subscribed topic is created or grows later. | Each is a scale or hostile-client concern beyond this project's demo cluster; each is listed in the site's limitations section and FINAL-AUDIT. Kafka solves them with a broker-side cleaning timestamp, a dirty-ratio cleaner with a fixed-size offset map, per-partition loading, and metadata-change rebalances. |
| B22 | Admin API per-IP limits key on the TCP peer address, never on `X-Forwarded-For` (spoofable). The site's route handler therefore caches cluster responses for a few seconds, so all visitors together cost one admin request per interval. `/health` keeps node id, roles and uptime (documented in API.md; no secret, no data). | Phase 6 review. |

## Benchmark and site

| # | Decision | Reason |
|---|---|---|
| W1 | Benchmark fairness: both clusters run 3 brokers in Docker on the same host, data on a named volume, 1 GiB heap per broker, same Java client workload (64 KB batches, 5 ms linger, 8 MiB client buffer, no compression, idempotence only with acks=all because Kafka requires that). | A first run wrote Kafka's data into the container overlay filesystem and gave LogForge an unbounded heap; those results were discarded and the run repeated. Known remaining differences are stated next to the results (client partitioner, controller layout). |
| W2 | Produce latency in throughput runs is measured at full speed and includes time queued in the client buffer; the e2e run at a fixed 1,000 records/s gives unsaturated latency. | Same definition for both systems, stated on the page. |
| W3 | Site dependencies: next 15, react 19, motion 12, recharts 2, tailwindcss 4 (+ @tailwindcss/postcss), typescript 5, eslint 9 with eslint-config-next. All named by the spec's Phase 9 stack. Fonts: Geist and Geist Mono through `next/font/google`, self-hosted at build time. pnpm installed globally as a tool. | Spec stack; the author's rule allows fonts the author approved (the spec names Geist). |
| W4 | CSP: scripts need a per-request nonce (`'strict-dynamic'`, no `'unsafe-inline'`, no `'unsafe-eval'` in production); `<style>` elements need the nonce; only inline style *attributes* are allowed (`style-src-attr 'unsafe-inline'`), because React and Recharts render them server-side. Style attributes cannot run code. | Spec: "CSP without unsafe-inline in production" is met for scripts and style elements. |
| W5 | The repository URL in the footer is assumed to be `github.com/rachitkumar2105/logforge`. | The repository has not been created yet; confirm before deploy. |
