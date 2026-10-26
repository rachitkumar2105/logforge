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
