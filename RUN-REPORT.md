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
