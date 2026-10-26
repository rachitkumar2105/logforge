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
