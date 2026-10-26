# AGENTS.md

Rules for any coding agent (and human) working in this repository.

## Non-negotiable
1. **No secrets in code, logs, chat or git.** Tokens come from environment variables only. `.env` is ignored; `.env.example` holds placeholders. Never log record keys, record values or tokens.
2. **No new dependencies without recording them in `docs/DECISIONS.md`** (name, version, module, reason). The broker module has zero runtime dependencies and must never import `kafka-clients`, `pulsar-client` or any embedded broker. Kafka is allowed only in `bench/`.
3. **Tests are never edited to make them pass.** If a test is wrong, say why in the commit message and in `RUN-REPORT.md`.
4. **Test output is required before any phase is called done.** Paste the real command and its real output into `RUN-REPORT.md`.
5. **Never fabricate numbers.** Every throughput, latency, recovery time or coverage figure must come from a command that was actually run, recorded next to the number with the full configuration and hardware.
6. **Durability claims require a process kill.** A claim about data loss is only valid if a test killed a broker (`SIGKILL`, `destroyForcibly`) and verified the log afterwards.
7. **Reports split `## Verified` from `## Not verified / assumed`.**

## Layout
- `broker/` hand-written broker (`dev.logforge.*`), JDK only.
- `client/` Java producer, consumer, admin client.
- `bench/` throughput harness plus the Kafka baseline.
- `chaos/` kill-and-verify scripts against Docker Compose.
- `web/` Next.js showcase site (pnpm only, never `npm install`).
- `docs/` PRD, architecture, protocol, API, design, decisions.

## Commands
- Build and test everything: `./mvnw verify`
- Format: `./mvnw spotless:apply`
- Broker data: `LOGFORGE_DATA_DIR` or `~/logforge-data/`, never inside the repo.
- Site: `cd web && pnpm install && pnpm dev`

## Workflow
Write failing tests first, commit them, implement, make them pass, run the phase gate, commit, update `RUN-REPORT.md`. One commit per concern.
