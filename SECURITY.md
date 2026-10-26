# Security

How LogForge is protected, what was checked and how. The full Phase 7 audit, with findings and proposed fixes, is in `SECURITY-AUDIT.md`. Nothing here claims the project is secure.

## Model

- **Broker protocol (port 9092)**: unauthenticated and unencrypted. It must only be reachable on a private network. Docker Compose puts every broker on an internal network with no published ports (audit F1 describes what an attacker on that network could do).
- **Admin HTTP API (8080)**: two bearer tokens from the environment. The read token can only GET; mutations need the admin token. Only the controller's admin API is published, on `127.0.0.1` locally and behind the platform's TLS edge in a deployment.
- **Website**: holds no secret in the browser. The read token lives in a server-side route handler; the visitor's browser only calls `/api/cluster` on the site's own origin.

## Checklist (spec S-items)

| Item | Verdict | Evidence |
|---|---|---|
| S2 XSS | see site section | Broker JSON escapes `<`, `>`, `&` (`Json.quote`, `JsonLogTest`). Site: React escaping only, `react/no-danger` lint rule, `git grep dangerouslySetInnerHTML` empty, nonce CSP. |
| S4 Payload limits | PASS | Frame size checked before allocation (`NioServerTest`), batch cap (`SingleBrokerTest`), fetch responses capped at 8 MiB (`SecurityLimitsTest`), admin bodies 64 KiB (`AdminHttpServerTest`). |
| S6 Rate limiting | PASS (admin API) | Per-IP and per-token buckets, 429 with `Retry-After` (`AdminHttpServerTest`). Binary protocol has no limits (audit F2). |
| S8 API secrets | PASS (broker) | Tokens only from env; refusal messages name the variable, never the value; gitleaks clean over all history. |
| S11 CORS | PASS | Exact allowlist, never `*`, wildcard refused at startup (`AdminHttpServerTest`). |
| S12 Web storage | see site section | |
| S13 Server-side permissions | PASS (admin API), FAIL (binary protocol, F1) | Read token gets 403 on create and delete (`AdminHttpServerTest.theReadTokenCannotMutateButTheAdminTokenCan`). |
| S16 SSRF | PASS | No endpoint fetches a user-supplied URL; the site's route handler fetches only `LOGFORGE_ADMIN_URL` from server config. |
| S17 Source maps | see site section | `productionBrowserSourceMaps: false` in `web/next.config.ts`. |
| S18 Default credentials | PASS | No default token; the admin server refuses to start without both (`AdminHttpServerTest.refusesToStartWithoutStrongDistinctTokens`). |
| S19 Logs | PASS | `SecurityLimitsTest.recordContentsAndTokensNeverReachTheLogs`. |
| S20 Dependencies | PASS (broker, client, site), accepted risk (bench) | See the dependency section. |

## Site

Filled in after the site is built and checked in a real browser (Phase 9). See `DEMO-REPORT.md`.

## Dependencies

**Java** (OWASP dependency-check 10.0.4 in Docker, NVD data downloaded 2026-10-26, OSS Index disabled, over every jar in the broker, client, bench and test classpaths: 16 jars, 19 dependencies analysed):

| Jar | Where it runs | Findings |
|---|---|---|
| `logforge-broker.jar` and every other jar | broker, client, tests | none |
| `kafka-clients-3.9.0.jar` | `bench/` only, the baseline's own client | CVE-2025-27817 (SASL/OAUTHBEARER endpoint file read and SSRF), CVE-2025-27818 (SASL JAAS config via Connect or alterConfigs), CVE-2026-35554 (producer buffer-pool race when a batch expires while in flight), CVE-2026-33558 (request contents in DEBUG logs) |

Assessment: the broker has zero runtime dependencies and none of these reach it. The bench client talks only to a throwaway local Kafka on a Docker network, with no SASL, no OAuth, no Connect and INFO logging, so the first, second and fourth do not apply. The third can misroute records only after a batch hits `delivery.timeout.ms`; the harness counts every send error, and a run with errors is not a clean measurement. The client stays on 3.9.0 to match the 3.9.0 brokers the spec names as the baseline; moving the bench to 3.9.2 or 4.x is an open decision for the owner.

**Site** (`pnpm audit`): four postcss advisories (GHSA-qx2v-qp2m-jg93, GHSA-6g55-p6wh-862q, GHSA-fxqj-rqcc-2cmp, GHSA-r28c-9q8g-f849) came in through `next > postcss@8.4.31`. A pnpm override pins that copy to 8.5.31, the version `@tailwindcss/postcss` already used. After it: `No known vulnerabilities found`.

## Reporting a problem

Open an issue on the GitHub repository; do not include secrets.
