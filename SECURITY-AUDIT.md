# Security audit (Phase 7)

Scope: everything in the repository at commit `3d79532` (broker, client, bench, Dockerfile, docker-compose.yml). The showcase site (`web/`) does not exist yet; its items are marked NOT VERIFIED and will be re-audited when it is built. Secret values are never printed here; where one appears it is masked as `****`.

This audit does not establish that the project is secure. It lists what was checked, how, and what remains.

## Findings

| # | Finding | File and line | Severity | Exploit scenario | Fix (proposed, not applied) |
|---|---|---|---|---|---|
| F1 | The binary protocol is unauthenticated, including the controller's internal and topic-admin APIs. CreateTopics, DeleteTopics, BrokerHeartbeat and AlterIsr are accepted from any connection. | `broker/src/main/java/dev/logforge/BrokerApis.java:86-94`; `controller/Controller.java` `heartbeat`, `alterIsr`, `deleteTopics` | High if port 9092 is reachable; mitigated today by network isolation | Anyone who can reach a node's 9092 can delete topics (bypassing the admin token that guards the HTTP API), register a fake broker id with an attacker-controlled host so the controller assigns replicas to it, or forge AlterIsr. | Add a cluster secret (`LOGFORGE_CLUSTER_TOKEN`, required at startup) that BrokerHeartbeat and AlterIsr must carry; refuse CreateTopics/DeleteTopics on the binary protocol unless the same secret is presented; move the Java `Admin` client to the token-protected HTTP API. Keep 9092 on the internal network. |
| F2 | No limit on concurrent connections, total or per IP, in `NioServer`. | `broker/src/main/java/dev/logforge/network/NioServer.java` (`acceptLoop`, no cap) | Medium | A client on the network opens tens of thousands of sockets and exhausts file descriptors; the broker can no longer accept replication or client connections. | Cap total connections (default 10,000) and per-IP connections (default 1,000); refuse excess at accept time and count refusals in a metric. |
| F3 | Broker status JSON in heartbeats is stored per broker id without a size cap (bounded only by the 16 MiB frame limit). | `controller/Controller.java` `heartbeat` (`b.status = ...`) | Medium (only via F1) | With F1, a client registers many fake broker ids, each with a 16 MiB status, and grows the controller's heap until it fails. | Reject status larger than 1 MiB; with F1's cluster secret, fake ids are no longer possible. |
| F4 | Admin API rate limits are in memory per node, not in a shared store. | `admin/RateLimiter.java` | Low | Only the controller serves the cluster API, so the limit is effectively global today. Running several admin replicas behind a load balancer would multiply the limit. | Document; revisit only if the admin API is ever replicated. |
| F5 | `/health` is public and returns node id, roles and uptime. | `admin/AdminHttpServer.java` (`/health`) | Low | An unauthenticated scanner learns which node is the controller and when it restarted. | Accepted in DECISIONS.md B22 (documented in API.md; no secret or data). Could be reduced to `{"status":"ok"}`. |
| F6 | The admin HTTP server speaks plain HTTP; TLS is expected at the platform edge. | `admin/AdminHttpServer.java`; `docker-compose.yml` (publishes `127.0.0.1:8080` only) | Low locally, depends on deployment | On a deployment without an HTTPS edge, the bearer token would cross the network in clear text. | Phase 10: deploy only behind the platform's TLS edge; verify with `curl -I https://...` and refuse plain HTTP at the edge. |
| F7 | No backups of partition data or controller metadata; durability rests on replication and Docker volumes. | `docker-compose.yml` (named volumes) | Low for a demo | Losing the Docker host loses all data, including `metadata.json`. | Document; snapshot volumes in the hosted deployment if data matters. |
| F8 | Unused code (listed, not deleted): `ProducerStateManager.producerCount()`, `NioServer.everMutedConnections()`, `LogSegment.batchesFrom()`, `ReplicaFetcherManager.fetcherCount()`, `JsonLog.isDebugEnabled()`. | as named | Info | None directly; dead code widens the surface to review. | Remove, or use them in metrics, after approval. |

## S1 to S20

| Item | Verdict | Evidence |
|---|---|---|
| S1 SQL injection | N/A | No database and no SQL anywhere (`git grep -nIiE 'jdbc|executeQuery|PreparedStatement'` finds nothing). Storage is append-only files. |
| S2 XSS | NOT VERIFIED (site not built) | Broker side: admin JSON escapes `<`, `>`, `&` and control characters (`Json.quote`); `JsonLogTest.escapesHostileStringsSoEveryLineStaysValidJson`. `git grep innerHTML|dangerouslySetInnerHTML` finds nothing. The site and its CSP are audited in Phase 9. |
| S3 CSRF | N/A for the broker | No cookies or sessions: the admin API authenticates with a bearer header only, which browsers do not attach automatically. Re-check for the site's route handlers in Phase 9. |
| S4 File uploads / payload limits | N/A (uploads); PASS (payload limits) | No upload feature. Frame size checked before allocation (`NioServerTest.malformedFramesCloseOnlyTheirConnection`, 2 GiB claim); batch cap `max.batch.bytes` (`SingleBrokerTest.errorsAreTypedPerPartition`); fetch allocation capped at 8 MiB (`SecurityLimitsTest`); admin bodies capped at 64 KiB (`AdminHttpServerTest.oversizedBodiesAreRejected`); array counts bounded by remaining bytes (`ProtocolReader.readArray`). |
| S5 BOLA/IDOR | N/A | There are no users or per-user objects. Access is two-level (read, admin) over the whole cluster. |
| S6 Rate limiting | PASS (admin API), FAIL (binary protocol, see F2) | Per-IP and per-token token buckets with 429 and `Retry-After` (`AdminHttpServerTest.rateLimitsReturn429WithRetryAfter`). No connection or request limits on port 9092. |
| S7 JWT | N/A | No JWTs. |
| S8 API secrets server-side | PASS (broker); NOT VERIFIED (site) | Tokens only from env (`NodeConfig.fromEnv`); no token literal in tracked files (masked grep found none); `.env` gitignored and not tracked (`git check-ignore .env`); refusal messages name the variable, never the value (`AdminHttpServerTest.refusesToStartWithoutStrongDistinctTokens`). |
| S9 Password hashing | N/A | No passwords. |
| S10 MFA | N/A | No accounts. |
| S11 CORS | PASS | Exact-match allowlist, origin echoed only on match, wildcard refused at startup (`AdminHttpServerTest.corsAllowsOnlyListedOriginsAndNeverAWildcard`). |
| S12 Tokens in web storage | NOT VERIFIED (site not built) | Re-checked in Phase 9. |
| S13 Server-side permissions | PASS (HTTP admin API), FAIL (binary protocol, see F1) | Deny by default: every HTTP path except `/health` needs a token; the read token gets 403 on POST and DELETE (`AdminHttpServerTest.theReadTokenCannotMutateButTheAdminTokenCan`). Binary-protocol DeleteTopics needs no credential. |
| S14 Row-level security | N/A | No database. |
| S15 Webhooks | N/A | No webhooks. |
| S16 SSRF | PASS | No endpoint fetches a user-supplied URL. Outbound connections go only to the configured controller address and to broker addresses in controller metadata (`git grep -nE 'new URL\(|HttpClient|URLConnection|openConnection' -- broker/src/main client/src/main` finds nothing). Caveat: with F1, an attacker could make brokers connect to an address it registers. |
| S17 Source maps | NOT VERIFIED (site not built) | The broker serves no static files. |
| S18 Default credentials | PASS | No default token exists; the admin server refuses to start without both tokens (length >= 24, different) (`AdminHttpServerTest.refusesToStartWithoutStrongDistinctTokens`). `.env.example` holds placeholders only. |
| S19 Sensitive data in logs | PASS | `SecurityLimitsTest.recordContentsAndTokensNeverReachTheLogs` (keys and values absent from captured logs, including error paths); `Record.toString` omits key and value (`JsonLogTest`); no stack traces in logs. |
| S20 Dependencies | NOT VERIFIED (scan running) | OWASP dependency-check 10.0.4 over every jar the build resolves (test scope, plus bench's kafka-clients and its compression libraries) is in progress; the broker jar itself has zero runtime dependencies. `pnpm audit` runs with the site. |

Also checked:
- **Git history**: `gitleaks v8.21.2 detect --log-opts=--all` over all 20 commits: `no leaks found`. Nothing needs rotating.
- **Exposed files**: the runtime image holds only `/app/logforge-broker.jar` (checked with `ls /app` in the image); `.dockerignore` excludes `.env`, `.git` and data. The admin server serves no files.
- **Debug mode**: no debug flag exists; log level defaults to INFO; errors return short codes, never stack traces.
- **Security headers (admin API)**: `X-Content-Type-Options: nosniff`, `Cache-Control: no-store`, `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY` on every response (asserted for `nosniff` in `AdminHttpServerTest`). HSTS and CSP belong to the HTTPS edge and the site.
- **Database not publicly reachable**: no database. Broker ports are not published in Compose (`docker compose ps` shows `9092/tcp` unpublished on brokers; only `127.0.0.1:8080->8080/tcp` on the controller).

## Verified

| Check | Command or test |
|---|---|
| No leaked secrets in history | `docker run zricethezav/gitleaks:v8.21.2 detect --source /repo --redact --log-opts=--all`: 20 commits, no leaks |
| `.env` ignored, only `.env.example` tracked | `git check-ignore -q .env`; `git ls-files \| grep -iE '\.env|\.pem$|\.key$'` |
| Token gate, 401/403, CORS, 413, 429, startup refusal | `./mvnw -pl broker test -Dtest=AdminHttpServerTest` (7/7) |
| Payload limits and log hygiene | `NioServerTest`, `SingleBrokerTest`, `SecurityLimitsTest`, `JsonLogTest` |
| Only the admin API published | `docker compose ps` after `docker compose up -d` |
| Runtime image contents and user | `docker run --entrypoint sh logforge:local -c 'ls /app; which mvn; id'` gave the jar only, no Maven, uid 100 (logforge) |

## Not verified or assumed

- Everything about the website (S2, S12, S17, CSP and other site headers, `pnpm audit`).
- S20 dependency scan results (pending at the time of writing).
- TLS: not tested; the plan is TLS termination at the hosting platform's edge.
- Behaviour under a real network attacker: F1 and F2 are inferred from code reading, not demonstrated against a running cluster.

## Risks that remain

1. F1: if port 9092 is ever exposed, the cluster can be reconfigured or destroyed without credentials.
2. F2: connection exhaustion from inside the network.
3. The single controller is a single point of control (by design).
4. Plain-text broker traffic on the internal network: anyone with access to that network can read records.
