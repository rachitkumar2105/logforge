# Final audit

State of `main` on 2026-10-27 (`github.com/rachitkumar2105/logforge`, default branch `main`). Production: site `https://logforge-five.vercel.app` (Vercel Hobby, production branch `main`), broker `https://logforge-broker.onrender.com` (Render free, one node, branch `main`, deploy of `bd6e791`). Every number below was measured on the final code on 2026-10-27 unless it says otherwise, with the command named; anything not checked is listed as such. This audit does not claim the project is secure or launch-ready.

## Phase gates

| Phase | Gate | Status | Evidence (2026-10-27, final code) |
|---|---|---|---|
| 0 Foundation | CI green; PROTOCOL.md complete enough for a third-party client | Met | GitHub Actions `ci` green on `main`. `ThirdPartyProtocolTest` builds frames from PROTOCOL.md alone. |
| 1 Record format | Corruption detection and offset lookup proven by test | Met | `RecordBatchTest`, `OffsetIndexTest`, `LogSegmentTest` in the full build. |
| 2 Log and recovery | SIGKILL recovery test passes 10 consecutive runs | Met | Linux: 10 consecutive runs of `LogCrashRecoveryTest`, 100 SIGKILLs, 1 torn tail (45,133 bytes) truncated, no acknowledged record lost. Windows: the same test in the full build, 10 SIGKILLs, 0 torn tails. |
| 3 Network | 1 million records, zero loss, zero duplicates at acks=1 | Met | `GATE 1M: produced 1000000 ... distinct=1000000, duplicates=0`. |
| 4 Replication | Zero acknowledged loss at acks=all across 10 leader kills | Met | `LeaderKillGateTest`: Windows 1,353,801 acked, Linux 1,480,001 acked, 0 missing, 0 duplicates. Linux `FollowerPauseProcessTest` (SIGSTOP): 50 acks=all writes in 2,748 ms with the follower out of the ISR. |
| 5 Groups, compaction | Exactly-once per partition under forced retry | Met | `ProducerRetryDedupTest`: 1 copy with idempotence, 2 without (control). |
| 6 Observability | Coverage gate; CI green; pushed | Met | `All coverage checks have been met.` (log 0.860, replication 0.877, minimum 0.80); CI green; pushed. |
| 7 Security | `/security-audit` committed; every FAIL fixed or accepted in writing | Met | `SECURITY-AUDIT.md` F1 to F8: six fixed, two accepted with reasons in `SECURITY.md`. Dependencies: no known vulnerabilities in broker, client, bench (kafka-clients 3.9.2) or site. |
| 8 Bench and chaos | Results from real runs; zero acknowledged loss at acks=all in every chaos scenario | Met | Re-measured on `main`: `bench/results` (22 results), `chaos/results` (5 scenarios PASS, 0 missing). The follower-pause anomaly is explained and fixed (below). |
| 9 Site | Nine-width screenshots, reduced motion, Tab walk, clean console, Lighthouse ≥90 on mobile | Met | `DEMO-REPORT.md`; nine widths and reduced motion retaken from production; Lighthouse (mobile, production): Performance 100, Accessibility 100, Best Practices 100, SEO 100. Also checked in Firefox, WebKit and phone emulation. |
| 10 Deploy | Production loads clean; live topology shows the real deployed cluster; site correct with the cluster stopped | Met, single node | Live in five browser/device targets; with the Render service suspended the page painted in 1.0 s with every section and the pill reached "broker asleep" at 91.5 s; resumed within 10 s. Uptime workflow run on `main` succeeded. |
| 11 Final audit | FINAL-AUDIT.md and INTERVIEW.md committed | Met | This file; `docs/INTERVIEW.md` with the re-measured numbers. |

Not launch-ready (spec Phase 10): no custom domain, and the privacy and terms pages are drafts labelled "not legal advice". Both are the owner's (steps at the end).

## Test suite

`./mvnw -B -ntp verify` on `main`: BUILD SUCCESS. Broker 114 tests, client 12 (1 skipped on Windows: the SIGSTOP test, which needs Linux and passed there), bench 1. Site: `pnpm lint`, `pnpm typecheck`, `pnpm test` (13 tests), `pnpm audit` (`No known vulnerabilities found`), `pnpm build`. CI runs the same on every push.

Line coverage (JaCoCo, broker; the 0.80 gate applies to `log` and `replication`):

| Package | Line coverage | Lines |
|---|---|---|
| `dev.logforge` | 0.779 | 646/829 |
| `dev.logforge.admin` | 0.884 | 488/552 |
| `dev.logforge.controller` | 0.910 | 364/400 |
| `dev.logforge.coordinator` | 0.857 | 361/421 |
| `dev.logforge.log` | 0.860 | 1172/1362 |
| `dev.logforge.network` | 0.867 | 358/413 |
| `dev.logforge.network.protocol` | 0.980 | 674/688 |
| `dev.logforge.replication` | 0.877 | 852/972 |
| `dev.logforge.util` | 0.849 | 236/278 |

## Follower-pause: root cause, fix, measured frequency

The first follower-pause run on 2026-10-26 moved leadership off a leader that was never paused. Repeated with full logging (`bash chaos/follower-pause-study.sh 22`):

| | Runs | Anomalous | FAIL | Longest heartbeat silence of an unpaused broker |
|---|---|---|---|---|
| Before the fix | 22 | 22 (100%) | 4 (about 15,000 acknowledged records unreadable each) | 6.8 to 7.5 s |
| After the fix | 22 | 0 | 0 | 249 to 262 ms |

Cause: when a broker was paused, the others' replica fetchers sat in a blocking read from it. The metadata update that moved leadership closed those fetchers from the heartbeat thread, and `BlockingChannel.close()` waited for the read to time out (about 7 s), so the healthy brokers stopped heartbeating and were declared dead. The fetcher shutdown also interrupted a thread that could be writing to the log, which closes the log's FileChannel; the broker then fenced itself as a storage failure and partitions whose remaining ISR was fenced went offline. Fix: non-blocking `close()`, no interrupts; regression tests `BlockingChannelTest` and `ReplicaFetcherShutdownTest` (both failed first). Details: `chaos/results/follower-pause-study/README.md`.

## Security checklist (S1 to S20)

| Item | Verdict | Evidence |
|---|---|---|
| S1 SQL injection | N/A | No database. |
| S2 XSS | PASS | React escaping only, `react/no-danger` lint rule, no `dangerouslySetInnerHTML`; per-request nonce CSP on every page; broker JSON escapes `<`, `>`, `&`. |
| S3 CSRF | N/A | No cookie authentication anywhere; the admin API takes bearer tokens. |
| S4 Payload limits | PASS | Frame size checked before allocation, batch cap, 8 MiB fetch cap, 64 KiB admin bodies, 1 MiB broker status (F3). |
| S5 BOLA/IDOR | N/A | No user-owned objects. |
| S6 Rate limiting | PASS (admin API), partial (binary protocol) | 429 with `Retry-After` per IP and per token; binary protocol: connection caps and three failed authentications close a connection, no request-rate limit. |
| S7 JWT | N/A | Not used. |
| S8 API secrets | PASS | Tokens from the environment only, generated by Render in production; the read token stays in the site's server-side route handlers; gitleaks over the full history: no leaks. |
| S9 Password hashing | N/A | No passwords. |
| S10 MFA | N/A | No accounts. |
| S11 CORS | PASS | Exact allowlist, wildcard refused at startup; production origin set in `render.yaml`. |
| S12 Web storage | PASS | The site stores no token in the browser. |
| S13 Server-side permissions | PASS | Read token cannot mutate (`AdminHttpServerTest`); binary-protocol cluster operations need the cluster token (`ClusterAuthTest`). |
| S14 Row-level security | N/A | No database. |
| S15 Webhooks | N/A | None. |
| S16 SSRF | PASS | No endpoint fetches a user-supplied URL; the site's admin URL comes from server configuration and must be HTTPS outside localhost. |
| S17 Source maps | PASS | `productionBrowserSourceMaps: false`. |
| S18 Default credentials | PASS | No default tokens; nodes refuse to start without strong, distinct tokens. |
| S19 Logs | PASS | No record contents or tokens in logs (`SecurityLimitsTest`). |
| S20 Dependencies | PASS | OWASP dependency-check: no findings for broker, client or bench jars (kafka-clients 3.9.2); `pnpm audit` clean. |

## Benchmarks (re-measured on main)

Run: `bash bench/run.sh (WARMUP=5 MEASURE=20)`, finished 2026-10-27 07:05 UTC (LogForge) and 2026-10-27 07:11 UTC (Kafka). LogForge 1.0.0-SNAPSHOT; Apache Kafka 3.9.0 brokers, client 3.9.2.
Fixed config unless the row says otherwise: 1024 B records, acks=1, 4 partitions, replication factor 3, batch 64 KB, linger 5 ms, client buffer 8 MiB, compression none, 1024 distinct keys, murmur2 partitioning (same in both clients), warm-up 5 s, measured 20 s.
Hardware: 12th Gen Intel Core i7-1255U (10 cores / 12 threads), 15.7 GiB RAM, Samsung MZVL2512HCJQ NVMe SSD, Windows 11 10.0.26300 with Docker Desktop (WSL 2). Containers: Docker 29.1.2 on Docker Desktop (containerized), 12 CPUs and 7798 MiB visible to containers; brokers and the client share this host. JVM: Eclipse Adoptium 21.0.12.1.

| Configuration | LogForge records/s | MB/s | p50 ms | p99 ms | Kafka records/s | MB/s | p50 ms | p99 ms | Kafka/LogForge MB/s |
|---|---|---|---|---|---|---|---|---|---|
| 100 B, acks=1 | 293,682 | 29.37 | 34 | 1120 | 534,300 | 53.43 | 131 | 448 | 1.82x |
| 1 KB, acks=1 (baseline) | 55,283 | 56.61 | 47 | 906 | 79,060 | 80.96 | 43 | 363 | 1.43x |
| 10 KB, acks=1 | 7,965 | 81.56 | 8 | 708 | 10,717 | 109.74 | 12 | 370 | 1.35x |
| 1 KB, acks=0 | 92,393 | 94.61 | 33 | 986 | 97,249 | 99.58 | 11 | 529 | 1.05x |
| 1 KB, acks=all | 46,757 | 47.88 | 94 | 769 | 71,715 | 73.44 | 16 | 499 | 1.53x |
| 1 KB, 1 partition | 32,222 | 32.99 | 202 | 904 | 31,483 | 32.24 | 122 | 1120 | 0.98x |
| 1 KB, 16 partitions | 81,359 | 83.31 | 18 | 1060 | 42,089 | 43.1 | 70 | 1089 | 0.52x |
| 1 KB, fsync never (same config as baseline) | 85,273 | 87.32 | 10 | 707 | 53,176 | 54.45 | 24 | 897 | 0.62x |
| 1 KB, fsync every 1 s | 58,508 | 59.91 | 32 | 739 | 43,038 | 44.07 | 29 | 1246 | 0.74x |
| 1 KB, fsync every batch | 3,562 | 3.65 | 220 | 5690 | 2,180 | 2.23 | 1138 | 8789 | 0.61x |

- e2e logforge: sent 25,000, received 25,000; send to receive p50 17.1 ms, p99 181.1 ms; ack to receive p50 9.36 ms, p99 149.9 ms
- e2e kafka: sent 25,000, received 25,000; send to receive p50 9.2 ms, p99 83.8 ms; ack to receive p50 0.54 ms, p99 15.7 ms

| Scenario | Fault | Acked | Missing | Duplicates | Recovery | Verdict | Finished (UTC) |
|---|---|---|---|---|---|---|---|
| leader-kill | SIGKILL the partition leader mid-write, restart it 15 s later | 81,958 | 0 | 0 | 2.92 s | PASS | 2026-10-27 07:17 |
| follower-pause | SIGSTOP a follower for 15 s (docker pause), then resume | 88,884 | 0 | 0 | 3.02 s | PASS | 2026-10-27 07:18 |
| disk-full | broker3's data volume is a 48 MiB tmpfs that fills under load | 105,418 | 0 | 0 | 0.16 s | PASS | 2026-10-27 07:20 |
| network-partition | Cut the partition leader off the cluster network for 15 s | 88,130 | 0 | 0 | 3.10 s | PASS | 2026-10-27 07:21 |
| restart-under-load | Graceful restart of the partition leader under load | 51,312 | 0 | 0 | 1.28 s | PASS | 2026-10-27 07:22 |

Workload: acks=all, idempotent=True, replication factor 3, min.insync.replicas 2, 3 partitions, 2000 records/s.

Reading: two runs of one configuration differ by up to 1.54x on this shared laptop, so only gaps beyond that count. Kafka was ahead with 100 B records (1.82x) and in end-to-end latency; LogForge was ahead with 16 partitions and with an fsync after every batch; everything else is within the noise. One run per configuration; no error bars. The site computes the same reading from the same files.

## Hosted mode at 512 MB

`bash bench/hosted-memory-test.sh` (the production image with `render.yaml`'s JVM settings, 512 MB, no swap, 0.1 CPU): 341,036 records at 500/s for 10 minutes, 0 failed, peak 54.6 MiB cgroup memory, 67 MB resident, no OOM kill, no restart, 0 error lines.

## Browsers and devices

Desktop Chromium, Firefox 156 and WebKit 26.6 at 1440×900, and iPhone 15 (WebKit) and Pixel 7 (Chromium) emulation with touch, against production: the home page, live topology, topology table, crash-recovery replay, segment explorer, benchmark controls and tape toggle all work, with no console errors, no horizontal overflow and no touch target under 24 px. **Real physical phones were not tested**, and Safari itself was not run (WebKit is its engine).

## Repository size

`du -sh --exclude=.git .` on a fresh clone of `82013f9` (2026-10-27, everything but this file): 9.3 MB (limit 25 MB).

## Verified

- All twelve gates above on the final code, with the commands and tests named; CI green on `main`.
- Production from `main`: live in five browser/device targets, Lighthouse mobile 100/100/100/100, console clean, the broker suspended and resumed, the uptime workflow run succeeded.
- The follower-pause anomaly explained, fixed and measured (22 of 22 anomalous before, 0 of 22 after).
- Every benchmark, chaos and hosted-mode number re-measured on `main` and replaced in `bench/results`, `chaos/results`, the site, README and docs.

## Not verified / assumed

- Real physical phones and desktop Safari.
- One benchmark run per configuration, so no error bars; Docker Desktop's Kubernetes ran in the background during both systems' runs.
- The first request after Render's 15 idle minutes was not timed; suspension and resume were.
- Vercel Preview deployments: `LOGFORGE_ADMIN_URL` is set for Preview, but the Preview copy of `LOGFORGE_READ_TOKEN` needs the secret pasted by the owner, so PR previews show the recorded state until then.
- Legal adequacy of the privacy and terms drafts.

## Limitations (also on the site and in DECISIONS.md)

A single fixed controller with no consensus; no TLS on the binary protocol and only a shared cluster token for cluster operations, so port 9092 must stay private; no zero-copy, no compression, no transactions; consumer groups without cooperative rebalancing; compaction that rescans the whole partition; the hosted demo is one ephemeral broker.

## Open items (the owner's)

### 1. Custom domain
1. Buy or choose a domain (a paid action, so not done here).
2. Vercel → project `logforge` → Settings → Domains → Add, enter the domain (for example `logforge.example.com`), and create the DNS record Vercel shows (an A record to Vercel's IP for an apex domain, or a CNAME to `cname.vercel-dns.com` for a subdomain) at the domain's registrar.
3. Wait until Vercel shows the domain as valid and the certificate as issued.
4. Allow the new origin on the broker: change `LOGFORGE_CORS_ORIGINS` in `render.yaml` to the new origin (comma-separated if both should work), commit, push to `main`; the Render Blueprint syncs it.
5. Re-run Lighthouse and the console sweep against the new domain.

### 2. Legal review of the privacy and terms drafts
1. Give `web/src/app/privacy/page.tsx` and `web/src/app/terms/page.tsx` to a lawyer for your jurisdiction, with the facts they rely on: no accounts, no cookies, no analytics, Vercel and Render as hosts, the demo broker stores only seeded sample records.
2. Replace the text with the reviewed version and remove the "Draft, not legal advice" label only once it is approved.
3. Commit to `main`; Vercel redeploys.
