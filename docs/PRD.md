# LogForge: Product Requirements Document

Spec version: 1.0 (source: `LogForge-BUILD-SPEC.pdf`, owner Rachit Kumar Singh)
Status: source of truth for the build agent. Where this document and the spec disagree, the spec wins and the disagreement is recorded in `DECISIONS.md`.

## 1. Problem, value proposition, target users

### Problem
Distributed-systems interviews and backend hiring loops probe whether a candidate understands durability, replication, failure recovery and throughput as an operator does. Reading about Kafka does not demonstrate that. A hand-built, measured, failure-tested log broker does.

### Value proposition
LogForge is a Kafka-style distributed append-only commit log written from first principles in Java 21, plus a showcase site that renders the running cluster live. Every durability claim is backed by a test that kills a process; every throughput number is backed by a recorded command, its full configuration and the hardware it ran on.

### Target users
| User | What they need |
|---|---|
| Interviewer / hiring engineer (primary) | In five minutes: see the live cluster, watch a recorded failover, check numbers against real Kafka, read honest limitations. |
| The author (operator) | Run the cluster locally and in Docker, regenerate benchmarks and chaos results with one command each, answer deep questions using the code. |
| Developer reading the repo | Implement a client from `PROTOCOL.md`; understand the write, replication and read paths from `ARCHITECTURE.md`. |

## 2. Core user journeys

1. **Visitor evaluates the project.** Lands on the site, sees the measured headline throughput (with its configuration), watches the live topology, scrubs the crash-recovery replay, enters an offset in the segment explorer, compares charts against Kafka, reads the limitations.
2. **Visitor while the cluster is down.** Same journey; the topology shows the recorded snapshot and states clearly that it is a snapshot and when it was captured.
3. **Operator runs the cluster.** `docker compose up` brings up a controller and three brokers; creates a topic with replication factor 3; produces and consumes with the Java client.
4. **Operator proves durability.** Runs the chaos command; leader kill, follower pause, disk full, network partition and restart under load each produce a recovery time and an integrity verdict in `chaos/results/`.
5. **Operator regenerates benchmarks.** Runs the bench command; results land in `bench/results/` with configuration and hardware, and the site picks them up at build time.

## 3. User stories

- As an interviewer, I can see which broker leads each partition and which replicas are in sync, refreshed from the real cluster.
- As an interviewer, I can scrub a recorded leader failure and see acknowledged offsets, the kill, the election and the catch-up with a measured recovery time.
- As an interviewer, I can see LogForge and Kafka numbers side by side with the hardware and versions underneath.
- As an operator, I can produce with `acks=0|1|all` and understand exactly what each guarantees.
- As an operator, I can choose an fsync policy (`never`, every N records, every N ms) and read the trade-off.
- As a consumer in a group, my partitions move to surviving members when a member dies, and consumption resumes from the committed offset with no gap.
- As a producer with idempotence on, a retried batch never creates a duplicate.
- As an operator, compacted topics keep only the latest value per key and drop tombstoned keys.

## 4. Features and functional requirements

### 4.1 Broker (Java 21, no Kafka libraries)
| ID | Requirement |
|---|---|
| B1 | Record batch format: length prefix, magic, crc32c, base offset, record count, timestamps, varint-encoded records with headers. Byte layout fixed in `PROTOCOL.md`. |
| B2 | Segmented log: append-only `.log` files plus memory-mapped sparse `.index` files (one entry per 4 KB by default); base offset in the file name. |
| B3 | Segment roll by size or age; retention by age or total size; active segment never deleted. |
| B4 | Recovery on startup: CRC-validate the active segment, truncate at the first invalid record, rebuild a missing or short index. |
| B5 | NIO server: one acceptor, N IO threads with own selectors, length-prefixed frames, pipelining, per-connection backpressure, hard frame-size cap checked before allocation. |
| B6 | APIs: Produce, Fetch, Metadata, ListOffsets, CreateTopics, DeleteTopics, FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch, InitProducerId, plus internal controller and replication APIs. |
| B7 | Topics with multiple partitions; murmur2 hash partitioner on key; documented round-robin default for null keys. |
| B8 | `acks` 0, 1, all; fsync policy configurable with an explicit default. |
| B9 | Replication: controller assigns leader and followers; followers pull; high watermark = min LEO across ISR; consumers read only to the high watermark. |
| B10 | ISR shrink on time lag, expand on catch-up; `acks=all` waits for the full ISR; leader failover from ISR; unclean election opt-in, off by default; follower truncation on divergent tail. |
| B11 | Consumer groups: join, range and round-robin assignment, heartbeat with session timeout, rebalance on join or leave, offsets stored in an internal compacted topic. |
| B12 | Idempotent producer: producer id, per-partition sequence numbers, broker dedupe window. |
| B13 | Log compaction for `cleanup.policy=compact` topics with tombstones. |
| B14 | Prometheus metrics and a token-protected admin HTTP API (topology, partitions, group lag, segment layout, health). Structured JSON logs with correlation ids. |

### 4.2 Clients (`client/`)
Producer with batching, linger, acks, retries and idempotence. Consumer with group membership, manual assignment, commit. Admin client for topics.

### 4.3 Bench and chaos
`bench/` measures throughput, latency percentiles, partition scaling, fsync impact and end-to-end latency for LogForge and for Apache Kafka in Docker, recording hardware and versions. `chaos/` scripts leader kill, follower pause, disk full, network partition and restart under load, recording recovery time and integrity verdicts.

### 4.4 Showcase site (`web/`)
Sections in order: Hero, Live cluster topology, Crash-recovery replay, Log segment explorer, Benchmark explorer, Architecture walkthrough, Honest limitations, Footer. Pages: `/`, `/privacy`, `/terms`, plus a read-only route handler `/api/cluster`.

## 5. Non-functional requirements

| Area | Requirement |
|---|---|
| Durability | No acknowledged-record loss at `acks=all` across 10 leader-kill cycles; SIGKILL recovery passes 10 consecutive runs. |
| Correctness | 1 million records at `acks=1` with zero loss and zero duplicates on one broker. |
| Honesty | Every number traceable to a recorded command; reports split Verified from Not verified. |
| Quality | CI green; JaCoCo line coverage at least 80% on `dev.logforge.log` and `dev.logforge.replication`. |
| Security | S-checklist in `SECURITY.md`; tokens from env only; broker ports internal only. |
| Site performance | Lighthouse performance and accessibility at 90 or above on mobile emulation; 60 fps target; polling throttled and paused when the tab is hidden. |
| Accessibility | Keyboard reachable, visible focus, skip link, landmarks, 4.5:1 body contrast, aria-live on live regions, data tables for every chart and the topology. |
| Responsive | Verified at 375, 414, 768, 1024, 1280, 1440, 1920, 2560, 3440 px. |
| Footprint | Repo under 25 MB excluding `.git`; no log data, `target/`, `node_modules` or secrets committed. |

## 6. Pages and screens

| Route | Content |
|---|---|
| `/` | All eight sections. |
| `/privacy` | Draft privacy page, labelled "draft, not legal advice". |
| `/terms` | Draft terms page, same label. |
| `/api/cluster` | Server-side route handler; holds the read token, proxies the admin API, falls back to the recorded snapshot. |

## 7. Roles and permissions

| Role | Credential | Can |
|---|---|---|
| Public visitor | none | View the site. Never sees a token. |
| Site server (route handler) | `LOGFORGE_READ_TOKEN` | Read topology, partitions, groups, segments. Cannot mutate. |
| Operator | `LOGFORGE_ADMIN_TOKEN` | Everything read can, plus create and delete topics through the admin API. |
| Broker clients | network reachability on the internal network | Produce, fetch, group APIs. No authentication (documented limitation; brokers are never publicly exposed). |

## 8. Data requirements

- Partition data: segment and index files under `LOGFORGE_DATA_DIR` (default `~/logforge-data/`), never in the repo.
- Controller metadata: topics, partition assignments, leaders, ISR and epochs in an atomically replaced metadata file in the controller's data dir.
- Consumer offsets: internal compacted topic `__consumer_offsets`.
- Bench and chaos results: JSON in `bench/results/` and `chaos/results/`, committed.
- Site: build-time copies of bench results, a recorded chaos timeline and a recorded cluster snapshot. No user data is collected.

## 9. Integrations

| Integration | Use |
|---|---|
| Docker Compose | Local 3-broker cluster, chaos, Kafka baseline. |
| Apache Kafka (Docker image, `kafka-clients` in `bench/` only) | Benchmark baseline. |
| Prometheus format | `/metrics` text exposition. |
| GitHub Actions | CI. |
| Railway or Fly.io | Cluster hosting (needs the author's account). |
| Vercel | Site hosting (needs the author's account). |

## 10. Loading, empty and error states (site)

| Section | Loading | Empty | Error / unreachable |
|---|---|---|---|
| Topology | Last known state stays visible; "updating" indicator | "No topics yet" with brokers drawn | Recorded snapshot, labelled with its capture time and "cluster unreachable" |
| Broker down | n/a | n/a | Node drains to the failure colour; ISR and leadership change shown |
| Replay | Static first frame | n/a | Replay data is bundled, cannot fail at runtime |
| Segment explorer | n/a | Prompt to enter an offset | Out-of-range offset explained inline |
| Benchmarks | Bundled at build time | "No results recorded" if the files are empty | n/a |

## 11. MVP scope, out of scope, success criteria

### MVP (all spec phases 0 to 11)
Everything in section 4.

### Out of scope (spec non-goals and recorded cuts)
Raft or ZooKeeper consensus for metadata (single fixed controller), cross-partition transactions, Connect, Streams, SASL or Kerberos, compression codecs, zero-copy `sendfile`, TLS between brokers.

### Success criteria
All eleven phase gates passed with evidence in `FINAL-AUDIT.md`; site live on Vercel and cluster reachable, both verified in a real browser; security audit with no unaccepted FAIL.

## 12. Assumptions (labelled)

- A1: The controller runs as a dedicated role. In Compose it is its own container next to three brokers; a single process can run both roles for the single-node demo.
- A2: Broker-to-broker and client-to-broker traffic is unauthenticated and plaintext on a private network.
- A3: The bench host is the author's laptop (hardware recorded in every result file).

## 13. Decisions requiring the author's approval

- D1: Pushing to GitHub (spec Phase 6). The author's global rules forbid pushing without approval.
- D2: Deploying to Railway or Fly.io and Vercel; accounts and secrets must be supplied by the author.
- D3: Custom domain purchase and DNS.
- D4: Legal review of the draft privacy and terms pages.
