# LogForge Architecture

Source of truth: `docs/PRD.md` (product), `docs/PROTOCOL.md` (bytes on the wire and on disk), `docs/API.md` (admin HTTP), `docs/DESIGN.md` (site visuals). Every choice that deviates from the spec is in `docs/DECISIONS.md`.

## 1. Stack

| Layer | Choice | Why |
|---|---|---|
| Broker | Java 21 (`--release 21`), JDK NIO, `java.util.zip.CRC32C`, `com.sun.net.httpserver` | The spec requires a hand-written engine. The JDK alone covers sockets, mmap, CRC32C and HTTP, so the broker has **zero runtime dependencies**. |
| Build | Maven 3.9 through the Maven wrapper, multi-module | One global cache (`~/.m2`), nothing vendored. |
| Tests | JUnit 5, AssertJ | Spec. Durability tests spawn real JVM processes so kills are real (`SIGKILL` on Linux, `TerminateProcess` on Windows). |
| Static analysis | Spotless (google-java-format), Error Prone, JaCoCo | Spec. |
| Clients | Plain Java in `client/`, depends on the broker module only for the shared wire classes | One source of truth for the byte format. |
| Bench | `bench/`, adds `kafka-clients` for the baseline only | Spec rule: Kafka never enters the broker module. |
| Site | Next.js App Router, TypeScript strict, Tailwind, `motion/react`, Recharts, pnpm | Spec. |
| Hosting | Docker Compose (controller + 3 brokers); Railway or Fly.io; Vercel | Spec. |

## 2. Process topology

```
               +-------------+   heartbeats (metadata pull),
               | controller  |<-- AlterIsr, CreateTopics -----------+
               | (role)      |                                      |
               | admin HTTP  |<-- site route handler (read token)   |
               +-------------+                                      |
                     ^  ^  ^                                        |
          heartbeat  |  |  |                                        |
        +------------+  |  +-------------+                          |
        |               |                |                          |
   +---------+     +---------+      +---------+                     |
   | broker1 |<----| broker2 |----->| broker3 |  followers PULL     |
   +---------+fetch+---------+ fetch+---------+  from leaders       |
        ^               ^                ^                          |
        +----- producers / consumers (binary protocol) -------------+
```

- **Controller** is a role, not a separate codebase. One fixed node owns cluster metadata. In Compose it is its own container; in the single-node demo the same process runs both roles. Limitation: no controller failover. While the controller is down, brokers keep serving with their last metadata, but no leader election, ISR change or topic creation happens.
- **Brokers** host partition replicas and serve clients. Each broker heartbeats the controller every `heartbeat.interval.ms`; the heartbeat carries the broker's per-partition status (LEO, HW, segment layout, group lag) and the response carries the full metadata snapshot when the broker's copy is stale. Metadata propagation is pull-based and self-healing: a broker that missed an update gets it on its next heartbeat.

## 3. Broker internals (`broker/src/main/java/dev/logforge/`)

| Package | Main classes | Responsibility |
|---|---|---|
| `log` | `RecordBatch`, `Record`, `LogSegment`, `OffsetIndex`, `Log`, `LogConfig`, `Compactor`, `FsyncPolicy`, `ProducerStateManager`, `LeaderEpochCache` | Byte format, segments, sparse index, roll, retention, recovery, truncation, compaction, idempotence and epoch state. |
| `network` | `NioServer`, `Connection`, `RequestHandler`, `protocol/*` | Acceptor + IO threads, framing, pipelining with in-order responses, backpressure, request codecs. |
| `replication` | `Partition`, `Replica`, `ReplicaFetcher`, `ReplicaManager`, `IsrManager`, `DelayedOperations` | Leader/follower state, follower pull loop, HW, ISR shrink and expand, `acks=all` waits, long-poll fetch. |
| `coordinator` | `GroupCoordinator`, `Assignors` | Groups, rebalance barrier, heartbeats, offsets in `__consumer_offsets`. |
| `controller` | `Controller`, `ClusterMetadata` | Broker liveness, partition assignment, leader election, ISR changes, metadata persistence. |
| `admin` | `AdminHttpServer`, `Metrics`, `RateLimiter` | JSON API, Prometheus exposition, token checks, CORS, rate limits. |
| `util` | `Json`, `JsonLog`, `Env` | Hand-written JSON writer and parser, structured logger, configuration. |
| (root) | `Main`, `Node` | Wires roles from configuration. |

### Threading
- 1 acceptor thread; `io.threads` IO threads, each with its own `Selector`; a pool of request handler threads.
- IO threads only parse frames and write responses. Handlers run on workers and return a `CompletableFuture`. A connection keeps a FIFO of pending responses and writes them strictly in request order, so pipelining is safe.
- Backpressure: a connection whose pending-response queue or outbound bytes exceed a bound stops reading (`OP_READ` cleared) until it drains. A slow consumer therefore only stalls itself.
- Each `Log` serialises appends with a lock; reads use positional `FileChannel.read` against a published log-end snapshot, so readers never block writers.
- One `ReplicaFetcher` thread per (follower broker, leader broker) pair.

## 4. Data flow

### Write path (`acks=all`)
1. Producer batches records per partition (`batch.size`, `linger.ms`) and sends Produce to the leader.
2. Leader validates frame size, CRC, sequence (idempotence), assigns offsets and leader epoch, appends to the active segment with one sequential write into the page cache, updates the sparse index, applies the fsync policy.
3. With `acks=0` nothing is returned; `acks=1` responds now; `acks=all` parks the request until HW passes the batch's last offset or the timeout fires.
4. Followers pull with Fetch(replicaId, offset = their LEO). The leader records each follower's LEO, recomputes HW = min LEO over the ISR, completes parked produces.

### Read path
Fetch(offset) -> segment lookup (floor entry in a skip-list keyed by base offset) -> index lookup (binary search, floor entry) -> forward scan of batch headers -> return whole batches up to `maxBytes`, never past HW for consumers. Long-poll waits up to `maxWaitMs` for data.

### Failover
Controller misses heartbeats for `broker.session.timeout.ms` -> marks broker dead -> for each partition it led, picks the first live replica in the ISR (or any live replica if `unclean.leader.election.enable=true`), bumps the leader epoch, removes the dead broker from the ISR -> brokers learn on their next heartbeat. A returning follower asks the leader for the end offset of its last epoch (OffsetForLeaderEpoch), truncates the divergent tail, then fetches.

## 5. Storage

There is no database. Storage is files under `LOGFORGE_DATA_DIR` (layout in `PROTOCOL.md` section 6). Durability relies on sequential writes into the OS page cache plus the configured fsync policy. There is intentionally no user-space read cache (see the comment in `LogSegment`). Controller metadata is one JSON file replaced atomically (write temp, fsync, rename).

## 6. Authentication and authorization

| Surface | Mechanism |
|---|---|
| Binary protocol (9092) | None. Internal network only; never published in Compose. Documented limitation. |
| Admin HTTP `GET /health` | Public, returns only liveness. |
| Admin HTTP read endpoints and `/metrics` | Bearer read token or admin token. |
| Admin HTTP mutations (create, delete topic) | Admin token only; read token receives 403. Deny by default. |
| Site | Visitor never holds a token. `/api/cluster` route handler holds the read token server-side. |

## 7. API architecture

- Binary protocol: length-prefixed request/response, API key + version + correlation id. `PROTOCOL.md`.
- Admin HTTP JSON: `API.md`.

## 8. External services

None at runtime for the broker. The site server calls only the admin API whose URL is fixed by an environment variable (no user-supplied URLs, so no SSRF surface).

## 9. Environment configuration

| Variable | Used by | Default |
|---|---|---|
| `LOGFORGE_NODE_ID` | all nodes | required |
| `LOGFORGE_ROLES` | `broker`, `controller`, or `broker,controller` | `broker,controller` |
| `LOGFORGE_LISTEN` | binary listener | `0.0.0.0:9092` |
| `LOGFORGE_ADVERTISED` | address placed in metadata | `localhost:<listen port>` |
| `LOGFORGE_CONTROLLER` | `host:port` of the controller | the node itself if it is the controller |
| `LOGFORGE_DATA_DIR` | data | `~/logforge-data/node-<id>` |
| `LOGFORGE_ADMIN_LISTEN` | admin HTTP | `0.0.0.0:8080` (`off` disables) |
| `LOGFORGE_ADMIN_TOKEN`, `LOGFORGE_READ_TOKEN` | admin HTTP | none; startup fails if unset |
| `LOGFORGE_CORS_ORIGINS` | admin HTTP | `http://localhost:3000` |
| `LOGFORGE_FSYNC` | `never`, `records:N`, `ms:N` | `ms:1000` |
| `LOGFORGE_UNCLEAN_ELECTION` | controller | `false` |
| Site: `LOGFORGE_ADMIN_URL`, `LOGFORGE_READ_TOKEN` | Next.js server only | unset means snapshot mode |

## 10. Error handling

- Protocol errors are typed error codes per partition (`PROTOCOL.md` section 4), never stack traces.
- Malformed or oversized frames close only the offending connection.
- IO errors on append roll back the partial write (truncate to the previous size); if that fails, the partition goes offline rather than serving a corrupt log.
- Admin HTTP returns `{ "error": "<code>" }` with no internal detail.

## 11. Security boundaries

Internet -> Vercel (site, CSP, HSTS) -> route handler (read token) -> platform TLS edge -> admin HTTP (token, rate limit, CORS allowlist) -> broker process. Broker binary ports exist only on the private Compose or platform network. Evidence in `SECURITY.md`.

## 12. Testing and deployment

- Unit tests per class; integration tests run real sockets in-process; durability tests spawn child JVMs and kill them.
- Coverage gate: JaCoCo, 80% lines on `dev.logforge.log` and `dev.logforge.replication`.
- Chaos and the Kafka comparison run against Docker Compose (`chaos/run-all.sh`, `bench/run.sh`).
- CI: GitHub Actions on Ubuntu: `./mvnw verify` (format check, Error Prone, tests, coverage), `pnpm lint && pnpm build` for the site.
- Deploy: multi-stage Docker image (Maven build stage, JRE-only runtime stage). Site on Vercel.

## 13. MVP versus future scale

| MVP | Future |
|---|---|
| Single fixed controller | Raft-replicated metadata (KRaft-style) |
| Heap copies on fetch | `FileChannel.transferTo` zero-copy |
| One fetcher thread per leader | Fetcher pools, incremental fetch sessions |
| Full metadata snapshot on change | Delta metadata log |
| No compression | Batch-level compression codecs |
| No client auth | mTLS or SASL |
