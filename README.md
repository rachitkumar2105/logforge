# LogForge

A Kafka-style distributed commit log written from first principles in Java 21, plus a showcase site that renders the running cluster live.

The broker is hand-written and has **zero runtime dependencies**: segment files, the sparse offset index, the record batch format (CRC-32C, varints), the NIO network layer, pull-based replication with an in-sync replica set, leader election, consumer groups, idempotent producers and log compaction. Apache Kafka appears only in `bench/`, as the comparison baseline.

Every durability claim in this repository comes from a test that killed a process, and every number comes from a recorded command with its full configuration. See `RUN-REPORT.md` for the evidence and `FINAL-AUDIT.md` for the summary.

## What is live and what is recorded

The hosted demo is **one broker** on Render's free tier (512 MB, no persistent disk, asleep after 15 idle minutes) behind the Next.js site on Vercel. It proves the broker runs: the site's topology section shows the real deployed node, labelled as one node, whenever it is awake, and a recorded snapshot, labelled as recorded, while it wakes or sleeps.

One node cannot show replication, ISR changes, leader election or failover. Those claims rest on recorded evidence from a **local three-node cluster**: the crash-recovery replay on the site (a recorded `chaos/run-all.sh` leader-kill run, played entirely in the browser), `bench/results/` and `chaos/results/`. Every measured number in this repository and on the site comes from that local cluster, with the hardware recorded next to it.

## Layout

| Path | What |
|---|---|
| `broker/` | The broker (`dev.logforge`): `log`, `network`, `replication`, `coordinator`, `controller`, `admin` |
| `client/` | Java producer, consumer (with groups) and admin client |
| `bench/` | Throughput and latency harness, LogForge vs Apache Kafka; chaos client |
| `chaos/` | Kill-and-verify scenarios against Docker Compose |
| `web/` | Next.js showcase site |
| `docs/` | PRD, architecture, wire protocol, admin API, design, decisions, interview notes |

## Run it

Prerequisites: JDK 21+, Docker with Compose, Node 22.18+ (it runs the TypeScript site tests directly) and pnpm 9 for the site.

```bash
./mvnw -B -ntp verify
```

```bash
cp .env.example .env
```

Fill `.env` with two different random tokens (`openssl rand -hex 32`), then:

```bash
docker compose up -d --build
```

The controller's admin API is on `http://127.0.0.1:8080` (token-protected; `/health` is public). Broker ports are not published; clients run on the `logforge_cluster` network.

```bash
bash bench/run.sh
```

```bash
bash chaos/run-all.sh
```

```bash
cd web && pnpm install && pnpm dev
```

## Deploy (free tiers)

**Broker on Render** (free web service, Docker, from this repository; `render.yaml` holds the settings). Set in the Render dashboard, Environment:

| Variable | Value |
|---|---|
| `LOGFORGE_ADMIN_TOKEN` | a random secret, at least 24 characters (`openssl rand -hex 32`) |
| `LOGFORGE_READ_TOKEN` | a different random secret, same length |
| `LOGFORGE_CORS_ORIGINS` | the site's production origin, e.g. `https://<project>.vercel.app` |

When creating the service by hand instead of from the Blueprint, also set `LOGFORGE_NODE_ID=1`, `LOGFORGE_ROLES=broker,controller`, `LOGFORGE_SEED_TOPIC=demo-orders:3:600`, `LOGFORGE_IO_THREADS=1`, `LOGFORGE_HANDLER_THREADS=2`, `JAVA_TOOL_OPTIONS` as in `render.yaml`, and the health check path `/health`. Render provides `PORT`; do not set it.

**Site on Vercel** (Hobby, root directory `web`). Set in Project Settings, Environment Variables, for Production and Preview:

| Variable | Value |
|---|---|
| `LOGFORGE_ADMIN_URL` | the Render service URL, e.g. `https://logforge-broker.onrender.com` |
| `LOGFORGE_READ_TOKEN` | the same read token as on Render |

Both are server-side only. Never prefix them with `NEXT_PUBLIC_`, and never give the site the admin token.

## Durability, in one table

| Setting | What is guaranteed | What can be lost |
|---|---|---|
| `acks=0` | Nothing | Anything; the producer never learns of a failure |
| `acks=1` | The leader appended the batch (page cache) | Batches the leader acknowledged but no follower copied, if the leader dies (measured 2026-10-27: `ReplicationTest`, 3 of 3 lost) |
| `acks=all` + `min.insync.replicas=2` | Every in-sync replica has the batch before the ack | Nothing, unless every in-sync replica fails at once (measured 2026-10-27: `LeaderKillGateTest`, 10 leader kills, 1,353,801 acknowledged on Windows and 1,480,001 on Linux, 0 lost; `chaos/run-all.sh`, five fault scenarios on the Docker cluster, 0 lost) |
| Unclean election (opt-in) | Availability when no in-sync replica survives | Committed records the elected replica never had |

**fsync policy** (`LOGFORGE_FSYNC`): the page cache already survives a process crash, because it belongs to the kernel, so a `SIGKILL` loses nothing acknowledged under any policy. fsync matters only for power loss or a kernel crash.

| Policy | Effect |
|---|---|
| `never` | Highest throughput; a power cut on a single broker can lose roughly the OS flush interval |
| `ms:1000` (default) | At most about one second of acknowledged writes exposed to a power cut |
| `records:1` | Flush every batch; safest and by far the slowest (see `fsync-records-1` in `bench/results`) |

## Limitations

A single fixed controller (no consensus), no TLS on the binary protocol and only a shared cluster token for cluster operations (producers and consumers need none; run it on a private network), no zero-copy, no compression, no transactions. The full list is on the site and in `docs/DECISIONS.md`.
