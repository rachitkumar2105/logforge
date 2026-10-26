# LogForge Admin HTTP API (v1)

Served by `AdminHttpServer` on every node (`LOGFORGE_ADMIN_LISTEN`, default `0.0.0.0:8080`). The cluster-wide endpoints answer only on the controller; brokers answer `/health`, `/metrics` and `/api/v1/local`. The binary client protocol is in `PROTOCOL.md`.

TLS is terminated at the hosting platform's edge; the process speaks plain HTTP on a private network.

## Authentication

| Header | Value |
|---|---|
| `Authorization` | `Bearer <token>` |

| Token (env var) | Grants |
|---|---|
| `LOGFORGE_READ_TOKEN` | all `GET` endpoints below except `/health` |
| `LOGFORGE_ADMIN_TOKEN` | everything the read token grants, plus `POST` and `DELETE` |

- The server refuses to start if either variable is unset or shorter than 24 characters, or if both are equal.
- Tokens are compared in constant time. They are never logged and never echoed back.
- Missing or wrong token: `401 {"error":"unauthorized"}`. Read token on a mutation: `403 {"error":"forbidden"}`.

## Rate limits

Token buckets per client IP (`LOGFORGE_RATE_PER_IP`, default 20 requests/s, burst 40) and per token (`LOGFORGE_RATE_PER_TOKEN`, default 50/s, burst 100). Exceeding either returns `429 {"error":"rate_limited"}` with a `Retry-After` header in whole seconds.

## CORS

`Access-Control-Allow-Origin` is set only when the request `Origin` exactly matches an entry of `LOGFORGE_CORS_ORIGINS` (comma-separated; default `http://localhost:3000`). No wildcard is ever sent. Allowed methods `GET, POST, DELETE, OPTIONS`; allowed headers `Authorization, Content-Type`.

## Security headers on every response

`X-Content-Type-Options: nosniff`, `Cache-Control: no-store`, `Content-Type: application/json; charset=utf-8` (or the Prometheus text type), `Referrer-Policy: no-referrer`.

## Limits

Request bodies over 64 KiB: `413 {"error":"payload_too_large"}`. Unknown paths: `404 {"error":"not_found"}`. Wrong method: `405 {"error":"method_not_allowed"}`. Internal failures: `500 {"error":"internal"}` (no stack trace, no detail).

---

## Endpoints

### `GET /health` (public)
```json
{ "status": "ok", "nodeId": 0, "roles": ["controller"], "uptimeMs": 81234 }
```

### `GET /metrics` (read token)
Prometheus text exposition format 0.0.4. Metric families:

| Metric | Type | Labels |
|---|---|---|
| `logforge_messages_in_total` | counter | `topic` |
| `logforge_messages_in_per_sec` | gauge (1-minute EWMA) | |
| `logforge_bytes_in_total` | counter | |
| `logforge_bytes_out_total` | counter | |
| `logforge_produce_latency_seconds` | histogram | `acks` |
| `logforge_fetch_latency_seconds` | histogram | `type` (`consumer`, `follower`) |
| `logforge_isr_size` | gauge | `topic`, `partition` |
| `logforge_under_replicated_partitions` | gauge | |
| `logforge_log_end_offset` | gauge | `topic`, `partition` |
| `logforge_high_watermark` | gauge | `topic`, `partition` |
| `logforge_consumer_group_lag` | gauge | `group`, `topic`, `partition` |

### `GET /api/v1/cluster` (read token, controller)
Full topology for the site.
```json
{
  "clusterId": "lf-7c1e",
  "controllerId": 0,
  "metadataVersion": 42,
  "capturedAtMs": 1761460000000,
  "brokers": [
    { "id": 1, "host": "broker1", "port": 9092, "alive": true, "lastHeartbeatMsAgo": 180 }
  ],
  "topics": [
    {
      "name": "orders",
      "cleanupPolicy": "delete",
      "partitions": [
        {
          "partition": 0,
          "leader": 1,
          "leaderEpoch": 3,
          "replicas": [1, 2, 3],
          "isr": [1, 2],
          "underReplicated": true,
          "offline": false,
          "replicaState": [
            { "brokerId": 1, "logEndOffset": 1200, "highWatermark": 1200 },
            { "brokerId": 2, "logEndOffset": 1200, "highWatermark": 1200 },
            { "brokerId": 3, "logEndOffset": 900,  "highWatermark": 900 }
          ]
        }
      ]
    }
  ],
  "groups": [
    {
      "groupId": "billing",
      "coordinator": 2,
      "state": "stable",
      "generation": 4,
      "members": 3,
      "lag": [ { "topic": "orders", "partition": 0, "committed": 1100, "highWatermark": 1200, "lag": 100 } ]
    }
  ],
  "rates": { "messagesInPerSec": 5120.4, "bytesInPerSec": 5242880.0, "bytesOutPerSec": 10485760.0 }
}
```
`replicaState` comes from each broker's last heartbeat, so it is at most one heartbeat interval old. A broker that is down keeps its last reported values with `alive: false`.

### `GET /api/v1/topics/{name}` (read token, controller)
One element of `topics` above plus `"configs": { "retention.ms": "604800000", ... }`. `404` if unknown.

### `GET /api/v1/groups` (read token, controller)
`{ "groups": [ ...as above... ] }`

### `GET /api/v1/segments/{topic}/{partition}` (read token, controller)
Segment layout reported by the partition leader (most recent 32 segments).
```json
{
  "topic": "orders", "partition": 0, "reportedBy": 1,
  "logStartOffset": 0, "logEndOffset": 1200,
  "segments": [
    { "baseOffset": 0, "sizeBytes": 1048512, "indexEntries": 255, "active": false },
    { "baseOffset": 1012, "sizeBytes": 196400, "indexEntries": 47, "active": true }
  ]
}
```

### `POST /api/v1/topics` (admin token, controller)
Body:
```json
{ "name": "orders", "partitions": 6, "replicationFactor": 3, "configs": { "min.insync.replicas": "2" } }
```
`201 {"name":"orders"}`; `400 {"error":"invalid_topic"|"invalid_partitions"|"invalid_replication_factor"|"invalid_body"}`; `409 {"error":"topic_exists"}`.

### `DELETE /api/v1/topics/{name}` (admin token, controller)
`200 {"deleted":"orders"}`; `404 {"error":"not_found"}`. Internal topics (`__*`) cannot be deleted (`400`).

### `GET /api/v1/local` (read token, any node)
This node's own status: the same JSON the broker sends in its heartbeat.
