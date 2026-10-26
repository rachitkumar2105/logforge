# LogForge Wire and Disk Protocol (v1)

This document is complete enough to implement a LogForge client in any language without reading the Java source. All multi-byte integers are **big-endian** (network order) unless marked as a varint.

The same record batch bytes travel producer -> leader -> disk -> follower -> consumer unchanged, except for two fields the leader stamps (`baseOffset`, `partitionLeaderEpoch`), which sit outside the CRC on purpose so stamping them does not invalidate the producer's checksum.

---

## 1. Primitive types

| Type | Encoding |
|---|---|
| `int8`, `int16`, `int32`, `int64` | Two's complement, big-endian, 1/2/4/8 bytes. |
| `uint32` | 4 bytes big-endian, read as unsigned (used only for the CRC). |
| `boolean` | `int8`, 0 = false, 1 = true. |
| `string` | `int16` length N, then N bytes of UTF-8. N = -1 means null. Max 32767 bytes. |
| `bytes` | `int32` length N, then N raw bytes. N = -1 means null. |
| `array<T>` | `int32` count N, then N elements of T. N = -1 means null. |
| `varint` | Zig-zag encoded signed 32-bit integer, then LEB128 (7 bits per byte, least significant group first, high bit = continuation). Max 5 bytes. Identical to protobuf `sint32`. |
| `varlong` | Same as `varint` for signed 64-bit. Max 10 bytes. |

Zig-zag: `encoded = (n << 1) ^ (n >> 31)` (for 64-bit, `>> 63`). Decoding: `n = (encoded >>> 1) ^ -(encoded & 1)`.

Examples: `0 -> 0x00`, `-1 -> 0x01`, `1 -> 0x02`, `-2 -> 0x03`, `63 -> 0x7E`, `64 -> 0x80 0x01`, `300 -> 0xD8 0x04`.

---

## 2. Record batch (on disk and on the wire)

A record batch is the unit of append, replication, CRC and fetch.

### 2.1 Batch header (61 bytes)

| Offset | Size | Field | Notes |
|---:|---:|---|---|
| 0 | 4 | `length` int32 | Number of bytes that follow this field. Total batch size = `length + 4`. Minimum 57 (header only, zero records is invalid; see 2.4). |
| 4 | 8 | `baseOffset` int64 | Offset of the first record. **Set by the leader on append**; producers send 0. |
| 12 | 4 | `partitionLeaderEpoch` int32 | Leader epoch at append. **Set by the leader**; producers send -1. |
| 16 | 1 | `magic` int8 | Format version. Always `1` for this spec. Any other value is `CORRUPT_MESSAGE`. |
| 17 | 4 | `crc` uint32 | CRC-32C (Castagnoli, polynomial `0x1EDC6F41`, reflected, init `0xFFFFFFFF`, final xor `0xFFFFFFFF`) over every byte from offset 21 to the end of the batch. |
| 21 | 2 | `attributes` int16 | Bits 0..2 compression codec, must be 0 (none). Bits 3..15 reserved, must be 0. |
| 23 | 4 | `lastOffsetDelta` int32 | `lastOffset - baseOffset`. Kept even if compaction later removes the last record, so the batch still claims its whole offset range. |
| 27 | 8 | `baseTimestamp` int64 | Milliseconds since Unix epoch of the first record. |
| 35 | 8 | `maxTimestamp` int64 | Largest record timestamp in the batch. Used by time-based retention. |
| 43 | 8 | `producerId` int64 | -1 if the producer is not idempotent. |
| 51 | 2 | `producerEpoch` int16 | -1 if not idempotent. |
| 53 | 4 | `baseSequence` int32 | Sequence number of the first record for this producer and partition. -1 if not idempotent. Record `i` has sequence `baseSequence + i`. |
| 57 | 4 | `recordCount` int32 | Number of records that follow. |
| 61 | ... | `records` | `recordCount` records, format below. |

### 2.2 Record

| Field | Type | Notes |
|---|---|---|
| `length` | varint | Bytes that follow, for this record. |
| `attributes` | int8 | Reserved, must be 0. |
| `timestampDelta` | varlong | `timestamp - baseTimestamp`. |
| `offsetDelta` | varint | `offset - baseOffset`. Strictly increasing within a batch; gaps are allowed (after compaction). |
| `keyLength` | varint | -1 = null key. 0 = empty key. |
| `key` | bytes | `keyLength` bytes. |
| `valueLength` | varint | -1 = null value (a **tombstone** on compacted topics). |
| `value` | bytes | `valueLength` bytes. |
| `headerCount` | varint | Number of headers. |
| `headers` | repeated | Each: `headerKeyLength` varint (>= 0), header key UTF-8 bytes, `headerValueLength` varint (-1 = null), header value bytes. |

### 2.3 Worked example

One record, key `"k"`, value `"v"`, no headers, timestamp equal to `baseTimestamp`, first batch of a partition:

```
00 00 00 42                 length = 66 (batch is 70 bytes)
00 00 00 00 00 00 00 00     baseOffset = 0
00 00 00 00                 partitionLeaderEpoch = 0
01                          magic = 1
xx xx xx xx                 crc32c of bytes 21..69
00 00                       attributes
00 00 00 00                 lastOffsetDelta = 0
00 00 01 92 ...             baseTimestamp
00 00 01 92 ...             maxTimestamp
FF FF FF FF FF FF FF FF     producerId = -1
FF FF                       producerEpoch = -1
FF FF FF FF                 baseSequence = -1
00 00 00 01                 recordCount = 1
-- record --
10                          length = 8 (zig-zag 16)
00                          attributes
00                          timestampDelta = 0
00                          offsetDelta = 0
02 6B                       keyLength = 1, "k"
02 76                       valueLength = 1, "v"
00                          headerCount = 0
```

### 2.4 Validation rules (broker side)

A batch is rejected with `CORRUPT_MESSAGE` if any of these hold: `length < 57`; `length + 4` exceeds the bytes available; `magic != 1`; CRC mismatch; `attributes != 0`; `recordCount < 1`; a record's varint is longer than its maximum; a record's declared length overruns the batch; the records do not consume exactly the batch bytes; `offsetDelta` is not strictly increasing or exceeds `lastOffsetDelta`. A batch larger than `max.batch.bytes` (default 1 MiB) is rejected with `MESSAGE_TOO_LARGE`.

On recovery the same rules decide where the valid log ends (see section 6).

---

## 3. Framing

Every request and response is one frame on a TCP connection.

```
Request:  int32 size | int16 apiKey | int16 apiVersion | int32 correlationId | string clientId | body
Response: int32 size | int32 correlationId | body
```

- `size` counts the bytes after the `size` field.
- The broker rejects `size < 10` or `size > max.request.bytes` (default 16 MiB) **before allocating a buffer** and closes that connection. Other connections are unaffected.
- `apiVersion` is 0 for every API in this spec. Any other value gets an `UNSUPPORTED_VERSION` response body of a single `int16` error code.
- `correlationId` is chosen by the client and echoed in the response.
- **Pipelining:** a client may send many requests without waiting. Responses come back in exactly the order requests were sent on that connection.
- `Produce` with `acks = 0` gets **no response frame at all**.
- If a request body cannot be parsed, the broker closes the connection (it cannot know where the next frame starts safely).

---

## 4. Error codes (`int16`)

| Code | Name | Retriable | Meaning |
|---:|---|:-:|---|
| 0 | `NONE` | | Success. |
| 1 | `OFFSET_OUT_OF_RANGE` | | Fetch offset below log start or above log end. |
| 2 | `CORRUPT_MESSAGE` | | Batch failed validation (2.4). |
| 3 | `UNKNOWN_TOPIC_OR_PARTITION` | yes | Topic or partition does not exist on this broker. |
| 4 | `INVALID_REQUEST` | | Semantically invalid request. |
| 5 | `LEADER_NOT_AVAILABLE` | yes | Partition has no live leader (offline). |
| 6 | `NOT_LEADER_OR_FOLLOWER` | yes | This broker is not the leader; refresh metadata. |
| 7 | `REQUEST_TIMED_OUT` | yes | `acks=all` did not complete within `timeoutMs`. The batch may or may not be committed; idempotent retry is safe. |
| 8 | `MESSAGE_TOO_LARGE` | | Batch exceeds `max.batch.bytes`. |
| 9 | `NOT_ENOUGH_REPLICAS` | yes | ISR smaller than `min.insync.replicas` for `acks=all`. Batch not appended. |
| 10 | `NOT_CONTROLLER` | yes | Controller API sent to a non-controller node. |
| 11 | `TOPIC_ALREADY_EXISTS` | | |
| 12 | `INVALID_REPLICATION_FACTOR` | | More replicas requested than live brokers, or < 1. |
| 13 | `INVALID_PARTITIONS` | | Partition count < 1 or > 1024. |
| 14 | `INVALID_TOPIC` | | Name not matching `[A-Za-z0-9._-]{1,200}` or starting with `__` from a client. |
| 15 | `COORDINATOR_NOT_AVAILABLE` | yes | Offsets partition has no leader or is still loading. |
| 16 | `NOT_COORDINATOR` | yes | Group request sent to the wrong broker; call FindCoordinator again. |
| 17 | `ILLEGAL_GENERATION` | | Member's generation is stale; rejoin. |
| 18 | `UNKNOWN_MEMBER_ID` | | Member was expired or never joined; rejoin with empty id. |
| 19 | `REBALANCE_IN_PROGRESS` | | Rejoin the group. |
| 20 | `INCONSISTENT_ASSIGNOR` | | Member asked for a different assignor than the group uses. |
| 21 | `OUT_OF_ORDER_SEQUENCE` | | Sequence gap for an idempotent producer. |
| 22 | `INVALID_PRODUCER_EPOCH` | | Producer epoch older than the one the broker knows. |
| 23 | `STORAGE_ERROR` | yes | Disk write failed (for example disk full). Nothing was appended. |
| 24 | `UNSUPPORTED_VERSION` | | |
| 25 | `FENCED_LEADER_EPOCH` | yes | Requester's leader epoch is older than the broker's. |
| 26 | `UNKNOWN_LEADER_EPOCH` | yes | Requester's leader epoch is newer than the broker's (broker metadata is behind). |
| 27 | `BROKER_NOT_AVAILABLE` | yes | |
| 28 | `UNKNOWN_SERVER_ERROR` | | Unexpected failure; details are only in the broker log. |

---

## 5. APIs

Notation: fields are listed in wire order. `[ ... ]` is `array`.

| Key | API | Sent to |
|---:|---|---|
| 0 | Produce | partition leader |
| 1 | Fetch | partition leader (consumers and followers) |
| 2 | ListOffsets | partition leader |
| 3 | Metadata | any node |
| 4 | CreateTopics | controller |
| 5 | DeleteTopics | controller |
| 6 | FindCoordinator | any broker |
| 7 | JoinGroup | group coordinator |
| 8 | Heartbeat | group coordinator |
| 9 | LeaveGroup | group coordinator |
| 10 | OffsetCommit | group coordinator |
| 11 | OffsetFetch | group coordinator |
| 12 | InitProducerId | any broker |
| 13 | OffsetForLeaderEpoch | partition leader (followers only) |
| 100 | BrokerHeartbeat | controller (internal) |
| 101 | AlterIsr | controller (internal) |

### 5.0 Produce (0)

Request:
```
acks          int16    0, 1, or -1 (all)
timeoutMs     int32    how long acks=-1 may wait for the ISR
topics [
  name        string
  partitions [
    partition int32
    records   bytes    exactly one record batch (section 2)
  ]
]
```
Response (omitted entirely when `acks = 0`):
```
topics [
  name        string
  partitions [
    partition      int32
    errorCode      int16
    baseOffset     int64   offset assigned to the first record, -1 on error
    logStartOffset int64
  ]
]
```
Semantics:
- `acks=0`: appended if possible, nothing returned. Loss is invisible to the producer.
- `acks=1`: response after the leader's local append (page cache, plus fsync if the policy says so). Lost if the leader dies before a follower copies it.
- `acks=-1`: response after the high watermark passes the batch's last offset, that is after every member of the current ISR has it. Rejected with `NOT_ENOUGH_REPLICAS` before append if `|ISR| < min.insync.replicas`.
- Idempotent batch (`producerId >= 0`): if `(producerId, producerEpoch, baseSequence)` matches one of the last 5 batches appended for that producer on this partition, nothing is appended and the original `baseOffset` is returned with `NONE`. If `baseSequence != lastSequence + 1` (and not a duplicate), `OUT_OF_ORDER_SEQUENCE`.

### 5.1 Fetch (1)

Request:
```
replicaId     int32    -1 for consumers, the follower's broker id for replication
maxWaitMs     int32    long-poll: wait up to this long for minBytes
minBytes      int32
maxBytes      int32    response cap across all partitions (at least one batch is always returned if available)
topics [
  name        string
  partitions [
    partition          int32
    currentLeaderEpoch int32   -1 = do not check
    fetchOffset        int64
    partitionMaxBytes  int32
  ]
]
```
Response:
```
topics [
  name        string
  partitions [
    partition      int32
    errorCode      int16
    highWatermark  int64
    logStartOffset int64
    logEndOffset   int64
    records        bytes   zero or more whole record batches
  ]
]
```
Semantics:
- Records are whole batches. The first batch may start before `fetchOffset`; the client skips records with `offset < fetchOffset`.
- The broker caps the records in one response at 8 MiB regardless of `maxBytes` and `partitionMaxBytes` (a single batch larger than the remaining budget is still returned whole, so progress is always possible).
- Consumers (`replicaId = -1`) never receive a batch whose offsets reach the high watermark or beyond.
- Followers receive data up to the leader's log end. The leader treats `fetchOffset` as the follower's log end offset, which drives the high watermark and the ISR.
- `currentLeaderEpoch` older than the leader's gives `FENCED_LEADER_EPOCH`; newer gives `UNKNOWN_LEADER_EPOCH`.

### 5.2 ListOffsets (2)

Request: `replicaId int32`, `topics [ name string, partitions [ partition int32, timestamp int64 ] ]`. Timestamp `-1` = latest (high watermark for consumers), `-2` = earliest (log start offset).
Response: `topics [ name string, partitions [ partition int32, errorCode int16, offset int64 ] ]`.

### 5.3 Metadata (3)

Request: `topics array<string>` (null = all topics).
Response:
```
clusterId      string
controllerId   int32
brokers [
  nodeId       int32
  host         string
  port         int32
  alive        boolean
]
topics [
  errorCode    int16
  name         string
  partitions [
    errorCode   int16
    partition   int32
    leader      int32   -1 = offline
    leaderEpoch int32
    replicas    array<int32>
    isr         array<int32>
  ]
]
```
The controller's own address appears in `brokers` (with its node id) so admin clients can reach it.

### 5.4 CreateTopics (4)

Request:
```
timeoutMs int32
topics [
  name              string
  numPartitions     int32
  replicationFactor int16
  configs [ key string, value string ]
]
```
Response: `topics [ name string, errorCode int16, errorMessage string(nullable) ]`.

Topic configs: `cleanup.policy` (`delete` | `compact`, default `delete`), `retention.ms` (default 604800000, -1 = unlimited), `retention.bytes` (default -1), `segment.bytes` (default 134217728), `segment.ms` (default 604800000), `index.interval.bytes` (default 4096), `min.insync.replicas` (default 1), `delete.retention.ms` (tombstone lifetime on compacted topics, default 86400000).

### 5.5 DeleteTopics (5)

Request: `timeoutMs int32`, `names array<string>`. Response: `topics [ name string, errorCode int16 ]`.

### 5.6 FindCoordinator (6)

Request: `groupId string`. Response: `errorCode int16, nodeId int32, host string, port int32`.
The coordinator is the leader of partition `abs(murmur2(groupId)) % N` of the internal topic `__consumer_offsets` (N = 3 by default).

### 5.7 JoinGroup (7)

Request:
```
groupId            string
sessionTimeoutMs   int32
rebalanceTimeoutMs int32
memberId           string   "" on first join
assignor           string   "range" or "roundrobin"
topics             array<string>   subscription
```
Response:
```
errorCode    int16
generationId int32
assignor     string
leaderId     string
memberId     string
members      array<string>   every member of this generation
assignment [
  topic      string
  partitions array<int32>
]
```
The coordinator holds the response until every member of the previous generation has rejoined or `rebalanceTimeoutMs` passes (members that did not rejoin are removed). It then computes the assignment server-side and answers all joiners together with the new generation. (Kafka delegates assignment to a client leader through SyncGroup; LogForge computes it on the coordinator, which removes one round trip and one API. See DECISIONS.md.)

### 5.8 Heartbeat (8)

Request: `groupId string, generationId int32, memberId string`. Response: `errorCode int16`. `REBALANCE_IN_PROGRESS` means rejoin now. A member that does not heartbeat within `sessionTimeoutMs` is removed and a rebalance starts.

### 5.9 LeaveGroup (9)

Request: `groupId string, memberId string`. Response: `errorCode int16`. Triggers a rebalance immediately.

### 5.10 OffsetCommit (10)

Request:
```
groupId      string
generationId int32    -1 for a consumer that does not use group membership
memberId     string
topics [
  name       string
  partitions [ partition int32, offset int64, metadata string(nullable) ]
]
```
Response: `topics [ name string, partitions [ partition int32, errorCode int16 ] ]`.
Commits are appended (acks = all) to `__consumer_offsets` as a record with key `group\u0000topic\u0000partition` and an 8-byte value (the offset). A commit from a stale generation gets `ILLEGAL_GENERATION`.

### 5.11 OffsetFetch (11)

Request: `groupId string`, `topics [ name string, partitions array<int32> ]` (null topics = all committed).
Response: `errorCode int16`, `topics [ name string, partitions [ partition int32, offset int64, errorCode int16 ] ]`. `offset = -1` means nothing committed. The top-level `errorCode` is `NOT_COORDINATOR` or `COORDINATOR_NOT_AVAILABLE` when the whole request is misdirected (then `topics` is empty).

### 5.12 InitProducerId (12)

Request: `transactionTimeoutMs int32` (unused, send 0). Response: `errorCode int16, producerId int64, producerEpoch int16`.
Producer ids are allocated as `(brokerId << 40) | counter`, where `counter` is persisted in blocks of 1000 so ids are never reused across restarts.

### 5.13 OffsetForLeaderEpoch (13)

Request: `replicaId int32, topics [ name string, partitions [ partition int32, currentLeaderEpoch int32, leaderEpoch int32 ] ]`.
Response: `topics [ name string, partitions [ partition int32, errorCode int16, leaderEpoch int32, endOffset int64 ] ]`.
The leader returns the largest epoch `E <= leaderEpoch` in its epoch history and the end offset of `E` (the start offset of the next epoch, or its log end if `E` is current). A follower truncates its log to `min(own LEO, endOffset)` before fetching. This is how a returning broker drops a divergent tail it wrote while it was a leader that has since been replaced.

### 5.14 BrokerHeartbeat (100, internal)

Request:
```
brokerId        int32
brokerEpoch     int64    process incarnation (start time in ms)
host            string   advertised host
port            int32
metadataVersion int64    version the broker currently holds, -1 if none
status          bytes    UTF-8 JSON, informational only (partition log end, high watermark, segment layout, group lag); see API.md. `bytes`, not `string`, because it can exceed 32 KiB.
```
Response:
```
errorCode       int16
metadataVersion int64
snapshot        bytes(nullable)   present only if the broker's version is older
```
Snapshot encoding:
```
version         int64
clusterId       string
controllerId    int32
brokers [ nodeId int32, host string, port int32, alive boolean ]
topics [
  name          string
  configs [ key string, value string ]
  partitions [
    partition      int32
    leader         int32
    leaderEpoch    int32
    partitionEpoch int32
    replicas       array<int32>
    isr            array<int32>
  ]
]
```
A broker becomes leader or follower for a partition when a snapshot shows a higher `leaderEpoch` for it.

### 5.15 AlterIsr (101, internal)

Request: `brokerId int32, brokerEpoch int64, topics [ name string, partitions [ partition int32, leaderEpoch int32, partitionEpoch int32, newIsr array<int32> ] ]`.
Response: `topics [ name string, partitions [ partition int32, errorCode int16, leaderEpoch int32, partitionEpoch int32, isr array<int32> ] ]`.
Accepted only from the current leader at the current `leaderEpoch` and `partitionEpoch`; `newIsr` must include the leader and only replicas; added members must be alive. On success `partitionEpoch` increments.

---

## 5.16 Partitioning (client side)

- Non-null key: `partition = (murmur2(keyBytes) & 0x7fffffff) % numPartitions`, where `murmur2` is the 32-bit MurmurHash2 with seed `0x9747b28c` (the same function Kafka's default partitioner uses), so a given key always maps to the same partition while the partition count is unchanged.
- Null key (documented default): round-robin. The producer starts at a random partition and advances by one for every null-key record, which spreads load evenly and makes no ordering promise.
- The same `murmur2` picks the consumer-offsets partition for a group id (5.6).

---

## 6. On-disk layout

```
<data dir>/
  meta.properties                         node.id, cluster.id
  producer-ids                            next unallocated producer id block
  controller/metadata.json                controller only
  <topic>-<partition>/
    00000000000000000000.log              record batches back to back
    00000000000000000000.index            sparse offset index
    00000000000000004211.log              next segment, base offset 4211
    00000000000000004211.index
```

- File names are the segment's base offset, zero-padded to 20 digits.
- `.log` is a concatenation of record batches (section 2). Nothing else.
- `.index` is a sequence of 8-byte entries: `relativeOffset int32` (batch base offset minus segment base offset), `position int32` (byte position of that batch in the `.log`). An entry is added before a batch is appended once at least `index.interval.bytes` (default 4096) have been written since the last entry. Entries are strictly increasing in both fields. The file is memory-mapped and pre-sized; trailing zero entries are not entries.
- Lookup of offset `t`: pick the segment with the greatest base offset `<= t`; binary-search its index for the greatest `relativeOffset <= t - base`; scan batch headers forward from that position to the first batch whose last offset `>= t`.
- Compaction writes `<base>.log.cleaned`, then renames it over `<base>.log` (atomic on the same filesystem) and rebuilds the index.

### Recovery
On startup each partition: for the active (last) segment, read batches from position 0 and validate each against section 2.4; truncate the file at the first invalid or incomplete batch; rebuild the index from the valid batches. For older segments, the index is rebuilt if it is missing, unreadable, or its last entry points beyond the file. The leader epoch history and the idempotent-producer state are rebuilt by scanning batch headers.
