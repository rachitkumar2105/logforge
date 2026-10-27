# Twenty hard questions about LogForge

Answers point at the code and at measured evidence (`RUN-REPORT.md`, `bench/results`, `chaos/results`) wherever possible.

---

### 1. Why is a sequential append plus the OS page cache faster than a custom cache in the JVM?

An append is one positional write at the end of a file (`LogSegment.append`). The kernel copies it into the page cache and returns; the disk later sees large sequential writes, which disks of every kind do at full bandwidth, with no seeks and no read-modify-write. Reads of recent data are served from the same page cache, so a consumer that keeps up never touches the disk.

A JVM cache would hold the same bytes twice (page cache and heap), put gigabytes on the garbage-collected heap, go cold on every broker restart while the page cache stays warm, and need its own invalidation on truncation and compaction. The kernel already does LRU-ish eviction across every partition at zero GC cost. The reasoning is written down in `LogSegment`'s class comment, which is why there is no read cache.

### 2. Why do followers pull instead of the leader pushing?

The follower's fetch offset *is* the acknowledgement: asking for "everything from offset N" tells the leader the follower has everything below N (`Replica.onFetch`). That single number drives the high watermark and the ISR with no separate ack protocol. A slow follower just asks less often and falls behind; the leader never buffers per-follower data, never blocks on a slow peer, and a follower that was away resumes from wherever its log ends. Pull also batches naturally: one fetch carries every partition the follower needs from that leader (`ReplicaFetcher`). With push the leader would need per-follower queues, retries and flow control, and one slow follower would back-pressure the leader.

### 3. What does the high watermark protect against?

Consumers reading something that a failover then erases. The HW is the smallest log end offset over the in-sync replicas (`IsrManager.maybeAdvanceHighWatermark`), so every record below it is on every ISR member. Consumers only read below it (`Partition.read` caps at HW for `replicaId = -1`) and `acks=all` completes only once it passes the batch. Whichever ISR member becomes leader next therefore has every record a consumer saw and every record a producer was told was safe. Without it, a consumer could act on a record that "never happened" after the leader died. `ReplicationTest.consumersOnlySeeRecordsBelowTheHighWatermark` shows a fetch returning only the replicated record while an unreplicated one sits above the HW.

### 4. Exactly what is lost at acks=1 when the leader dies?

Every batch the leader appended and acknowledged that no follower had fetched yet. The leader answers `acks=1` right after its own append (`ReplicaManager.appendRecords`), before any follower pulls. If it dies, the new leader is elected from the ISR and never had those offsets; the old leader, when it returns, asks where its epoch ended (OffsetForLeaderEpoch) and truncates them (`ReplicaFetcher.truncate`). `ReplicationTest.acksOneLosesRecords...` pauses replication, acknowledges three records at `acks=1`, kills the leader and measures: 3 of 3 lost, and the returning broker truncates to match the new leader.

### 5. Why is unclean leader election dangerous?

It elects a replica that is not in the ISR, which by definition lacks some committed records. Those records are lost permanently, offsets are reused for different data, and consumers that already read the old records now see a log that disagrees with what they consumed. LogForge keeps a partition offline instead (leader -1, last ISR kept so the next returning ISR member is elected cleanly) unless `LOGFORGE_UNCLEAN_ELECTION=true` (`Controller.elect`). `ReplicationTest.uncleanElectionWhenEnabledPicksAnOutOfSyncReplica` demonstrates the loss.

### 6. How does the sparse index trade memory for scan cost?

One 8-byte entry per 4 KB of log (`index.interval.bytes`), not per record: a 128 MB segment needs about 32k entries (256 KB) instead of millions, so every segment's index stays mapped in memory. A lookup binary-searches the index for the greatest entry at or below the target (`OffsetIndex.lookup`), then reads batch headers forward (`LogSegment.translateOffset`), at most about one interval of data. Doubling the interval halves index memory and doubles the worst-case scan. The site's segment explorer runs this exact lookup over a real partition.

### 7. How does idempotent production actually deduplicate?

The producer gets a producer id (InitProducerId) and numbers its records per partition (0, 1, 2, ...); each batch carries its first sequence. It keeps one batch in flight per partition, and a retry is the exact same bytes with the same sequence. The leader keeps, per producer per partition, the sequence ranges of its last five batches (`ProducerStateManager`); a batch matching one of them is answered with the original offsets and nothing is appended. A gap in sequence is rejected (OUT_OF_ORDER_SEQUENCE). Followers rebuild the same state from the batches they replicate, so dedupe survives failover (`IdempotentFailoverTest`), and the state is rebuilt from batch headers on restart. The gate test drops the first produce response through a proxy: with idempotence one copy is stored, without it two (`ProducerRetryDedupTest`).

### 8. What is a rebalance storm, and how do you reduce it?

Every membership change stops the whole group for one round: members stop fetching, rejoin, and wait at the barrier. If rejoining itself takes longer than a session timeout (slow processing between polls, GC pauses, a long rebalance), members are expired, which triggers another rebalance, and so on. Mitigations: session timeouts well above heartbeat intervals and heartbeating from a background thread (`GroupMembership`), one barrier per round so late joiners wait for the next one instead of restarting the current one (`GroupCoordinator.join`), never expiring a member that is blocked in JoinGroup, and committing before rejoining so a rebalance costs little rework. Kafka goes further with static membership and incremental cooperative rebalancing, which LogForge does not have.

### 9. Why does compaction need tombstones?

Compaction keeps only the newest record per key. Deleting a key by just removing its records would let an older value of that key resurface from an older segment, and a consumer rebuilding state from the beginning would never learn the key was deleted. A tombstone (null value) is the newest record for the key, so it shadows all older values; it is kept for `delete.retention.ms` so consumers that are behind still see the delete, then removed (`Compactor`, `CompactorTest`).

### 10. What would break first at 100 brokers?

The single controller and the metadata path. Every broker heartbeats it and pulls a full metadata snapshot on any change; with thousands of partitions each change ships megabytes to 100 brokers, and leader elections are computed on one thread under one lock. Broker status JSON (with segment layouts) would also flood the controller. Next would be per-broker fetcher fan-out (one fetcher thread per leader, so up to 99 threads and connections per broker) and the admin API building the whole cluster view per request. Fixes: delta metadata (a replicated metadata log, as KRaft does), fetcher pools with incremental fetch sessions, and sampled status.

### 11. Why do the offset and leader epoch sit before the CRC in the batch header?

The leader assigns both on append. With them outside the checksummed region, the producer's CRC stays valid from producer to disk to follower to consumer, and the broker never recomputes it on the hot path; corruption anywhere in the records is still caught (`RecordBatch`, PROTOCOL.md section 2).

### 12. How does recovery decide where the log really ends after a crash?

Only the active segment can have a torn tail. On startup it is read batch by batch, each validated (length, magic, CRC, record framing, monotonic offsets), and the file is truncated at the first invalid or incomplete batch; the index is rebuilt from the valid batches (`LogSegment.recover`). Sealed segments get their index rebuilt only if it is missing, inconsistent or short. On Linux, 100 real SIGKILLs produced 11 torn tails; all were truncated and no acknowledged record was lost (`RUN-REPORT.md`, Phase 4).

### 13. Why does a SIGKILL not lose acknowledged data even with fsync off?

The write reached the kernel's page cache before the ack; the page cache belongs to the kernel, not the process, so the process dying does not discard it. Only power loss or a kernel crash can, which is what the fsync policy is for (`FsyncPolicy`).

### 14. What exactly does `acks=all` wait for, and what if the ISR shrinks?

For the high watermark to pass the batch's last offset, i.e. every member of the *current* ISR has it (`DelayedProduce`). If a follower falls behind for `replica.lag.time.max.ms` it is removed (`IsrManager.maybeShrink`) and the HW advances on the rest, so producers do not stall forever; `min.insync.replicas` stops the ISR from shrinking to a single copy silently (NOT_ENOUGH_REPLICAS). The SIGSTOP test shows writes resuming once the paused follower leaves the ISR.

### 15. Why must a shrink go through the controller before the leader uses it?

The controller elects the next leader from *its* copy of the ISR. If the leader dropped a follower locally and advanced the HW, then died, the controller could elect that follower, which lacks committed records. So shrinks apply only after AlterIsr succeeds, while expansions count in the HW immediately (that can only make the HW more conservative) (`IsrManager`).

### 16. How does a returning broker find its divergent tail?

It asks the leader: "where does my latest epoch end in your log?" (OffsetForLeaderEpoch). The leader answers with the start of its next epoch. The follower truncates to the minimum of that, its own end for the returned epoch, and its log end, then fetches (`ReplicaFetcher.truncate`). Truncating to the old high watermark instead can lose committed data or keep a tail the new leader never had.

### 17. Why did ISR expansion need the leader-epoch start offset?

A new leader's HW can lag what the old leader committed. A follower that is only "caught up to the HW" might still miss committed records and, once in the ISR, be elected. Requiring the fetch offset to reach the start of the leader's current epoch closes that hole (found in code review, fixed, DECISIONS.md B20).

### 18. How is backpressure applied in the network layer?

Each connection keeps a FIFO of responses in request order. When it holds 16 pending responses (because the client is not reading), the IO thread stops reading that socket, so the client's further requests stay in its own TCP buffers and broker memory for it is bounded; other connections are unaffected because sockets are non-blocking (`NioServer`). `NioServerTest.aSlowConsumerDoesNotStallOtherConnections` shows a fast client unaffected by one that never reads.

### 19. How are hostile frames handled?

The 4-byte size is checked before any allocation (a 2 GiB claim costs nothing); array counts are bounded by the bytes left; any parse error closes only that connection; fetch responses are capped at 8 MiB regardless of what the client asks (`ProtocolReader`, `NioServer`, `SecurityLimitsTest`). The cluster's own operations (topic administration, broker registration, ISR changes, follower fetches) need the cluster token, presented once per connection with Authenticate and compared in constant time; three wrong tokens close the connection, and accept caps connections per address and in total (`ClusterAuthTest`, `ConnectionLimitsTest`, SECURITY-AUDIT F1 and F2). The token is not encryption, so port 9092 still stays on a private network.

### 20. Why is LogForge slower or faster than Kafka in the benchmark, and how do you know the comparison is fair?

Setup (DECISIONS.md W1): both clusters ran three brokers in Docker on the same laptop (i7-1255U, WSL 2), data on named volumes, 1 GiB heaps, driven by the same harness through their own Java clients with 64 KB batches, 5 ms linger, an 8 MiB client buffer, no compression and keyed records (1,024 keys, the same murmur2 hash in both clients, so every record lands on the same partition in both systems). Two earlier runs were discarded: one wrote Kafka's data to the container overlay filesystem and gave LogForge an unbounded heap, the other sent keyless records, which Kafka's sticky partitioner and LogForge's round-robin spread differently.

Measured (`bench/results`, 2026-10-26, 20 s windows): at acks=all Kafka did 68.7 MB/s against LogForge's 42.5 (1.6x); with one partition 80.7 against 45.7 (1.8x); with 10 KB records 77.7 against 52.0 (1.5x). Acknowledged records reached a consumer after a median 0.3 ms on Kafka and 10.0 ms on LogForge. With 100 B records LogForge was ahead (28.8 against 24.9 MB/s), and at acks=1 they were level.

Why, from the code (not profiled): LogForge's producer keeps one request in flight per partition while Kafka pipelines up to five per connection, which matters most with one partition and with acks=all, where each request waits for replication; followers and consumers get bytes copied through the JVM heap instead of sendfile; and LogForge's consumer waits for every leader's long poll to return before handing records over, which explains most of the end-to-end gap.

How I know the noise level: the 1 KB baseline and the fsync never run are the same configuration measured minutes apart. LogForge gave 49.7 and 77.0 MB/s, Kafka 40.9 and 77.0. Anything inside that spread, including LogForge being "ahead" at 100 B, says nothing about the brokers; the 1.5x to 1.8x gaps above are consistent in direction but would need repeated runs to put error bars on. The site shows every number with its configuration and hardware.
