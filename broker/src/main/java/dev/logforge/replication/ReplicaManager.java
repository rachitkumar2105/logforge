package dev.logforge.replication;

import dev.logforge.TopicPartition;
import dev.logforge.log.CorruptRecordException;
import dev.logforge.log.EpochEndOffset;
import dev.logforge.log.InvalidProducerEpochException;
import dev.logforge.log.Log;
import dev.logforge.log.LogAppendInfo;
import dev.logforge.log.LogConfig;
import dev.logforge.log.LogManager;
import dev.logforge.log.LogSegmentInfo;
import dev.logforge.log.LogStorageException;
import dev.logforge.log.OutOfOrderSequenceException;
import dev.logforge.log.RecordBatch;
import dev.logforge.log.RecordTooLargeException;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.ListOffsets;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.Replication;
import dev.logforge.util.JsonLog;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * All partition replicas on this broker. Applies leadership from controller metadata, serves
 * produce and fetch, and parks requests that must wait (acks=all, long-poll fetch).
 */
public final class ReplicaManager implements Closeable {
  private static final JsonLog LOG = JsonLog.get(ReplicaManager.class);

  private final int brokerId;
  private final LogManager logManager;
  private final LogConfig defaultConfig;
  private final DelayedOperations purgatory;
  private final IsrManager.AlterIsrSender alterIsr;
  private final LongSupplier clock;
  private final Map<TopicPartition, Partition> partitions = new ConcurrentHashMap<>();
  private volatile FetcherControl fetchers = FetcherControl.NONE;
  private volatile long appliedMetadataVersion = -1;
  private final Set<TopicPartition> strayReported = ConcurrentHashMap.newKeySet();

  /** Upper bound on how long a client may park a request (acks=all timeout, fetch maxWaitMs). */
  static final long MAX_PARK_MS = 30_000;

  /**
   * Upper bound on the record bytes one fetch response carries, whatever the client asks for. A
   * read allocates a buffer this large at most, so a request with maxBytes = 2^31 cannot make the
   * broker allocate a whole segment (SECURITY.md S4).
   */
  static final int MAX_FETCH_BYTES = 8 * 1024 * 1024;

  private final LongAdder messagesIn = new LongAdder();
  private final LongAdder bytesIn = new LongAdder();
  private final LongAdder bytesOut = new LongAdder();

  /** Hook for metrics: produce and fetch latencies. */
  public interface LatencyObserver {
    void produce(int acks, long nanos);

    void fetch(boolean follower, long nanos);

    /** Records appended as leader (not duplicates). */
    default void append(String topic, int records, int bytes) {}

    /** Bytes returned by fetches. */
    default void bytesOut(long bytes) {}

    LatencyObserver NONE =
        new LatencyObserver() {
          @Override
          public void produce(int acks, long nanos) {}

          @Override
          public void fetch(boolean follower, long nanos) {}
        };
  }

  private volatile LatencyObserver latency = LatencyObserver.NONE;

  public ReplicaManager(
      int brokerId,
      LogManager logManager,
      LogConfig defaultConfig,
      DelayedOperations purgatory,
      IsrManager.AlterIsrSender alterIsr,
      LongSupplier clock) {
    this.brokerId = brokerId;
    this.logManager = logManager;
    this.defaultConfig = defaultConfig;
    this.purgatory = purgatory;
    this.alterIsr = alterIsr;
    this.clock = clock;
  }

  public void setFetcherControl(FetcherControl control) {
    this.fetchers = control;
  }

  public void setLatencyObserver(LatencyObserver observer) {
    this.latency = observer;
  }

  public int brokerId() {
    return brokerId;
  }

  public Partition partition(TopicPartition tp) {
    return partitions.get(tp);
  }

  public Map<TopicPartition, Partition> partitions() {
    return Map.copyOf(partitions);
  }

  public long appliedMetadataVersion() {
    return appliedMetadataVersion;
  }

  public long messagesIn() {
    return messagesIn.sum();
  }

  public long bytesIn() {
    return bytesIn.sum();
  }

  public long bytesOut() {
    return bytesOut.sum();
  }

  // ------------------------------------------------------------------ metadata

  /**
   * Makes local state match the controller's snapshot: become leader or follower where the leader
   * epoch moved, adopt ISR changes, and delete replicas that are no longer assigned here.
   */
  public synchronized void applyMetadata(ControllerApis.Snapshot snapshot) {
    if (snapshot.version() <= appliedMetadataVersion) {
      return;
    }
    Set<TopicPartition> assignedHere = new HashSet<>();
    for (ControllerApis.TopicInfo topic : snapshot.topics()) {
      LogConfig config;
      try {
        config = defaultConfig.withTopicConfigs(topic.configs());
      } catch (IllegalArgumentException e) {
        LOG.error("bad topic config, using defaults", e, "topic", topic.name());
        config = defaultConfig;
      }
      for (ControllerApis.PartitionInfo p : topic.partitions()) {
        if (!p.replicas().contains(brokerId)) {
          continue;
        }
        TopicPartition tp = new TopicPartition(topic.name(), p.partition());
        assignedHere.add(tp);
        try {
          applyPartition(tp, config, p);
        } catch (IOException | RuntimeException e) {
          LOG.error("failed to apply partition state", e, "partition", tp.toString());
        }
      }
    }
    for (TopicPartition tp : List.copyOf(partitions.keySet())) {
      if (!assignedHere.contains(tp)) {
        removePartition(tp);
      }
    }
    // Directories on disk that no snapshot has assigned here are left alone and only reported.
    // Deleting them would make data loss a function of controller state: a controller restarted
    // with an empty or lost metadata file would otherwise tell every broker to wipe its disk.
    // Only partitions this broker was actually serving are deleted (above), when a topic it knew
    // is removed.
    try {
      for (TopicPartition tp : logManager.partitionsOnDisk()) {
        if (!assignedHere.contains(tp) && !strayReported.contains(tp)) {
          strayReported.add(tp);
          LOG.warn(
              "partition directory not assigned to this broker; kept", "partition", tp.toString());
        }
      }
    } catch (IOException e) {
      LOG.error("could not scan data dir", e);
    }
    appliedMetadataVersion = snapshot.version();
  }

  private void applyPartition(TopicPartition tp, LogConfig config, ControllerApis.PartitionInfo p)
      throws IOException {
    Partition partition = partitions.computeIfAbsent(tp, this::newPartition);
    boolean epochMoved = p.leaderEpoch() > partition.leaderEpoch() || partition.log() == null;
    if (epochMoved) {
      Log log = logManager.getOrCreate(tp, config);
      if (p.leader() == brokerId) {
        fetchers.unfollow(tp);
        partition.makeLeader(
            log, config, p.leaderEpoch(), p.partitionEpoch(), p.replicas(), p.isr());
        LOG.info(
            "became leader", "partition", tp.toString(), "epoch", p.leaderEpoch(), "isr", p.isr());
      } else {
        partition.makeFollower(
            log, config, p.leader(), p.leaderEpoch(), p.partitionEpoch(), p.replicas());
        if (p.leader() >= 0) {
          fetchers.follow(tp, p.leader(), p.leaderEpoch());
        } else {
          fetchers.unfollow(tp);
        }
        LOG.info(
            "became follower",
            "partition",
            tp.toString(),
            "leader",
            p.leader(),
            "epoch",
            p.leaderEpoch());
      }
      purgatory.checkAndComplete(tp);
    } else if (p.partitionEpoch() > partition.partitionEpoch()) {
      partition.updateIsrFromMetadata(p.partitionEpoch(), p.isr());
    }
  }

  private Partition newPartition(TopicPartition tp) {
    return new Partition(tp, brokerId, clock, alterIsr, () -> purgatory.checkAndComplete(tp));
  }

  private void removePartition(TopicPartition tp) {
    Partition p = partitions.remove(tp);
    fetchers.unfollow(tp);
    purgatory.checkAndComplete(tp);
    try {
      logManager.delete(tp);
      LOG.info(
          "removed partition", "partition", tp.toString(), "hadLog", p != null && p.log() != null);
    } catch (IOException e) {
      LOG.error("failed to delete partition", e, "partition", tp.toString());
    }
  }

  // ------------------------------------------------------------------ produce

  /**
   * Appends one batch per partition. Completes with {@code null} for acks=0, immediately for
   * acks=1, and once the high watermark covers every batch (or on timeout) for acks=all.
   */
  public CompletableFuture<Produce.Response> appendRecords(Produce.Request request) {
    long start = System.nanoTime();
    short acks = request.acks();
    Map<TopicPartition, Produce.PartitionResult> results = new LinkedHashMap<>();
    Map<TopicPartition, Long> waitingFor = new LinkedHashMap<>();
    for (Produce.TopicData t : request.topics()) {
      for (Produce.PartitionData pd : t.partitions()) {
        TopicPartition tp = new TopicPartition(t.name(), pd.partition());
        Produce.PartitionResult r = appendOne(tp, pd.records(), acks);
        results.put(tp, r);
        if (r.errorCode() == Errors.NONE.code() && acks == -1) {
          waitingFor.put(tp, r.baseOffset() >= 0 ? lastOffsetOf(pd.records()) + 1 : 0);
        }
      }
    }
    if (acks == 0) {
      latency.produce(0, System.nanoTime() - start);
      return CompletableFuture.completedFuture(null);
    }
    if (acks != -1 || waitingFor.isEmpty()) {
      latency.produce(acks, System.nanoTime() - start);
      return CompletableFuture.completedFuture(toResponse(results));
    }
    CompletableFuture<Produce.Response> future = new CompletableFuture<>();
    DelayedProduce op = new DelayedProduce(results, waitingFor, future, start);
    purgatory.watch(
        op, waitingFor.keySet(), Math.min(Math.max(0, request.timeoutMs()), MAX_PARK_MS));
    return future;
  }

  private static long lastOffsetOf(ByteBuffer stampedBatch) {
    // The leader stamped the batch in place, so its header now carries the assigned offsets.
    return RecordBatch.wrap(stampedBatch).lastOffset();
  }

  private Produce.PartitionResult appendOne(TopicPartition tp, ByteBuffer records, short acks) {
    Partition partition = partitions.get(tp);
    if (partition == null) {
      return failed(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }
    if (records == null || RecordBatch.sizeIfComplete(records) != records.remaining()) {
      return failed(tp, Errors.CORRUPT_MESSAGE); // exactly one complete batch per partition
    }
    try {
      RecordBatch batch = RecordBatch.wrap(records);
      LogAppendInfo info = partition.appendAsLeader(batch, acks);
      if (!info.duplicate()) {
        messagesIn.add(info.records());
        bytesIn.add(info.bytes());
        latency.append(tp.topic(), info.records(), info.bytes());
        purgatory.checkAndComplete(tp); // followers and consumers parked on this partition
      } else {
        // Make the waiting logic see the original offsets for an idempotent retry.
        batch.setBaseOffset(info.firstOffset());
      }
      return new Produce.PartitionResult(
          tp.partition(), Errors.NONE.code(), info.firstOffset(), partition.log().logStartOffset());
    } catch (Partition.NotLeaderException e) {
      return failed(tp, Errors.NOT_LEADER_OR_FOLLOWER);
    } catch (Partition.NotEnoughReplicasException e) {
      return failed(tp, Errors.NOT_ENOUGH_REPLICAS);
    } catch (CorruptRecordException e) {
      return failed(tp, Errors.CORRUPT_MESSAGE);
    } catch (RecordTooLargeException e) {
      return failed(tp, Errors.MESSAGE_TOO_LARGE);
    } catch (OutOfOrderSequenceException e) {
      return failed(tp, Errors.OUT_OF_ORDER_SEQUENCE);
    } catch (InvalidProducerEpochException e) {
      return failed(tp, Errors.INVALID_PRODUCER_EPOCH);
    } catch (LogStorageException e) {
      LOG.error("append failed", e, "partition", tp.toString());
      return failed(tp, Errors.STORAGE_ERROR);
    }
  }

  private static Produce.PartitionResult failed(TopicPartition tp, Errors e) {
    return new Produce.PartitionResult(tp.partition(), e.code(), -1L, -1L);
  }

  private static Produce.Response toResponse(Map<TopicPartition, Produce.PartitionResult> results) {
    Map<String, List<Produce.PartitionResult>> byTopic = new LinkedHashMap<>();
    results.forEach((tp, r) -> byTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(r));
    List<Produce.TopicResult> topics = new ArrayList<>();
    byTopic.forEach((t, rs) -> topics.add(new Produce.TopicResult(t, rs)));
    return new Produce.Response(topics);
  }

  /** acks=all: waits until the high watermark passes every appended batch. */
  private final class DelayedProduce extends DelayedOperations.Operation {
    private final Map<TopicPartition, Produce.PartitionResult> results;
    private final Map<TopicPartition, Long> requiredOffsets;
    private final Map<TopicPartition, Integer> epochs = new ConcurrentHashMap<>();
    private final CompletableFuture<Produce.Response> future;
    private final long startNanos;

    DelayedProduce(
        Map<TopicPartition, Produce.PartitionResult> results,
        Map<TopicPartition, Long> requiredOffsets,
        CompletableFuture<Produce.Response> future,
        long startNanos) {
      this.results = results;
      this.requiredOffsets = requiredOffsets;
      this.future = future;
      this.startNanos = startNanos;
      for (TopicPartition tp : requiredOffsets.keySet()) {
        Partition p = partitions.get(tp);
        epochs.put(tp, p == null ? -1 : p.leaderEpoch());
      }
    }

    private Errors status(TopicPartition tp) {
      Partition p = partitions.get(tp);
      if (p == null || !p.isLeader() || p.leaderEpoch() != epochs.get(tp)) {
        // Leadership moved: we cannot know whether the new leader has the batch. The producer
        // retries; with idempotence the retry is deduplicated.
        return Errors.NOT_LEADER_OR_FOLLOWER;
      }
      return p.highWatermark() >= requiredOffsets.get(tp) ? Errors.NONE : null;
    }

    @Override
    protected boolean canComplete() {
      for (TopicPartition tp : requiredOffsets.keySet()) {
        if (status(tp) == null) {
          return false;
        }
      }
      return true;
    }

    @Override
    protected void onComplete(boolean expired) {
      Map<TopicPartition, Produce.PartitionResult> out = new LinkedHashMap<>(results);
      for (TopicPartition tp : requiredOffsets.keySet()) {
        Errors s = status(tp);
        if (s == null) {
          s = Errors.REQUEST_TIMED_OUT;
        }
        if (s != Errors.NONE) {
          Produce.PartitionResult r = results.get(tp);
          out.put(
              tp,
              new Produce.PartitionResult(
                  r.partition(), s.code(), r.baseOffset(), r.logStartOffset()));
        }
      }
      latency.produce(-1, System.nanoTime() - startNanos);
      future.complete(toResponse(out));
    }
  }

  // ------------------------------------------------------------------ fetch

  /** Serves a fetch now, or parks it until data arrives or {@code maxWaitMs} passes. */
  public CompletableFuture<Fetch.Response> fetch(Fetch.Request request) {
    long start = System.nanoTime();
    Fetch.Response now = readAll(request);
    boolean anyError = false;
    for (Fetch.TopicResult t : now.topics()) {
      for (Fetch.PartitionResult p : t.partitions()) {
        anyError |= p.errorCode() != Errors.NONE.code();
      }
    }
    if (request.maxWaitMs() <= 0
        || anyError
        || now.sizeInBytes() >= Math.max(1, request.minBytes())) {
      return CompletableFuture.completedFuture(observe(now, request, start));
    }
    CompletableFuture<Fetch.Response> future = new CompletableFuture<>();
    List<TopicPartition> keys = new ArrayList<>();
    for (Fetch.TopicData t : request.topics()) {
      for (Fetch.PartitionData p : t.partitions()) {
        keys.add(new TopicPartition(t.name(), p.partition()));
      }
    }
    purgatory.watch(
        new DelayedOperations.Operation() {
          @Override
          protected boolean canComplete() {
            for (Fetch.TopicData t : request.topics()) {
              for (Fetch.PartitionData p : t.partitions()) {
                Partition part = partitions.get(new TopicPartition(t.name(), p.partition()));
                if (part == null || part.hasDataBeyond(request.replicaId(), p.fetchOffset())) {
                  return true;
                }
              }
            }
            return false;
          }

          @Override
          protected void onComplete(boolean expired) {
            future.complete(observe(readAll(request), request, start));
          }
        },
        keys,
        Math.min(request.maxWaitMs(), MAX_PARK_MS));
    return future;
  }

  private Fetch.Response observe(Fetch.Response r, Fetch.Request req, long startNanos) {
    bytesOut.add(r.sizeInBytes());
    latency.bytesOut(r.sizeInBytes());
    latency.fetch(req.isFromFollower(), System.nanoTime() - startNanos);
    return r;
  }

  private Fetch.Response readAll(Fetch.Request request) {
    List<Fetch.TopicResult> topics = new ArrayList<>();
    int budget = Math.max(1, Math.min(request.maxBytes(), MAX_FETCH_BYTES));
    boolean anyData = false;
    for (Fetch.TopicData t : request.topics()) {
      List<Fetch.PartitionResult> parts = new ArrayList<>();
      for (Fetch.PartitionData pd : t.partitions()) {
        TopicPartition tp = new TopicPartition(t.name(), pd.partition());
        Partition p = partitions.get(tp);
        if (p == null || p.log() == null) {
          parts.add(
              new Fetch.PartitionResult(
                  pd.partition(),
                  Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                  -1,
                  -1,
                  -1,
                  ByteBuffer.allocate(0)));
          continue;
        }
        if (anyData && budget <= 0) {
          // Budget spent: answer the rest empty instead of one more batch each.
          parts.add(
              new Fetch.PartitionResult(
                  pd.partition(),
                  Errors.NONE.code(),
                  p.highWatermark(),
                  p.log().logStartOffset(),
                  p.logEndOffset(),
                  ByteBuffer.allocate(0)));
          continue;
        }
        int max = Math.max(1, Math.min(Math.min(pd.partitionMaxBytes(), MAX_FETCH_BYTES), budget));
        Partition.ReadResult rr =
            p.read(request.replicaId(), pd.currentLeaderEpoch(), pd.fetchOffset(), max);
        budget -= rr.records().remaining();
        anyData |= rr.records().hasRemaining();
        parts.add(
            new Fetch.PartitionResult(
                pd.partition(),
                rr.error().code(),
                rr.highWatermark(),
                rr.logStartOffset(),
                rr.logEndOffset(),
                rr.records()));
      }
      topics.add(new Fetch.TopicResult(t.name(), parts));
    }
    return new Fetch.Response(topics);
  }

  // ------------------------------------------------------------------ offsets and epochs

  public ListOffsets.Response listOffsets(ListOffsets.Request request) {
    List<ListOffsets.TopicResult> topics = new ArrayList<>();
    for (ListOffsets.TopicData t : request.topics()) {
      List<ListOffsets.PartitionResult> parts = new ArrayList<>();
      for (ListOffsets.PartitionData pd : t.partitions()) {
        Partition p = partitions.get(new TopicPartition(t.name(), pd.partition()));
        if (p == null || p.log() == null) {
          parts.add(
              new ListOffsets.PartitionResult(
                  pd.partition(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), -1));
        } else if (!p.isLeader()) {
          parts.add(
              new ListOffsets.PartitionResult(
                  pd.partition(), Errors.NOT_LEADER_OR_FOLLOWER.code(), -1));
        } else {
          long offset =
              pd.timestamp() == ListOffsets.EARLIEST ? p.log().logStartOffset() : p.highWatermark();
          parts.add(new ListOffsets.PartitionResult(pd.partition(), Errors.NONE.code(), offset));
        }
      }
      topics.add(new ListOffsets.TopicResult(t.name(), parts));
    }
    return new ListOffsets.Response(topics);
  }

  public Replication.OffsetForLeaderEpochResponse offsetForLeaderEpoch(
      Replication.OffsetForLeaderEpochRequest request) {
    List<Replication.EpochTopicResult> topics = new ArrayList<>();
    for (Replication.EpochTopic t : request.topics()) {
      List<Replication.EpochEnd> parts = new ArrayList<>();
      for (Replication.EpochPartition pd : t.partitions()) {
        Partition p = partitions.get(new TopicPartition(t.name(), pd.partition()));
        if (p == null || p.log() == null) {
          parts.add(
              new Replication.EpochEnd(
                  pd.partition(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), -1, -1));
        } else if (!p.isLeader()) {
          parts.add(
              new Replication.EpochEnd(
                  pd.partition(), Errors.NOT_LEADER_OR_FOLLOWER.code(), -1, -1));
        } else if (pd.currentLeaderEpoch() >= 0 && pd.currentLeaderEpoch() < p.leaderEpoch()) {
          parts.add(
              new Replication.EpochEnd(pd.partition(), Errors.FENCED_LEADER_EPOCH.code(), -1, -1));
        } else if (pd.currentLeaderEpoch() > p.leaderEpoch()) {
          parts.add(
              new Replication.EpochEnd(pd.partition(), Errors.UNKNOWN_LEADER_EPOCH.code(), -1, -1));
        } else {
          EpochEndOffset e = p.endOffsetForEpoch(pd.leaderEpoch());
          parts.add(
              new Replication.EpochEnd(
                  pd.partition(), Errors.NONE.code(), e.epoch(), e.endOffset()));
        }
      }
      topics.add(new Replication.EpochTopicResult(t.name(), parts));
    }
    return new Replication.OffsetForLeaderEpochResponse(topics);
  }

  // ------------------------------------------------------------------ periodic work

  /** Proposes ISR shrinks for followers that stopped keeping up. */
  public void isrShrinkTick(long lagMaxMs) {
    for (Partition p : partitions.values()) {
      try {
        p.maybeShrinkIsr(lagMaxMs);
      } catch (RuntimeException e) {
        LOG.error("ISR check failed", e, "partition", p.topicPartition().toString());
      }
    }
  }

  /** High watermark used to bound retention; Long.MIN_VALUE-safe for unknown partitions. */
  public long retentionBound(TopicPartition tp) {
    Partition p = partitions.get(tp);
    return p == null ? 0L : p.highWatermark();
  }

  /** Per-partition status for the controller heartbeat and the admin API. */
  public List<Map<String, Object>> status() {
    List<Map<String, Object>> out = new ArrayList<>();
    for (Partition p : partitions.values()) {
      Log log = p.log();
      if (log == null) {
        continue;
      }
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("topic", p.topicPartition().topic());
      m.put("partition", p.topicPartition().partition());
      m.put("leader", p.isLeader());
      m.put("leaderEpoch", p.leaderEpoch());
      m.put("logStartOffset", log.logStartOffset());
      m.put("logEndOffset", log.logEndOffset());
      m.put("highWatermark", p.highWatermark());
      if (p.isLeader()) {
        List<Map<String, Object>> segs = new ArrayList<>();
        List<LogSegmentInfo> infos = log.segmentInfos();
        for (LogSegmentInfo s : infos.subList(Math.max(0, infos.size() - 32), infos.size())) {
          Map<String, Object> sm = new LinkedHashMap<>();
          sm.put("baseOffset", s.baseOffset());
          sm.put("nextOffset", s.nextOffset());
          sm.put("sizeBytes", s.sizeBytes());
          sm.put("indexEntries", s.indexEntries());
          sm.put("active", s.active());
          segs.add(sm);
        }
        m.put("segments", segs);
        List<Map<String, Object>> followers = new ArrayList<>();
        p.followerStates()
            .forEach(
                (id, r) -> {
                  Map<String, Object> fm = new LinkedHashMap<>();
                  fm.put("brokerId", id);
                  fm.put("logEndOffset", r.logEndOffset());
                  fm.put("lastCaughtUpMsAgo", clock.getAsLong() - r.lastCaughtUpTimeMs());
                  followers.add(fm);
                });
        m.put("followers", followers);
        m.put("isr", List.copyOf(p.inSyncReplicas()));
      }
      out.add(m);
    }
    return out;
  }

  @Override
  public void close() {
    purgatory.close();
  }
}
