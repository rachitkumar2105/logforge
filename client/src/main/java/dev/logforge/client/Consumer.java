package dev.logforge.client;

import dev.logforge.TopicPartition;
import dev.logforge.log.Record;
import dev.logforge.log.RecordBatch;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.ListOffsets;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Reads records from assigned partitions. Not thread-safe: one thread polls.
 *
 * <p>Each {@link #poll} sends one Fetch per partition leader, in parallel, as a long poll (the
 * broker holds the request until data arrives or {@code fetchMaxWaitMs} passes). Records come back
 * in offset order per partition; the consumer only ever sees offsets below the high watermark, so
 * it never reads a record a leader failover could take back.
 */
public final class Consumer implements AutoCloseable {
  private final ConsumerConfig config;
  private final ClientMetadata metadata;
  private final ExecutorService fetchPool;
  private final Map<Integer, dev.logforge.network.BlockingChannel> fetchChannels = new HashMap<>();
  private final Set<TopicPartition> assignment = new LinkedHashSet<>();
  private final Map<TopicPartition, Long> positions = new HashMap<>();
  private final ArrayDeque<ConsumerRecord> buffered = new ArrayDeque<>();
  private GroupMembership group;

  public Consumer(ConsumerConfig config) {
    this.config = config;
    this.metadata =
        new ClientMetadata(
            config.bootstrapServers(),
            config.clientId(),
            config.requestTimeoutMs() + config.fetchMaxWaitMs());
    this.metadata.refresh();
    this.fetchPool =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "logforge-consumer-fetch");
              t.setDaemon(true);
              return t;
            });
  }

  ClientMetadata metadata() {
    return metadata;
  }

  ConsumerConfig config() {
    return config;
  }

  /** Manually assigns partitions (no group membership). */
  public void assign(Collection<TopicPartition> partitions) {
    if (group != null) {
      throw new IllegalStateException("subscribe() and assign() cannot be mixed");
    }
    setAssignment(partitions);
  }

  /** Replaces the assignment, keeping positions of partitions that stay. */
  void setAssignment(Collection<TopicPartition> partitions) {
    assignment.clear();
    assignment.addAll(partitions);
    positions.keySet().retainAll(assignment);
    buffered.removeIf(r -> !assignment.contains(new TopicPartition(r.topic(), r.partition())));
  }

  /** Joins {@code config.groupId()} and consumes the partitions the coordinator assigns. */
  public void subscribe(List<String> topics) {
    if (config.groupId() == null) {
      throw new IllegalStateException("subscribe() needs a groupId");
    }
    if (group == null) {
      group = new GroupMembership(this, topics);
    }
    group.ensureActive();
  }

  public Set<TopicPartition> assignment() {
    return Set.copyOf(assignment);
  }

  public void seek(TopicPartition tp, long offset) {
    positions.put(tp, offset);
    buffered.removeIf(r -> r.topic().equals(tp.topic()) && r.partition() == tp.partition());
  }

  /** Next offset to be returned for {@code tp}, or -1 if not yet known. */
  public long position(TopicPartition tp) {
    return positions.getOrDefault(tp, -1L);
  }

  Map<TopicPartition, Long> positions() {
    return Map.copyOf(positions);
  }

  /** Commits the current positions to the group (synchronous). */
  public void commitSync() {
    if (group == null) {
      throw new IllegalStateException("commitSync() needs subscribe()");
    }
    group.commit(currentConsumedPositions());
  }

  private Map<TopicPartition, Long> currentConsumedPositions() {
    Map<TopicPartition, Long> out = new LinkedHashMap<>();
    for (TopicPartition tp : assignment) {
      Long p = positions.get(tp);
      if (p != null) {
        out.put(tp, p);
      }
    }
    // Records already fetched but not yet handed out are not consumed: rewind to the first one.
    for (ConsumerRecord r : buffered) {
      TopicPartition tp = new TopicPartition(r.topic(), r.partition());
      out.merge(tp, r.offset(), Math::min);
    }
    return out;
  }

  /** Returns records fetched within {@code timeout}; empty if none arrived. */
  public List<ConsumerRecord> poll(Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    do {
      if (group != null) {
        group.poll();
      }
      if (!buffered.isEmpty()) {
        return drainBuffer();
      }
      if (assignment.isEmpty()) {
        sleepQuietly(Math.min(50, remainingMs(deadline)));
        continue;
      }
      ensurePositions();
      fetchOnce((int) Math.min(config.fetchMaxWaitMs(), Math.max(0, remainingMs(deadline))));
      if (!buffered.isEmpty()) {
        return drainBuffer();
      }
    } while (System.nanoTime() < deadline);
    return List.of();
  }

  private static long remainingMs(long deadlineNanos) {
    return TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
  }

  private static void sleepQuietly(long ms) {
    if (ms <= 0) {
      return;
    }
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private List<ConsumerRecord> drainBuffer() {
    List<ConsumerRecord> out = new ArrayList<>(Math.min(buffered.size(), config.maxPollRecords()));
    while (!buffered.isEmpty() && out.size() < config.maxPollRecords()) {
      ConsumerRecord r = buffered.poll();
      out.add(r);
      positions.put(new TopicPartition(r.topic(), r.partition()), r.offset() + 1);
    }
    return out;
  }

  /**
   * Resolves a start offset for partitions that have none: committed offset, else the reset policy.
   */
  private void ensurePositions() {
    List<TopicPartition> missing = new ArrayList<>();
    for (TopicPartition tp : assignment) {
      if (!positions.containsKey(tp)) {
        missing.add(tp);
      }
    }
    if (missing.isEmpty()) {
      return;
    }
    if (group != null) {
      Map<TopicPartition, Long> committed = group.fetchCommitted(missing);
      committed.forEach(
          (tp, off) -> {
            if (off >= 0) {
              positions.put(tp, off);
            }
          });
    }
    for (TopicPartition tp : missing) {
      if (!positions.containsKey(tp)) {
        resetPosition(tp);
      }
    }
  }

  private void resetPosition(TopicPartition tp) {
    int leader = metadata.leaderFor(tp);
    if (leader < 0) {
      metadata.refresh();
      leader = metadata.leaderFor(tp);
      if (leader < 0) {
        return;
      }
    }
    long ts =
        config.autoOffsetReset() == ConsumerConfig.OffsetReset.EARLIEST
            ? ListOffsets.EARLIEST
            : ListOffsets.LATEST;
    ListOffsets.Request req =
        new ListOffsets.Request(
            -1,
            List.of(
                new ListOffsets.TopicData(
                    tp.topic(), List.of(new ListOffsets.PartitionData(tp.partition(), ts)))));
    try {
      ListOffsets.PartitionResult r =
          ListOffsets.Response.read(
                  metadata.channel(leader).request(ApiKeys.LIST_OFFSETS, req::write))
              .topics()
              .get(0)
              .partitions()
              .get(0);
      if (r.errorCode() == Errors.NONE.code()) {
        positions.put(tp, r.offset());
      } else {
        metadata.refresh();
      }
    } catch (IOException | RuntimeException e) {
      // Leader unreachable: next poll retries after a metadata refresh.
      safeRefresh();
    }
  }

  private void safeRefresh() {
    try {
      metadata.refresh();
    } catch (RuntimeException e) {
      // Cluster unreachable right now.
    }
  }

  private void fetchOnce(int maxWaitMs) {
    Map<Integer, List<TopicPartition>> byLeader = new LinkedHashMap<>();
    boolean refresh = false;
    for (TopicPartition tp : assignment) {
      if (!positions.containsKey(tp)) {
        continue;
      }
      int leader = metadata.leaderFor(tp);
      if (leader < 0) {
        refresh = true;
        continue;
      }
      byLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(tp);
    }
    if (refresh) {
      safeRefresh();
    }
    if (byLeader.isEmpty()) {
      sleepQuietly(Math.min(100, maxWaitMs));
      return;
    }
    Map<Integer, CompletableFuture<Fetch.Response>> futures = new LinkedHashMap<>();
    for (Map.Entry<Integer, List<TopicPartition>> e : byLeader.entrySet()) {
      Fetch.Request req = buildFetch(e.getValue(), maxWaitMs);
      int leader = e.getKey();
      futures.put(
          leader,
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return Fetch.Response.read(
                      fetchChannel(leader).request(ApiKeys.FETCH, req::write));
                } catch (IOException ex) {
                  throw new ClientException("fetch failed", ex);
                }
              },
              fetchPool));
    }
    boolean needRefresh = false;
    for (CompletableFuture<Fetch.Response> f : futures.values()) {
      Fetch.Response resp;
      try {
        resp = f.get(maxWaitMs + (long) config.requestTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (Exception ex) {
        needRefresh = true;
        continue;
      }
      needRefresh |= handleFetch(resp);
    }
    if (needRefresh) {
      safeRefresh();
    }
  }

  /** One unshared connection per leader for long polls, replaced if the broker's address moved. */
  private synchronized dev.logforge.network.BlockingChannel fetchChannel(int leader) {
    dev.logforge.network.BlockingChannel ch = fetchChannels.get(leader);
    ClientMetadata.Address addr = metadata.address(leader);
    if (ch != null
        && addr != null
        && (!ch.host().equals(addr.host()) || ch.port() != addr.port())) {
      ch.close();
      fetchChannels.remove(leader);
      ch = null;
    }
    if (ch == null) {
      ch = metadata.newChannel(leader);
      fetchChannels.put(leader, ch);
    }
    return ch;
  }

  private Fetch.Request buildFetch(List<TopicPartition> tps, int maxWaitMs) {
    Map<String, List<Fetch.PartitionData>> byTopic = new LinkedHashMap<>();
    for (TopicPartition tp : tps) {
      byTopic
          .computeIfAbsent(tp.topic(), k -> new ArrayList<>())
          .add(
              new Fetch.PartitionData(
                  tp.partition(), -1, positions.get(tp), config.partitionMaxBytes()));
    }
    List<Fetch.TopicData> topics = new ArrayList<>();
    byTopic.forEach((t, ps) -> topics.add(new Fetch.TopicData(t, ps)));
    return new Fetch.Request(
        Fetch.CONSUMER_REPLICA_ID, maxWaitMs, 1, config.fetchMaxBytes(), topics);
  }

  /** Buffers records; returns true if metadata should be refreshed. */
  private boolean handleFetch(Fetch.Response resp) {
    boolean refresh = false;
    for (Fetch.TopicResult t : resp.topics()) {
      for (Fetch.PartitionResult p : t.partitions()) {
        TopicPartition tp = new TopicPartition(t.name(), p.partition());
        Long pos = positions.get(tp);
        if (pos == null || !assignment.contains(tp)) {
          continue;
        }
        Errors err = Errors.forCode(p.errorCode());
        if (err == Errors.OFFSET_OUT_OF_RANGE) {
          positions.remove(tp); // re-resolved by the reset policy on the next poll
          continue;
        }
        if (err != Errors.NONE) {
          refresh = true;
          continue;
        }
        long next = pos;
        boolean added = false;
        for (RecordBatch b : RecordBatch.iterate(p.records())) {
          for (Record r : b.records()) {
            if (r.offset() < next) {
              continue; // the first batch may start before our position
            }
            buffered.add(
                new ConsumerRecord(
                    t.name(),
                    p.partition(),
                    r.offset(),
                    r.timestamp(),
                    r.key(),
                    r.value(),
                    r.headers()));
            added = true;
            next = r.offset() + 1;
          }
          // A batch can end with offsets removed by compaction: skip past the whole batch.
          next = Math.max(next, b.lastOffset() + 1);
        }
        if (!added && next > pos) {
          positions.put(tp, next); // the range held only compacted-away offsets
        }
      }
    }
    return refresh;
  }

  @Override
  public void close() {
    if (group != null) {
      group.leave();
    }
    fetchChannels.values().forEach(dev.logforge.network.BlockingChannel::close);
    fetchPool.shutdownNow();
    metadata.close();
  }
}
