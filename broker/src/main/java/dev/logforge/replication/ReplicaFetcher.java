package dev.logforge.replication;

import dev.logforge.TopicPartition;
import dev.logforge.log.EpochEndOffset;
import dev.logforge.log.Log;
import dev.logforge.log.RecordBatch;
import dev.logforge.network.BlockingChannel;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.Replication;
import dev.logforge.util.JsonLog;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;

/**
 * The follower's pull loop for every partition it follows from one leader.
 *
 * <h2>Why followers pull</h2>
 *
 * The follower asks for "everything from my log end offset". That single number tells the leader
 * exactly how far this replica has got, which is what drives the high watermark and ISR, with no
 * acknowledgement protocol of its own. A slow follower simply asks less often and falls behind; the
 * leader never buffers per-follower state or blocks on a slow peer, and a follower that was away
 * resumes from wherever its log ends. With push, the leader would need to track and retry per
 * follower, and a slow follower would back-pressure the leader.
 *
 * <h2>Truncate, then fetch</h2>
 *
 * Whenever a partition is (re)assigned to this fetcher, the follower first asks the leader where
 * its own latest leader epoch ends in the leader's log (OffsetForLeaderEpoch) and truncates to that
 * point. That removes any tail it wrote as a leader of an older epoch that the current leader never
 * had. Only then does it fetch.
 */
final class ReplicaFetcher extends Thread {
  private static final JsonLog LOG = JsonLog.get(ReplicaFetcher.class);

  private static final class State {
    volatile int leaderEpoch;
    volatile boolean needsTruncation = true;
    volatile long backoffUntilMs;

    State(int leaderEpoch) {
      this.leaderEpoch = leaderEpoch;
    }
  }

  private final int localId;
  private final int leaderId;
  private final ReplicaManager replicaManager;
  private final IntFunction<InetSocketAddress> addressOf;
  private final BooleanSupplier paused;
  private final int maxWaitMs;
  private final int maxBytes;
  private final Map<TopicPartition, State> partitions = new LinkedHashMap<>();
  private BlockingChannel channel;
  private int consecutiveFailures;
  private volatile boolean running = true;

  ReplicaFetcher(
      int localId,
      int leaderId,
      ReplicaManager replicaManager,
      IntFunction<InetSocketAddress> addressOf,
      BooleanSupplier paused,
      int maxWaitMs,
      int maxBytes) {
    super("logforge-fetcher-" + leaderId);
    setDaemon(true);
    this.localId = localId;
    this.leaderId = leaderId;
    this.replicaManager = replicaManager;
    this.addressOf = addressOf;
    this.paused = paused;
    this.maxWaitMs = maxWaitMs;
    this.maxBytes = maxBytes;
  }

  synchronized void add(TopicPartition tp, int leaderEpoch) {
    State s = partitions.get(tp);
    if (s == null) {
      partitions.put(tp, new State(leaderEpoch));
    } else if (s.leaderEpoch != leaderEpoch) {
      // Update in place: the run loop may hold this object from its last snapshot.
      s.leaderEpoch = leaderEpoch;
      s.needsTruncation = true;
      s.backoffUntilMs = 0;
    }
    notifyAll();
  }

  synchronized void remove(TopicPartition tp) {
    partitions.remove(tp);
  }

  synchronized boolean isEmpty() {
    return partitions.isEmpty();
  }

  void shutdown() {
    running = false;
    interrupt();
    if (channel != null) {
      channel.close();
    }
  }

  @Override
  public void run() {
    JsonLog.setCorrelationId("fetcher-" + leaderId);
    while (running) {
      try {
        if (paused.getAsBoolean()) {
          Thread.sleep(20);
          continue;
        }
        Map<TopicPartition, State> work = snapshot();
        if (work.isEmpty()) {
          synchronized (this) {
            wait(100);
          }
          continue;
        }
        BlockingChannel ch = channel();
        if (ch == null) {
          Thread.sleep(100);
          continue;
        }
        Map<TopicPartition, State> truncate = new LinkedHashMap<>();
        Map<TopicPartition, State> fetch = new LinkedHashMap<>();
        work.forEach((tp, s) -> (s.needsTruncation ? truncate : fetch).put(tp, s));
        if (!truncate.isEmpty()) {
          truncate(ch, truncate);
        }
        if (!fetch.isEmpty()) {
          fetch(ch, fetch);
        } else if (truncate.isEmpty()) {
          Thread.sleep(50); // everything is backing off
        }
      } catch (InterruptedException e) {
        return;
      } catch (IOException | RuntimeException e) {
        if (running) {
          consecutiveFailures++;
          if (consecutiveFailures == 1 || consecutiveFailures % 50 == 0) {
            // A dead leader fails every attempt until metadata moves us elsewhere: log sparingly.
            LOG.warn(
                "fetch from leader failed",
                "leader",
                leaderId,
                "error",
                e.getClass().getSimpleName(),
                "consecutiveFailures",
                consecutiveFailures);
          }
          if (channel != null) {
            channel.disconnect();
          }
          sleepQuietly(200);
        }
      }
    }
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private synchronized Map<TopicPartition, State> snapshot() {
    long now = System.currentTimeMillis();
    Map<TopicPartition, State> out = new LinkedHashMap<>();
    partitions.forEach(
        (tp, s) -> {
          if (s.backoffUntilMs <= now) {
            out.put(tp, s);
          }
        });
    return out;
  }

  private BlockingChannel channel() {
    InetSocketAddress addr = addressOf.apply(leaderId);
    if (addr == null) {
      return null;
    }
    if (channel == null
        || !channel.host().equals(addr.getHostString())
        || channel.port() != addr.getPort()) {
      if (channel != null) {
        channel.close();
      }
      channel =
          new BlockingChannel(
              addr.getHostString(), addr.getPort(), "replica-" + localId, maxWaitMs + 10_000);
    }
    return channel;
  }

  private void backoff(State s, long ms) {
    synchronized (this) {
      s.backoffUntilMs = System.currentTimeMillis() + ms;
    }
  }

  private void truncate(BlockingChannel ch, Map<TopicPartition, State> work) throws IOException {
    Map<String, List<Replication.EpochPartition>> byTopic = new LinkedHashMap<>();
    Map<TopicPartition, State> asked = new HashMap<>();
    for (Map.Entry<TopicPartition, State> e : work.entrySet()) {
      Partition p = replicaManager.partition(e.getKey());
      if (p == null || p.log() == null) {
        continue;
      }
      int latest = p.log().epochCache().latestEpoch();
      if (latest < 0) {
        e.getValue().needsTruncation = false; // empty log: nothing to diverge
        continue;
      }
      asked.put(e.getKey(), e.getValue());
      byTopic
          .computeIfAbsent(e.getKey().topic(), k -> new ArrayList<>())
          .add(
              new Replication.EpochPartition(
                  e.getKey().partition(), e.getValue().leaderEpoch, latest));
    }
    if (byTopic.isEmpty()) {
      return;
    }
    List<Replication.EpochTopic> topics = new ArrayList<>();
    byTopic.forEach((t, ps) -> topics.add(new Replication.EpochTopic(t, ps)));
    Replication.OffsetForLeaderEpochRequest req =
        new Replication.OffsetForLeaderEpochRequest(localId, topics);
    Replication.OffsetForLeaderEpochResponse resp =
        Replication.OffsetForLeaderEpochResponse.read(
            ch.request(ApiKeys.OFFSET_FOR_LEADER_EPOCH, req::write));
    for (Replication.EpochTopicResult t : resp.topics()) {
      for (Replication.EpochEnd r : t.partitions()) {
        TopicPartition tp = new TopicPartition(t.name(), r.partition());
        State s = asked.get(tp);
        Partition p = replicaManager.partition(tp);
        if (s == null || p == null) {
          continue;
        }
        Errors err = Errors.forCode(r.errorCode());
        if (err != Errors.NONE) {
          backoff(s, 100); // our metadata or the leader's is behind; it settles within a heartbeat
          continue;
        }
        Log log = p.log();
        long leo = log.logEndOffset();
        long target;
        if (r.leaderEpoch() < 0) {
          // The leader has no epoch at or below ours: fall back to what we know is committed.
          target = Math.min(leo, p.highWatermark());
        } else {
          EpochEndOffset mine = log.epochCache().endOffsetFor(r.leaderEpoch(), leo);
          long myEnd = mine.isUndefined() ? leo : mine.endOffset();
          target = Math.min(leo, Math.min(r.endOffset(), myEnd));
        }
        if (target < leo) {
          LOG.info(
              "truncating divergent tail",
              "partition",
              tp.toString(),
              "from",
              leo,
              "to",
              target,
              "leaderEpoch",
              r.leaderEpoch());
          p.truncateTo(target);
        }
        s.needsTruncation = false;
      }
    }
  }

  private void fetch(BlockingChannel ch, Map<TopicPartition, State> work) throws IOException {
    Map<String, List<Fetch.PartitionData>> byTopic = new LinkedHashMap<>();
    for (Map.Entry<TopicPartition, State> e : work.entrySet()) {
      Partition p = replicaManager.partition(e.getKey());
      if (p == null || p.log() == null) {
        continue;
      }
      byTopic
          .computeIfAbsent(e.getKey().topic(), k -> new ArrayList<>())
          .add(
              new Fetch.PartitionData(
                  e.getKey().partition(), e.getValue().leaderEpoch, p.logEndOffset(), maxBytes));
    }
    if (byTopic.isEmpty()) {
      Thread.yield();
      return;
    }
    List<Fetch.TopicData> topics = new ArrayList<>();
    byTopic.forEach((t, ps) -> topics.add(new Fetch.TopicData(t, ps)));
    Fetch.Request req = new Fetch.Request(localId, maxWaitMs, 1, maxBytes, topics);
    Fetch.Response resp = Fetch.Response.read(ch.request(ApiKeys.FETCH, req::write));
    if (paused.getAsBoolean()) {
      return; // paused while the long poll was outstanding: a stalled follower applies nothing
    }
    consecutiveFailures = 0;
    for (Fetch.TopicResult t : resp.topics()) {
      for (Fetch.PartitionResult r : t.partitions()) {
        TopicPartition tp = new TopicPartition(t.name(), r.partition());
        State s = work.get(tp);
        Partition p = replicaManager.partition(tp);
        if (s == null || p == null || p.log() == null || p.isLeader()) {
          continue;
        }
        Errors err = Errors.forCode(r.errorCode());
        switch (err) {
          case NONE -> {
            long leo = p.logEndOffset();
            for (RecordBatch b : RecordBatch.iterate(r.records())) {
              if (b.lastOffset() < leo) {
                continue;
              }
              if (b.baseOffset() > p.logEndOffset() && !p.config().compact()) {
                // Only compaction may leave offset gaps. Anything else means the logs diverged:
                // re-run the epoch handshake instead of appending a hole.
                LOG.warn(
                    "offset gap from leader; re-validating",
                    "partition",
                    tp.toString(),
                    "logEnd",
                    p.logEndOffset(),
                    "batchBase",
                    b.baseOffset());
                s.needsTruncation = true;
                break;
              }
              p.appendAsFollower(b);
            }
            p.updateFollowerHighWatermark(r.highWatermark());
          }
          case OFFSET_OUT_OF_RANGE -> {
            long leo = p.logEndOffset();
            if (leo < r.logStartOffset()) {
              LOG.info(
                  "follower behind leader log start; restarting log",
                  "partition",
                  tp.toString(),
                  "at",
                  r.logStartOffset());
              p.truncateFullyAndStartAt(r.logStartOffset());
            } else {
              synchronized (this) {
                s.needsTruncation = true;
              }
            }
          }
          default -> backoff(s, 100);
        }
      }
    }
  }
}
