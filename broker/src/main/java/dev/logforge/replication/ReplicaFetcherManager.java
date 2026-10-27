package dev.logforge.replication;

import dev.logforge.TopicPartition;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

/** One {@link ReplicaFetcher} thread per leader broker this broker follows partitions from. */
public final class ReplicaFetcherManager implements FetcherControl, Closeable {
  private final int localId;
  private final ReplicaManager replicaManager;
  private final IntFunction<InetSocketAddress> addressOf;
  private final int maxWaitMs;
  private final int maxBytes;
  private final String clusterToken;
  private final Map<Integer, ReplicaFetcher> fetchers = new HashMap<>();
  private final Map<TopicPartition, Integer> leaders = new HashMap<>();
  private volatile boolean paused;

  public ReplicaFetcherManager(
      int localId,
      ReplicaManager replicaManager,
      IntFunction<InetSocketAddress> addressOf,
      int maxWaitMs,
      int maxBytes,
      String clusterToken) {
    this.clusterToken = clusterToken;
    this.localId = localId;
    this.replicaManager = replicaManager;
    this.addressOf = addressOf;
    this.maxWaitMs = maxWaitMs;
    this.maxBytes = maxBytes;
  }

  @Override
  public synchronized void follow(TopicPartition tp, int leaderId, int leaderEpoch) {
    Integer previous = leaders.put(tp, leaderId);
    if (previous != null && previous != leaderId) {
      detach(tp, previous);
    }
    ReplicaFetcher f =
        fetchers.computeIfAbsent(
            leaderId,
            id -> {
              ReplicaFetcher nf =
                  new ReplicaFetcher(
                      localId,
                      id,
                      replicaManager,
                      addressOf,
                      () -> paused,
                      maxWaitMs,
                      maxBytes,
                      clusterToken);
              nf.start();
              return nf;
            });
    f.add(tp, leaderEpoch);
  }

  @Override
  public synchronized void unfollow(TopicPartition tp) {
    Integer leader = leaders.remove(tp);
    if (leader != null) {
      detach(tp, leader);
    }
  }

  private void detach(TopicPartition tp, int leaderId) {
    ReplicaFetcher f = fetchers.get(leaderId);
    if (f != null) {
      f.remove(tp);
      if (f.isEmpty()) {
        fetchers.remove(leaderId);
        f.shutdown();
      }
    }
  }

  /** Suspends all pulling (used by tests to model a stalled follower). */
  public void setPaused(boolean paused) {
    this.paused = paused;
  }

  @Override
  public synchronized void close() {
    fetchers.values().forEach(ReplicaFetcher::shutdown);
    fetchers.clear();
    leaders.clear();
  }
}
