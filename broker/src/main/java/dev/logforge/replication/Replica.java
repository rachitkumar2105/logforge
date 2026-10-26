package dev.logforge.replication;

/**
 * What a leader knows about one follower of a partition, learned only from that follower's fetch
 * requests: the offset it asked for is its log end offset (it has everything before it).
 */
public final class Replica {
  public static final long UNKNOWN = -1L;

  private final int brokerId;
  private long logEndOffset = UNKNOWN;
  private long lastCaughtUpTimeMs;
  private long lastFetchTimeMs;

  Replica(int brokerId, long nowMs) {
    this.brokerId = brokerId;
    // Start "caught up" so a new leader does not evict followers before they had a chance to fetch.
    this.lastCaughtUpTimeMs = nowMs;
  }

  public int brokerId() {
    return brokerId;
  }

  public long logEndOffset() {
    return logEndOffset;
  }

  public long lastCaughtUpTimeMs() {
    return lastCaughtUpTimeMs;
  }

  public long lastFetchTimeMs() {
    return lastFetchTimeMs;
  }

  /**
   * Records a fetch at {@code fetchOffset}. The follower counts as caught up if it asked for
   * everything the leader had at that moment.
   */
  void onFetch(long fetchOffset, long leaderLogEnd, long nowMs) {
    logEndOffset = fetchOffset;
    lastFetchTimeMs = nowMs;
    if (fetchOffset >= leaderLogEnd) {
      lastCaughtUpTimeMs = nowMs;
    }
  }
}
