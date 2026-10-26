package dev.logforge.replication;

import dev.logforge.TopicPartition;
import dev.logforge.log.EpochEndOffset;
import dev.logforge.log.Log;
import dev.logforge.log.LogAppendInfo;
import dev.logforge.log.LogConfig;
import dev.logforge.log.OffsetOutOfRangeException;
import dev.logforge.log.RecordBatch;
import dev.logforge.network.protocol.Errors;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/**
 * One partition replica on this broker, either leader or follower.
 *
 * <p>Locking: {@link #leaderIsrLock} is held for reading by appends and fetches and for writing by
 * leadership changes, so a batch can never be appended under an epoch the broker has already given
 * up. {@link #isrMonitor} guards the ISR and high watermark, which change on every follower fetch.
 */
public final class Partition {
  /** This broker is not the leader for the partition. */
  public static final class NotLeaderException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    NotLeaderException(String message) {
      super(message);
    }
  }

  /** acks=all refused because the ISR is smaller than min.insync.replicas. */
  public static final class NotEnoughReplicasException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    NotEnoughReplicasException(String message) {
      super(message);
    }
  }

  /** Outcome of a read. {@code records} is empty (never null) when there is nothing to return. */
  public record ReadResult(
      Errors error,
      long highWatermark,
      long logStartOffset,
      long logEndOffset,
      ByteBuffer records) {
    static ReadResult error(Errors e) {
      return new ReadResult(e, -1, -1, -1, ByteBuffer.allocate(0));
    }
  }

  private final TopicPartition tp;
  private final int localId;
  private final LongSupplier clock;
  private final Runnable onStateChange;
  private final ReentrantReadWriteLock leaderIsrLock = new ReentrantReadWriteLock();
  private final Object isrMonitor = new Object();
  private final IsrManager isr;

  private volatile Log log;
  private volatile LogConfig config;
  private volatile int leaderId = -1;
  private volatile int leaderEpoch = -1;
  private volatile int partitionEpoch = -1;
  private volatile List<Integer> replicas = List.of();
  private volatile long followerHighWatermark;

  /**
   * @param onStateChange called after the high watermark, ISR or leadership may have changed, so
   *     parked requests watching this partition can be re-checked
   */
  public Partition(
      TopicPartition tp,
      int localId,
      LongSupplier clock,
      IsrManager.AlterIsrSender alterIsr,
      Runnable onStateChange) {
    this.tp = tp;
    this.localId = localId;
    this.clock = clock;
    this.onStateChange = onStateChange;
    this.isr = new IsrManager(tp, localId, alterIsr, this::runUnderLock);
  }

  /** Runs a task holding the ISR monitor (controller callbacks), then re-checks parked requests. */
  private void runUnderLock(Runnable task) {
    synchronized (isrMonitor) {
      task.run();
      Log l = log;
      if (l != null && isLeader()) {
        isr.maybeAdvanceHighWatermark(l.logEndOffset());
      }
    }
    onStateChange.run();
  }

  public TopicPartition topicPartition() {
    return tp;
  }

  public Log log() {
    return log;
  }

  public LogConfig config() {
    return config;
  }

  public boolean isLeader() {
    return leaderId == localId;
  }

  public int leaderId() {
    return leaderId;
  }

  public int leaderEpoch() {
    return leaderEpoch;
  }

  public int partitionEpoch() {
    return partitionEpoch;
  }

  public List<Integer> replicas() {
    return replicas;
  }

  public Set<Integer> inSyncReplicas() {
    synchronized (isrMonitor) {
      return isLeader() ? isr.isr() : Set.of();
    }
  }

  public long highWatermark() {
    if (isLeader()) {
      synchronized (isrMonitor) {
        return isr.highWatermark();
      }
    }
    return followerHighWatermark;
  }

  public long logEndOffset() {
    Log l = log;
    return l == null ? -1 : l.logEndOffset();
  }

  /** Becomes leader at {@code epoch}. Followers start "caught up" and with unknown log ends. */
  public void makeLeader(
      Log log,
      LogConfig config,
      int epoch,
      int partitionEpoch,
      List<Integer> replicas,
      List<Integer> isrList) {
    leaderIsrLock.writeLock().lock();
    try {
      long previousHw = highWatermark();
      this.log = log;
      this.config = config;
      this.leaderEpoch = epoch;
      this.partitionEpoch = partitionEpoch;
      this.replicas = List.copyOf(replicas);
      this.leaderId = localId;
      long startHw = Math.max(log.logStartOffset(), Math.min(previousHw, log.logEndOffset()));
      synchronized (isrMonitor) {
        isr.becomeLeader(
            epoch,
            partitionEpoch,
            replicas,
            isrList,
            startHw,
            log.logEndOffset(),
            clock.getAsLong());
        isr.maybeAdvanceHighWatermark(log.logEndOffset());
      }
    } finally {
      leaderIsrLock.writeLock().unlock();
    }
    onStateChange.run();
  }

  /** Becomes a follower of {@code leader} at {@code epoch} (leader -1 means offline). */
  public void makeFollower(
      Log log,
      LogConfig config,
      int leader,
      int epoch,
      int partitionEpoch,
      List<Integer> replicas) {
    leaderIsrLock.writeLock().lock();
    try {
      long previousHw = highWatermark();
      this.log = log;
      this.config = config;
      this.leaderEpoch = epoch;
      this.partitionEpoch = partitionEpoch;
      this.replicas = List.copyOf(replicas);
      this.leaderId = leader;
      this.followerHighWatermark = Math.min(previousHw, log.logEndOffset());
    } finally {
      leaderIsrLock.writeLock().unlock();
    }
    onStateChange.run();
  }

  /** Applies an ISR change the controller made on its own (for example after a broker died). */
  public void updateIsrFromMetadata(int partitionEpoch, List<Integer> isrList) {
    if (!isLeader()) {
      this.partitionEpoch = Math.max(this.partitionEpoch, partitionEpoch);
      return;
    }
    synchronized (isrMonitor) {
      isr.updateFromMetadata(partitionEpoch, isrList);
      this.partitionEpoch = isr.partitionEpoch();
      isr.maybeAdvanceHighWatermark(log.logEndOffset());
    }
    onStateChange.run();
  }

  /** Leader append. Caller re-checks parked requests afterwards. */
  public LogAppendInfo appendAsLeader(RecordBatch batch, int requiredAcks) {
    leaderIsrLock.readLock().lock();
    try {
      if (!isLeader()) {
        throw new NotLeaderException(tp + " leader is " + leaderId);
      }
      if (requiredAcks == -1) {
        int size;
        synchronized (isrMonitor) {
          size = isr.isr().size();
        }
        if (size < config.minInsyncReplicas()) {
          throw new NotEnoughReplicasException(
              tp
                  + " ISR size "
                  + size
                  + " below min.insync.replicas "
                  + config.minInsyncReplicas());
        }
      }
      LogAppendInfo info = log.appendAsLeader(batch, leaderEpoch);
      synchronized (isrMonitor) {
        isr.maybeAdvanceHighWatermark(log.logEndOffset());
      }
      return info;
    } finally {
      leaderIsrLock.readLock().unlock();
    }
  }

  /**
   * Serves a fetch. A fetch from a follower ({@code replicaId >= 0}) first records the follower's
   * position, which can move the high watermark and grow the ISR; it then reads up to the log end.
   * A consumer reads up to the high watermark only.
   */
  public ReadResult read(int replicaId, int currentLeaderEpoch, long fetchOffset, int maxBytes) {
    boolean[] changed = {false};
    ReadResult result = readLocked(replicaId, currentLeaderEpoch, fetchOffset, maxBytes, changed);
    if (changed[0]) {
      onStateChange.run(); // outside the lock: completing parked requests re-enters partitions
    }
    return result;
  }

  private ReadResult readLocked(
      int replicaId, int currentLeaderEpoch, long fetchOffset, int maxBytes, boolean[] changedOut) {
    leaderIsrLock.readLock().lock();
    try {
      if (!isLeader()) {
        return ReadResult.error(Errors.NOT_LEADER_OR_FOLLOWER);
      }
      if (currentLeaderEpoch >= 0 && currentLeaderEpoch < leaderEpoch) {
        return ReadResult.error(Errors.FENCED_LEADER_EPOCH);
      }
      if (currentLeaderEpoch > leaderEpoch) {
        return ReadResult.error(Errors.UNKNOWN_LEADER_EPOCH);
      }
      Log l = log;
      long leo = l.logEndOffset();
      boolean changed = false;
      long hw;
      synchronized (isrMonitor) {
        if (replicaId >= 0 && fetchOffset <= leo) {
          changed = isr.onFollowerFetch(replicaId, fetchOffset, leo, clock.getAsLong());
        }
        hw = isr.highWatermark();
      }
      changedOut[0] = changed;
      long maxOffset = replicaId >= 0 ? Long.MAX_VALUE : hw;
      try {
        ByteBuffer records = l.read(fetchOffset, maxBytes, maxOffset);
        return new ReadResult(Errors.NONE, hw, l.logStartOffset(), leo, records);
      } catch (OffsetOutOfRangeException e) {
        return new ReadResult(
            Errors.OFFSET_OUT_OF_RANGE, hw, l.logStartOffset(), leo, ByteBuffer.allocate(0));
      }
    } finally {
      leaderIsrLock.readLock().unlock();
    }
  }

  /** True if a parked fetch at {@code fetchOffset} would now get data. */
  public boolean hasDataBeyond(int replicaId, long fetchOffset) {
    if (!isLeader()) {
      return true; // answer now with NOT_LEADER
    }
    long limit = replicaId >= 0 ? logEndOffset() : highWatermark();
    return limit > fetchOffset;
  }

  /** Proposes removing followers that stopped keeping up. */
  public void maybeShrinkIsr(long lagMaxMs) {
    leaderIsrLock.readLock().lock();
    try {
      if (!isLeader()) {
        return;
      }
      synchronized (isrMonitor) {
        isr.maybeShrink(clock.getAsLong(), lagMaxMs);
      }
    } finally {
      leaderIsrLock.readLock().unlock();
    }
  }

  /** Where {@code epoch} ends in the leader's log (OffsetForLeaderEpoch). */
  public EpochEndOffset endOffsetForEpoch(int epoch) {
    Log l = log;
    if (l == null) {
      return EpochEndOffset.UNDEFINED;
    }
    return l.epochCache().endOffsetFor(epoch, l.logEndOffset());
  }

  // ------------------------------------------------------------------ follower side

  public LogAppendInfo appendAsFollower(RecordBatch batch) {
    return log.appendAsFollower(batch);
  }

  public void updateFollowerHighWatermark(long leaderHw) {
    followerHighWatermark = Math.min(leaderHw, logEndOffset());
  }

  public void truncateTo(long offset) throws IOException {
    log.truncateTo(offset);
    followerHighWatermark = Math.min(followerHighWatermark, log.logEndOffset());
  }

  public void truncateFullyAndStartAt(long offset) throws IOException {
    log.truncateFullyAndStartAt(offset);
    followerHighWatermark = offset;
  }

  /** Follower positions as the leader sees them, for the admin API. */
  public java.util.Map<Integer, Replica> followerStates() {
    synchronized (isrMonitor) {
      return isr.followers();
    }
  }
}
