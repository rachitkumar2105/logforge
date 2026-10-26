package dev.logforge.replication;

import dev.logforge.TopicPartition;
import dev.logforge.network.protocol.Errors;
import dev.logforge.util.JsonLog;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Leader-side in-sync replica set and high watermark for one partition.
 *
 * <h2>High watermark</h2>
 *
 * The high watermark (HW) is the smallest log end offset across the ISR: every in-sync replica has
 * every record below it. Consumers only read below the HW, and {@code acks=all} completes when the
 * HW passes the batch. That is what makes a committed record survive the leader's death: whichever
 * ISR member becomes leader already has it. Without the HW a consumer could read a record that a
 * leader failover then erases, so the consumer would have seen something that "never happened".
 *
 * <h2>Shrink</h2>
 *
 * A follower that has not been caught up (fetched up to the leader's log end) for {@code
 * replica.lag.time.max.ms} is removed, so one slow or paused broker cannot hold the HW back and
 * stall every {@code acks=all} producer. Time-based rather than offset-based, because a burst of
 * writes makes healthy followers briefly many offsets behind.
 *
 * <h2>Expand</h2>
 *
 * A follower outside the ISR rejoins when its log end reaches the HW.
 *
 * <h2>Ordering with the controller</h2>
 *
 * Changes are proposed to the controller (AlterIsr) and only take effect when it accepts them,
 * because the controller elects the next leader from its own copy of the ISR. A shrink applied
 * locally first could advance the HW past a follower the controller still considers in sync; if the
 * leader then died, that follower could be elected without the committed records. A pending
 * expansion, on the other hand, is counted in the HW immediately (the "maximal ISR"), which can
 * only make the HW more conservative. One AlterIsr is in flight per partition at a time.
 *
 * <p>Not thread-safe: the owning {@link Partition} serialises access.
 */
public final class IsrManager {
  private static final JsonLog LOG = JsonLog.get(IsrManager.class);

  /** Sends AlterIsr to the controller and calls back with the outcome. */
  public interface AlterIsrSender {
    void send(
        TopicPartition tp,
        int leaderEpoch,
        int partitionEpoch,
        List<Integer> newIsr,
        Consumer<AlterResult> callback);
  }

  /** Controller's answer to AlterIsr. */
  public record AlterResult(Errors error, int leaderEpoch, int partitionEpoch, List<Integer> isr) {}

  private final TopicPartition tp;
  private final int localId;
  private final AlterIsrSender sender;
  private final Consumer<Runnable> underPartitionLock;
  private final Map<Integer, Replica> followers = new HashMap<>();
  private Set<Integer> isr = new TreeSet<>();
  private Set<Integer> pendingAdd = new TreeSet<>();
  private int leaderEpoch = -1;
  private int partitionEpoch = -1;
  private boolean inFlight;
  private long highWatermark;
  private long leaderEpochStartOffset;

  /**
   * @param sender sends AlterIsr to the controller (null: never propose, e.g. in unit tests)
   * @param underPartitionLock runs a task holding the owning partition's lock, then re-checks
   *     parked requests; used for the controller's answer, which arrives on another thread
   */
  IsrManager(
      TopicPartition tp,
      int localId,
      AlterIsrSender sender,
      Consumer<Runnable> underPartitionLock) {
    this.tp = tp;
    this.localId = localId;
    this.sender = sender;
    this.underPartitionLock = underPartitionLock;
  }

  void becomeLeader(
      int leaderEpoch,
      int partitionEpoch,
      List<Integer> replicas,
      List<Integer> isr,
      long startHw,
      long epochStartOffset,
      long nowMs) {
    this.leaderEpochStartOffset = epochStartOffset;
    this.leaderEpoch = leaderEpoch;
    this.partitionEpoch = partitionEpoch;
    this.isr = new TreeSet<>(isr);
    this.isr.add(localId);
    this.pendingAdd = new TreeSet<>();
    this.inFlight = false;
    this.highWatermark = startHw;
    followers.clear();
    for (int r : replicas) {
      if (r != localId) {
        followers.put(r, new Replica(r, nowMs));
      }
    }
  }

  /** Applies an ISR from controller metadata if it is newer than what we hold. */
  void updateFromMetadata(int partitionEpoch, List<Integer> isr) {
    if (partitionEpoch > this.partitionEpoch) {
      this.partitionEpoch = partitionEpoch;
      this.isr = new TreeSet<>(isr);
      this.pendingAdd.removeAll(this.isr);
    }
  }

  public Set<Integer> isr() {
    return Set.copyOf(isr);
  }

  public int partitionEpoch() {
    return partitionEpoch;
  }

  public long highWatermark() {
    return highWatermark;
  }

  void setHighWatermark(long hw) {
    this.highWatermark = hw;
  }

  public Map<Integer, Replica> followers() {
    return Map.copyOf(followers);
  }

  /**
   * Records a follower fetch. Returns true if the high watermark moved. May propose an expansion.
   */
  boolean onFollowerFetch(int followerId, long fetchOffset, long leaderLogEnd, long nowMs) {
    Replica r = followers.get(followerId);
    if (r == null) {
      return false;
    }
    r.onFetch(fetchOffset, leaderLogEnd, nowMs);
    // Caught up to the HW *and* to the start of this leader's epoch. A new leader's HW can lag the
    // offset the previous leader had committed; the epoch start offset is a safe lower bound, so a
    // follower missing committed records cannot rejoin the ISR and later be elected.
    if (!isr.contains(followerId)
        && !pendingAdd.contains(followerId)
        && fetchOffset >= Math.max(highWatermark, leaderEpochStartOffset)) {
      Set<Integer> proposed = new TreeSet<>(isr);
      proposed.add(followerId);
      if (propose(proposed)) {
        pendingAdd.add(followerId);
        LOG.info(
            "proposing ISR expand",
            "partition",
            tp.toString(),
            "follower",
            followerId,
            "isr",
            List.copyOf(proposed));
      }
    }
    return maybeAdvanceHighWatermark(leaderLogEnd);
  }

  /** Proposes removing followers that have not caught up within {@code lagMaxMs}. */
  void maybeShrink(long nowMs, long lagMaxMs) {
    Set<Integer> laggards = new LinkedHashSet<>();
    for (int id : isr) {
      if (id == localId) {
        continue;
      }
      Replica r = followers.get(id);
      if (r == null || nowMs - r.lastCaughtUpTimeMs() > lagMaxMs) {
        laggards.add(id);
      }
    }
    if (laggards.isEmpty()) {
      return;
    }
    Set<Integer> proposed = new TreeSet<>(isr);
    proposed.removeAll(laggards);
    if (propose(proposed)) {
      LOG.info(
          "proposing ISR shrink",
          "partition",
          tp.toString(),
          "removed",
          List.copyOf(laggards),
          "isr",
          List.copyOf(proposed));
    }
  }

  private boolean propose(Set<Integer> proposed) {
    if (inFlight || sender == null) {
      return false;
    }
    inFlight = true;
    int epochAtSend = leaderEpoch;
    sender.send(
        tp,
        leaderEpoch,
        partitionEpoch,
        new ArrayList<>(proposed),
        result -> underPartitionLock.accept(() -> onAlterResult(epochAtSend, result)));
    return true;
  }

  /** Must be called with the partition's lock held (Partition wraps it). */
  void onAlterResult(int epochAtSend, AlterResult result) {
    inFlight = false;
    if (epochAtSend != leaderEpoch) {
      return; // leadership changed meanwhile; metadata will tell us the truth
    }
    if (result.error() == Errors.NONE) {
      isr = new TreeSet<>(result.isr());
      partitionEpoch = result.partitionEpoch();
      pendingAdd.removeAll(isr);
      LOG.info(
          "ISR changed",
          "partition",
          tp.toString(),
          "isr",
          List.copyOf(isr),
          "partitionEpoch",
          partitionEpoch);
    } else {
      pendingAdd.clear();
      LOG.warn("AlterIsr rejected", "partition", tp.toString(), "error", result.error().name());
    }
  }

  /** HW = min log end over the ISR plus pending additions. Returns true if it moved forward. */
  boolean maybeAdvanceHighWatermark(long leaderLogEnd) {
    long min = leaderLogEnd;
    Set<Integer> maximal = new TreeSet<>(isr);
    maximal.addAll(pendingAdd);
    for (int id : maximal) {
      if (id == localId) {
        continue;
      }
      Replica r = followers.get(id);
      long leo = r == null ? Replica.UNKNOWN : r.logEndOffset();
      if (leo == Replica.UNKNOWN) {
        return false; // a member we have not heard from yet: we cannot prove anything newer
      }
      min = Math.min(min, leo);
    }
    if (min > highWatermark) {
      highWatermark = min;
      return true;
    }
    return false;
  }
}
