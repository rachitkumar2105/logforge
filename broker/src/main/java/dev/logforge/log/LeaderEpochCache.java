package dev.logforge.log;

import java.util.ArrayList;
import java.util.List;

/**
 * History of leader epochs for one partition: for each epoch, the first offset written in it.
 *
 * <p>This is what lets a follower that returns after a failure find exactly where its log diverged
 * from the current leader's. The follower asks "where does my latest epoch end in your log?" and
 * truncates to that point. Truncating to the high watermark instead would be wrong in two ways: it
 * can throw away committed records the follower legitimately had, and in double-failure cases it
 * can keep a tail the new leader never had, leaving two replicas with different data at the same
 * offset.
 *
 * <p>Rebuilt from batch headers on startup and after truncation, so it can never disagree with the
 * log. Guarded by the owning log's lock.
 */
public final class LeaderEpochCache {
  private record Entry(int epoch, long startOffset) {}

  private final List<Entry> entries = new ArrayList<>();

  /** Records that {@code epoch} begins at {@code startOffset}, if it is newer than the latest. */
  public synchronized void assign(int epoch, long startOffset) {
    if (epoch < 0) {
      return;
    }
    if (!entries.isEmpty()) {
      Entry last = entries.get(entries.size() - 1);
      if (epoch <= last.epoch()) {
        return;
      }
    }
    entries.add(new Entry(epoch, startOffset));
  }

  public synchronized int latestEpoch() {
    return entries.isEmpty() ? -1 : entries.get(entries.size() - 1).epoch();
  }

  /**
   * The largest epoch {@code <= requested} and the offset where it ends: the start of the next
   * epoch, or {@code logEndOffset} if it is the latest.
   */
  public synchronized EpochEndOffset endOffsetFor(int requested, long logEndOffset) {
    int idx = -1;
    for (int i = entries.size() - 1; i >= 0; i--) {
      if (entries.get(i).epoch() <= requested) {
        idx = i;
        break;
      }
    }
    if (idx < 0) {
      return EpochEndOffset.UNDEFINED;
    }
    long end = idx == entries.size() - 1 ? logEndOffset : entries.get(idx + 1).startOffset();
    return new EpochEndOffset(entries.get(idx).epoch(), end);
  }

  public synchronized void clear() {
    entries.clear();
  }
}
