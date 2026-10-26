package dev.logforge.replication;

import dev.logforge.TopicPartition;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Requests that cannot be answered yet: {@code acks=all} produces waiting for the high watermark,
 * and long-poll fetches waiting for data. Each operation watches the partitions it depends on;
 * whenever a partition's log end or high watermark moves, the broker calls {@link
 * #checkAndComplete} for it. Every operation also has a timeout, so none can hang a connection.
 */
public final class DelayedOperations implements Closeable {

  /** One parked request. Completes exactly once: when its condition holds, or on timeout. */
  public abstract static class Operation {
    private final AtomicBoolean completed = new AtomicBoolean();
    private volatile ScheduledFuture<?> timeoutTask;
    private volatile Collection<TopicPartition> keys = List.of();
    private volatile DelayedOperations owner;

    /** True when the response can be produced now. */
    protected abstract boolean canComplete();

    /** Builds and sends the response. Called once. */
    protected abstract void onComplete(boolean expired);

    public boolean isCompleted() {
      return completed.get();
    }

    boolean tryComplete() {
      return !completed.get() && canComplete() && forceComplete(false);
    }

    boolean forceComplete(boolean expired) {
      if (!completed.compareAndSet(false, true)) {
        return false;
      }
      ScheduledFuture<?> t = timeoutTask;
      if (t != null) {
        t.cancel(false);
      }
      DelayedOperations o = owner;
      if (o != null) {
        o.unwatch(this, keys);
      }
      onComplete(expired);
      return true;
    }
  }

  private final ScheduledExecutorService timer;
  private final Map<TopicPartition, Set<Operation>> watchers = new ConcurrentHashMap<>();

  public DelayedOperations() {
    this.timer =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "logforge-purgatory");
              t.setDaemon(true);
              return t;
            });
  }

  /** Parks {@code op} until it completes or {@code timeoutMs} passes. */
  public void watch(Operation op, Collection<TopicPartition> keys, long timeoutMs) {
    op.owner = this;
    op.keys = List.copyOf(keys);
    for (TopicPartition k : op.keys) {
      watchers.computeIfAbsent(k, x -> ConcurrentHashMap.newKeySet()).add(op);
    }
    op.timeoutTask =
        timer.schedule(() -> op.forceComplete(true), Math.max(0, timeoutMs), TimeUnit.MILLISECONDS);
    // The condition may have become true between the caller's check and registration.
    op.tryComplete();
  }

  /** Re-checks every operation watching {@code key}. */
  public void checkAndComplete(TopicPartition key) {
    Set<Operation> ops = watchers.get(key);
    if (ops == null) {
      return;
    }
    for (Operation op : List.copyOf(ops)) {
      op.tryComplete();
    }
  }

  private void unwatch(Operation op, Collection<TopicPartition> keys) {
    for (TopicPartition k : keys) {
      Set<Operation> ops = watchers.get(k);
      if (ops != null) {
        ops.remove(op);
        if (ops.isEmpty()) {
          watchers.remove(k, ops);
        }
      }
    }
  }

  public int pending() {
    int n = 0;
    for (Set<Operation> s : watchers.values()) {
      n += s.size();
    }
    return n;
  }

  @Override
  public void close() {
    for (Set<Operation> ops : watchers.values()) {
      for (Operation op : List.copyOf(ops)) {
        op.forceComplete(true);
      }
    }
    timer.shutdownNow();
  }
}
