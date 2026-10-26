package dev.logforge.log;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-partition state for idempotent producers: for each producer id, its current epoch and the
 * sequence ranges of its last {@value #WINDOW} batches.
 *
 * <p>How deduplication works: a producer numbers its records per partition (0, 1, 2, ...) and
 * stamps each batch with the sequence of its first record. If the producer times out waiting for an
 * ack, it resends the exact same batch with the exact same sequence. When that retry arrives, the
 * leader finds a batch with the same first and last sequence in this window and answers with the
 * offsets it already assigned, appending nothing. A sequence that skips ahead means a batch was
 * lost in between, which is rejected rather than silently creating a gap.
 *
 * <p>The window is a few batches, not the whole history, because the producer keeps at most one
 * batch in flight per partition and only retries recent batches. The state is rebuilt from batch
 * headers on startup, so a retry that arrives after a broker restart is still recognised, and
 * followers build the same state from replicated batches, so it survives leader failover.
 */
public final class ProducerStateManager {
  static final int WINDOW = 5;

  private record BatchMeta(int firstSeq, int lastSeq, long firstOffset, long lastOffset) {}

  private static final class ProducerEntry {
    short epoch;
    final Deque<BatchMeta> batches = new ArrayDeque<>();
  }

  /** What to do with an incoming batch. */
  public sealed interface Check {}

  /** Append it. */
  public record Accept() implements Check {}

  /** Already appended: answer with these offsets. */
  public record Duplicate(long firstOffset, long lastOffset) implements Check {}

  private final Map<Long, ProducerEntry> producers = new HashMap<>();

  /** Decides whether an idempotent batch is new, a duplicate, or invalid. */
  public synchronized Check check(RecordBatch batch) {
    long pid = batch.producerId();
    if (pid < 0) {
      return new Accept();
    }
    ProducerEntry e = producers.get(pid);
    if (e == null) {
      return new Accept();
    }
    short epoch = batch.producerEpoch();
    if (epoch < e.epoch) {
      throw new InvalidProducerEpochException(
          "producer " + pid + " epoch " + epoch + " is older than " + e.epoch);
    }
    if (epoch > e.epoch) {
      if (batch.baseSequence() != 0) {
        throw new OutOfOrderSequenceException(
            "producer " + pid + " new epoch " + epoch + " must start at sequence 0");
      }
      return new Accept();
    }
    for (BatchMeta m : e.batches) {
      if (m.firstSeq() == batch.baseSequence() && m.lastSeq() == batch.lastSequence()) {
        return new Duplicate(m.firstOffset(), m.lastOffset());
      }
    }
    int expected = e.batches.isEmpty() ? batch.baseSequence() : e.batches.peekLast().lastSeq() + 1;
    if (batch.baseSequence() != expected) {
      throw new OutOfOrderSequenceException(
          "producer " + pid + " expected sequence " + expected + ", got " + batch.baseSequence());
    }
    return new Accept();
  }

  /** Records an appended batch (on the leader after append, on followers when replicating). */
  public synchronized void update(RecordBatch batch) {
    long pid = batch.producerId();
    if (pid < 0) {
      return;
    }
    ProducerEntry e = producers.computeIfAbsent(pid, k -> new ProducerEntry());
    if (batch.producerEpoch() > e.epoch || e.batches.isEmpty()) {
      if (batch.producerEpoch() != e.epoch) {
        e.batches.clear();
      }
      e.epoch = batch.producerEpoch();
    }
    e.batches.addLast(
        new BatchMeta(
            batch.baseSequence(), batch.lastSequence(), batch.baseOffset(), batch.lastOffset()));
    while (e.batches.size() > WINDOW) {
      e.batches.removeFirst();
    }
  }

  public synchronized void clear() {
    producers.clear();
  }

  public synchronized int producerCount() {
    return producers.size();
  }
}
