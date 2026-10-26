package dev.logforge.client;

import dev.logforge.util.Murmur2;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Picks a partition for a record (PROTOCOL.md 5.16).
 *
 * <ul>
 *   <li>Non-null key: {@code (murmur2(key) & 0x7fffffff) % numPartitions}, so a key always lands on
 *       the same partition (and in the same order) while the partition count is unchanged.
 *   <li>Null key: round-robin from a random starting point. Spreads load evenly; no ordering
 *       promise between records.
 * </ul>
 */
public final class Partitioner {
  private final AtomicInteger counter = new AtomicInteger(ThreadLocalRandom.current().nextInt());

  public int partition(byte[] key, int numPartitions) {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("numPartitions must be positive");
    }
    if (key == null) {
      return Math.floorMod(counter.getAndIncrement(), numPartitions);
    }
    return Murmur2.partitionFor(key, numPartitions);
  }
}
