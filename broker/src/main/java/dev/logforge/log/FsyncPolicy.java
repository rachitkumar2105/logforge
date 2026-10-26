package dev.logforge.log;

/**
 * When the log calls {@code fsync} ({@code FileChannel.force}).
 *
 * <p>Every append is written into the OS page cache immediately. That alone survives a process
 * crash (the kernel owns those pages, not the JVM), so {@code SIGKILL} never loses an acknowledged
 * record under any policy. fsync only matters for power loss or a kernel crash:
 *
 * <ul>
 *   <li>{@code never}: the OS flushes when it chooses (typically within ~30 s on Linux). Highest
 *       throughput. A single-broker power cut can lose that window; with replication the data
 *       survives if any in-sync replica's machine kept power, which is Kafka's default stance.
 *   <li>{@code records:N}: fsync after every N appended records. N=1 is the safest and slowest
 *       setting (one disk flush per batch on the hot path).
 *   <li>{@code ms:N}: fsync at most every N milliseconds (also driven by a background flusher so an
 *       idle log is flushed). Bounds power-loss exposure to roughly N ms of acknowledged writes.
 * </ul>
 *
 * <p>The default is {@code ms:1000}: a one-second bounded loss window on power failure for a single
 * broker, at a cost of one flush per second per partition.
 */
public sealed interface FsyncPolicy {
  /** The explicit default: {@code ms:1000}. */
  static FsyncPolicy defaultPolicy() {
    return new EveryMs(1000);
  }

  record Never() implements FsyncPolicy {
    @Override
    public String toString() {
      return "never";
    }
  }

  record EveryRecords(long records) implements FsyncPolicy {
    public EveryRecords {
      if (records < 1) {
        throw new IllegalArgumentException("records must be >= 1");
      }
    }

    @Override
    public String toString() {
      return "records:" + records;
    }
  }

  record EveryMs(long millis) implements FsyncPolicy {
    public EveryMs {
      if (millis < 1) {
        throw new IllegalArgumentException("millis must be >= 1");
      }
    }

    @Override
    public String toString() {
      return "ms:" + millis;
    }
  }

  /** Parses {@code never}, {@code records:N} or {@code ms:N}. */
  static FsyncPolicy parse(String text) {
    String s = text.trim().toLowerCase(java.util.Locale.ROOT);
    if (s.equals("never")) {
      return new Never();
    }
    int colon = s.indexOf(':');
    if (colon > 0) {
      String kind = s.substring(0, colon);
      long n = Long.parseLong(s.substring(colon + 1));
      if (kind.equals("records")) {
        return new EveryRecords(n);
      }
      if (kind.equals("ms")) {
        return new EveryMs(n);
      }
    }
    throw new IllegalArgumentException(
        "fsync policy must be never, records:N or ms:N, got " + text);
  }

  /** Should the log flush now, given what has happened since the last flush? */
  default boolean shouldFlush(long unflushedRecords, long msSinceLastFlush) {
    return switch (this) {
      case Never n -> false;
      case EveryRecords r -> unflushedRecords >= r.records();
      case EveryMs m -> unflushedRecords > 0 && msSinceLastFlush >= m.millis();
    };
  }
}
