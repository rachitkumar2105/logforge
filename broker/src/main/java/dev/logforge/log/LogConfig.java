package dev.logforge.log;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-partition log settings. Topic-level keys (see PROTOCOL.md 5.4) map onto these fields.
 *
 * @param segmentBytes roll the active segment once it reaches this size
 * @param segmentMs roll the active segment once it is this old (if non-empty)
 * @param indexIntervalBytes write one index entry per this many bytes of log
 * @param maxIndexBytes pre-allocated size of an active segment's index
 * @param retentionMs delete whole segments whose newest record is older than this (-1: never)
 * @param retentionBytes delete oldest segments while the partition is larger than this (-1: never)
 * @param compact true for {@code cleanup.policy=compact}
 * @param deleteRetentionMs how long a tombstone survives compaction
 * @param minInsyncReplicas acks=all is refused when the ISR is smaller than this
 * @param maxBatchBytes largest accepted record batch
 * @param fsync when appends are forced to stable storage
 */
public record LogConfig(
    int segmentBytes,
    long segmentMs,
    int indexIntervalBytes,
    int maxIndexBytes,
    long retentionMs,
    long retentionBytes,
    boolean compact,
    long deleteRetentionMs,
    int minInsyncReplicas,
    int maxBatchBytes,
    FsyncPolicy fsync) {

  public static LogConfig defaults() {
    return new LogConfig(
        128 * 1024 * 1024,
        7L * 24 * 3600 * 1000,
        4096,
        10 * 1024 * 1024,
        7L * 24 * 3600 * 1000,
        -1L,
        false,
        24L * 3600 * 1000,
        1,
        1024 * 1024,
        FsyncPolicy.defaultPolicy());
  }

  public LogConfig withSegmentBytes(int v) {
    return new LogConfig(
        v,
        segmentMs,
        indexIntervalBytes,
        maxIndexBytes,
        retentionMs,
        retentionBytes,
        compact,
        deleteRetentionMs,
        minInsyncReplicas,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withSegmentMs(long v) {
    return new LogConfig(
        segmentBytes,
        v,
        indexIntervalBytes,
        maxIndexBytes,
        retentionMs,
        retentionBytes,
        compact,
        deleteRetentionMs,
        minInsyncReplicas,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withIndexIntervalBytes(int v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        v,
        maxIndexBytes,
        retentionMs,
        retentionBytes,
        compact,
        deleteRetentionMs,
        minInsyncReplicas,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withMaxIndexBytes(int v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        indexIntervalBytes,
        v,
        retentionMs,
        retentionBytes,
        compact,
        deleteRetentionMs,
        minInsyncReplicas,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withRetentionMs(long v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        indexIntervalBytes,
        maxIndexBytes,
        v,
        retentionBytes,
        compact,
        deleteRetentionMs,
        minInsyncReplicas,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withRetentionBytes(long v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        indexIntervalBytes,
        maxIndexBytes,
        retentionMs,
        v,
        compact,
        deleteRetentionMs,
        minInsyncReplicas,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withCompact(boolean v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        indexIntervalBytes,
        maxIndexBytes,
        retentionMs,
        retentionBytes,
        v,
        deleteRetentionMs,
        minInsyncReplicas,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withDeleteRetentionMs(long v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        indexIntervalBytes,
        maxIndexBytes,
        retentionMs,
        retentionBytes,
        compact,
        v,
        minInsyncReplicas,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withMinInsyncReplicas(int v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        indexIntervalBytes,
        maxIndexBytes,
        retentionMs,
        retentionBytes,
        compact,
        deleteRetentionMs,
        v,
        maxBatchBytes,
        fsync);
  }

  public LogConfig withMaxBatchBytes(int v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        indexIntervalBytes,
        maxIndexBytes,
        retentionMs,
        retentionBytes,
        compact,
        deleteRetentionMs,
        minInsyncReplicas,
        v,
        fsync);
  }

  public LogConfig withFsync(FsyncPolicy v) {
    return new LogConfig(
        segmentBytes,
        segmentMs,
        indexIntervalBytes,
        maxIndexBytes,
        retentionMs,
        retentionBytes,
        compact,
        deleteRetentionMs,
        minInsyncReplicas,
        maxBatchBytes,
        v);
  }

  /** Applies topic configs (string keys from CreateTopics) on top of these defaults. */
  public LogConfig withTopicConfigs(Map<String, String> configs) {
    LogConfig c = this;
    for (Map.Entry<String, String> e : configs.entrySet()) {
      String v = e.getValue();
      c =
          switch (e.getKey()) {
            case "segment.bytes" -> c.withSegmentBytes(Integer.parseInt(v));
            case "segment.ms" -> c.withSegmentMs(Long.parseLong(v));
            case "index.interval.bytes" -> c.withIndexIntervalBytes(Integer.parseInt(v));
            case "retention.ms" -> c.withRetentionMs(Long.parseLong(v));
            case "retention.bytes" -> c.withRetentionBytes(Long.parseLong(v));
            case "cleanup.policy" -> c.withCompact(parsePolicy(v));
            case "delete.retention.ms" -> c.withDeleteRetentionMs(Long.parseLong(v));
            case "min.insync.replicas" -> c.withMinInsyncReplicas(Integer.parseInt(v));
            case "max.batch.bytes" -> c.withMaxBatchBytes(Integer.parseInt(v));
            default -> throw new IllegalArgumentException("unknown topic config " + e.getKey());
          };
    }
    c.validate();
    return c;
  }

  private static boolean parsePolicy(String v) {
    return switch (v) {
      case "compact" -> true;
      case "delete" -> false;
      default -> throw new IllegalArgumentException("cleanup.policy must be delete or compact");
    };
  }

  public void validate() {
    if (segmentBytes < 1024) {
      throw new IllegalArgumentException("segment.bytes must be >= 1024");
    }
    if (indexIntervalBytes < 1) {
      throw new IllegalArgumentException("index.interval.bytes must be >= 1");
    }
    if (minInsyncReplicas < 1) {
      throw new IllegalArgumentException("min.insync.replicas must be >= 1");
    }
    if (maxBatchBytes < RecordBatch.HEADER_SIZE) {
      throw new IllegalArgumentException("max.batch.bytes too small");
    }
  }

  public Map<String, String> toTopicConfigs() {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("cleanup.policy", compact ? "compact" : "delete");
    m.put("segment.bytes", Integer.toString(segmentBytes));
    m.put("segment.ms", Long.toString(segmentMs));
    m.put("index.interval.bytes", Integer.toString(indexIntervalBytes));
    m.put("retention.ms", Long.toString(retentionMs));
    m.put("retention.bytes", Long.toString(retentionBytes));
    m.put("delete.retention.ms", Long.toString(deleteRetentionMs));
    m.put("min.insync.replicas", Integer.toString(minInsyncReplicas));
    m.put("max.batch.bytes", Integer.toString(maxBatchBytes));
    return m;
  }
}
