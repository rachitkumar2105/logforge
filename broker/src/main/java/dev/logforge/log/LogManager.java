package dev.logforge.log;

import dev.logforge.TopicPartition;
import dev.logforge.util.JsonLog;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * Owns every partition log on this broker, under one data directory. Runs the periodic
 * housekeeping: fsync for the {@code ms:N} policy, retention, and compaction.
 */
public final class LogManager implements Closeable {
  private static final JsonLog LOG = JsonLog.get(LogManager.class);

  private final Path dataDir;
  private final LongSupplier clock;
  private final Map<TopicPartition, Log> logs = new ConcurrentHashMap<>();

  public LogManager(Path dataDir, LongSupplier clock) throws IOException {
    this.dataDir = dataDir;
    this.clock = clock;
    Files.createDirectories(dataDir);
  }

  public Path dataDir() {
    return dataDir;
  }

  /** Partition directories found on disk at startup (logs are opened when assigned). */
  public List<TopicPartition> partitionsOnDisk() throws IOException {
    try (Stream<Path> files = Files.list(dataDir)) {
      return files
          .filter(Files::isDirectory)
          .map(p -> p.getFileName().toString())
          .filter(
              n -> n.lastIndexOf('-') > 0 && n.substring(n.lastIndexOf('-') + 1).matches("\\d+"))
          .map(TopicPartition::fromDirName)
          .toList();
    }
  }

  /** Opens (and recovers) the log, creating it if needed. */
  public Log getOrCreate(TopicPartition tp, LogConfig config) throws IOException {
    Log existing = logs.get(tp);
    if (existing != null) {
      return existing;
    }
    synchronized (this) {
      existing = logs.get(tp);
      if (existing != null) {
        return existing;
      }
      Log log = Log.open(dataDir.resolve(tp.dirName()), config, clock);
      if (log.lastRecovery().truncatedBytes() > 0) {
        LOG.warn(
            "recovery truncated a torn tail",
            "partition",
            tp.toString(),
            "bytes",
            log.lastRecovery().truncatedBytes());
      }
      logs.put(tp, log);
      return log;
    }
  }

  public Log get(TopicPartition tp) {
    return logs.get(tp);
  }

  public Map<TopicPartition, Log> all() {
    return Map.copyOf(logs);
  }

  /** Closes and removes a partition's files. */
  public void delete(TopicPartition tp) throws IOException {
    Log log = logs.remove(tp);
    if (log != null) {
      log.delete();
    } else {
      Path dir = dataDir.resolve(tp.dirName());
      if (Files.isDirectory(dir)) {
        try (Stream<Path> files = Files.list(dir)) {
          for (Path f : files.toList()) {
            Files.deleteIfExists(f);
          }
        }
        Files.deleteIfExists(dir);
      }
    }
  }

  /** Called often (every ~100 ms): applies the {@code ms:N} fsync policy to idle logs. */
  public void flushTick() {
    for (Log log : logs.values()) {
      try {
        log.maybeFlushOnTimer();
      } catch (RuntimeException e) {
        LOG.error("flush failed", e, "dir", log.dir().getFileName().toString());
      }
    }
  }

  /**
   * Applies retention to every log. {@code highWatermark} gives, per partition, the offset below
   * which data is replicated; nothing at or above it is deleted.
   */
  public int retentionTick(ToLongFunction<TopicPartition> highWatermark) {
    int deleted = 0;
    long now = clock.getAsLong();
    for (Map.Entry<TopicPartition, Log> e : logs.entrySet()) {
      try {
        deleted += e.getValue().deleteRetentionExpired(now, highWatermark.applyAsLong(e.getKey()));
      } catch (IOException | RuntimeException ex) {
        LOG.error("retention failed", ex, "partition", e.getKey().toString());
      }
    }
    return deleted;
  }

  @Override
  public void close() throws IOException {
    IOException first = null;
    for (Log log : logs.values()) {
      try {
        log.close();
      } catch (IOException e) {
        if (first == null) {
          first = e;
        }
      }
    }
    logs.clear();
    if (first != null) {
      throw first;
    }
  }
}
