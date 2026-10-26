package dev.logforge.log;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * A partition's log: an ordered list of segments, the last of which is active.
 *
 * <ul>
 *   <li><b>Append</b> goes to the active segment; the log rolls to a new segment when the active
 *       one would exceed {@code segment.bytes} or is older than {@code segment.ms}.
 *   <li><b>Read</b> from any offset: segment (floor lookup in a skip list keyed by base offset),
 *       then the segment's sparse index, then a short forward scan.
 *   <li><b>Recovery</b> on open: the active segment is CRC-scanned and truncated at the first
 *       invalid batch; sealed segments get their index rebuilt if it is missing or short.
 *   <li><b>Retention</b> deletes whole sealed segments by age or total size. The active segment is
 *       never deleted, and neither is anything at or above the high watermark (a follower may still
 *       need it).
 *   <li><b>Truncation</b> removes the tail, used when a follower's log diverged from its leader.
 * </ul>
 *
 * <p>Writers (append, roll, truncate, retention, flush) hold {@link #lock}. Readers take no lock:
 * they read the volatile log end and the concurrent segment map, and each segment publishes its
 * size only after a write completes.
 */
public final class Log implements Closeable {
  private final Path dir;
  private final LogConfig config;
  private final LongSupplier clock;
  private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
  private final ReentrantLock lock = new ReentrantLock();
  private final LeaderEpochCache epochCache = new LeaderEpochCache();
  private final ProducerStateManager producerState = new ProducerStateManager();

  private volatile long logStartOffset;
  private volatile long logEndOffset;
  // Guarded by lock.
  private long unflushedRecords;
  private long flushCount;
  private long lastFlushMs;
  private long activeSegmentStartMs;
  private LogSegment.RecoveryResult lastRecovery = new LogSegment.RecoveryResult(0, 0);

  private Log(Path dir, LogConfig config, LongSupplier clock) {
    this.dir = dir;
    this.config = config;
    this.clock = clock;
  }

  public static Log open(Path dir, LogConfig config) throws IOException {
    return open(dir, config, System::currentTimeMillis);
  }

  /** Opens (creating if needed) the log in {@code dir} and runs recovery. */
  public static Log open(Path dir, LogConfig config, LongSupplier clock) throws IOException {
    config.validate();
    Log log = new Log(dir, config, clock);
    log.load();
    return log;
  }

  private void load() throws IOException {
    Files.createDirectories(dir);
    List<Long> bases = new ArrayList<>();
    try (Stream<Path> files = Files.list(dir)) {
      for (Path f : files.toList()) {
        String name = f.getFileName().toString();
        if (name.endsWith(".cleaned") || name.endsWith(".deleted")) {
          // Leftover of an interrupted compaction or delete: the original is still authoritative.
          Files.deleteIfExists(f);
        } else if (name.endsWith(".log")) {
          bases.add(Long.parseLong(name.substring(0, name.length() - 4)));
        }
      }
    }
    try (Stream<Path> files = Files.list(dir)) {
      for (Path f : files.toList()) {
        String name = f.getFileName().toString();
        if (name.endsWith(".index")) {
          long base = Long.parseLong(name.substring(0, name.length() - 6));
          if (!bases.contains(base)) {
            Files.deleteIfExists(f); // orphan index
          }
        }
      }
    }
    bases.sort(null);
    if (bases.isEmpty()) {
      segments.put(0L, LogSegment.create(dir, 0L, config));
    } else {
      try {
        for (int i = 0; i < bases.size(); i++) {
          long base = bases.get(i);
          boolean active = i == bases.size() - 1;
          LogSegment seg =
              active
                  ? LogSegment.open(dir, base, config)
                  : LogSegment.openSealed(dir, base, config);
          segments.put(base, seg);
          if (active) {
            lastRecovery = seg.recover();
          }
        }
      } catch (IOException | RuntimeException e) {
        for (LogSegment s : segments.values()) {
          try {
            s.close();
          } catch (IOException suppressed) {
            e.addSuppressed(suppressed);
          }
        }
        throw e;
      }
    }
    logStartOffset = segments.firstKey();
    logEndOffset = activeSegment().nextOffset();
    activeSegmentStartMs = clock.getAsLong();
    lastFlushMs = clock.getAsLong();
    rebuildDerivedState();
  }

  /** Rebuilds the leader-epoch history and producer state from batch headers. */
  private void rebuildDerivedState() throws IOException {
    epochCache.clear();
    producerState.clear();
    for (LogSegment seg : segments.values()) {
      seg.forEachBatch(
          (batch, position) -> {
            epochCache.assign(batch.partitionLeaderEpoch(), batch.baseOffset());
            producerState.update(batch);
            seg.observeTimestamp(batch.maxTimestamp());
            return true;
          });
    }
  }

  private LogSegment activeSegment() {
    return segments.lastEntry().getValue();
  }

  /**
   * Appends a batch from a producer: validates it, checks idempotence, assigns offsets starting at
   * the log end and stamps the leader epoch.
   */
  public LogAppendInfo appendAsLeader(RecordBatch batch, int leaderEpoch) {
    if (batch.sizeInBytes() > config.maxBatchBytes()) {
      throw new RecordTooLargeException(
          "batch of "
              + batch.sizeInBytes()
              + " bytes exceeds max.batch.bytes "
              + config.maxBatchBytes());
    }
    batch.validate();
    lock.lock();
    try {
      if (producerState.check(batch) instanceof ProducerStateManager.Duplicate d) {
        return new LogAppendInfo(d.firstOffset(), d.lastOffset(), batch.recordCount(), 0, true);
      }
      batch.setBaseOffset(logEndOffset);
      batch.setPartitionLeaderEpoch(leaderEpoch);
      return appendLocked(batch);
    } finally {
      lock.unlock();
    }
  }

  /** Appends a batch copied from the leader, keeping its offsets and epoch. */
  public LogAppendInfo appendAsFollower(RecordBatch batch) {
    batch.validate();
    lock.lock();
    try {
      if (batch.baseOffset() < logEndOffset) {
        throw new IllegalArgumentException(
            "follower append at " + batch.baseOffset() + " below log end " + logEndOffset);
      }
      return appendLocked(batch);
    } finally {
      lock.unlock();
    }
  }

  private LogAppendInfo appendLocked(RecordBatch batch) {
    try {
      maybeRoll(batch);
      activeSegment().append(batch);
    } catch (UncheckedIOException | IOException e) {
      throw new LogStorageException("append failed in " + dir.getFileName(), e);
    }
    epochCache.assign(batch.partitionLeaderEpoch(), batch.baseOffset());
    producerState.update(batch);
    logEndOffset = batch.nextOffset();
    unflushedRecords += batch.recordCount();
    long now = clock.getAsLong();
    if (config.fsync().shouldFlush(unflushedRecords, now - lastFlushMs)) {
      flushLocked(now);
    }
    return new LogAppendInfo(
        batch.baseOffset(), batch.lastOffset(), batch.recordCount(), batch.sizeInBytes(), false);
  }

  private void maybeRoll(RecordBatch batch) throws IOException {
    LogSegment active = activeSegment();
    if (active.isEmpty()) {
      return;
    }
    boolean full = (long) active.sizeInBytes() + batch.sizeInBytes() > config.segmentBytes();
    boolean old = clock.getAsLong() - activeSegmentStartMs >= config.segmentMs();
    boolean indexFull = active.index().isFull();
    boolean offsetOverflow = batch.lastOffset() - active.baseOffset() > Integer.MAX_VALUE;
    if (full || old || indexFull || offsetOverflow) {
      roll();
    }
  }

  /** Seals the active segment and starts a new one at the log end. */
  public void roll() throws IOException {
    lock.lock();
    try {
      LogSegment active = activeSegment();
      if (active.isEmpty()) {
        return;
      }
      active.seal();
      long base = logEndOffset;
      segments.put(base, LogSegment.create(dir, base, config));
      activeSegmentStartMs = clock.getAsLong();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Whole batches starting with the one that contains {@code offset}, up to {@code maxBytes} (at
   * least one batch if any), never including a batch that reaches {@code maxOffsetExclusive}.
   */
  public ByteBuffer read(long offset, int maxBytes, long maxOffsetExclusive) {
    for (int attempt = 0; attempt < 2; attempt++) {
      long start = logStartOffset;
      long end = logEndOffset;
      if (offset < start || offset > end) {
        throw new OffsetOutOfRangeException(
            "offset " + offset + " outside [" + start + ", " + end + "] in " + dir.getFileName());
      }
      if (offset == end) {
        return ByteBuffer.allocate(0);
      }
      Long floor = segments.floorKey(offset);
      Map<Long, LogSegment> candidates = floor == null ? segments : segments.tailMap(floor, true);
      try {
        for (LogSegment seg : candidates.values()) {
          ByteBuffer buf = seg.read(offset, maxBytes, maxOffsetExclusive);
          if (buf.hasRemaining()) {
            return buf;
          }
        }
        return ByteBuffer.allocate(0);
      } catch (UncheckedIOException e) {
        if (!(e.getCause() instanceof ClosedChannelException)) {
          throw e;
        }
        // Retention or truncation closed the segment under us. Retry once with fresh bounds.
      }
    }
    throw new OffsetOutOfRangeException("offset " + offset + " was removed during the read");
  }

  /** Removes every record at or after {@code offset}. Used by followers whose tail diverged. */
  public void truncateTo(long offset) throws IOException {
    lock.lock();
    try {
      if (offset >= logEndOffset) {
        return;
      }
      if (offset < logStartOffset) {
        truncateFullyAndStartAt(offset);
        return;
      }
      // Segments that start at or after the target go entirely.
      for (Long base : List.copyOf(segments.tailMap(offset, true).keySet())) {
        segments.remove(base).delete();
      }
      if (segments.isEmpty()) {
        segments.put(offset, LogSegment.create(dir, offset, config));
        logStartOffset = offset;
      } else {
        // The segment that straddles the target loses its tail and becomes active again.
        LogSegment active = activeSegment();
        active.truncateTo(offset);
        active.reopenForAppends();
      }
      logEndOffset = activeSegment().nextOffset();
      activeSegmentStartMs = clock.getAsLong();
      rebuildDerivedState();
    } finally {
      lock.unlock();
    }
  }

  /** Deletes everything and restarts the log at {@code offset}. */
  public void truncateFullyAndStartAt(long offset) throws IOException {
    lock.lock();
    try {
      for (LogSegment seg : List.copyOf(segments.values())) {
        seg.delete();
      }
      segments.clear();
      segments.put(offset, LogSegment.create(dir, offset, config));
      logStartOffset = offset;
      logEndOffset = offset;
      activeSegmentStartMs = clock.getAsLong();
      epochCache.clear();
      producerState.clear();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Applies retention. Deletes sealed segments, oldest first, while the oldest is expired by age or
   * the log is over its size limit. Stops at the first segment that is not eligible, at the active
   * segment, and at any segment holding offsets at or above {@code maxDeletableOffset}.
   *
   * @return number of segments deleted
   */
  public int deleteRetentionExpired(long nowMs, long maxDeletableOffset) throws IOException {
    if (config.compact()) {
      return 0;
    }
    lock.lock();
    try {
      long total = sizeInBytes();
      int deleted = 0;
      while (segments.size() > 1) {
        LogSegment oldest = segments.firstEntry().getValue();
        if (oldest.nextOffset() > maxDeletableOffset) {
          break;
        }
        boolean expired =
            config.retentionMs() >= 0 && nowMs - oldest.maxTimestamp() > config.retentionMs();
        boolean oversize =
            config.retentionBytes() >= 0 && total - oldest.sizeInBytes() >= config.retentionBytes();
        if (!expired && !oversize) {
          break;
        }
        segments.remove(oldest.baseOffset());
        logStartOffset = segments.firstKey();
        total -= oldest.sizeInBytes();
        oldest.delete();
        deleted++;
      }
      return deleted;
    } finally {
      lock.unlock();
    }
  }

  /** Forces the active segment to disk. */
  public void flush() throws IOException {
    lock.lock();
    try {
      flushLocked(clock.getAsLong());
    } finally {
      lock.unlock();
    }
  }

  private void flushLocked(long now) {
    try {
      activeSegment().flush();
    } catch (IOException e) {
      throw new LogStorageException("flush failed in " + dir.getFileName(), e);
    }
    unflushedRecords = 0;
    lastFlushMs = now;
    flushCount++;
  }

  /** Called periodically by the broker so the {@code ms:N} policy also flushes idle logs. */
  public void maybeFlushOnTimer() {
    lock.lock();
    try {
      long now = clock.getAsLong();
      if (config.fsync().shouldFlush(unflushedRecords, now - lastFlushMs)) {
        flushLocked(now);
      }
    } finally {
      lock.unlock();
    }
  }

  public long logStartOffset() {
    return logStartOffset;
  }

  public long logEndOffset() {
    return logEndOffset;
  }

  public long sizeInBytes() {
    long total = 0;
    for (LogSegment s : segments.values()) {
      total += s.sizeInBytes();
    }
    return total;
  }

  public List<LogSegmentInfo> segmentInfos() {
    List<LogSegmentInfo> out = new ArrayList<>();
    LogSegment active = activeSegment();
    for (LogSegment s : segments.values()) {
      out.add(
          new LogSegmentInfo(
              s.baseOffset(),
              s.nextOffset(),
              s.sizeInBytes(),
              s.index().entryCount(),
              s.maxTimestamp(),
              s == active));
    }
    return out;
  }

  /** Segments in offset order. The last one is active. For the compactor. */
  List<LogSegment> segmentsSnapshot() {
    return List.copyOf(segments.values());
  }

  ReentrantLock lock() {
    return lock;
  }

  /** Replaces a sealed segment with a compacted copy. Caller holds {@link #lock}. */
  void replaceSegment(long baseOffset, LogSegment replacement) {
    segments.put(baseOffset, replacement);
  }

  public LeaderEpochCache epochCache() {
    return epochCache;
  }

  public ProducerStateManager producerState() {
    return producerState;
  }

  public LogSegment.RecoveryResult lastRecovery() {
    return lastRecovery;
  }

  public long unflushedRecords() {
    lock.lock();
    try {
      return unflushedRecords;
    } finally {
      lock.unlock();
    }
  }

  public long flushCount() {
    lock.lock();
    try {
      return flushCount;
    } finally {
      lock.unlock();
    }
  }

  public LogConfig config() {
    return config;
  }

  public Path dir() {
    return dir;
  }

  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      IOException first = null;
      for (LogSegment s : segments.values()) {
        try {
          s.flush();
          s.close();
        } catch (IOException e) {
          if (first == null) {
            first = e;
          }
        }
      }
      if (first != null) {
        throw first;
      }
    } finally {
      lock.unlock();
    }
  }

  /** Closes and removes the partition directory. */
  public void delete() throws IOException {
    lock.lock();
    try {
      for (LogSegment s : segments.values()) {
        s.delete();
      }
      segments.clear();
      try (Stream<Path> files = Files.list(dir)) {
        for (Path f : files.toList()) {
          Files.deleteIfExists(f);
        }
      }
      Files.deleteIfExists(dir);
    } finally {
      lock.unlock();
    }
  }
}
