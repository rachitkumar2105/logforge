package dev.logforge.log;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * One segment of a partition: an append-only {@code .log} file of record batches plus its sparse
 * {@code .index}.
 *
 * <h2>Why appends are fast</h2>
 *
 * Every append is one positional write at the end of the file. The kernel copies it into the page
 * cache and returns; the disk sees large sequential writes when the kernel flushes, which even a
 * spinning disk does at full bandwidth, and which SSDs handle without write amplification from
 * random updates. There is no in-place update anywhere in this class.
 *
 * <h2>Why there is no custom read cache</h2>
 *
 * Reads go straight to {@link FileChannel#read(ByteBuffer, long)}. Recently written data is still
 * in the OS page cache, so consumers that keep up (the common case) read from RAM. A second cache
 * inside the JVM would hold the same bytes twice, put gigabytes on the garbage-collected heap, go
 * cold on every broker restart while the page cache stays warm, and need its own invalidation on
 * truncation. The page cache already does LRU-ish eviction across all partitions with zero GC cost,
 * so we deliberately lean on it.
 *
 * <h2>Concurrency</h2>
 *
 * One writer (the owning {@link Log}'s lock). Readers run concurrently and only look at bytes below
 * {@link #sizeInBytes()}, which is volatile and published after each write completes, so a reader
 * never sees a partially written batch.
 */
public final class LogSegment implements Closeable {
  private static final int SCAN_CHUNK = 64 * 1024;

  private final Path dir;
  private final long baseOffset;
  private final LogConfig config;
  private final FileChannel channel;
  private final OffsetIndex index;
  private final Path logFile;
  private final Path indexFile;

  private volatile int size;
  private volatile long nextOffset;
  private volatile long maxTimestamp = -1L;
  private int bytesSinceLastIndexEntry;
  private long createdAtMs;

  /** Outcome of {@link #recover()}. */
  public record RecoveryResult(int validBatches, long truncatedBytes) {}

  private LogSegment(Path dir, long baseOffset, LogConfig config, boolean create, boolean sealed)
      throws IOException {
    this.dir = dir;
    this.baseOffset = baseOffset;
    this.config = config;
    this.logFile = dir.resolve(fileName(baseOffset, ".log"));
    this.indexFile = dir.resolve(fileName(baseOffset, ".index"));
    if (create) {
      Files.deleteIfExists(logFile);
      Files.deleteIfExists(indexFile);
    }
    this.channel =
        FileChannel.open(
            logFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    this.index = new OffsetIndex(indexFile, baseOffset, config.maxIndexBytes(), !sealed);
    long fileSize = channel.size();
    if (fileSize > Integer.MAX_VALUE) {
      throw new IOException("segment larger than 2 GiB: " + logFile);
    }
    this.size = (int) fileSize;
    this.nextOffset = baseOffset;
    this.createdAtMs = System.currentTimeMillis();
    if (!create && size > 0) {
      boolean rebuild = !index.isConsistentWith(size);
      if (!rebuild && loadTail()) {
        // The index ends earlier than the data says it should ("shorter than the records file"),
        // for example after a crash between a log write and the index write reaching disk.
        rebuild = true;
      }
      if (rebuild) {
        if (sealed) {
          index.resize(config.maxIndexBytes());
        }
        rebuildIndex();
        if (sealed) {
          index.trimToValidSize();
        }
      }
      loadTail();
    }
  }

  public static String fileName(long baseOffset, String suffix) {
    return String.format("%020d%s", baseOffset, suffix);
  }

  /** Creates a new empty segment, replacing any files with the same base offset. */
  public static LogSegment create(Path dir, long baseOffset, LogConfig config) throws IOException {
    return new LogSegment(dir, baseOffset, config, true, false);
  }

  /**
   * Opens an existing segment as the active one (index pre-sized for appends). Call {@link
   * #recover()} before appending to a segment that may have been shut down uncleanly.
   */
  public static LogSegment open(Path dir, long baseOffset, LogConfig config) throws IOException {
    return new LogSegment(dir, baseOffset, config, false, false);
  }

  /**
   * Opens an existing sealed segment. Its index is mapped at its current size and only rewritten if
   * it is missing, inconsistent or short.
   */
  public static LogSegment openSealed(Path dir, long baseOffset, LogConfig config)
      throws IOException {
    return new LogSegment(dir, baseOffset, config, false, true);
  }

  public long baseOffset() {
    return baseOffset;
  }

  public Path logFile() {
    return logFile;
  }

  public Path indexFile() {
    return indexFile;
  }

  public OffsetIndex index() {
    return index;
  }

  public int sizeInBytes() {
    return size;
  }

  /** Offset that the next appended batch will start at (one past the last record here). */
  public long nextOffset() {
    return nextOffset;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  /** Newest record timestamp in this segment, or -1 if empty. Drives time-based retention. */
  public long maxTimestamp() {
    return maxTimestamp;
  }

  public long createdAtMs() {
    return createdAtMs;
  }

  /**
   * Appends a batch whose offsets are already assigned. The write goes into the page cache; the
   * fsync policy is applied by the owning {@link Log}, not here.
   */
  public void append(RecordBatch batch) {
    if (batch.baseOffset() < nextOffset) {
      throw new IllegalArgumentException(
          "batch base offset " + batch.baseOffset() + " below segment next offset " + nextOffset);
    }
    int position = size;
    ByteBuffer buf = batch.buffer();
    int bytes = buf.remaining();
    if ((long) position + bytes > Integer.MAX_VALUE) {
      throw new IllegalStateException("segment would exceed 2 GiB; roll first");
    }
    try {
      long written = 0;
      while (buf.hasRemaining()) {
        written += channel.write(buf, position + written);
      }
    } catch (IOException e) {
      // A failed write (disk full, IO error) can leave a partial batch on disk. Cut it off so the
      // file still ends on a batch boundary; recovery would do the same, but doing it now keeps
      // the live log valid without a restart.
      try {
        channel.truncate(position);
      } catch (IOException e2) {
        e.addSuppressed(e2);
      }
      throw new UncheckedIOException(e);
    }
    if (isEmpty()) {
      createdAtMs = System.currentTimeMillis();
    }
    if (bytesSinceLastIndexEntry >= config.indexIntervalBytes() && !index.isFull()) {
      index.append(batch.baseOffset(), position);
      bytesSinceLastIndexEntry = 0;
    }
    bytesSinceLastIndexEntry += bytes;
    maxTimestamp = Math.max(maxTimestamp, batch.maxTimestamp());
    nextOffset = batch.nextOffset();
    size = position + bytes; // publish: readers may now see this batch
  }

  /**
   * Byte position of the batch containing {@code offset} (or the first batch after it, if
   * compaction removed it), or -1 if no batch here reaches {@code offset}. Index floor lookup, then
   * a header-only forward scan.
   */
  public int translateOffset(long offset) {
    int limit = size;
    if (offset >= nextOffset) {
      return -1;
    }
    int pos = offset <= baseOffset ? 0 : index.lookup(offset).position();
    ByteBuffer header = ByteBuffer.allocate(RecordBatch.HEADER_SIZE);
    while (pos < limit) {
      readHeader(header, pos);
      long last =
          header.getLong(RecordBatch.BASE_OFFSET_OFFSET)
              + header.getInt(RecordBatch.LAST_OFFSET_DELTA_OFFSET);
      if (last >= offset) {
        return pos;
      }
      pos += header.getInt(RecordBatch.LENGTH_OFFSET) + RecordBatch.LOG_OVERHEAD;
    }
    return -1;
  }

  /**
   * Reads whole batches starting with the one containing {@code startOffset}, up to {@code
   * maxBytes} (but always at least one batch if one exists).
   */
  public ByteBuffer read(long startOffset, int maxBytes) {
    return read(startOffset, maxBytes, Long.MAX_VALUE);
  }

  /**
   * Like {@link #read(long, int)} but never returns a batch whose last offset is at or beyond
   * {@code maxOffsetExclusive} (the high watermark, for consumers).
   */
  public ByteBuffer read(long startOffset, int maxBytes, long maxOffsetExclusive) {
    int limit = size;
    int start = translateOffset(startOffset);
    if (start < 0) {
      return ByteBuffer.allocate(0);
    }
    ByteBuffer header = ByteBuffer.allocate(RecordBatch.HEADER_SIZE);
    readHeader(header, start);
    int firstSize = header.getInt(RecordBatch.LENGTH_OFFSET) + RecordBatch.LOG_OVERHEAD;
    long firstLast =
        header.getLong(RecordBatch.BASE_OFFSET_OFFSET)
            + header.getInt(RecordBatch.LAST_OFFSET_DELTA_OFFSET);
    if (firstLast >= maxOffsetExclusive) {
      return ByteBuffer.allocate(0);
    }
    int want = Math.max(firstSize, Math.min(maxBytes, limit - start));
    ByteBuffer buf = ByteBuffer.allocate(want);
    readFully(buf, start);
    buf.flip();
    // Walk the batches we read and cut at the last whole one that is below the cap.
    int end = 0;
    while (end < buf.limit()) {
      if (buf.limit() - end < RecordBatch.HEADER_SIZE) {
        break;
      }
      int batchSize = buf.getInt(end) + RecordBatch.LOG_OVERHEAD;
      long last =
          buf.getLong(end + RecordBatch.BASE_OFFSET_OFFSET)
              + buf.getInt(end + RecordBatch.LAST_OFFSET_DELTA_OFFSET);
      if (end + batchSize > buf.limit() || last >= maxOffsetExclusive) {
        break;
      }
      end += batchSize;
    }
    buf.limit(end);
    return buf;
  }

  /**
   * Validates every batch from the start of the file, truncates at the first invalid or incomplete
   * one, and rebuilds the index from the valid batches. Used on the active segment at startup,
   * where an unclean shutdown can leave a torn tail.
   */
  public RecoveryResult recover() throws IOException {
    index.reset();
    bytesSinceLastIndexEntry = 0;
    maxTimestamp = -1L;
    long fileSize = channel.size();
    int[] valid = {0};
    long[] expectedNext = {baseOffset};
    int validEnd =
        scan(
            0,
            (int) Math.min(fileSize, Integer.MAX_VALUE),
            true,
            (batch, position) -> {
              if (batch.baseOffset() < expectedNext[0]) {
                return false; // offsets went backwards: treat as corruption
              }
              if (bytesSinceLastIndexEntry >= config.indexIntervalBytes() && !index.isFull()) {
                index.append(batch.baseOffset(), position);
                bytesSinceLastIndexEntry = 0;
              }
              bytesSinceLastIndexEntry += batch.sizeInBytes();
              maxTimestamp = Math.max(maxTimestamp, batch.maxTimestamp());
              expectedNext[0] = batch.nextOffset();
              valid[0]++;
              return true;
            });
    long truncated = fileSize - validEnd;
    if (truncated > 0) {
      channel.truncate(validEnd);
    }
    size = validEnd;
    nextOffset = expectedNext[0];
    return new RecoveryResult(valid[0], truncated);
  }

  /** Visitor for {@link #forEachBatch}. Return false to stop. */
  public interface BatchVisitor {
    boolean visit(RecordBatch batch, int position) throws IOException;
  }

  /**
   * Visits every batch in order without CRC validation (data was validated on the way in). The
   * batch passed to the visitor is a view into a reused scan buffer: it is only valid during the
   * call and must be copied if kept.
   */
  public void forEachBatch(BatchVisitor visitor) throws IOException {
    scan(0, size, false, visitor);
  }

  /** Copies of every batch from {@code position} to the end. For tests and small segments. */
  public List<RecordBatch> batchesFrom(int position) throws IOException {
    List<RecordBatch> out = new ArrayList<>();
    scan(
        position,
        size,
        false,
        (b, p) -> {
          ByteBuffer copy = ByteBuffer.allocate(b.sizeInBytes());
          copy.put(b.buffer()).flip();
          return out.add(RecordBatch.wrap(copy));
        });
    return out;
  }

  /**
   * Reads batches in large chunks so a full-segment scan costs few syscalls. Returns the position
   * after the last batch the visitor accepted (and that was valid, when {@code validate}).
   */
  private int scan(int from, int limit, boolean validate, BatchVisitor visitor) throws IOException {
    int pos = from;
    ByteBuffer chunk = ByteBuffer.allocate(SCAN_CHUNK);
    int chunkStart = from;
    chunk.limit(0);
    while (pos < limit) {
      int offsetInChunk = pos - chunkStart;
      if (chunk.limit() - offsetInChunk < RecordBatch.HEADER_SIZE
          || chunk.limit() - offsetInChunk
              < chunk.getInt(offsetInChunk) + RecordBatch.LOG_OVERHEAD) {
        // Refill so the chunk starts at pos and holds at least this batch.
        int need = RecordBatch.HEADER_SIZE;
        if (limit - pos >= RecordBatch.LOG_OVERHEAD) {
          ByteBuffer len = ByteBuffer.allocate(4);
          readFully(len, pos);
          int declared = len.getInt(0);
          if (declared < RecordBatch.MIN_LENGTH || declared > config.maxBatchBytes()) {
            return pos;
          }
          need = declared + RecordBatch.LOG_OVERHEAD;
        }
        if ((long) pos + need > limit) {
          return pos; // torn tail
        }
        if (chunk.capacity() < need) {
          chunk = ByteBuffer.allocate(need);
        }
        chunk.clear();
        chunk.limit(Math.min(chunk.capacity(), limit - pos));
        readFully(chunk, pos);
        chunk.flip();
        chunkStart = pos;
        offsetInChunk = 0;
      }
      int declared = chunk.getInt(offsetInChunk);
      if (declared < RecordBatch.MIN_LENGTH || declared > config.maxBatchBytes()) {
        return pos;
      }
      int batchSize = declared + RecordBatch.LOG_OVERHEAD;
      if ((long) pos + batchSize > limit) {
        return pos;
      }
      RecordBatch batch = RecordBatch.wrap(chunk.slice(offsetInChunk, batchSize));
      if (validate && !batch.isValid()) {
        return pos;
      }
      if (!visitor.visit(batch, pos)) {
        return pos;
      }
      pos += batchSize;
    }
    return pos;
  }

  /** Rebuilds the index of a sealed segment whose index is missing or inconsistent. */
  public void rebuildIndex() throws IOException {
    index.reset();
    bytesSinceLastIndexEntry = 0;
    scan(
        0,
        size,
        false,
        (batch, position) -> {
          if (bytesSinceLastIndexEntry >= config.indexIntervalBytes() && !index.isFull()) {
            index.append(batch.baseOffset(), position);
            bytesSinceLastIndexEntry = 0;
          }
          bytesSinceLastIndexEntry += batch.sizeInBytes();
          return true;
        });
  }

  /**
   * Finds the last batch by scanning forward from the last index entry.
   *
   * @return true if the index is short: more than one interval of data follows its last entry
   *     (excluding the final batch), so an entry is missing
   */
  private boolean loadTail() throws IOException {
    IndexEntry last = index.lastEntry();
    long[] next = {baseOffset};
    long[] maxTs = {-1L};
    int[] bytesSince = {0};
    int[] lastSize = {0};
    scan(
        last.position(),
        size,
        false,
        (batch, position) -> {
          next[0] = batch.nextOffset();
          maxTs[0] = Math.max(maxTs[0], batch.maxTimestamp());
          bytesSince[0] += batch.sizeInBytes();
          lastSize[0] = batch.sizeInBytes();
          return true;
        });
    nextOffset = next[0];
    maxTimestamp = maxTs[0];
    bytesSinceLastIndexEntry = bytesSince[0];
    boolean couldHaveMoreEntries =
        index.entryCount() < config.maxIndexBytes() / OffsetIndex.ENTRY_SIZE;
    return couldHaveMoreEntries && bytesSince[0] - lastSize[0] >= config.indexIntervalBytes();
  }

  /** Raises the recorded newest timestamp (used when the owning log scans every batch). */
  void observeTimestamp(long timestamp) {
    if (timestamp > maxTimestamp) {
      maxTimestamp = timestamp;
    }
  }

  /**
   * Drops every batch whose last offset is at or after {@code offset}. Truncation works on whole
   * batches; replication only truncates at batch boundaries (epoch end offsets and the high
   * watermark always fall on one).
   */
  public void truncateTo(long offset) throws IOException {
    int pos = translateOffset(offset);
    if (pos < 0) {
      return;
    }
    ByteBuffer header = ByteBuffer.allocate(RecordBatch.HEADER_SIZE);
    readHeader(header, pos);
    long batchBase = header.getLong(RecordBatch.BASE_OFFSET_OFFSET);
    channel.truncate(pos);
    size = pos;
    index.truncateTo(batchBase);
    nextOffset = Math.max(baseOffset, batchBase);
    IndexEntry last = index.lastEntry();
    bytesSinceLastIndexEntry = pos - last.position();
  }

  /** Makes this segment the active one again after truncation removed later segments. */
  public void reopenForAppends() throws IOException {
    index.resize(config.maxIndexBytes());
  }

  /** Called when the log rolls past this segment: shrink the index to its real size. */
  public void seal() throws IOException {
    flush();
    index.trimToValidSize();
  }

  public void flush() throws IOException {
    channel.force(false);
    index.flush();
  }

  private void readHeader(ByteBuffer header, int position) {
    header.clear();
    readFully(header, position);
    header.flip();
  }

  private void readFully(ByteBuffer buf, long position) {
    try {
      long pos = position;
      while (buf.hasRemaining()) {
        int n = channel.read(buf, pos);
        if (n < 0) {
          throw new EOFException("unexpected end of " + logFile + " at " + pos);
        }
        pos += n;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      index.close();
    } finally {
      channel.close();
    }
  }

  /** Closes and removes both files. */
  public void delete() throws IOException {
    try {
      channel.close();
    } finally {
      index.closeAndDelete();
      Files.deleteIfExists(logFile);
    }
  }

  public Path dir() {
    return dir;
  }

  @Override
  public String toString() {
    return "LogSegment(base=" + baseOffset + ", size=" + size + ", next=" + nextOffset + ")";
  }
}
