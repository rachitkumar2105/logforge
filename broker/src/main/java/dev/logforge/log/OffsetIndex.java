package dev.logforge.log;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Sparse index from offset to byte position for one segment.
 *
 * <p>Each entry is 8 bytes: {@code relativeOffset int32, position int32}. An entry is written only
 * every {@code index.interval.bytes} of log data (4 KB by default), so a 128 MB segment needs about
 * 32k entries (256 KB) instead of one per record. The cost of that sparseness is a short forward
 * scan: a lookup lands at most one interval before the target and walks batch headers from there.
 * That trade (memory for a bounded scan) is what lets every segment's index stay resident.
 *
 * <p>The file is memory-mapped and pre-sized to {@code maxIndexBytes} while the segment is active,
 * so appends are plain memory writes. It is trimmed to its real size when closed or rolled.
 *
 * <p>Thread model: one writer (the log's append lock), many readers. {@code entries} is volatile
 * and written after the entry bytes, so a reader never sees a half-written entry.
 */
public final class OffsetIndex implements Closeable {
  public static final int ENTRY_SIZE = 8;

  private final Path file;
  private final long baseOffset;
  private final RandomAccessFile raf;
  private MappedByteBuffer mmap;
  private int maxEntries;
  private volatile int entries;

  /** Opens an index for appends, pre-sizing the file to {@code maxIndexBytes}. */
  public OffsetIndex(Path file, long baseOffset, int maxIndexBytes) throws IOException {
    this(file, baseOffset, maxIndexBytes, true);
  }

  /**
   * @param preallocate true for the active segment (grow the file so appends are memory writes);
   *     false for a sealed segment, which maps the file at its current size and never resizes it
   */
  public OffsetIndex(Path file, long baseOffset, int maxIndexBytes, boolean preallocate)
      throws IOException {
    this.file = file;
    this.baseOffset = baseOffset;
    boolean existed = Files.exists(file);
    this.raf = new RandomAccessFile(file.toFile(), "rw");
    long existingLength = existed ? raf.length() : 0L;
    int existing = roundDown((int) Math.min(existingLength, Integer.MAX_VALUE));
    int capacity =
        preallocate ? Math.max(Math.max(roundDown(maxIndexBytes), ENTRY_SIZE), existing) : existing;
    if (capacity != existingLength) {
      setLength(raf, capacity);
    }
    this.maxEntries = capacity / ENTRY_SIZE;
    this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, capacity);
    this.entries = existed ? countValidEntries(Math.min(existing / ENTRY_SIZE, maxEntries)) : 0;
  }

  /**
   * {@code RandomAccessFile.setLength} with a short retry. On Windows a file cannot change length
   * while any process has a view of it mapped, and on-access antivirus scanners briefly map freshly
   * written files. The failure is transient, so a few retries over about a second resolve it.
   */
  static void setLength(RandomAccessFile raf, long length) throws IOException {
    IOException last = null;
    for (int attempt = 0; attempt < 40; attempt++) {
      try {
        raf.setLength(length);
        return;
      } catch (IOException e) {
        last = e;
        try {
          Thread.sleep(25);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
    throw last;
  }

  private static int roundDown(int bytes) {
    return bytes - (bytes % ENTRY_SIZE);
  }

  /**
   * A pre-sized file left behind by a crash has zeroed slots after the real entries. Real entries
   * are strictly increasing in both fields, so the first slot that is not is the end.
   */
  private int countValidEntries(int slots) {
    int n = 0;
    int prevRel = -1;
    int prevPos = -1;
    for (int i = 0; i < slots; i++) {
      int rel = mmap.getInt(i * ENTRY_SIZE);
      int pos = mmap.getInt(i * ENTRY_SIZE + 4);
      if (rel <= prevRel || pos <= prevPos || rel < 0 || pos < 0) {
        if (!(i == 0 && rel == 0 && pos == 0)) {
          break;
        }
      }
      prevRel = rel;
      prevPos = pos;
      n++;
    }
    return n;
  }

  public Path file() {
    return file;
  }

  public long baseOffset() {
    return baseOffset;
  }

  public int entryCount() {
    return entries;
  }

  public int maxEntries() {
    return maxEntries;
  }

  public boolean isFull() {
    return entries >= maxEntries;
  }

  public IndexEntry entry(int n) {
    if (n < 0 || n >= entries) {
      throw new IndexOutOfBoundsException(n);
    }
    return new IndexEntry(
        baseOffset + mmap.getInt(n * ENTRY_SIZE), mmap.getInt(n * ENTRY_SIZE + 4));
  }

  public IndexEntry lastEntry() {
    int n = entries;
    return n == 0 ? new IndexEntry(baseOffset, 0) : entry(n - 1);
  }

  /** Adds an entry. Offsets and positions must both be strictly greater than the last entry's. */
  public void append(long offset, int position) {
    if (isFull()) {
      throw new IllegalStateException("index full: " + file);
    }
    long rel = offset - baseOffset;
    if (rel < 0 || rel > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "offset " + offset + " outside segment base " + baseOffset);
    }
    int n = entries;
    if (n > 0) {
      IndexEntry last = entry(n - 1);
      if (offset <= last.offset() || position <= last.position()) {
        throw new IllegalArgumentException(
            "index entries must increase: " + offset + "@" + position + " after " + last);
      }
    }
    mmap.putInt(n * ENTRY_SIZE, (int) rel);
    mmap.putInt(n * ENTRY_SIZE + 4, position);
    entries = n + 1;
  }

  /**
   * Greatest entry whose offset is at most {@code target}, by binary search. Returns (base, 0) when
   * the target is before the first entry or the index is empty: the scan then starts at the top of
   * the segment.
   */
  public IndexEntry lookup(long target) {
    int n = entries;
    long rel = target - baseOffset;
    int lo = 0;
    int hi = n - 1;
    int found = -1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      int midRel = mmap.getInt(mid * ENTRY_SIZE);
      if (midRel <= rel) {
        found = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return found < 0 ? new IndexEntry(baseOffset, 0) : entry(found);
  }

  /** Removes every entry whose offset is at or above {@code offset}. */
  public void truncateTo(long offset) {
    int n = entries;
    int keep = 0;
    while (keep < n && entry(keep).offset() < offset) {
      keep++;
    }
    truncateToEntries(keep);
  }

  public void truncateToEntries(int keep) {
    int n = entries;
    if (keep >= n) {
      return;
    }
    entries = keep;
    for (int i = keep * ENTRY_SIZE; i < n * ENTRY_SIZE; i++) {
      mmap.put(i, (byte) 0);
    }
  }

  public void reset() {
    truncateToEntries(0);
  }

  /** True if the last entry points inside a log of {@code logSizeBytes} bytes. */
  public boolean isConsistentWith(long logSizeBytes) {
    int n = entries;
    return n == 0 || entry(n - 1).position() < logSizeBytes;
  }

  /** Grows or shrinks the backing file, keeping the entries. */
  public void resize(int maxIndexBytes) throws IOException {
    int capacity = Math.max(roundDown(maxIndexBytes), roundDown(entries * ENTRY_SIZE));
    capacity = Math.max(capacity, ENTRY_SIZE);
    mmap.force();
    MappedBuffers.unmap(mmap);
    setLength(raf, capacity);
    mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, capacity);
    maxEntries = capacity / ENTRY_SIZE;
  }

  public void flush() {
    mmap.force();
  }

  /** Shrinks the file to exactly its entries, so a reopened index needs no scan. */
  public void trimToValidSize() throws IOException {
    int bytes = entries * ENTRY_SIZE;
    mmap.force();
    MappedBuffers.unmap(mmap);
    setLength(raf, bytes);
    mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bytes);
    maxEntries = entries;
  }

  @Override
  public void close() throws IOException {
    try {
      trimToValidSize();
    } finally {
      MappedBuffers.unmap(mmap);
      raf.close();
    }
  }

  /** Closes without trimming and deletes the file. */
  public void closeAndDelete() throws IOException {
    MappedBuffers.unmap(mmap);
    raf.close();
    Files.deleteIfExists(file);
  }
}
