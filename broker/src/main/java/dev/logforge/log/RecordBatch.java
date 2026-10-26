package dev.logforge.log;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * A view over the bytes of one record batch. The layout is specified byte by byte in
 * docs/PROTOCOL.md section 2; the constants below are the field offsets from that table.
 *
 * <p>The same bytes are used on the wire and on disk. The leader stamps {@code baseOffset} and
 * {@code partitionLeaderEpoch} in place; both sit before the CRC'd region so the producer's
 * checksum stays valid end to end and the broker never recomputes it on the hot path.
 */
public final class RecordBatch {
  public static final byte MAGIC = 1;

  public static final int LENGTH_OFFSET = 0;
  public static final int BASE_OFFSET_OFFSET = 4;
  public static final int LEADER_EPOCH_OFFSET = 12;
  public static final int MAGIC_OFFSET = 16;
  public static final int CRC_OFFSET = 17;
  public static final int ATTRIBUTES_OFFSET = 21;
  public static final int LAST_OFFSET_DELTA_OFFSET = 23;
  public static final int BASE_TIMESTAMP_OFFSET = 27;
  public static final int MAX_TIMESTAMP_OFFSET = 35;
  public static final int PRODUCER_ID_OFFSET = 43;
  public static final int PRODUCER_EPOCH_OFFSET = 51;
  public static final int BASE_SEQUENCE_OFFSET = 53;
  public static final int RECORD_COUNT_OFFSET = 57;
  public static final int RECORDS_OFFSET = 61;

  /** Bytes before the records. */
  public static final int HEADER_SIZE = RECORDS_OFFSET;

  /** Size of the length prefix itself. */
  public static final int LOG_OVERHEAD = 4;

  /** Smallest legal value of the length field (a header with no records is still rejected). */
  public static final int MIN_LENGTH = HEADER_SIZE - LOG_OVERHEAD;

  public static final long NO_PRODUCER_ID = -1L;
  public static final short NO_PRODUCER_EPOCH = -1;
  public static final int NO_SEQUENCE = -1;
  public static final int NO_PARTITION_LEADER_EPOCH = -1;

  private final ByteBuffer buffer;

  private RecordBatch(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  /**
   * Wraps the bytes from {@code buf.position()} to {@code buf.limit()} without validating them. The
   * view shares content with {@code buf}.
   */
  public static RecordBatch wrap(ByteBuffer buf) {
    return new RecordBatch(buf.slice());
  }

  /**
   * Returns the total size of the batch starting at {@code buf.position()} if all of its bytes are
   * present, or -1 if the buffer ends before the batch does or the length field is impossible.
   */
  public static int sizeIfComplete(ByteBuffer buf) {
    int remaining = buf.remaining();
    if (remaining < LOG_OVERHEAD) {
      return -1;
    }
    int length = buf.getInt(buf.position());
    if (length < MIN_LENGTH || length > Integer.MAX_VALUE - LOG_OVERHEAD) {
      return -1;
    }
    int size = length + LOG_OVERHEAD;
    return size <= remaining ? size : -1;
  }

  /**
   * Splits a buffer of back-to-back batches. Stops at the first incomplete batch (for example a
   * fetch response cut at {@code maxBytes}). Does not validate CRCs and does not move {@code buf}.
   */
  public static List<RecordBatch> iterate(ByteBuffer buf) {
    List<RecordBatch> out = new ArrayList<>();
    ByteBuffer view = buf.duplicate();
    while (true) {
      int size = sizeIfComplete(view);
      if (size < 0) {
        return out;
      }
      ByteBuffer slice = view.slice(view.position(), size);
      out.add(new RecordBatch(slice));
      view.position(view.position() + size);
    }
  }

  /** A read-only duplicate of the batch bytes, positioned at 0. */
  public ByteBuffer buffer() {
    ByteBuffer dup = buffer.duplicate();
    dup.position(0);
    dup.limit(Math.min(buffer.limit(), declaredSize()));
    return dup;
  }

  private int declaredSize() {
    if (buffer.limit() < LOG_OVERHEAD) {
      return buffer.limit();
    }
    long size = (long) buffer.getInt(LENGTH_OFFSET) + LOG_OVERHEAD;
    return size < 0 || size > Integer.MAX_VALUE ? buffer.limit() : (int) size;
  }

  public int sizeInBytes() {
    return buffer.getInt(LENGTH_OFFSET) + LOG_OVERHEAD;
  }

  public long baseOffset() {
    return buffer.getLong(BASE_OFFSET_OFFSET);
  }

  public void setBaseOffset(long offset) {
    buffer.putLong(BASE_OFFSET_OFFSET, offset);
  }

  public int partitionLeaderEpoch() {
    return buffer.getInt(LEADER_EPOCH_OFFSET);
  }

  public void setPartitionLeaderEpoch(int epoch) {
    buffer.putInt(LEADER_EPOCH_OFFSET, epoch);
  }

  public byte magic() {
    return buffer.get(MAGIC_OFFSET);
  }

  public long checksum() {
    return Integer.toUnsignedLong(buffer.getInt(CRC_OFFSET));
  }

  public short attributes() {
    return buffer.getShort(ATTRIBUTES_OFFSET);
  }

  public int lastOffsetDelta() {
    return buffer.getInt(LAST_OFFSET_DELTA_OFFSET);
  }

  public long lastOffset() {
    return baseOffset() + lastOffsetDelta();
  }

  public long nextOffset() {
    return lastOffset() + 1;
  }

  public long baseTimestamp() {
    return buffer.getLong(BASE_TIMESTAMP_OFFSET);
  }

  public long maxTimestamp() {
    return buffer.getLong(MAX_TIMESTAMP_OFFSET);
  }

  public long producerId() {
    return buffer.getLong(PRODUCER_ID_OFFSET);
  }

  public short producerEpoch() {
    return buffer.getShort(PRODUCER_EPOCH_OFFSET);
  }

  public int baseSequence() {
    return buffer.getInt(BASE_SEQUENCE_OFFSET);
  }

  /** Sequence of the last record, or -1 if the batch is not idempotent. */
  public int lastSequence() {
    int base = baseSequence();
    if (base == NO_SEQUENCE) {
      return NO_SEQUENCE;
    }
    return base + lastOffsetDelta();
  }

  public boolean hasProducerId() {
    return producerId() >= 0;
  }

  public int recordCount() {
    return buffer.getInt(RECORD_COUNT_OFFSET);
  }

  /** CRC-32C over every byte from {@code attributes} to the end of the batch. */
  public long computeChecksum() {
    CRC32C crc = new CRC32C();
    ByteBuffer region = buffer.duplicate();
    region.limit(sizeInBytes());
    region.position(ATTRIBUTES_OFFSET);
    crc.update(region);
    return crc.getValue();
  }

  public boolean isValid() {
    try {
      validate();
      return true;
    } catch (CorruptRecordException e) {
      return false;
    }
  }

  /**
   * Applies every rule from PROTOCOL.md section 2.4. Throws on the first violation. A batch that
   * passes is safe to append and to decode.
   */
  public void validate() {
    if (buffer.limit() < HEADER_SIZE) {
      throw new CorruptRecordException(
          "batch truncated: " + buffer.limit() + " bytes, header needs " + HEADER_SIZE);
    }
    int length = buffer.getInt(LENGTH_OFFSET);
    if (length < MIN_LENGTH) {
      throw new CorruptRecordException("batch length " + length + " below minimum " + MIN_LENGTH);
    }
    if ((long) length + LOG_OVERHEAD > buffer.limit()) {
      throw new CorruptRecordException(
          "batch truncated: declares "
              + (length + LOG_OVERHEAD)
              + " bytes, have "
              + buffer.limit());
    }
    if (magic() != MAGIC) {
      throw new CorruptRecordException("unknown magic " + magic());
    }
    long stored = checksum();
    long computed = computeChecksum();
    if (stored != computed) {
      throw new CorruptRecordException("crc mismatch: stored " + stored + ", computed " + computed);
    }
    if (attributes() != 0) {
      throw new CorruptRecordException("unsupported attributes " + attributes());
    }
    if (recordCount() < 1) {
      throw new CorruptRecordException("record count " + recordCount());
    }
    if (lastOffsetDelta() < 0) {
      throw new CorruptRecordException("negative lastOffsetDelta");
    }
    decode(true);
  }

  /** Decodes every record. Throws {@link CorruptRecordException} on malformed framing. */
  public List<Record> records() {
    return decode(false);
  }

  private List<Record> decode(boolean checkOnly) {
    int count = recordCount();
    if (count < 0 || count > sizeInBytes()) {
      throw new CorruptRecordException("impossible record count " + count);
    }
    List<Record> out = checkOnly ? null : new ArrayList<>(count);
    ByteBuffer buf = buffer.duplicate();
    int end = sizeInBytes();
    if (end > buf.limit()) {
      throw new CorruptRecordException("batch truncated");
    }
    buf.limit(end);
    buf.position(RECORDS_OFFSET);
    long base = baseOffset();
    long baseTs = baseTimestamp();
    int lastDelta = lastOffsetDelta();
    int prevDelta = -1;
    for (int i = 0; i < count; i++) {
      int recLen = Varint.readVarint(buf);
      if (recLen < 0 || recLen > buf.remaining()) {
        throw new CorruptRecordException("record " + i + " length " + recLen + " overruns batch");
      }
      int recEnd = buf.position() + recLen;
      ByteBuffer rec = buf.duplicate();
      rec.limit(recEnd);
      rec.get(); // attributes
      long tsDelta = Varint.readVarlong(rec);
      int offsetDelta = Varint.readVarint(rec);
      if (offsetDelta <= prevDelta || offsetDelta > lastDelta) {
        throw new CorruptRecordException(
            "record " + i + " offsetDelta " + offsetDelta + " out of order");
      }
      prevDelta = offsetDelta;
      byte[] key = readBytes(rec);
      byte[] value = readBytes(rec);
      int headerCount = Varint.readVarint(rec);
      if (headerCount < 0 || headerCount > rec.remaining()) {
        throw new CorruptRecordException("header count " + headerCount);
      }
      List<Header> headers = headerCount == 0 ? List.of() : new ArrayList<>(headerCount);
      for (int h = 0; h < headerCount; h++) {
        byte[] hk = readBytes(rec);
        if (hk == null) {
          throw new CorruptRecordException("null header key");
        }
        byte[] hv = readBytes(rec);
        headers.add(new Header(new String(hk, UTF_8), hv));
      }
      if (rec.position() != recEnd) {
        throw new CorruptRecordException("record " + i + " has trailing bytes");
      }
      buf.position(recEnd);
      if (out != null) {
        out.add(new Record(base + offsetDelta, baseTs + tsDelta, key, value, headers));
      }
    }
    if (buf.hasRemaining()) {
      throw new CorruptRecordException(buf.remaining() + " bytes after the last record");
    }
    return out;
  }

  private static byte[] readBytes(ByteBuffer buf) {
    int len = Varint.readVarint(buf);
    if (len == -1) {
      return null;
    }
    if (len < -1 || len > buf.remaining()) {
      throw new CorruptRecordException("field length " + len + " overruns record");
    }
    byte[] out = new byte[len];
    buf.get(out);
    return out;
  }

  @Override
  public String toString() {
    return "RecordBatch(baseOffset="
        + baseOffset()
        + ", lastOffset="
        + lastOffset()
        + ", records="
        + recordCount()
        + ", bytes="
        + sizeInBytes()
        + ")";
  }

  public static Builder builder(long baseTimestamp) {
    return new Builder(baseTimestamp);
  }

  /** Builds one batch in memory. Not thread-safe. */
  public static final class Builder {
    private final long baseTimestamp;
    private long baseOffset = 0L;
    private long producerId = NO_PRODUCER_ID;
    private short producerEpoch = NO_PRODUCER_EPOCH;
    private int baseSequence = NO_SEQUENCE;
    private int partitionLeaderEpoch = NO_PARTITION_LEADER_EPOCH;
    private long explicitLastOffset = -1L;
    private long maxTimestamp = Long.MIN_VALUE;
    private long nextOffset = 0L;
    private final List<PendingRecord> records = new ArrayList<>();
    private int recordsSize = 0;

    /** {@code size} includes the length varint; {@code bodySize} is the value of that varint. */
    @SuppressWarnings("ArrayRecordComponent") // private, never compared
    private record PendingRecord(
        int offsetDelta,
        long tsDelta,
        byte[] key,
        byte[] value,
        List<Header> headers,
        int bodySize,
        int size) {}

    private Builder(long baseTimestamp) {
      this.baseTimestamp = baseTimestamp;
    }

    /** Base offset to write. Producers leave this at 0; the leader stamps the real one. */
    public Builder baseOffset(long offset) {
      if (!records.isEmpty()) {
        throw new IllegalStateException("set baseOffset before appending records");
      }
      this.baseOffset = offset;
      this.nextOffset = offset;
      return this;
    }

    public Builder producer(long producerId, short producerEpoch, int baseSequence) {
      this.producerId = producerId;
      this.producerEpoch = producerEpoch;
      this.baseSequence = baseSequence;
      return this;
    }

    public Builder partitionLeaderEpoch(int epoch) {
      this.partitionLeaderEpoch = epoch;
      return this;
    }

    /** Overrides the last offset of the batch (compaction keeps the original range). */
    public Builder lastOffset(long offset) {
      this.explicitLastOffset = offset;
      return this;
    }

    public Builder append(long timestamp, byte[] key, byte[] value, Header... headers) {
      return appendWithOffset(nextOffset, timestamp, key, value, List.of(headers));
    }

    public Builder appendWithOffset(
        long offset, long timestamp, byte[] key, byte[] value, List<Header> headers) {
      if (offset < nextOffset) {
        throw new IllegalArgumentException("offsets must increase: " + offset + " < " + nextOffset);
      }
      long delta = offset - baseOffset;
      if (delta > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("offset delta too large");
      }
      long tsDelta = timestamp - baseTimestamp;
      int body = 1 + Varint.sizeOfVarlong(tsDelta) + Varint.sizeOfVarint((int) delta);
      body += sizeOfBytes(key) + sizeOfBytes(value) + Varint.sizeOfVarint(headers.size());
      List<Header> hs = List.copyOf(headers);
      for (Header h : hs) {
        body += sizeOfBytes(h.key().getBytes(UTF_8)) + sizeOfBytes(h.value());
      }
      int size = Varint.sizeOfVarint(body) + body;
      records.add(new PendingRecord((int) delta, tsDelta, key, value, hs, body, size));
      recordsSize += size;
      maxTimestamp = Math.max(maxTimestamp, timestamp);
      nextOffset = offset + 1;
      return this;
    }

    public int recordCount() {
      return records.size();
    }

    /** Bytes the batch would occupy if built now. */
    public int estimatedSizeInBytes() {
      return HEADER_SIZE + recordsSize;
    }

    private static int sizeOfBytes(byte[] b) {
      return b == null ? Varint.sizeOfVarint(-1) : Varint.sizeOfVarint(b.length) + b.length;
    }

    private static void writeBytes(ByteBuffer buf, byte[] b) {
      if (b == null) {
        Varint.writeVarint(buf, -1);
      } else {
        Varint.writeVarint(buf, b.length);
        buf.put(b);
      }
    }

    public RecordBatch build() {
      if (records.isEmpty()) {
        throw new IllegalStateException("a batch needs at least one record");
      }
      long lastOffset = explicitLastOffset >= 0 ? explicitLastOffset : nextOffset - 1;
      if (lastOffset < nextOffset - 1) {
        throw new IllegalStateException("lastOffset below the last record");
      }
      int size = HEADER_SIZE + recordsSize;
      ByteBuffer buf = ByteBuffer.allocate(size);
      buf.putInt(size - LOG_OVERHEAD);
      buf.putLong(baseOffset);
      buf.putInt(partitionLeaderEpoch);
      buf.put(MAGIC);
      buf.putInt(0); // crc placeholder
      buf.putShort((short) 0);
      buf.putInt((int) (lastOffset - baseOffset));
      buf.putLong(baseTimestamp);
      buf.putLong(maxTimestamp);
      buf.putLong(producerId);
      buf.putShort(producerEpoch);
      buf.putInt(baseSequence);
      buf.putInt(records.size());
      for (PendingRecord r : records) {
        writeRecord(buf, r);
      }
      buf.flip();
      RecordBatch batch = new RecordBatch(buf);
      buf.putInt(CRC_OFFSET, (int) batch.computeChecksum());
      return batch;
    }

    private static void writeRecord(ByteBuffer buf, PendingRecord r) {
      Varint.writeVarint(buf, r.bodySize);
      buf.put((byte) 0);
      Varint.writeVarlong(buf, r.tsDelta);
      Varint.writeVarint(buf, r.offsetDelta);
      writeBytes(buf, r.key);
      writeBytes(buf, r.value);
      Varint.writeVarint(buf, r.headers.size());
      for (Header h : r.headers) {
        writeBytes(buf, h.key().getBytes(UTF_8));
        writeBytes(buf, h.value());
      }
    }
  }
}
