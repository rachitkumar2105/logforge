package dev.logforge.log;

import java.util.List;

/** One decoded record with its absolute offset and timestamp. */
@SuppressWarnings("JavaLangClash") // the domain name is Record; java.lang.Record is never used here
public final class Record {
  private final long offset;
  private final long timestamp;
  private final byte[] key;
  private final byte[] value;
  private final List<Header> headers;

  public Record(long offset, long timestamp, byte[] key, byte[] value, List<Header> headers) {
    this.offset = offset;
    this.timestamp = timestamp;
    this.key = key;
    this.value = value;
    this.headers = List.copyOf(headers);
  }

  public long offset() {
    return offset;
  }

  public long timestamp() {
    return timestamp;
  }

  /** Raw key; null for a null key. Callers must not mutate it. */
  public byte[] key() {
    return key;
  }

  /** Raw value; null for a tombstone. Callers must not mutate it. */
  public byte[] value() {
    return value;
  }

  public List<Header> headers() {
    return headers;
  }

  /** A null value marks a delete on a compacted topic. */
  public boolean isTombstone() {
    return value == null;
  }

  @Override
  public String toString() {
    // Never print keys or values: they are user data and must not reach logs.
    return "Record(offset=" + offset + ", timestamp=" + timestamp + ")";
  }
}
