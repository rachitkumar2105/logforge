package dev.logforge.log;

import java.util.Arrays;
import java.util.Objects;

/** A record header: a UTF-8 key and an optional byte value. */
public final class Header {
  private final String key;
  private final byte[] value;

  public Header(String key, byte[] value) {
    this.key = Objects.requireNonNull(key, "header key");
    this.value = value;
  }

  public String key() {
    return key;
  }

  /** The raw value; may be null. Callers must not mutate it. */
  public byte[] value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Header h && key.equals(h.key) && Arrays.equals(value, h.value);
  }

  @Override
  public int hashCode() {
    return 31 * key.hashCode() + Arrays.hashCode(value);
  }

  @Override
  public String toString() {
    return "Header(" + key + ", " + (value == null ? "null" : value.length + " bytes") + ")";
  }
}
