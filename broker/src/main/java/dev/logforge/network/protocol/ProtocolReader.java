package dev.logforge.network.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Reads the primitive types of PROTOCOL.md section 1 from a buffer. Every read checks the bytes
 * remaining first, and array counts are bounded by the bytes left, so a hostile length can never
 * cause a large allocation: it fails with {@link InvalidRequestException} instead.
 */
public final class ProtocolReader {
  private final ByteBuffer buf;

  public ProtocolReader(ByteBuffer buf) {
    this.buf = buf;
  }

  private void need(int n) {
    if (buf.remaining() < n) {
      throw new InvalidRequestException("need " + n + " bytes, have " + buf.remaining());
    }
  }

  public int remaining() {
    return buf.remaining();
  }

  public byte readByte() {
    need(1);
    return buf.get();
  }

  public boolean readBoolean() {
    return readByte() != 0;
  }

  public short readShort() {
    need(2);
    return buf.getShort();
  }

  public int readInt() {
    need(4);
    return buf.getInt();
  }

  public long readLong() {
    need(8);
    return buf.getLong();
  }

  public String readString() {
    short len = readShort();
    if (len == -1) {
      return null;
    }
    if (len < 0) {
      throw new InvalidRequestException("string length " + len);
    }
    need(len);
    byte[] bytes = new byte[len];
    buf.get(bytes);
    return new String(bytes, UTF_8);
  }

  /** A string that must not be null. */
  public String readNonNullString() {
    String s = readString();
    if (s == null) {
      throw new InvalidRequestException("unexpected null string");
    }
    return s;
  }

  /** Returns a slice sharing the underlying buffer (no copy), or null. */
  public ByteBuffer readBytes() {
    int len = readInt();
    if (len == -1) {
      return null;
    }
    if (len < 0) {
      throw new InvalidRequestException("bytes length " + len);
    }
    need(len);
    ByteBuffer slice = buf.slice(buf.position(), len);
    buf.position(buf.position() + len);
    return slice;
  }

  /** Reads an array; returns null for a null array. Each element must take at least one byte. */
  public <T> List<T> readArray(Function<ProtocolReader, T> element) {
    int count = readInt();
    if (count == -1) {
      return null;
    }
    if (count < 0 || count > buf.remaining()) {
      throw new InvalidRequestException(
          "array count " + count + " with " + buf.remaining() + " bytes left");
    }
    List<T> out = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      out.add(element.apply(this));
    }
    return out;
  }

  public <T> List<T> readNonNullArray(Function<ProtocolReader, T> element) {
    List<T> out = readArray(element);
    if (out == null) {
      throw new InvalidRequestException("unexpected null array");
    }
    return out;
  }

  public List<Integer> readIntArray() {
    return readNonNullArray(ProtocolReader::readInt);
  }

  public Map<String, String> readStringMap() {
    List<String[]> pairs =
        readNonNullArray(r -> new String[] {r.readNonNullString(), r.readNonNullString()});
    Map<String, String> m = new LinkedHashMap<>();
    for (String[] p : pairs) {
      m.put(p[0], p[1]);
    }
    return m;
  }

  /** Fails if bytes are left over: a well-formed body is consumed exactly. */
  public void expectEnd() {
    if (buf.hasRemaining()) {
      throw new InvalidRequestException(buf.remaining() + " trailing bytes");
    }
  }
}
