package dev.logforge.network.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** Writes the primitive types of PROTOCOL.md section 1 into a growable buffer. */
public final class ProtocolWriter {
  private ByteBuffer buf;

  public ProtocolWriter() {
    this(256);
  }

  public ProtocolWriter(int initialCapacity) {
    this.buf = ByteBuffer.allocate(initialCapacity);
  }

  private void ensure(int n) {
    if (buf.remaining() < n) {
      int cap = Math.max(buf.capacity() * 2, buf.position() + n);
      ByteBuffer bigger = ByteBuffer.allocate(cap);
      buf.flip();
      bigger.put(buf);
      buf = bigger;
    }
  }

  public int position() {
    return buf.position();
  }

  public ProtocolWriter writeByte(byte v) {
    ensure(1);
    buf.put(v);
    return this;
  }

  public ProtocolWriter writeBoolean(boolean v) {
    return writeByte((byte) (v ? 1 : 0));
  }

  public ProtocolWriter writeShort(short v) {
    ensure(2);
    buf.putShort(v);
    return this;
  }

  public ProtocolWriter writeInt(int v) {
    ensure(4);
    buf.putInt(v);
    return this;
  }

  public ProtocolWriter writeLong(long v) {
    ensure(8);
    buf.putLong(v);
    return this;
  }

  public ProtocolWriter writeString(String s) {
    if (s == null) {
      return writeShort((short) -1);
    }
    byte[] b = s.getBytes(UTF_8);
    if (b.length > Short.MAX_VALUE) {
      throw new IllegalArgumentException("string longer than 32767 bytes");
    }
    writeShort((short) b.length);
    ensure(b.length);
    buf.put(b);
    return this;
  }

  public ProtocolWriter writeBytes(ByteBuffer b) {
    if (b == null) {
      return writeInt(-1);
    }
    ByteBuffer dup = b.duplicate();
    writeInt(dup.remaining());
    ensure(dup.remaining());
    buf.put(dup);
    return this;
  }

  public ProtocolWriter writeBytes(byte[] b) {
    return writeBytes(b == null ? null : ByteBuffer.wrap(b));
  }

  public <T> ProtocolWriter writeArray(List<T> items, BiConsumer<ProtocolWriter, T> element) {
    if (items == null) {
      return writeInt(-1);
    }
    writeInt(items.size());
    for (T t : items) {
      element.accept(this, t);
    }
    return this;
  }

  public ProtocolWriter writeIntArray(List<Integer> items) {
    return writeArray(items, ProtocolWriter::writeInt);
  }

  public ProtocolWriter writeStringMap(Map<String, String> m) {
    writeInt(m.size());
    for (Map.Entry<String, String> e : m.entrySet()) {
      writeString(e.getKey());
      writeString(e.getValue());
    }
    return this;
  }

  public void putInt(int position, int value) {
    buf.putInt(position, value);
  }

  /** The bytes written so far, as a new buffer positioned at 0. */
  public ByteBuffer toBuffer() {
    ByteBuffer out = buf.duplicate();
    out.flip();
    return out;
  }
}
