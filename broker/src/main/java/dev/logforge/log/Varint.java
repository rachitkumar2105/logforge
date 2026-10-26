package dev.logforge.log;

import java.nio.ByteBuffer;

/**
 * Zig-zag LEB128 varints, identical to protobuf {@code sint32}/{@code sint64}.
 *
 * <p>Zig-zag maps small negative numbers to small unsigned numbers (0, -1, 1, -2 -> 0, 1, 2, 3) so
 * that -1, used for "null" lengths, costs one byte instead of five.
 */
public final class Varint {
  private Varint() {}

  public static void writeVarint(ByteBuffer buf, int value) {
    int v = (value << 1) ^ (value >> 31);
    while ((v & 0xFFFFFF80) != 0) {
      buf.put((byte) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    buf.put((byte) v);
  }

  public static int readVarint(ByteBuffer buf) {
    int value = 0;
    int shift = 0;
    for (int i = 0; i < 5; i++) {
      if (!buf.hasRemaining()) {
        throw new CorruptRecordException("varint runs past the end of the buffer");
      }
      byte b = buf.get();
      value |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return (value >>> 1) ^ -(value & 1);
      }
      shift += 7;
    }
    throw new CorruptRecordException("varint longer than 5 bytes");
  }

  public static void writeVarlong(ByteBuffer buf, long value) {
    long v = (value << 1) ^ (value >> 63);
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      buf.put((byte) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    buf.put((byte) v);
  }

  public static long readVarlong(ByteBuffer buf) {
    long value = 0L;
    int shift = 0;
    for (int i = 0; i < 10; i++) {
      if (!buf.hasRemaining()) {
        throw new CorruptRecordException("varlong runs past the end of the buffer");
      }
      byte b = buf.get();
      value |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return (value >>> 1) ^ -(value & 1);
      }
      shift += 7;
    }
    throw new CorruptRecordException("varlong longer than 10 bytes");
  }

  public static int sizeOfVarint(int value) {
    int v = (value << 1) ^ (value >> 31);
    int bytes = 1;
    while ((v & 0xFFFFFF80) != 0) {
      bytes++;
      v >>>= 7;
    }
    return bytes;
  }

  public static int sizeOfVarlong(long value) {
    long v = (value << 1) ^ (value >> 63);
    int bytes = 1;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      bytes++;
      v >>>= 7;
    }
    return bytes;
  }
}
