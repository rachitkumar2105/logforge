package dev.logforge.network.protocol;

/** The fields every request frame starts with (PROTOCOL.md section 3). */
public record RequestHeader(short apiKey, short apiVersion, int correlationId, String clientId) {
  /** Bytes of header that precede the body, excluding the 4-byte size, before the client id. */
  public static final int FIXED_SIZE = 2 + 2 + 4;

  public static RequestHeader read(ProtocolReader r) {
    return new RequestHeader(r.readShort(), r.readShort(), r.readInt(), r.readString());
  }

  public void write(ProtocolWriter w) {
    w.writeShort(apiKey).writeShort(apiVersion).writeInt(correlationId).writeString(clientId);
  }
}
