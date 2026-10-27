package dev.logforge.network.protocol;

/**
 * Authenticate (API key 102): proves a connection belongs to the cluster by presenting the cluster
 * token. Topic administration, broker heartbeats, ISR changes and follower fetches are refused on a
 * connection that has not done this (PROTOCOL.md section 5, SECURITY-AUDIT F1).
 */
public final class Authenticate {
  private Authenticate() {}

  public record Request(String token) {
    public static Request read(ProtocolReader r) {
      return new Request(r.readNonNullString());
    }

    public void write(ProtocolWriter w) {
      w.writeString(token);
    }

    @Override
    public String toString() {
      return "Authenticate.Request[token=****]";
    }
  }

  public record Response(short errorCode) {
    public static Response read(ProtocolReader r) {
      return new Response(r.readShort());
    }

    public void write(ProtocolWriter w) {
      w.writeShort(errorCode);
    }
  }
}
