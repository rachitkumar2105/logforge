package dev.logforge.network.protocol;

import java.util.List;

/** Metadata (key 3), PROTOCOL.md 5.3. */
public final class Metadata {
  private Metadata() {}

  public record Request(List<String> topics) {
    public static Request read(ProtocolReader r) {
      return new Request(r.readArray(ProtocolReader::readNonNullString));
    }

    public void write(ProtocolWriter w) {
      w.writeArray(topics, ProtocolWriter::writeString);
    }
  }

  public record Broker(int nodeId, String host, int port, boolean alive) {
    static Broker read(ProtocolReader r) {
      return new Broker(r.readInt(), r.readNonNullString(), r.readInt(), r.readBoolean());
    }

    void write(ProtocolWriter w) {
      w.writeInt(nodeId).writeString(host).writeInt(port).writeBoolean(alive);
    }
  }

  public record PartitionMeta(
      short errorCode,
      int partition,
      int leader,
      int leaderEpoch,
      List<Integer> replicas,
      List<Integer> isr) {}

  public record TopicMeta(short errorCode, String name, List<PartitionMeta> partitions) {}

  public record Response(
      String clusterId, int controllerId, List<Broker> brokers, List<TopicMeta> topics) {
    public static Response read(ProtocolReader r) {
      String clusterId = r.readString();
      int controllerId = r.readInt();
      List<Broker> brokers = r.readNonNullArray(Broker::read);
      List<TopicMeta> topics =
          r.readNonNullArray(
              t ->
                  new TopicMeta(
                      t.readShort(),
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p ->
                              new PartitionMeta(
                                  p.readShort(),
                                  p.readInt(),
                                  p.readInt(),
                                  p.readInt(),
                                  p.readIntArray(),
                                  p.readIntArray()))));
      return new Response(clusterId, controllerId, brokers, topics);
    }

    public void write(ProtocolWriter w) {
      w.writeString(clusterId).writeInt(controllerId);
      w.writeArray(brokers, (bw, b) -> b.write(bw));
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeShort(t.errorCode()).writeString(t.name());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeShort(p.errorCode())
                        .writeInt(p.partition())
                        .writeInt(p.leader())
                        .writeInt(p.leaderEpoch())
                        .writeIntArray(p.replicas())
                        .writeIntArray(p.isr()));
          });
    }
  }
}
