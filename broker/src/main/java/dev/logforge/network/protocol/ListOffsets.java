package dev.logforge.network.protocol;

import java.util.List;

/** ListOffsets (key 2), PROTOCOL.md 5.2. */
public final class ListOffsets {
  private ListOffsets() {}

  public static final long LATEST = -1L;
  public static final long EARLIEST = -2L;

  public record PartitionData(int partition, long timestamp) {}

  public record TopicData(String name, List<PartitionData> partitions) {}

  public record Request(int replicaId, List<TopicData> topics) {
    public static Request read(ProtocolReader r) {
      int replicaId = r.readInt();
      return new Request(
          replicaId,
          r.readNonNullArray(
              t ->
                  new TopicData(
                      t.readNonNullString(),
                      t.readNonNullArray(p -> new PartitionData(p.readInt(), p.readLong())))));
    }

    public void write(ProtocolWriter w) {
      w.writeInt(replicaId);
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(), (pw, p) -> pw.writeInt(p.partition()).writeLong(p.timestamp()));
          });
    }
  }

  public record PartitionResult(int partition, short errorCode, long offset) {}

  public record TopicResult(String name, List<PartitionResult> partitions) {}

  public record Response(List<TopicResult> topics) {
    public static Response read(ProtocolReader r) {
      return new Response(
          r.readNonNullArray(
              t ->
                  new TopicResult(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p -> new PartitionResult(p.readInt(), p.readShort(), p.readLong())))));
    }

    public void write(ProtocolWriter w) {
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeInt(p.partition()).writeShort(p.errorCode()).writeLong(p.offset()));
          });
    }
  }
}
