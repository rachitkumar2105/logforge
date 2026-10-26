package dev.logforge.network.protocol;

import java.nio.ByteBuffer;
import java.util.List;

/** Produce (key 0), PROTOCOL.md 5.0. */
public final class Produce {
  private Produce() {}

  public record PartitionData(int partition, ByteBuffer records) {}

  public record TopicData(String name, List<PartitionData> partitions) {}

  public record Request(short acks, int timeoutMs, List<TopicData> topics) {
    public static Request read(ProtocolReader r) {
      short acks = r.readShort();
      int timeout = r.readInt();
      List<TopicData> topics =
          r.readNonNullArray(
              t ->
                  new TopicData(
                      t.readNonNullString(),
                      t.readNonNullArray(p -> new PartitionData(p.readInt(), p.readBytes()))));
      return new Request(acks, timeout, topics);
    }

    public void write(ProtocolWriter w) {
      w.writeShort(acks).writeInt(timeoutMs);
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(), (pw, p) -> pw.writeInt(p.partition()).writeBytes(p.records()));
          });
    }
  }

  public record PartitionResult(
      int partition, short errorCode, long baseOffset, long logStartOffset) {}

  public record TopicResult(String name, List<PartitionResult> partitions) {}

  public record Response(List<TopicResult> topics) {
    public static Response read(ProtocolReader r) {
      return new Response(
          r.readNonNullArray(
              t ->
                  new TopicResult(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p ->
                              new PartitionResult(
                                  p.readInt(), p.readShort(), p.readLong(), p.readLong())))));
    }

    public void write(ProtocolWriter w) {
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeInt(p.partition())
                        .writeShort(p.errorCode())
                        .writeLong(p.baseOffset())
                        .writeLong(p.logStartOffset()));
          });
    }
  }
}
