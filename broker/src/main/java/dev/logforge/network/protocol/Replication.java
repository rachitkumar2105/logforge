package dev.logforge.network.protocol;

import java.util.List;

/** InitProducerId (12) and OffsetForLeaderEpoch (13), PROTOCOL.md 5.12 and 5.13. */
public final class Replication {
  private Replication() {}

  // ---- InitProducerId ----

  public record InitProducerIdRequest(int transactionTimeoutMs) {
    public static InitProducerIdRequest read(ProtocolReader r) {
      return new InitProducerIdRequest(r.readInt());
    }

    public void write(ProtocolWriter w) {
      w.writeInt(transactionTimeoutMs);
    }
  }

  public record InitProducerIdResponse(short errorCode, long producerId, short producerEpoch) {
    public static InitProducerIdResponse read(ProtocolReader r) {
      return new InitProducerIdResponse(r.readShort(), r.readLong(), r.readShort());
    }

    public void write(ProtocolWriter w) {
      w.writeShort(errorCode).writeLong(producerId).writeShort(producerEpoch);
    }
  }

  // ---- OffsetForLeaderEpoch ----

  public record EpochPartition(int partition, int currentLeaderEpoch, int leaderEpoch) {}

  public record EpochTopic(String name, List<EpochPartition> partitions) {}

  public record OffsetForLeaderEpochRequest(int replicaId, List<EpochTopic> topics) {
    public static OffsetForLeaderEpochRequest read(ProtocolReader r) {
      int replicaId = r.readInt();
      return new OffsetForLeaderEpochRequest(
          replicaId,
          r.readNonNullArray(
              t ->
                  new EpochTopic(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p -> new EpochPartition(p.readInt(), p.readInt(), p.readInt())))));
    }

    public void write(ProtocolWriter w) {
      w.writeInt(replicaId);
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeInt(p.partition())
                        .writeInt(p.currentLeaderEpoch())
                        .writeInt(p.leaderEpoch()));
          });
    }
  }

  public record EpochEnd(int partition, short errorCode, int leaderEpoch, long endOffset) {}

  public record EpochTopicResult(String name, List<EpochEnd> partitions) {}

  public record OffsetForLeaderEpochResponse(List<EpochTopicResult> topics) {
    public static OffsetForLeaderEpochResponse read(ProtocolReader r) {
      return new OffsetForLeaderEpochResponse(
          r.readNonNullArray(
              t ->
                  new EpochTopicResult(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p ->
                              new EpochEnd(
                                  p.readInt(), p.readShort(), p.readInt(), p.readLong())))));
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
                        .writeInt(p.leaderEpoch())
                        .writeLong(p.endOffset()));
          });
    }
  }
}
