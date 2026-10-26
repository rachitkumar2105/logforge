package dev.logforge.network.protocol;

import java.nio.ByteBuffer;
import java.util.List;

/** Fetch (key 1), PROTOCOL.md 5.1. Used by consumers and by followers. */
public final class Fetch {
  private Fetch() {}

  public static final int CONSUMER_REPLICA_ID = -1;

  public record PartitionData(
      int partition, int currentLeaderEpoch, long fetchOffset, int partitionMaxBytes) {}

  public record TopicData(String name, List<PartitionData> partitions) {}

  public record Request(
      int replicaId, int maxWaitMs, int minBytes, int maxBytes, List<TopicData> topics) {
    public boolean isFromFollower() {
      return replicaId >= 0;
    }

    public static Request read(ProtocolReader r) {
      int replicaId = r.readInt();
      int maxWait = r.readInt();
      int minBytes = r.readInt();
      int maxBytes = r.readInt();
      List<TopicData> topics =
          r.readNonNullArray(
              t ->
                  new TopicData(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p ->
                              new PartitionData(
                                  p.readInt(), p.readInt(), p.readLong(), p.readInt()))));
      return new Request(replicaId, maxWait, minBytes, maxBytes, topics);
    }

    public void write(ProtocolWriter w) {
      w.writeInt(replicaId).writeInt(maxWaitMs).writeInt(minBytes).writeInt(maxBytes);
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeInt(p.partition())
                        .writeInt(p.currentLeaderEpoch())
                        .writeLong(p.fetchOffset())
                        .writeInt(p.partitionMaxBytes()));
          });
    }
  }

  public record PartitionResult(
      int partition,
      short errorCode,
      long highWatermark,
      long logStartOffset,
      long logEndOffset,
      ByteBuffer records) {}

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
                                  p.readInt(),
                                  p.readShort(),
                                  p.readLong(),
                                  p.readLong(),
                                  p.readLong(),
                                  p.readBytes())))));
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
                        .writeLong(p.highWatermark())
                        .writeLong(p.logStartOffset())
                        .writeLong(p.logEndOffset())
                        .writeBytes(p.records()));
          });
    }

    public int sizeInBytes() {
      int n = 0;
      for (TopicResult t : topics) {
        for (PartitionResult p : t.partitions()) {
          n += p.records() == null ? 0 : p.records().remaining();
        }
      }
      return n;
    }
  }
}
