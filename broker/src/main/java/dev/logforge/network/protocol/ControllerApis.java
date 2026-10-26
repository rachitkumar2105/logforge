package dev.logforge.network.protocol;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Internal broker-to-controller APIs: BrokerHeartbeat (100) and AlterIsr (101), plus the metadata
 * snapshot carried in heartbeat responses. PROTOCOL.md 5.14 and 5.15.
 */
public final class ControllerApis {
  private ControllerApis() {}

  // ---- BrokerHeartbeat ----

  public record HeartbeatRequest(
      int brokerId,
      long brokerEpoch,
      String host,
      int port,
      long metadataVersion,
      ByteBuffer status) {
    public static HeartbeatRequest read(ProtocolReader r) {
      return new HeartbeatRequest(
          r.readInt(),
          r.readLong(),
          r.readNonNullString(),
          r.readInt(),
          r.readLong(),
          r.readBytes());
    }

    public void write(ProtocolWriter w) {
      w.writeInt(brokerId)
          .writeLong(brokerEpoch)
          .writeString(host)
          .writeInt(port)
          .writeLong(metadataVersion)
          .writeBytes(status);
    }
  }

  public record HeartbeatResponse(short errorCode, long metadataVersion, ByteBuffer snapshot) {
    public static HeartbeatResponse read(ProtocolReader r) {
      return new HeartbeatResponse(r.readShort(), r.readLong(), r.readBytes());
    }

    public void write(ProtocolWriter w) {
      w.writeShort(errorCode).writeLong(metadataVersion).writeBytes(snapshot);
    }
  }

  // ---- Metadata snapshot ----

  public record BrokerInfo(int nodeId, String host, int port, boolean alive) {}

  public record PartitionInfo(
      int partition,
      int leader,
      int leaderEpoch,
      int partitionEpoch,
      List<Integer> replicas,
      List<Integer> isr) {}

  public record TopicInfo(
      String name, Map<String, String> configs, List<PartitionInfo> partitions) {}

  public record Snapshot(
      long version,
      String clusterId,
      int controllerId,
      List<BrokerInfo> brokers,
      List<TopicInfo> topics) {
    public static Snapshot read(ProtocolReader r) {
      long version = r.readLong();
      String clusterId = r.readNonNullString();
      int controllerId = r.readInt();
      List<BrokerInfo> brokers =
          r.readNonNullArray(
              b ->
                  new BrokerInfo(b.readInt(), b.readNonNullString(), b.readInt(), b.readBoolean()));
      List<TopicInfo> topics =
          r.readNonNullArray(
              t ->
                  new TopicInfo(
                      t.readNonNullString(),
                      t.readStringMap(),
                      t.readNonNullArray(
                          p ->
                              new PartitionInfo(
                                  p.readInt(),
                                  p.readInt(),
                                  p.readInt(),
                                  p.readInt(),
                                  p.readIntArray(),
                                  p.readIntArray()))));
      return new Snapshot(version, clusterId, controllerId, brokers, topics);
    }

    public void write(ProtocolWriter w) {
      w.writeLong(version).writeString(clusterId).writeInt(controllerId);
      w.writeArray(
          brokers,
          (bw, b) ->
              bw.writeInt(b.nodeId())
                  .writeString(b.host())
                  .writeInt(b.port())
                  .writeBoolean(b.alive()));
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name()).writeStringMap(t.configs());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeInt(p.partition())
                        .writeInt(p.leader())
                        .writeInt(p.leaderEpoch())
                        .writeInt(p.partitionEpoch())
                        .writeIntArray(p.replicas())
                        .writeIntArray(p.isr()));
          });
    }

    public ByteBuffer encode() {
      ProtocolWriter w = new ProtocolWriter(1024);
      write(w);
      return w.toBuffer();
    }

    public static Snapshot decode(ByteBuffer buf) {
      ProtocolReader r = new ProtocolReader(buf.duplicate());
      Snapshot s = read(r);
      r.expectEnd();
      return s;
    }
  }

  // ---- AlterIsr ----

  public record AlterPartition(
      int partition, int leaderEpoch, int partitionEpoch, List<Integer> newIsr) {}

  public record AlterTopic(String name, List<AlterPartition> partitions) {}

  public record AlterIsrRequest(int brokerId, long brokerEpoch, List<AlterTopic> topics) {
    public static AlterIsrRequest read(ProtocolReader r) {
      int brokerId = r.readInt();
      long brokerEpoch = r.readLong();
      return new AlterIsrRequest(
          brokerId,
          brokerEpoch,
          r.readNonNullArray(
              t ->
                  new AlterTopic(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p ->
                              new AlterPartition(
                                  p.readInt(), p.readInt(), p.readInt(), p.readIntArray())))));
    }

    public void write(ProtocolWriter w) {
      w.writeInt(brokerId).writeLong(brokerEpoch);
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeInt(p.partition())
                        .writeInt(p.leaderEpoch())
                        .writeInt(p.partitionEpoch())
                        .writeIntArray(p.newIsr()));
          });
    }
  }

  public record AlterResult(
      int partition, short errorCode, int leaderEpoch, int partitionEpoch, List<Integer> isr) {}

  public record AlterTopicResult(String name, List<AlterResult> partitions) {}

  public record AlterIsrResponse(List<AlterTopicResult> topics) {
    public static AlterIsrResponse read(ProtocolReader r) {
      return new AlterIsrResponse(
          r.readNonNullArray(
              t ->
                  new AlterTopicResult(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p ->
                              new AlterResult(
                                  p.readInt(),
                                  p.readShort(),
                                  p.readInt(),
                                  p.readInt(),
                                  p.readIntArray())))));
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
                        .writeInt(p.partitionEpoch())
                        .writeIntArray(p.isr()));
          });
    }
  }
}
