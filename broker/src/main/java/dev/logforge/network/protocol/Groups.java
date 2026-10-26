package dev.logforge.network.protocol;

import java.util.List;

/**
 * Group APIs, PROTOCOL.md 5.6 to 5.11: FindCoordinator (6), JoinGroup (7), Heartbeat (8),
 * LeaveGroup (9), OffsetCommit (10), OffsetFetch (11).
 */
public final class Groups {
  private Groups() {}

  // ---- FindCoordinator ----

  public record FindCoordinatorRequest(String groupId) {
    public static FindCoordinatorRequest read(ProtocolReader r) {
      return new FindCoordinatorRequest(r.readNonNullString());
    }

    public void write(ProtocolWriter w) {
      w.writeString(groupId);
    }
  }

  public record FindCoordinatorResponse(short errorCode, int nodeId, String host, int port) {
    public static FindCoordinatorResponse read(ProtocolReader r) {
      return new FindCoordinatorResponse(r.readShort(), r.readInt(), r.readString(), r.readInt());
    }

    public void write(ProtocolWriter w) {
      w.writeShort(errorCode).writeInt(nodeId).writeString(host).writeInt(port);
    }
  }

  // ---- JoinGroup ----

  public record JoinRequest(
      String groupId,
      int sessionTimeoutMs,
      int rebalanceTimeoutMs,
      String memberId,
      String assignor,
      List<String> topics) {
    public static JoinRequest read(ProtocolReader r) {
      return new JoinRequest(
          r.readNonNullString(),
          r.readInt(),
          r.readInt(),
          r.readNonNullString(),
          r.readNonNullString(),
          r.readNonNullArray(ProtocolReader::readNonNullString));
    }

    public void write(ProtocolWriter w) {
      w.writeString(groupId)
          .writeInt(sessionTimeoutMs)
          .writeInt(rebalanceTimeoutMs)
          .writeString(memberId)
          .writeString(assignor);
      w.writeArray(topics, ProtocolWriter::writeString);
    }
  }

  public record TopicAssignment(String topic, List<Integer> partitions) {}

  public record JoinResponse(
      short errorCode,
      int generationId,
      String assignor,
      String leaderId,
      String memberId,
      List<String> members,
      List<TopicAssignment> assignment) {
    public static JoinResponse error(short code, String memberId) {
      return new JoinResponse(code, -1, "", "", memberId, List.of(), List.of());
    }

    public static JoinResponse read(ProtocolReader r) {
      return new JoinResponse(
          r.readShort(),
          r.readInt(),
          r.readNonNullString(),
          r.readNonNullString(),
          r.readNonNullString(),
          r.readNonNullArray(ProtocolReader::readNonNullString),
          r.readNonNullArray(t -> new TopicAssignment(t.readNonNullString(), t.readIntArray())));
    }

    public void write(ProtocolWriter w) {
      w.writeShort(errorCode)
          .writeInt(generationId)
          .writeString(assignor)
          .writeString(leaderId)
          .writeString(memberId);
      w.writeArray(members, ProtocolWriter::writeString);
      w.writeArray(assignment, (aw, a) -> aw.writeString(a.topic()).writeIntArray(a.partitions()));
    }
  }

  // ---- Heartbeat / LeaveGroup ----

  public record HeartbeatRequest(String groupId, int generationId, String memberId) {
    public static HeartbeatRequest read(ProtocolReader r) {
      return new HeartbeatRequest(r.readNonNullString(), r.readInt(), r.readNonNullString());
    }

    public void write(ProtocolWriter w) {
      w.writeString(groupId).writeInt(generationId).writeString(memberId);
    }
  }

  public record LeaveRequest(String groupId, String memberId) {
    public static LeaveRequest read(ProtocolReader r) {
      return new LeaveRequest(r.readNonNullString(), r.readNonNullString());
    }

    public void write(ProtocolWriter w) {
      w.writeString(groupId).writeString(memberId);
    }
  }

  /** Response body for Heartbeat and LeaveGroup: a single error code. */
  public record ErrorResponse(short errorCode) {
    public static ErrorResponse read(ProtocolReader r) {
      return new ErrorResponse(r.readShort());
    }

    public void write(ProtocolWriter w) {
      w.writeShort(errorCode);
    }
  }

  // ---- OffsetCommit ----

  public record CommitPartition(int partition, long offset, String metadata) {}

  public record CommitTopic(String name, List<CommitPartition> partitions) {}

  public record OffsetCommitRequest(
      String groupId, int generationId, String memberId, List<CommitTopic> topics) {
    public static OffsetCommitRequest read(ProtocolReader r) {
      return new OffsetCommitRequest(
          r.readNonNullString(),
          r.readInt(),
          r.readNonNullString(),
          r.readNonNullArray(
              t ->
                  new CommitTopic(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p -> new CommitPartition(p.readInt(), p.readLong(), p.readString())))));
    }

    public void write(ProtocolWriter w) {
      w.writeString(groupId).writeInt(generationId).writeString(memberId);
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeInt(p.partition()).writeLong(p.offset()).writeString(p.metadata()));
          });
    }
  }

  public record PartitionError(int partition, short errorCode) {}

  public record TopicErrors(String name, List<PartitionError> partitions) {}

  public record OffsetCommitResponse(List<TopicErrors> topics) {
    public static OffsetCommitResponse read(ProtocolReader r) {
      return new OffsetCommitResponse(
          r.readNonNullArray(
              t ->
                  new TopicErrors(
                      t.readNonNullString(),
                      t.readNonNullArray(p -> new PartitionError(p.readInt(), p.readShort())))));
    }

    public void write(ProtocolWriter w) {
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(), (pw, p) -> pw.writeInt(p.partition()).writeShort(p.errorCode()));
          });
    }
  }

  // ---- OffsetFetch ----

  public record FetchTopic(String name, List<Integer> partitions) {}

  public record OffsetFetchRequest(String groupId, List<FetchTopic> topics) {
    public static OffsetFetchRequest read(ProtocolReader r) {
      return new OffsetFetchRequest(
          r.readNonNullString(),
          r.readArray(t -> new FetchTopic(t.readNonNullString(), t.readIntArray())));
    }

    public void write(ProtocolWriter w) {
      w.writeString(groupId);
      w.writeArray(topics, (tw, t) -> tw.writeString(t.name()).writeIntArray(t.partitions()));
    }
  }

  public record FetchedOffset(int partition, long offset, short errorCode) {}

  public record FetchedTopic(String name, List<FetchedOffset> partitions) {}

  public record OffsetFetchResponse(short errorCode, List<FetchedTopic> topics) {
    public static OffsetFetchResponse read(ProtocolReader r) {
      return new OffsetFetchResponse(
          r.readShort(),
          r.readNonNullArray(
              t ->
                  new FetchedTopic(
                      t.readNonNullString(),
                      t.readNonNullArray(
                          p -> new FetchedOffset(p.readInt(), p.readLong(), p.readShort())))));
    }

    public void write(ProtocolWriter w) {
      w.writeShort(errorCode);
      w.writeArray(
          topics,
          (tw, t) -> {
            tw.writeString(t.name());
            tw.writeArray(
                t.partitions(),
                (pw, p) ->
                    pw.writeInt(p.partition()).writeLong(p.offset()).writeShort(p.errorCode()));
          });
    }
  }
}
