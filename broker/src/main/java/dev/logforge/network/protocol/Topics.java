package dev.logforge.network.protocol;

import java.util.List;
import java.util.Map;

/**
 * CreateTopics (key 4) and DeleteTopics (key 5), PROTOCOL.md 5.4 and 5.5. Sent to the controller.
 */
public final class Topics {
  private Topics() {}

  public record NewTopic(
      String name, int numPartitions, short replicationFactor, Map<String, String> configs) {}

  public record CreateRequest(int timeoutMs, List<NewTopic> topics) {
    public static CreateRequest read(ProtocolReader r) {
      int timeout = r.readInt();
      return new CreateRequest(
          timeout,
          r.readNonNullArray(
              t ->
                  new NewTopic(
                      t.readNonNullString(), t.readInt(), t.readShort(), t.readStringMap())));
    }

    public void write(ProtocolWriter w) {
      w.writeInt(timeoutMs);
      w.writeArray(
          topics,
          (tw, t) ->
              tw.writeString(t.name())
                  .writeInt(t.numPartitions())
                  .writeShort(t.replicationFactor())
                  .writeStringMap(t.configs()));
    }
  }

  public record CreateResult(String name, short errorCode, String errorMessage) {}

  public record CreateResponse(List<CreateResult> topics) {
    public static CreateResponse read(ProtocolReader r) {
      return new CreateResponse(
          r.readNonNullArray(
              t -> new CreateResult(t.readNonNullString(), t.readShort(), t.readString())));
    }

    public void write(ProtocolWriter w) {
      w.writeArray(
          topics,
          (tw, t) ->
              tw.writeString(t.name()).writeShort(t.errorCode()).writeString(t.errorMessage()));
    }
  }

  public record DeleteRequest(int timeoutMs, List<String> names) {
    public static DeleteRequest read(ProtocolReader r) {
      int timeout = r.readInt();
      return new DeleteRequest(timeout, r.readNonNullArray(ProtocolReader::readNonNullString));
    }

    public void write(ProtocolWriter w) {
      w.writeInt(timeoutMs);
      w.writeArray(names, ProtocolWriter::writeString);
    }
  }

  public record DeleteResult(String name, short errorCode) {}

  public record DeleteResponse(List<DeleteResult> topics) {
    public static DeleteResponse read(ProtocolReader r) {
      return new DeleteResponse(
          r.readNonNullArray(t -> new DeleteResult(t.readNonNullString(), t.readShort())));
    }

    public void write(ProtocolWriter w) {
      w.writeArray(topics, (tw, t) -> tw.writeString(t.name()).writeShort(t.errorCode()));
    }
  }
}
