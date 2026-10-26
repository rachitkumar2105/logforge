package dev.logforge.client;

import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.ListOffsets;
import dev.logforge.network.protocol.Metadata;
import dev.logforge.network.protocol.Topics;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Topic administration. Requests go to the controller; the client finds it through metadata. */
public final class Admin implements AutoCloseable {
  private final ClientMetadata metadata;
  private final int timeoutMs;

  public Admin(String bootstrapServers) {
    this(bootstrapServers, 30_000);
  }

  public Admin(String bootstrapServers, int timeoutMs) {
    this.metadata = new ClientMetadata(bootstrapServers, "logforge-admin", timeoutMs);
    this.timeoutMs = timeoutMs;
    this.metadata.refresh();
  }

  /**
   * Creates a topic and waits until every partition's leader is serving it.
   *
   * @throws ClientException with the controller's error code on failure
   */
  public void createTopic(
      String name, int partitions, int replicationFactor, Map<String, String> configs) {
    Topics.CreateRequest req =
        new Topics.CreateRequest(
            timeoutMs,
            List.of(new Topics.NewTopic(name, partitions, (short) replicationFactor, configs)));
    Topics.CreateResult r =
        controllerCall(ApiKeys.CREATE_TOPICS, req::write, Topics.CreateResponse::read)
            .topics()
            .get(0);
    if (r.errorCode() != Errors.NONE.code()) {
      throw new ClientException(
          Errors.forCode(r.errorCode()), "create topic " + name + ": " + r.errorMessage());
    }
    awaitServing(name, partitions);
  }

  public void deleteTopic(String name) {
    Topics.DeleteRequest req = new Topics.DeleteRequest(timeoutMs, List.of(name));
    Topics.DeleteResult r =
        controllerCall(ApiKeys.DELETE_TOPICS, req::write, Topics.DeleteResponse::read)
            .topics()
            .get(0);
    if (r.errorCode() != Errors.NONE.code()) {
      throw new ClientException(Errors.forCode(r.errorCode()), "delete topic " + name);
    }
  }

  public Metadata.Response describeCluster() {
    return metadata.refresh();
  }

  private <T> T controllerCall(
      short apiKey,
      java.util.function.Consumer<dev.logforge.network.protocol.ProtocolWriter> body,
      java.util.function.Function<dev.logforge.network.protocol.ProtocolReader, T> reader) {
    metadata.refresh();
    int controller = metadata.controllerId();
    if (controller < 0) {
      throw new ClientException(Errors.NOT_CONTROLLER, "no controller in metadata");
    }
    try {
      return reader.apply(metadata.channel(controller).request(apiKey, body));
    } catch (IOException e) {
      throw new ClientException("controller unreachable", e);
    }
  }

  /** Waits until each partition has a leader that answers ListOffsets for it. */
  private void awaitServing(String topic, int partitions) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    java.util.Set<Integer> pending = new java.util.TreeSet<>();
    for (int p = 0; p < partitions; p++) {
      pending.add(p);
    }
    while (!pending.isEmpty()) {
      if (System.currentTimeMillis() > deadline) {
        throw new ClientException(
            Errors.REQUEST_TIMED_OUT, "topic " + topic + " partitions " + pending + " not ready");
      }
      try {
        metadata.refresh(); // once per round, not once per partition per round
      } catch (RuntimeException e) {
        // retry next round
      }
      pending.removeIf(p -> leaderServes(topic, p));
      if (!pending.isEmpty()) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ClientException("interrupted", e);
        }
      }
    }
  }

  private boolean leaderServes(String topic, int partition) {
    try {
      int leader = metadata.leaderFor(new dev.logforge.TopicPartition(topic, partition));
      if (leader < 0) {
        return false;
      }
      ListOffsets.Request req =
          new ListOffsets.Request(
              -1,
              List.of(
                  new ListOffsets.TopicData(
                      topic,
                      List.of(new ListOffsets.PartitionData(partition, ListOffsets.LATEST)))));
      ListOffsets.PartitionResult r =
          ListOffsets.Response.read(
                  metadata.channel(leader).request(ApiKeys.LIST_OFFSETS, req::write))
              .topics()
              .get(0)
              .partitions()
              .get(0);
      return r.errorCode() == Errors.NONE.code();
    } catch (IOException | RuntimeException e) {
      return false;
    }
  }

  @Override
  public void close() {
    metadata.close();
  }
}
