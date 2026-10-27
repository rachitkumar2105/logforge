package dev.logforge;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.log.RecordBatch;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.ListOffsets;
import dev.logforge.network.protocol.Metadata;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.Topics;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SingleBrokerTest {

  @TempDir Path dir;
  Node node;
  TestClient client;

  @BeforeEach
  void start() throws Exception {
    node = TestNodes.single(dir);
    client = new TestClient("127.0.0.1", node.port());
  }

  @AfterEach
  void stop() throws Exception {
    client.close();
    node.close();
  }

  private void createTopic(String name, int partitions, Map<String, String> configs)
      throws Exception {
    Topics.CreateResponse r = client.createTopic(name, partitions, 1, configs);
    assertThat(r.topics().get(0).errorCode()).isEqualTo(Errors.NONE.code());
    for (int p = 0; p < partitions; p++) {
      int part = p;
      TestNodes.awaitTrue(() -> node.isLeaderFor(name, part), Duration.ofSeconds(10));
    }
  }

  @Test
  void metadataListsTopicsPartitionsAndTheBroker() throws Exception {
    createTopic("orders", 3, Map.of());
    Metadata.Response md = client.metadata(null);
    assertThat(md.brokers()).extracting(Metadata.Broker::nodeId).contains(0);
    Metadata.TopicMeta orders =
        md.topics().stream().filter(t -> t.name().equals("orders")).findFirst().orElseThrow();
    assertThat(orders.partitions()).hasSize(3);
    assertThat(orders.partitions()).allSatisfy(p -> assertThat(p.leader()).isEqualTo(0));
    assertThat(md.controllerId()).isEqualTo(0);

    Metadata.Response unknown = client.metadata(List.of("nope"));
    assertThat(unknown.topics().get(0).errorCode())
        .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
  }

  @Test
  void createTopicValidatesInput() throws Exception {
    assertThat(client.createTopic("bad name!", 1, 1, Map.of()).topics().get(0).errorCode())
        .isEqualTo(Errors.INVALID_TOPIC.code());
    assertThat(client.createTopic("__internal", 1, 1, Map.of()).topics().get(0).errorCode())
        .isEqualTo(Errors.INVALID_TOPIC.code());
    assertThat(client.createTopic("zero", 0, 1, Map.of()).topics().get(0).errorCode())
        .isEqualTo(Errors.INVALID_PARTITIONS.code());
    assertThat(client.createTopic("rf", 1, 3, Map.of()).topics().get(0).errorCode())
        .isEqualTo(Errors.INVALID_REPLICATION_FACTOR.code());
    assertThat(client.createTopic("cfg", 1, 1, Map.of("bogus", "1")).topics().get(0).errorCode())
        .isEqualTo(Errors.INVALID_REQUEST.code());
    createTopic("dup", 1, Map.of());
    assertThat(client.createTopic("dup", 1, 1, Map.of()).topics().get(0).errorCode())
        .isEqualTo(Errors.TOPIC_ALREADY_EXISTS.code());
  }

  @Test
  void produceAndFetchWithEveryAcksMode() throws Exception {
    createTopic("acks", 1, Map.of());
    Produce.PartitionResult one = client.produce("acks", 0, (short) 1, TestClient.batch("a", "b"));
    assertThat(one.errorCode()).isEqualTo(Errors.NONE.code());
    assertThat(one.baseOffset()).isZero();

    Produce.PartitionResult all = client.produce("acks", 0, (short) -1, TestClient.batch("c"));
    assertThat(all.errorCode()).isEqualTo(Errors.NONE.code());
    assertThat(all.baseOffset()).isEqualTo(2L);

    assertThat(client.produce("acks", 0, (short) 0, TestClient.batch("d"))).isNull();
    // acks=0 sends nothing back: the next response on this connection is for the next request.
    TestNodes.awaitTrue(() -> node.logEndOffset("acks", 0) == 4L, Duration.ofSeconds(5));

    Fetch.PartitionResult f = client.fetch("acks", 0, 0L, 0);
    assertThat(f.errorCode()).isEqualTo(Errors.NONE.code());
    assertThat(f.highWatermark()).isEqualTo(4L);
    List<String> values =
        RecordBatch.iterate(f.records()).stream()
            .flatMap(b -> b.records().stream())
            .map(r -> new String(r.value(), UTF_8))
            .toList();
    assertThat(values).containsExactly("a", "b", "c", "d");
  }

  @Test
  void fetchFromTheMiddleSkipsNothingAndLongPollWaitsForData() throws Exception {
    createTopic("poll", 1, Map.of());
    for (int i = 0; i < 10; i++) {
      client.produce("poll", 0, (short) 1, TestClient.batch("v" + i));
    }
    Fetch.PartitionResult mid = client.fetch("poll", 0, 7L, 0);
    assertThat(RecordBatch.iterate(mid.records()).get(0).baseOffset()).isEqualTo(7L);

    Thread producer =
        new Thread(
            () -> {
              try (TestClient other = new TestClient("127.0.0.1", node.port())) {
                Thread.sleep(300);
                other.produce("poll", 0, (short) 1, TestClient.batch("late"));
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    producer.start();
    long t0 = System.nanoTime();
    Fetch.PartitionResult waited = client.fetch("poll", 0, 10L, 5_000);
    long ms = (System.nanoTime() - t0) / 1_000_000;
    producer.join();
    assertThat(waited.records().remaining()).isPositive();
    assertThat(ms).isBetween(200L, 4_000L);

    long t1 = System.nanoTime();
    Fetch.PartitionResult empty = client.fetch("poll", 0, 11L, 300);
    assertThat(empty.records().remaining()).isZero();
    assertThat((System.nanoTime() - t1) / 1_000_000).isGreaterThanOrEqualTo(250L);
  }

  @Test
  void errorsAreTypedPerPartition() throws Exception {
    createTopic("errs", 1, Map.of("max.batch.bytes", "1024"));
    assertThat(client.produce("missing", 0, (short) 1, TestClient.batch("x")).errorCode())
        .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    assertThat(client.produce("errs", 5, (short) 1, TestClient.batch("x")).errorCode())
        .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    assertThat(client.produce("errs", 0, (short) 1, TestClient.batch("y".repeat(2000))).errorCode())
        .isEqualTo(Errors.MESSAGE_TOO_LARGE.code());

    RecordBatch corrupt = TestClient.batch("value");
    java.nio.ByteBuffer b = corrupt.buffer();
    b.put(b.limit() - 2, (byte) (b.get(b.limit() - 2) ^ 1));
    assertThat(client.produce("errs", 0, (short) 1, corrupt).errorCode())
        .isEqualTo(Errors.CORRUPT_MESSAGE.code());

    client.produce("errs", 0, (short) 1, TestClient.batch("ok"));
    assertThat(client.fetch("errs", 0, 5L, 0).errorCode())
        .isEqualTo(Errors.OFFSET_OUT_OF_RANGE.code());
    assertThat(client.fetch(-1, 99, "errs", 0, 0L, 0).errorCode())
        .isEqualTo(Errors.UNKNOWN_LEADER_EPOCH.code());
  }

  @Test
  void listOffsetsReturnsEarliestAndLatest() throws Exception {
    createTopic("lo", 1, Map.of());
    for (int i = 0; i < 5; i++) {
      client.produce("lo", 0, (short) 1, TestClient.batch("v"));
    }
    ListOffsets.PartitionResult latest = client.listOffset("lo", 0, ListOffsets.LATEST);
    ListOffsets.PartitionResult earliest = client.listOffset("lo", 0, ListOffsets.EARLIEST);
    assertThat(latest.offset()).isEqualTo(5L);
    assertThat(earliest.offset()).isZero();
  }

  @Test
  void dataSurvivesAGracefulRestart() throws Exception {
    createTopic("durable", 2, Map.of());
    for (int i = 0; i < 20; i++) {
      client.produce("durable", i % 2, (short) 1, TestClient.batch("v" + i));
    }
    client.close();
    node.close();

    node = TestNodes.single(dir);
    client = new TestClient("127.0.0.1", node.port());
    TestNodes.awaitTrue(() -> node.isLeaderFor("durable", 1), Duration.ofSeconds(10));
    assertThat(client.fetch("durable", 1, 0L, 0).highWatermark()).isEqualTo(10L);
    assertThat(client.produce("durable", 0, (short) 1, TestClient.batch("after")).baseOffset())
        .isEqualTo(10L);
  }

  @Test
  void deleteTopicRemovesItsData() throws Exception {
    createTopic("gone", 1, Map.of());
    client.produce("gone", 0, (short) 1, TestClient.batch("v"));
    Topics.DeleteResponse r =
        Topics.DeleteResponse.read(
            client.call(
                dev.logforge.network.protocol.ApiKeys.DELETE_TOPICS,
                w -> new Topics.DeleteRequest(5_000, List.of("gone")).write(w)));
    assertThat(r.topics().get(0).errorCode()).isEqualTo(Errors.NONE.code());
    TestNodes.awaitTrue(() -> !node.hasPartition("gone", 0), Duration.ofSeconds(10));
    // The broker drops the partition from its map, then deletes the files: wait for the files.
    java.nio.file.Path partitionDir = dir.resolve("node-0").resolve("gone-0");
    TestNodes.awaitTrue(() -> !java.nio.file.Files.exists(partitionDir), Duration.ofSeconds(10));
    assertThat(java.nio.file.Files.exists(partitionDir)).isFalse();
  }
}
