package dev.logforge.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.Node;
import dev.logforge.TopicPartition;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EndToEndTest {

  @TempDir Path dir;
  Node node;

  @BeforeEach
  void start() throws Exception {
    node = EmbeddedNode.start(dir);
  }

  @AfterEach
  void stop() throws Exception {
    node.close();
  }

  @Test
  void produceThenConsumeAcrossARealSocket() throws Exception {
    String bootstrap = EmbeddedNode.bootstrap(node);
    try (Admin admin = new Admin(bootstrap, TestToken.VALUE)) {
      admin.createTopic("orders", 3, 1, Map.of());
    }

    Map<Integer, List<String>> sentPerPartition = new HashMap<>();
    try (Producer producer =
        new Producer(ProducerConfig.builder(bootstrap).acks(Acks.LEADER).lingerMs(5).build())) {
      List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
      for (int i = 0; i < 1_000; i++) {
        futures.add(
            producer.send(
                "orders", ("key-" + (i % 37)).getBytes(UTF_8), ("value-" + i).getBytes(UTF_8)));
      }
      producer.flush();
      for (int i = 0; i < futures.size(); i++) {
        RecordMetadata md = futures.get(i).get();
        assertThat(md.topic()).isEqualTo("orders");
        sentPerPartition.computeIfAbsent(md.partition(), k -> new ArrayList<>()).add("value-" + i);
      }
    }
    assertThat(sentPerPartition).hasSize(3);

    Map<Integer, List<String>> received = new HashMap<>();
    try (Consumer consumer = new Consumer(ConsumerConfig.builder(bootstrap).build())) {
      consumer.assign(
          List.of(
              new TopicPartition("orders", 0),
              new TopicPartition("orders", 1),
              new TopicPartition("orders", 2)));
      long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      int total = 0;
      while (total < 1_000 && System.nanoTime() < deadline) {
        for (ConsumerRecord r : consumer.poll(Duration.ofMillis(200))) {
          received
              .computeIfAbsent(r.partition(), k -> new ArrayList<>())
              .add(new String(r.value(), UTF_8));
          total++;
        }
      }
    }
    // Same records, same per-partition order.
    assertThat(received).isEqualTo(sentPerPartition);
  }

  @Test
  void recordsWithTheSameKeyLandOnTheSamePartitionInOrder() throws Exception {
    String bootstrap = EmbeddedNode.bootstrap(node);
    try (Admin admin = new Admin(bootstrap, TestToken.VALUE)) {
      admin.createTopic("keyed", 4, 1, Map.of());
    }
    List<RecordMetadata> mds = new ArrayList<>();
    try (Producer producer = new Producer(ProducerConfig.builder(bootstrap).build())) {
      for (int i = 0; i < 50; i++) {
        mds.add(
            producer.send("keyed", "same-key".getBytes(UTF_8), ("v" + i).getBytes(UTF_8)).get());
      }
    }
    assertThat(mds).extracting(RecordMetadata::partition).containsOnly(mds.get(0).partition());
    for (int i = 1; i < mds.size(); i++) {
      assertThat(mds.get(i).offset()).isGreaterThan(mds.get(i - 1).offset());
    }
  }

  @Test
  void acksAllAndFireAndForgetBothDeliver() throws Exception {
    String bootstrap = EmbeddedNode.bootstrap(node);
    try (Admin admin = new Admin(bootstrap, TestToken.VALUE)) {
      admin.createTopic("acks", 1, 1, Map.of());
    }
    try (Producer all = new Producer(ProducerConfig.builder(bootstrap).acks(Acks.ALL).build());
        Producer none =
            new Producer(
                ProducerConfig.builder(bootstrap).acks(Acks.NONE).idempotent(false).build())) {
      assertThat(all.send("acks", null, "a".getBytes(UTF_8)).get().offset()).isZero();
      RecordMetadata fireAndForget = none.send("acks", null, "b".getBytes(UTF_8)).get();
      assertThat(fireAndForget.offset()).isEqualTo(RecordMetadata.UNKNOWN_OFFSET);
      none.flush();
    }
    try (Consumer consumer = new Consumer(ConsumerConfig.builder(bootstrap).build())) {
      consumer.assign(List.of(new TopicPartition("acks", 0)));
      List<String> values = new ArrayList<>();
      long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (values.size() < 2 && System.nanoTime() < deadline) {
        consumer
            .poll(Duration.ofMillis(200))
            .forEach(r -> values.add(new String(r.value(), UTF_8)));
      }
      assertThat(values).containsExactly("a", "b");
    }
  }
}
