package dev.logforge.client;

import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.Node;
import dev.logforge.TopicPartition;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 3 gate: one million records produced at acks=1 and consumed back with zero loss and zero
 * duplicates. Each record's value carries its sequence number; the consumer marks each number in a
 * bit set and fails on a second sighting.
 */
@Tag("gate")
class MillionRecordsTest {
  static final int N = 1_000_000;

  @TempDir Path dir;

  @Test
  void oneMillionRecordsAtAcksOneZeroLossZeroDuplicates() throws Exception {
    try (Node node = EmbeddedNode.start(dir)) {
      String bootstrap = EmbeddedNode.bootstrap(node);
      try (Admin admin = new Admin(bootstrap)) {
        admin.createTopic("million", 4, 1, Map.of());
      }

      long t0 = System.nanoTime();
      AtomicLong failures = new AtomicLong();
      try (Producer producer =
          new Producer(
              ProducerConfig.builder(bootstrap)
                  .acks(Acks.LEADER)
                  .batchSize(64 * 1024)
                  .lingerMs(5)
                  .build())) {
        byte[] padding = new byte[92];
        for (int i = 0; i < N; i++) {
          ByteBuffer value = ByteBuffer.allocate(100).putInt(i).putInt(~i).put(padding);
          producer
              .send("million", null, value.array())
              .whenComplete(
                  (md, err) -> {
                    if (err != null) {
                      failures.incrementAndGet();
                    }
                  });
        }
        producer.flush();
      }
      long produceMs = (System.nanoTime() - t0) / 1_000_000;
      assertThat(failures.get()).as("failed sends").isZero();

      BitSet seen = new BitSet(N);
      long duplicates = 0;
      long received = 0;
      long t1 = System.nanoTime();
      try (Consumer consumer =
          new Consumer(ConsumerConfig.builder(bootstrap).maxPollRecords(10_000).build())) {
        consumer.assign(
            List.of(
                new TopicPartition("million", 0),
                new TopicPartition("million", 1),
                new TopicPartition("million", 2),
                new TopicPartition("million", 3)));
        long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        while (received < N && System.nanoTime() < deadline) {
          for (ConsumerRecord r : consumer.poll(Duration.ofMillis(500))) {
            ByteBuffer v = ByteBuffer.wrap(r.value());
            int id = v.getInt();
            assertThat(v.getInt()).as("value integrity").isEqualTo(~id);
            if (seen.get(id)) {
              duplicates++;
            }
            seen.set(id);
            received++;
          }
        }
      }
      long consumeMs = (System.nanoTime() - t1) / 1_000_000;
      System.out.printf(
          "GATE 1M: produced %d in %d ms, consumed %d in %d ms, distinct=%d, duplicates=%d%n",
          N, produceMs, received, consumeMs, seen.cardinality(), duplicates);
      assertThat(duplicates).as("duplicates").isZero();
      assertThat(seen.cardinality()).as("distinct records received").isEqualTo(N);
      assertThat(received).isEqualTo(N);
    }
  }
}
