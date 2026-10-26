package dev.logforge.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.Node;
import dev.logforge.TopicPartition;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsumerGroupTest {

  @TempDir Path dir;

  /** A group member on its own thread: poll, record, commit. */
  private static final class Member implements AutoCloseable {
    final Consumer consumer;
    final Thread thread;
    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicBoolean crashed = new AtomicBoolean(false);
    final Map<Integer, List<Long>> seen = new ConcurrentHashMap<>();

    Member(String bootstrap, String name) {
      consumer =
          new Consumer(
              ConsumerConfig.builder(bootstrap)
                  .clientId(name)
                  .groupId("billing")
                  .assignor("range")
                  .sessionTimeoutMs(1_500)
                  .heartbeatIntervalMs(200)
                  .rebalanceTimeoutMs(5_000)
                  .maxPollRecords(50)
                  .build());
      thread =
          new Thread(
              () -> {
                consumer.subscribe(List.of("orders"));
                while (running.get()) {
                  List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(100));
                  if (crashed.get()) {
                    return; // died after fetching: nothing below is done, nothing committed
                  }
                  for (ConsumerRecord r : records) {
                    seen.computeIfAbsent(r.partition(), k -> new ArrayList<>()).add(r.offset());
                  }
                  if (!records.isEmpty()) {
                    try {
                      consumer.commitSync();
                    } catch (ClientException e) {
                      // a rebalance started; the records will be redelivered (at-least-once)
                    }
                  }
                }
              },
              name);
    }

    void start() {
      thread.start();
    }

    /** Stop dead: no LeaveGroup, no final commit, heartbeats cease. */
    void crash() throws InterruptedException {
      crashed.set(true);
      running.set(false);
      consumer.simulateCrash();
      thread.join(5_000);
    }

    @Override
    public void close() throws InterruptedException {
      running.set(false);
      thread.join(10_000);
      if (!crashed.get()) {
        consumer.close();
      }
    }
  }

  @Test
  void aDeadMembersPartitionsMoveAndResumeFromTheCommittedOffsetWithNoGap() throws Exception {
    try (Node node = EmbeddedNode.start(dir)) {
      String bootstrap = EmbeddedNode.bootstrap(node);
      try (Admin admin = new Admin(bootstrap)) {
        admin.createTopic("orders", 6, 1, Map.of());
      }
      Map<Integer, Long> produced = new ConcurrentHashMap<>();
      try (Producer p = new Producer(ProducerConfig.builder(bootstrap).acks(Acks.ALL).build())) {
        List<java.util.concurrent.CompletableFuture<RecordMetadata>> sent = new ArrayList<>();
        for (int i = 0; i < 3_000; i++) {
          sent.add(p.send("orders", ("k" + i).getBytes(UTF_8), ("v" + i).getBytes(UTF_8)));
        }
        for (var f : sent) {
          produced.merge(f.get().partition(), 1L, Long::sum);
        }
      }

      Member m1 = new Member(bootstrap, "m1");
      Member m2 = new Member(bootstrap, "m2");
      Member m3 = new Member(bootstrap, "m3");
      m1.start();
      m2.start();
      m3.start();
      waitUntil(
          () ->
              m1.consumer.assignment().size() == 2
                  && m2.consumer.assignment().size() == 2
                  && m3.consumer.assignment().size() == 2,
          30);
      Set<TopicPartition> victimPartitions = m3.consumer.assignment();
      Thread.sleep(300);
      m3.crash();

      // The survivors take over m3's partitions once its session expires.
      waitUntil(() -> m1.consumer.assignment().size() + m2.consumer.assignment().size() == 6, 30);
      for (TopicPartition tp : victimPartitions) {
        int p = tp.partition();
        waitUntil(() -> count(m1, m2, m3, p) >= produced.getOrDefault(p, 0L), 30);
      }
      waitUntil(() -> totalDistinct(m1, m2, m3) == 3_000, 30);
      m1.close();
      m2.close();

      for (int p = 0; p < 6; p++) {
        Set<Long> all = new HashSet<>();
        for (Member m : List.of(m1, m2, m3)) {
          all.addAll(m.seen.getOrDefault(p, List.of()));
        }
        long expected = produced.getOrDefault(p, 0L);
        assertThat(all)
            .as("partition %d: every offset consumed (no gap)", p)
            .hasSize((int) expected);
        for (long o = 0; o < expected; o++) {
          assertThat(all).contains(o);
        }
      }
      // The takeover starts at the committed offset: whatever m3 committed is not read again.
      for (TopicPartition tp : victimPartitions) {
        List<Long> byVictim = m3.seen.getOrDefault(tp.partition(), List.of());
        List<Long> bySurvivors = new ArrayList<>(m1.seen.getOrDefault(tp.partition(), List.of()));
        bySurvivors.addAll(m2.seen.getOrDefault(tp.partition(), List.of()));
        long firstTakeover = bySurvivors.stream().min(Long::compare).orElse(Long.MAX_VALUE);
        long victimMax = byVictim.stream().max(Long::compare).orElse(-1L);
        System.out.printf(
            "partition %d: victim consumed up to %d, survivors resumed at %d%n",
            tp.partition(), victimMax, firstTakeover);
        assertThat(firstTakeover).as("resume point").isLessThanOrEqualTo(victimMax + 1);
      }
    }
  }

  private static long count(Member a, Member b, Member c, int partition) {
    Set<Long> all = new HashSet<>();
    for (Member m : List.of(a, b, c)) {
      all.addAll(m.seen.getOrDefault(partition, List.of()));
    }
    return all.size();
  }

  private static long totalDistinct(Member a, Member b, Member c) {
    long n = 0;
    for (int p = 0; p < 6; p++) {
      n += count(a, b, c, p);
    }
    return n;
  }

  private static void waitUntil(java.util.function.BooleanSupplier cond, int seconds)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("condition not met in " + seconds + " s");
      }
      Thread.sleep(50);
    }
  }
}
