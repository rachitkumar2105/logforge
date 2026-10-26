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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three consumers in a group; one dies without leaving. Its partitions must move to the survivors
 * and consumption must resume exactly at the dead member's last committed offset, with no gap.
 *
 * <p>Each member does a little "work" per poll (10 records, 30 ms) so the victim is reliably killed
 * part-way through its partitions, after at least one commit on each of them. Only reads a survivor
 * makes after the crash count as the takeover (a survivor may have owned the same partition briefly
 * before the group settled at three members).
 */
class ConsumerGroupTest {

  static final int RECORDS = 3_000;

  @TempDir Path dir;

  private static final class Member implements AutoCloseable {
    final Consumer consumer;
    final Thread thread;
    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicBoolean crashed = new AtomicBoolean(false);
    final Map<Integer, List<Long>> seen = new ConcurrentHashMap<>();
    final Map<Integer, List<long[]>> timeline = new ConcurrentHashMap<>(); // {offset, nanoTime}
    final Map<Integer, Long> committed = new ConcurrentHashMap<>();

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
                  .maxPollRecords(10)
                  .build());
      thread =
          new Thread(
              () -> {
                consumer.subscribe(List.of("orders"));
                while (running.get()) {
                  List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(100));
                  if (crashed.get()) {
                    return;
                  }
                  long now = System.nanoTime();
                  for (ConsumerRecord r : records) {
                    seen.computeIfAbsent(r.partition(), k -> new CopyOnWriteArrayList<>())
                        .add(r.offset());
                    timeline
                        .computeIfAbsent(r.partition(), k -> new CopyOnWriteArrayList<>())
                        .add(new long[] {r.offset(), now});
                  }
                  if (!records.isEmpty()) {
                    sleep(30); // processing time
                    try {
                      consumer.commitSync();
                      for (TopicPartition tp : consumer.assignment()) {
                        long pos = consumer.position(tp);
                        if (pos >= 0) {
                          committed.put(tp.partition(), pos);
                        }
                      }
                    } catch (ClientException e) {
                      // a rebalance started; uncommitted records are redelivered (at-least-once)
                    }
                  }
                }
              },
              name);
    }

    void start() {
      thread.start();
    }

    /** Stop dead: no LeaveGroup, no further commit, heartbeats cease. */
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

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
        List<CompletableFuture<RecordMetadata>> sent = new ArrayList<>();
        for (int i = 0; i < RECORDS; i++) {
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
      // Kill the victim once it has committed real progress on each of its partitions, but
      // before it has finished them.
      waitUntil(
          () ->
              victimPartitions.stream()
                  .anyMatch(
                      tp -> {
                        long c = m3.committed.getOrDefault(tp.partition(), 0L);
                        return c >= 40 && c < produced.get(tp.partition());
                      }),
          30);
      long crashNanos = System.nanoTime();
      m3.crash();
      Map<Integer, Long> victimCommitted = Map.copyOf(m3.committed);

      waitUntil(() -> m1.consumer.assignment().size() + m2.consumer.assignment().size() == 6, 30);
      waitUntil(() -> totalDistinct(m1, m2, m3) == RECORDS, 30);
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
      int midway = 0;
      for (TopicPartition tp : victimPartitions) {
        int p = tp.partition();
        long committedByVictim = victimCommitted.get(p);
        List<Long> bySurvivors = new ArrayList<>();
        for (Member m : List.of(m1, m2)) {
          for (long[] e : m.timeline.getOrDefault(p, List.of())) {
            if (e[1] > crashNanos) {
              bySurvivors.add(e[0]);
            }
          }
        }
        long victimMax = m3.seen.getOrDefault(p, List.of()).stream().max(Long::compare).orElse(-1L);
        if (committedByVictim >= produced.get(p)) {
          // The victim finished this partition before dying: nothing is left to take over.
          System.out.printf(
              "partition %d: victim finished it (committed %d)%n", p, committedByVictim);
          assertThat(bySurvivors).isEmpty();
          continue;
        }
        midway++;
        long firstTakeover = bySurvivors.stream().min(Long::compare).orElseThrow();
        System.out.printf(
            "partition %d (%d records): victim read to %d and committed %d; survivors resumed at %d%n",
            p, produced.get(p), victimMax, committedByVictim, firstTakeover);
        assertThat(firstTakeover)
            .as("resumes exactly at the committed offset")
            .isEqualTo(committedByVictim);
      }
      assertThat(midway).as("at least one partition was taken over mid-way").isPositive();
    }
  }

  private static long totalDistinct(Member a, Member b, Member c) {
    long n = 0;
    for (int p = 0; p < 6; p++) {
      Set<Long> all = new HashSet<>();
      for (Member m : List.of(a, b, c)) {
        all.addAll(m.seen.getOrDefault(p, List.of()));
      }
      n += all.size();
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
      Thread.sleep(20);
    }
  }
}
