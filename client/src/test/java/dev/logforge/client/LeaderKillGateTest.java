package dev.logforge.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.TopicPartition;
import dev.logforge.network.protocol.Metadata;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 4 gate: zero acknowledged-record loss at acks=all across 10 leader-kill cycles.
 *
 * <p>Four JVM processes: a controller and three brokers. A topic with replication factor 3 and
 * min.insync.replicas=2. An idempotent acks=all producer writes numbered records continuously. Each
 * cycle kills the current partition leader with no warning, waits for a new leader, restarts the
 * dead broker and waits for it to rejoin the ISR. At the end a consumer reads the partition and
 * every record the producer saw acknowledged must be present exactly once.
 */
@Tag("gate")
class LeaderKillGateTest {
  static final int CYCLES = 10;

  @TempDir Path dir;

  @Test
  void zeroAcknowledgedLossAtAcksAllAcrossTenLeaderKills() throws Exception {
    try (ProcessCluster cluster = new ProcessCluster(dir, 3, Map.of())) {
      String bootstrap = cluster.bootstrap();
      Admin admin = awaitAdmin(bootstrap);
      awaitBrokers(admin, 3);
      admin.createTopic("gate", 1, 3, Map.of("min.insync.replicas", "2"));

      Set<Long> acked = ConcurrentHashMap.newKeySet();
      AtomicLong failed = new AtomicLong();
      AtomicBoolean running = new AtomicBoolean(true);
      Thread writer =
          new Thread(
              () -> {
                try (Producer p =
                    new Producer(
                        ProducerConfig.builder(bootstrap)
                            .acks(Acks.ALL)
                            .lingerMs(2)
                            .retries(1_000)
                            .retryBackoffMs(50)
                            .requestTimeoutMs(5_000)
                            .deliveryTimeoutMs(120_000)
                            .build())) {
                  long i = 0;
                  while (running.get()) {
                    long id = i++;
                    p.send("gate", null, Long.toString(id).getBytes(UTF_8))
                        .whenComplete(
                            (md, err) -> {
                              if (err == null) {
                                acked.add(id);
                              } else {
                                failed.incrementAndGet();
                              }
                            });
                    if (id % 200 == 0) {
                      Thread.sleep(5); // keep the write rate moderate so cycles stay short
                    }
                  }
                  p.flush();
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              },
              "gate-writer");
      writer.start();

      for (int cycle = 1; cycle <= CYCLES; cycle++) {
        Thread.sleep(1_000);
        int leader = leaderOf(admin, "gate");
        long ackedBefore = acked.size();
        long t0 = System.nanoTime();
        cluster.kill(leader);
        int newLeader = awaitNewLeader(admin, "gate", leader);
        long electionMs = (System.nanoTime() - t0) / 1_000_000;
        cluster.restart(leader);
        awaitIsrSize(admin, "gate", 3);
        long rejoinMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf(
            "cycle %d: killed leader %d, new leader %d after %d ms, ISR whole again after %d ms, acked so far %d (+%d)%n",
            cycle,
            leader,
            newLeader,
            electionMs,
            rejoinMs,
            acked.size(),
            acked.size() - ackedBefore);
      }
      running.set(false);
      writer.join(180_000);

      Set<Long> seen = new java.util.HashSet<>();
      long duplicates = 0;
      try (Consumer consumer =
          new Consumer(ConsumerConfig.builder(bootstrap).maxPollRecords(5_000).build())) {
        consumer.assign(java.util.List.of(new TopicPartition("gate", 0)));
        long idle = 0;
        while (idle < 10) {
          var batch = consumer.poll(Duration.ofMillis(500));
          if (batch.isEmpty()) {
            idle++;
          }
          for (ConsumerRecord r : batch) {
            idle = 0;
            if (!seen.add(Long.parseLong(new String(r.value(), UTF_8)))) {
              duplicates++;
            }
          }
        }
      }
      long missing = acked.stream().filter(id -> !seen.contains(id)).count();
      System.out.printf(
          "GATE leader-kill: cycles=%d acked=%d failedSends=%d consumed=%d missingAcked=%d duplicates=%d%n",
          CYCLES, acked.size(), failed.get(), seen.size(), missing, duplicates);
      admin.close();
      assertThat(acked.size()).isGreaterThan(1_000);
      assertThat(missing).as("acknowledged records missing after %d leader kills", CYCLES).isZero();
      assertThat(duplicates).as("duplicates (idempotent producer)").isZero();
    }
  }

  static Admin awaitAdmin(String bootstrap) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (true) {
      try {
        return new Admin(bootstrap, 10_000);
      } catch (RuntimeException e) {
        if (System.nanoTime() > deadline) {
          throw e;
        }
        Thread.sleep(200);
      }
    }
  }

  static void awaitBrokers(Admin admin, int n) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        long alive =
            admin.describeCluster().brokers().stream().filter(Metadata.Broker::alive).count();
        if (alive >= n + 1) { // brokers plus the controller entry
          return;
        }
      } catch (RuntimeException e) {
        // still starting
      }
      Thread.sleep(200);
    }
    throw new AssertionError("brokers did not register");
  }

  static Metadata.PartitionMeta partition(Admin admin, String topic) {
    for (Metadata.TopicMeta t : admin.describeCluster().topics()) {
      if (t.name().equals(topic)) {
        return t.partitions().get(0);
      }
    }
    return null;
  }

  static int leaderOf(Admin admin, String topic) {
    Metadata.PartitionMeta p = partition(admin, topic);
    return p == null ? -1 : p.leader();
  }

  static int awaitNewLeader(Admin admin, String topic, int old) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        int l = leaderOf(admin, topic);
        if (l > 0 && l != old) {
          return l;
        }
      } catch (RuntimeException e) {
        // the broker we asked may be the one that died
      }
      Thread.sleep(50);
    }
    throw new AssertionError("no new leader elected");
  }

  static void awaitIsrSize(Admin admin, String topic, int size) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        Metadata.PartitionMeta p = partition(admin, topic);
        if (p != null && p.isr().size() == size) {
          return;
        }
      } catch (RuntimeException e) {
        // retry
      }
      Thread.sleep(100);
    }
    throw new AssertionError("ISR did not recover to " + size);
  }
}
