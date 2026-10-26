package dev.logforge.bench;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.TopicPartition;
import dev.logforge.client.Acks;
import dev.logforge.client.Admin;
import dev.logforge.client.Consumer;
import dev.logforge.client.ConsumerConfig;
import dev.logforge.client.ConsumerRecord;
import dev.logforge.client.Producer;
import dev.logforge.client.ProducerConfig;
import dev.logforge.util.Json;
import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The workload and the verifier for chaos runs.
 *
 * <p>{@code chaos-produce} writes numbered records at a fixed rate with an idempotent acks=all
 * producer and logs every acknowledgement ({@code id ackEpochMillis}) until a stop file appears.
 * The fault (kill, pause, disconnect, disk full, restart) is injected from outside by
 * chaos/run-all.sh while this runs.
 *
 * <p>{@code chaos-verify} reads every partition from the start and checks that every acknowledged
 * id is present, counts duplicates, and measures the recovery time as the longest gap between
 * consecutive acknowledgements: under a steady send rate, that gap is the window in which the
 * cluster could not accept writes. Using only the producer's own clock avoids comparing clocks
 * across containers.
 */
final class ChaosClient {
  private ChaosClient() {}

  static void produce(Map<String, String> a) throws Exception {
    String bootstrap = a.get("bootstrap");
    String topic = a.get("topic");
    Path acksFile = Path.of(a.get("acks"));
    Path stop = Path.of(a.get("stop"));
    int rate = Integer.parseInt(a.getOrDefault("rate", "2000"));
    int partitions = Integer.parseInt(a.getOrDefault("partitions", "3"));
    try (Admin admin = new Admin(bootstrap, 60_000)) {
      admin.createTopic(topic, partitions, 3, Map.of("min.insync.replicas", "2"));
    }
    ConcurrentLinkedQueue<String> acked = new ConcurrentLinkedQueue<>();
    AtomicLong failures = new AtomicLong();
    long sent = 0;
    try (BufferedWriter w = Files.newBufferedWriter(acksFile, UTF_8);
        Producer p =
            new Producer(
                ProducerConfig.builder(bootstrap)
                    .acks(Acks.ALL)
                    .idempotent(true)
                    .lingerMs(2)
                    .retries(10_000)
                    .retryBackoffMs(50)
                    .requestTimeoutMs(5_000)
                    .deliveryTimeoutMs(300_000)
                    .build())) {
      long interval = 1_000_000_000L / rate;
      long t0 = System.nanoTime();
      System.out.println("chaos-produce: started at " + Instant.now());
      while (!Files.exists(stop)) {
        long due = t0 + sent * interval;
        long now = System.nanoTime();
        if (now < due) {
          Thread.sleep(Math.max(0, (due - now) / 1_000_000));
          continue;
        }
        long id = sent++;
        p.send(topic, null, ByteBuffer.allocate(8).putLong(id).array())
            .whenComplete(
                (md, err) -> {
                  if (err == null) {
                    acked.add(id + " " + System.currentTimeMillis());
                  } else {
                    failures.incrementAndGet();
                  }
                });
        String line;
        while ((line = acked.poll()) != null) {
          w.write(line);
          w.newLine();
        }
      }
      p.flush();
      String line;
      while ((line = acked.poll()) != null) {
        w.write(line);
        w.newLine();
      }
    }
    System.out.printf("chaos-produce: sent=%d failed=%d%n", sent, failures.get());
  }

  static void verify(Map<String, String> a) throws Exception {
    String bootstrap = a.get("bootstrap");
    String topic = a.get("topic");
    String scenario = a.get("scenario");
    int partitions = Integer.parseInt(a.getOrDefault("partitions", "3"));
    List<long[]> acks = new ArrayList<>();
    long maxId = -1;
    for (String line : Files.readAllLines(Path.of(a.get("acks")), UTF_8)) {
      String[] parts = line.trim().split(" ");
      if (parts.length == 2) {
        long id = Long.parseLong(parts[0]);
        acks.add(new long[] {id, Long.parseLong(parts[1])});
        maxId = Math.max(maxId, id);
      }
    }
    acks.sort((x, y) -> Long.compare(x[1], y[1]));
    long longestGap = 0;
    long gapAt = 0;
    for (int i = 1; i < acks.size(); i++) {
      long gap = acks.get(i)[1] - acks.get(i - 1)[1];
      if (gap > longestGap) {
        longestGap = gap;
        gapAt = acks.get(i - 1)[1];
      }
    }
    BitSet seen = new BitSet();
    long duplicates = 0;
    long consumed = 0;
    try (Consumer c =
        new Consumer(ConsumerConfig.builder(bootstrap).maxPollRecords(20_000).build())) {
      List<TopicPartition> tps = new ArrayList<>();
      for (int p = 0; p < partitions; p++) {
        tps.add(new TopicPartition(topic, p));
      }
      c.assign(tps);
      int idle = 0;
      while (idle < 20) {
        List<ConsumerRecord> batch = c.poll(Duration.ofMillis(500));
        if (batch.isEmpty()) {
          idle++;
        }
        for (ConsumerRecord r : batch) {
          idle = 0;
          int id = (int) ByteBuffer.wrap(r.value()).getLong();
          if (seen.get(id)) {
            duplicates++;
          }
          seen.set(id);
          consumed++;
        }
      }
    }
    long missing = 0;
    for (long[] ack : acks) {
      if (!seen.get((int) ack[0])) {
        missing++;
      }
    }
    Map<String, Object> doc = new LinkedHashMap<>();
    doc.put("scenario", scenario);
    doc.put("description", a.getOrDefault("description", ""));
    doc.put("finishedAt", Instant.now().toString());
    doc.put("acked", acks.size());
    doc.put("consumed", consumed);
    doc.put("missingAcked", missing);
    doc.put("duplicates", duplicates);
    doc.put("recoveryTimeMs", longestGap);
    doc.put("recoveryGapStartedAtEpochMs", gapAt);
    doc.put("verdict", missing == 0 ? "PASS" : "FAIL");
    doc.put(
        "workload",
        Json.obj(
            "acks",
            "all",
            "idempotent",
            true,
            "replicationFactor",
            3,
            "minInsyncReplicas",
            2,
            "partitions",
            partitions,
            "ratePerSec",
            a.getOrDefault("rate", "2000")));
    doc.put("hardware", Hardware.describe());
    doc.put("command", a.getOrDefault("command", "(not recorded)"));
    Path out = Path.of(a.get("out"));
    Files.writeString(out, Json.write(doc) + "\n", UTF_8);
    System.out.println(Json.write(doc));
  }
}
