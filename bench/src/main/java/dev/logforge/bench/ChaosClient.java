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
    int size = Math.max(8, Integer.parseInt(a.getOrDefault("size", "8")));
    // --rf 1 drives a single-node broker (the hosted-mode memory test); chaos runs use 3.
    int rf = Integer.parseInt(a.getOrDefault("rf", "3"));
    try (Admin admin = new Admin(bootstrap, 60_000, Main.clusterToken())) {
      admin.createTopic(
          topic, partitions, rf, Map.of("min.insync.replicas", Integer.toString(Math.min(2, rf))));
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
        p.send(topic, null, ByteBuffer.allocate(size).putLong(0, id).array())
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

  /**
   * Polls the controller's admin API (token from the LOGFORGE_READ_TOKEN environment variable,
   * never a command-line argument) every 200 ms and appends each cluster view, with this
   * container's clock, to a JSON-lines file until the stop file appears.
   */
  static void record(Map<String, String> a) throws Exception {
    String admin = a.get("admin");
    String token = System.getenv("LOGFORGE_READ_TOKEN");
    Path out = Path.of(a.get("out"));
    Path stop = Path.of(a.get("stop"));
    java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
    java.net.http.HttpRequest req =
        java.net.http.HttpRequest.newBuilder(java.net.URI.create(admin + "/api/v1/cluster"))
            .header("Authorization", "Bearer " + token)
            .timeout(Duration.ofSeconds(2))
            .build();
    try (BufferedWriter w = Files.newBufferedWriter(out, UTF_8)) {
      while (!Files.exists(stop)) {
        long t = System.currentTimeMillis();
        try {
          java.net.http.HttpResponse<String> r =
              http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
          if (r.statusCode() == 200) {
            w.write("{\"t\":" + t + ",\"cluster\":" + r.body() + "}");
            w.newLine();
            w.flush();
          }
        } catch (java.io.IOException e) {
          // controller briefly unreachable: skip this sample
        }
        Thread.sleep(200);
      }
    }
  }

  /**
   * Joins the recorded cluster views with the producer's ack log into the replay the website plays:
   * one frame per sample with broker liveness, partition leaders, ISRs, replica log end offsets and
   * the cumulative acknowledged count, plus the measured events.
   */
  @SuppressWarnings("unchecked")
  static Map<String, Object> replay(
      Path timeline, String topic, List<long[]> acksByTime, long gapStart, long gapMs)
      throws Exception {
    List<Object> frames = new ArrayList<>();
    long t0 = -1;
    int ackIdx = 0;
    for (String line : Files.readAllLines(timeline, UTF_8)) {
      Map<String, Object> sample = Json.parseObject(line);
      long t = ((Number) sample.get("t")).longValue();
      if (t0 < 0) {
        t0 = t;
      }
      while (ackIdx < acksByTime.size() && acksByTime.get(ackIdx)[1] <= t) {
        ackIdx++;
      }
      Map<String, Object> cluster = (Map<String, Object>) sample.get("cluster");
      List<Object> brokers = new ArrayList<>();
      for (Object b : (List<Object>) cluster.get("brokers")) {
        Map<String, Object> bm = (Map<String, Object>) b;
        if (Boolean.TRUE.equals(bm.get("controller"))) {
          continue;
        }
        brokers.add(Json.obj("id", bm.get("id"), "alive", bm.get("alive")));
      }
      List<Object> partitions = new ArrayList<>();
      for (Object to : (List<Object>) cluster.get("topics")) {
        Map<String, Object> tm = (Map<String, Object>) to;
        if (!topic.equals(tm.get("name"))) {
          continue;
        }
        for (Object po : (List<Object>) tm.get("partitions")) {
          Map<String, Object> pm = (Map<String, Object>) po;
          partitions.add(
              Json.obj(
                  "partition", pm.get("partition"),
                  "leader", pm.get("leader"),
                  "leaderEpoch", pm.get("leaderEpoch"),
                  "isr", pm.get("isr"),
                  "replicaState", pm.get("replicaState")));
        }
      }
      frames.add(
          Json.obj("t", t - t0, "acked", ackIdx, "brokers", brokers, "partitions", partitions));
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("topic", topic);
    out.put("sampleIntervalMs", 200);
    out.put("leaderStoppedAtMs", gapStart - t0);
    out.put("writesResumedAtMs", gapStart + gapMs - t0);
    out.put("recoveryTimeMs", gapMs);
    out.put("frames", frames);
    return out;
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
    if (a.containsKey("timeline") && Files.exists(Path.of(a.get("timeline")))) {
      Map<String, Object> replay =
          replay(Path.of(a.get("timeline")), topic, acks, gapAt, longestGap);
      Files.writeString(Path.of(a.get("replay")), Json.write(replay) + "\n", UTF_8);
    }
    System.out.println(Json.write(doc));
  }
}
