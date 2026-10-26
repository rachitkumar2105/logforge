package dev.logforge.bench;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.util.Json;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Benchmark and chaos entry point.
 *
 * <pre>
 * suite     --system logforge|kafka --bootstrap H:P[,H:P] --suite core|fsync --fsync LABEL
 *           --out DIR --command "the command that ran this"
 * summarize --in DIR
 * chaos-produce / chaos-verify  (see ChaosClient)
 * </pre>
 */
public final class Main {
  private Main() {}

  static Map<String, String> args(String[] a, int from) {
    Map<String, String> m = new HashMap<>();
    for (int i = from; i + 1 < a.length; i += 2) {
      m.put(a[i].replaceFirst("^--", ""), a[i + 1]);
    }
    return m;
  }

  public static void main(String[] argv) throws Exception {
    if (argv.length == 0) {
      System.err.println("usage: suite|summarize|chaos-produce|chaos-verify ...");
      System.exit(2);
    }
    Map<String, String> a = args(argv, 1);
    switch (argv[0]) {
      case "suite" -> suite(a);
      case "summarize" -> summarize(Path.of(a.get("in")));
      case "chaos-produce" -> ChaosClient.produce(a);
      case "chaos-verify" -> ChaosClient.verify(a);
      default -> {
        System.err.println("unknown command " + argv[0]);
        System.exit(2);
      }
    }
  }

  /** The configurations of each suite. One dimension varies at a time from the 1 KB baseline. */
  static List<BenchConfig> configs(
      String suite, String system, String fsync, int warmup, int measure) {
    int rf = 3;
    int batch = 64 * 1024;
    int linger = 5;
    Map<String, String> none = Map.of();
    List<BenchConfig> out = new ArrayList<>();
    switch (suite) {
      case "core" -> {
        out.add(
            new BenchConfig(
                "size-100B",
                "throughput",
                100,
                "1",
                4,
                rf,
                batch,
                linger,
                fsync,
                none,
                warmup,
                measure,
                0));
        out.add(
            new BenchConfig(
                "baseline-1KB",
                "throughput",
                1024,
                "1",
                4,
                rf,
                batch,
                linger,
                fsync,
                none,
                warmup,
                measure,
                0));
        out.add(
            new BenchConfig(
                "size-10KB",
                "throughput",
                10240,
                "1",
                4,
                rf,
                batch,
                linger,
                fsync,
                none,
                warmup,
                measure,
                0));
        out.add(
            new BenchConfig(
                "acks-0",
                "throughput",
                1024,
                "0",
                4,
                rf,
                batch,
                linger,
                fsync,
                none,
                warmup,
                measure,
                0));
        out.add(
            new BenchConfig(
                "acks-all",
                "throughput",
                1024,
                "all",
                4,
                rf,
                batch,
                linger,
                fsync,
                none,
                warmup,
                measure,
                0));
        out.add(
            new BenchConfig(
                "partitions-1",
                "throughput",
                1024,
                "1",
                1,
                rf,
                batch,
                linger,
                fsync,
                none,
                warmup,
                measure,
                0));
        out.add(
            new BenchConfig(
                "partitions-16",
                "throughput",
                1024,
                "1",
                16,
                rf,
                batch,
                linger,
                fsync,
                none,
                warmup,
                measure,
                0));
        out.add(
            new BenchConfig(
                "e2e-acks-all",
                "e2e",
                1024,
                "all",
                4,
                rf,
                batch,
                linger,
                fsync,
                none,
                warmup,
                measure,
                1000));
      }
      case "fsync" -> {
        // LogForge applies fsync as a broker policy (set by the runner); Kafka as topic configs.
        Map<String, String> kafkaFlush =
            switch (fsync) {
              case "ms:1000" -> Map.of("flush.ms", "1000");
              case "records:1" -> Map.of("flush.messages", "1");
              default -> Map.of();
            };
        Map<String, String> topicConfigs = "kafka".equals(system) ? kafkaFlush : none;
        String label = "fsync-" + fsync.replace(':', '-');
        out.add(
            new BenchConfig(
                label,
                "throughput",
                1024,
                "1",
                4,
                rf,
                batch,
                linger,
                fsync,
                topicConfigs,
                warmup,
                measure,
                0));
      }
      default -> throw new IllegalArgumentException("unknown suite " + suite);
    }
    return out;
  }

  static void suite(Map<String, String> a) throws Exception {
    String system = a.get("system");
    String bootstrap = a.get("bootstrap");
    Path out = Path.of(a.getOrDefault("out", "/results"));
    String fsync = a.getOrDefault("fsync", "never");
    int warmup = Integer.parseInt(a.getOrDefault("warmup", "5"));
    int measure = Integer.parseInt(a.getOrDefault("measure", "20"));
    Files.createDirectories(out);
    try (SystemUnderTest sys =
        "kafka".equals(system) ? new KafkaSystem(bootstrap) : new LogForgeSystem(bootstrap)) {
      for (BenchConfig c : configs(a.get("suite"), system, fsync, warmup, measure)) {
        System.out.printf("[%s] %s %s ...%n", system, c.label(), c.mode());
        Map<String, Object> results =
            "e2e".equals(c.mode()) ? Harness.endToEnd(sys, c) : Harness.throughput(sys, c);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("system", sys.name());
        doc.put("version", sys.version());
        doc.put("finishedAt", Instant.now().toString());
        doc.put("config", c.describe());
        doc.put("results", results);
        doc.put("hardware", Hardware.describe());
        doc.put("command", a.getOrDefault("command", "(not recorded)"));
        String file = system + "-" + c.label() + ".json";
        Files.writeString(out.resolve(file), Json.write(doc) + "\n", UTF_8);
        System.out.printf("[%s] %s -> %s%n", system, c.label(), Json.write(results));
      }
    }
  }

  /** Combines every result file into summary.json, sorted by label and system. */
  static void summarize(Path dir) throws IOException {
    List<Object> all = new ArrayList<>();
    try (Stream<Path> files = Files.list(dir)) {
      for (Path p : files.sorted().toList()) {
        String n = p.getFileName().toString();
        if (n.endsWith(".json") && !n.equals("summary.json")) {
          all.add(Json.parse(Files.readString(p, UTF_8)));
        }
      }
    }
    Files.writeString(
        dir.resolve("summary.json"), Json.write(Map.of("results", all)) + "\n", UTF_8);
    System.out.println("summary.json: " + all.size() + " results");
  }
}
