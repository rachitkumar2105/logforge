package dev.logforge.admin;

import dev.logforge.replication.ReplicaManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Broker metrics in Prometheus text exposition format 0.0.4, hand-written (no client library).
 *
 * <p>Counters and histograms are updated on the request path with lock-free adders; gauges are
 * computed at scrape time from live broker state.
 */
public final class Metrics implements ReplicaManager.LatencyObserver {

  /** One labelled gauge value. */
  public record Sample(Map<String, String> labels, double value) {}

  private record GaugeDef(String name, String help, Supplier<List<Sample>> samples) {}

  /** Bucket upper bounds in seconds, rendered exactly as written here. */
  private static final String[] BUCKET_LABELS = {
    "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5",
    "5", "10"
  };

  private static final long[] BUCKET_NANOS = new long[BUCKET_LABELS.length];

  static {
    for (int i = 0; i < BUCKET_LABELS.length; i++) {
      BUCKET_NANOS[i] = Math.round(Double.parseDouble(BUCKET_LABELS[i]) * 1_000_000_000L);
    }
  }

  /** Cumulative histogram: count of observations at or below each bound. */
  private static final class Histogram {
    final AtomicLongArray buckets = new AtomicLongArray(BUCKET_NANOS.length);
    final LongAdder count = new LongAdder();
    final LongAdder sumNanos = new LongAdder();

    void observe(long nanos) {
      for (int i = 0; i < BUCKET_NANOS.length; i++) {
        if (nanos <= BUCKET_NANOS[i]) {
          buckets.incrementAndGet(i);
        }
      }
      count.increment();
      sumNanos.add(nanos);
    }
  }

  private final LongSupplier clock;
  private final Map<String, LongAdder> messagesIn = new ConcurrentHashMap<>();
  private final LongAdder messagesInAll = new LongAdder();
  private final LongAdder bytesIn = new LongAdder();
  private final LongAdder bytesOut = new LongAdder();
  private final Map<String, Histogram> produceLatency = new ConcurrentHashMap<>();
  private final Map<String, Histogram> fetchLatency = new ConcurrentHashMap<>();
  private final List<GaugeDef> gauges = new ArrayList<>();

  // 1-minute exponentially weighted rates, ticked every few seconds.
  private long lastTickMs;
  private long lastMessages;
  private long lastBytesIn;
  private long lastBytesOut;
  private volatile double messagesRate;
  private volatile double bytesInRate;
  private volatile double bytesOutRate;
  private boolean primed;

  public Metrics(LongSupplier clock) {
    this.clock = clock;
    this.lastTickMs = clock.getAsLong();
  }

  public void recordAppend(String topic, int records, int bytes) {
    messagesIn.computeIfAbsent(topic, t -> new LongAdder()).add(records);
    messagesInAll.add(records);
    bytesIn.add(bytes);
  }

  public void recordBytesOut(long bytes) {
    bytesOut.add(bytes);
  }

  public void recordProduceLatency(int acks, long nanos) {
    String label = acks == -1 ? "all" : Integer.toString(acks);
    produceLatency.computeIfAbsent(label, k -> new Histogram()).observe(nanos);
  }

  public void recordFetchLatency(boolean follower, long nanos) {
    fetchLatency
        .computeIfAbsent(follower ? "follower" : "consumer", k -> new Histogram())
        .observe(nanos);
  }

  @Override
  public void produce(int acks, long nanos) {
    recordProduceLatency(acks, nanos);
  }

  @Override
  public void fetch(boolean follower, long nanos) {
    recordFetchLatency(follower, nanos);
  }

  @Override
  public void append(String topic, int records, int bytes) {
    recordAppend(topic, records, bytes);
  }

  @Override
  public void bytesOut(long bytes) {
    recordBytesOut(bytes);
  }

  public synchronized void gauge(String name, String help, Supplier<List<Sample>> samples) {
    gauges.add(new GaugeDef(name, help, samples));
  }

  /** Updates the EWMA rates; call every few seconds. */
  public synchronized void tickRates() {
    long now = clock.getAsLong();
    double seconds = (now - lastTickMs) / 1000.0;
    if (seconds <= 0) {
      return;
    }
    long m = messagesInAll.sum();
    long bi = bytesIn.sum();
    long bo = bytesOut.sum();
    double alpha = 1 - Math.exp(-seconds / 60.0);
    double mr = (m - lastMessages) / seconds;
    double bir = (bi - lastBytesIn) / seconds;
    double bor = (bo - lastBytesOut) / seconds;
    if (!primed) {
      messagesRate = mr;
      bytesInRate = bir;
      bytesOutRate = bor;
      primed = true;
    } else {
      messagesRate += alpha * (mr - messagesRate);
      bytesInRate += alpha * (bir - bytesInRate);
      bytesOutRate += alpha * (bor - bytesOutRate);
    }
    lastMessages = m;
    lastBytesIn = bi;
    lastBytesOut = bo;
    lastTickMs = now;
  }

  public double messagesInPerSec() {
    return messagesRate;
  }

  public double bytesInPerSec() {
    return bytesInRate;
  }

  public double bytesOutPerSec() {
    return bytesOutRate;
  }

  public long messagesInTotal() {
    return messagesInAll.sum();
  }

  // ------------------------------------------------------------------ rendering

  static String escape(String v) {
    StringBuilder sb = new StringBuilder(v.length());
    for (char c : v.toCharArray()) {
      switch (c) {
        case '\\' -> sb.append("\\\\");
        case '"' -> sb.append("\\\"");
        case '\n' -> sb.append("\\n");
        default -> sb.append(c);
      }
    }
    return sb.toString();
  }

  private static String labels(Map<String, String> labels) {
    if (labels.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, String> e : new TreeMap<>(labels).entrySet()) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      sb.append(e.getKey()).append("=\"").append(escape(e.getValue())).append('"');
    }
    return sb.append('}').toString();
  }

  private static String number(double v) {
    if (v == Math.rint(v) && Math.abs(v) < 1e15) {
      return Long.toString((long) v);
    }
    return Double.toString(v);
  }

  private static void header(StringBuilder sb, String name, String type, String help) {
    sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
    sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
  }

  private static void histogram(
      StringBuilder sb, String name, String help, String labelKey, Map<String, Histogram> hs) {
    header(sb, name, "histogram", help);
    for (Map.Entry<String, Histogram> e : new TreeMap<>(hs).entrySet()) {
      Histogram h = e.getValue();
      String lk = labelKey + "=\"" + escape(e.getKey()) + "\"";
      for (int i = 0; i < BUCKET_LABELS.length; i++) {
        sb.append(name)
            .append("_bucket{")
            .append(lk)
            .append(",le=\"")
            .append(BUCKET_LABELS[i])
            .append("\"} ")
            .append(h.buckets.get(i))
            .append('\n');
      }
      long count = h.count.sum();
      sb.append(name)
          .append("_bucket{")
          .append(lk)
          .append(",le=\"+Inf\"} ")
          .append(count)
          .append('\n');
      sb.append(name)
          .append("_sum{")
          .append(lk)
          .append("} ")
          .append(h.sumNanos.sum() / 1e9)
          .append('\n');
      sb.append(name).append("_count{").append(lk).append("} ").append(count).append('\n');
    }
  }

  public synchronized String render() {
    StringBuilder sb = new StringBuilder(4096);
    header(sb, "logforge_messages_in_total", "counter", "Records appended as leader, per topic.");
    for (Map.Entry<String, LongAdder> e : new TreeMap<>(messagesIn).entrySet()) {
      sb.append("logforge_messages_in_total")
          .append(labels(Map.of("topic", e.getKey())))
          .append(' ')
          .append(e.getValue().sum())
          .append('\n');
    }
    header(
        sb,
        "logforge_messages_in_per_sec",
        "gauge",
        "Records appended per second, 1-minute exponentially weighted.");
    sb.append("logforge_messages_in_per_sec ").append(number(messagesRate)).append('\n');
    header(sb, "logforge_bytes_in_total", "counter", "Bytes of record batches appended as leader.");
    sb.append("logforge_bytes_in_total ").append(bytesIn.sum()).append('\n');
    header(
        sb, "logforge_bytes_out_total", "counter", "Bytes of record batches returned by fetches.");
    sb.append("logforge_bytes_out_total ").append(bytesOut.sum()).append('\n');
    histogram(
        sb,
        "logforge_produce_latency_seconds",
        "Time from receiving a produce to answering it.",
        "acks",
        produceLatency);
    histogram(
        sb,
        "logforge_fetch_latency_seconds",
        "Time from receiving a fetch to answering it (includes long-poll wait).",
        "type",
        fetchLatency);
    for (GaugeDef g : gauges) {
      header(sb, g.name(), "gauge", g.help());
      for (Sample s : g.samples().get()) {
        sb.append(g.name())
            .append(labels(s.labels()))
            .append(' ')
            .append(number(s.value()))
            .append('\n');
      }
    }
    return sb.toString();
  }
}
