package dev.logforge.bench;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Runs one configuration against one system and returns the measured numbers.
 *
 * <ul>
 *   <li><b>throughput</b>: one producer sends as fast as the client allows (its buffer applies
 *       backpressure) for warmup + measure seconds. Throughput counts acknowledgements that arrive
 *       inside the measure window. Latency is per record, send call to acknowledgement, for records
 *       sent inside the window: under saturation it includes time queued in the client buffer.
 *   <li><b>e2e</b>: a fixed send rate, one consumer reading concurrently in the same JVM (one
 *       clock). Reports send-to-receive and acknowledgement-to-receive latency.
 * </ul>
 */
final class Harness {
  private Harness() {}

  private static byte[][] payloads(int size) {
    Random rnd = new Random(7);
    byte[][] p = new byte[16][size];
    for (byte[] b : p) {
      rnd.nextBytes(b); // incompressible; compression is off on both systems anyway
    }
    return p;
  }

  static Map<String, Object> percentiles(long[] nanos, int n) {
    long[] s = Arrays.copyOf(nanos, n);
    Arrays.sort(s);
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("samples", n);
    if (n == 0) {
      return m;
    }
    m.put("p50", s[(int) Math.min(n - 1, Math.floor(n * 0.50))] / 1e6);
    m.put("p95", s[(int) Math.min(n - 1, Math.floor(n * 0.95))] / 1e6);
    m.put("p99", s[(int) Math.min(n - 1, Math.floor(n * 0.99))] / 1e6);
    m.put("max", s[n - 1] / 1e6);
    return m;
  }

  static Map<String, Object> throughput(SystemUnderTest sys, BenchConfig c) throws Exception {
    String topic = "bench-" + c.label() + "-" + UUID.randomUUID().toString().substring(0, 8);
    sys.createTopic(topic, c.partitions(), c.replicationFactor(), c.topicConfigs());
    byte[][] payloads = payloads(c.recordSize());
    long[] latency = new long[4_000_000];
    AtomicInteger samples = new AtomicInteger();
    LongAdder acked = new LongAdder();
    AtomicLong errors = new AtomicLong();
    long sent = 0;
    long start = System.nanoTime();
    long windowStart = start + c.warmupSeconds() * 1_000_000_000L;
    long windowEnd = windowStart + c.measureSeconds() * 1_000_000_000L;
    try (SystemUnderTest.AsyncProducer producer = sys.producer(c, topic)) {
      while (true) {
        long sendNanos = System.nanoTime();
        if (sendNanos >= windowEnd) {
          break;
        }
        boolean measured = sendNanos >= windowStart;
        producer.send(
            null,
            payloads[(int) (sent & 15)],
            (offset, err) -> {
              long now = System.nanoTime();
              if (err != null) {
                errors.incrementAndGet();
                return;
              }
              if (now >= windowStart && now < windowEnd) {
                acked.increment();
              }
              if (measured) {
                int i = samples.getAndIncrement();
                if (i < latency.length) {
                  latency[i] = now - sendNanos;
                }
              }
            });
        sent++;
      }
      producer.flush();
    }
    double perSec = acked.sum() / (double) c.measureSeconds();
    Map<String, Object> r = new LinkedHashMap<>();
    r.put("recordsPerSec", Math.round(perSec));
    r.put("mbPerSec", Math.round(perSec * c.recordSize() / 1e6 * 100) / 100.0);
    r.put("ackedInWindow", acked.sum());
    r.put("sendErrors", errors.get());
    r.put("produceLatencyMs", percentiles(latency, Math.min(samples.get(), latency.length)));
    return r;
  }

  static Map<String, Object> endToEnd(SystemUnderTest sys, BenchConfig c) throws Exception {
    String topic = "bench-" + c.label() + "-" + UUID.randomUUID().toString().substring(0, 8);
    sys.createTopic(topic, c.partitions(), c.replicationFactor(), c.topicConfigs());
    int total = c.ratePerSec() * (c.warmupSeconds() + c.measureSeconds());
    int firstMeasured = c.ratePerSec() * c.warmupSeconds();
    long[] sendAt = new long[total];
    long[] ackAt = new long[total];
    long[] recvAt = new long[total];
    AtomicInteger received = new AtomicInteger();
    Thread reader =
        new Thread(
            () -> {
              try (SystemUnderTest.PollingConsumer consumer = sys.consumer(topic, c.partitions())) {
                long deadline =
                    System.nanoTime()
                        + (c.warmupSeconds() + c.measureSeconds() + 60) * 1_000_000_000L;
                while (received.get() < total && System.nanoTime() < deadline) {
                  for (byte[] v : consumer.poll(100)) {
                    long now = System.nanoTime();
                    int id = ByteBuffer.wrap(v).getInt();
                    if (id >= 0 && id < total && recvAt[id] == 0) {
                      recvAt[id] = now;
                      received.incrementAndGet();
                    }
                  }
                }
              }
            },
            "e2e-reader");
    reader.start();
    Thread.sleep(2_000); // let the consumer position itself before the first record
    byte[] pad = new byte[Math.max(0, c.recordSize() - 4)];
    long intervalNanos = 1_000_000_000L / c.ratePerSec();
    try (SystemUnderTest.AsyncProducer producer = sys.producer(c, topic)) {
      long t0 = System.nanoTime();
      for (int i = 0; i < total; i++) {
        long due = t0 + i * intervalNanos;
        while (System.nanoTime() < due) {
          Thread.onSpinWait();
        }
        ByteBuffer v = ByteBuffer.allocate(4 + pad.length).putInt(i).put(pad);
        int id = i;
        sendAt[i] = System.nanoTime();
        producer.send(
            null, v.array(), (off, err) -> ackAt[id] = err == null ? System.nanoTime() : -1);
      }
      producer.flush();
    }
    reader.join(120_000);
    long[] e2e = new long[total];
    long[] ackToRecv = new long[total];
    int n = 0;
    int m = 0;
    int negative = 0;
    for (int i = firstMeasured; i < total; i++) {
      if (recvAt[i] > 0) {
        e2e[n++] = recvAt[i] - sendAt[i];
        if (ackAt[i] > 0) {
          long d = recvAt[i] - ackAt[i];
          if (d < 0) {
            negative++; // the consumer saw it before the producer's callback ran
            d = 0;
          }
          ackToRecv[m++] = d;
        }
      }
    }
    Map<String, Object> r = new LinkedHashMap<>();
    r.put("sent", total);
    r.put("received", received.get());
    r.put("sendToReceiveMs", percentiles(e2e, n));
    r.put("ackToReceiveMs", percentiles(ackToRecv, m));
    r.put("receivedBeforeAckCallback", negative);
    return r;
  }
}
