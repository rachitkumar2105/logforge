package dev.logforge.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MetricsTest {

  @Test
  void rendersCountersGaugesAndHistogramsInPrometheusTextFormat() {
    AtomicLong clock = new AtomicLong(0);
    Metrics m = new Metrics(clock::get);
    m.recordAppend("orders", 10, 1_000);
    m.recordAppend("orders", 5, 500);
    m.recordAppend("audit", 1, 100);
    m.recordProduceLatency(-1, 3_000_000L); // 3 ms
    m.recordProduceLatency(-1, 30_000_000L); // 30 ms
    m.recordFetchLatency(true, 1_000_000L);
    m.recordBytesOut(4_096);
    m.gauge(
        "logforge_isr_size",
        "ISR size per partition",
        () ->
            java.util.List.of(
                new Metrics.Sample(java.util.Map.of("topic", "orders", "partition", "0"), 3)));

    String text = m.render();

    assertThat(text).contains("# TYPE logforge_messages_in_total counter");
    assertThat(text).contains("logforge_messages_in_total{topic=\"orders\"} 15");
    assertThat(text).contains("logforge_messages_in_total{topic=\"audit\"} 1");
    assertThat(text).contains("logforge_bytes_in_total 1600");
    assertThat(text).contains("logforge_bytes_out_total 4096");
    assertThat(text).contains("# TYPE logforge_produce_latency_seconds histogram");
    assertThat(text)
        .contains("logforge_produce_latency_seconds_bucket{acks=\"all\",le=\"0.005\"} 1");
    assertThat(text)
        .contains("logforge_produce_latency_seconds_bucket{acks=\"all\",le=\"0.05\"} 2");
    assertThat(text)
        .contains("logforge_produce_latency_seconds_bucket{acks=\"all\",le=\"+Inf\"} 2");
    assertThat(text).contains("logforge_produce_latency_seconds_count{acks=\"all\"} 2");
    assertThat(text).contains("logforge_produce_latency_seconds_sum{acks=\"all\"} 0.033");
    assertThat(text).contains("logforge_fetch_latency_seconds_count{type=\"follower\"} 1");
    assertThat(text).contains("logforge_isr_size{partition=\"0\",topic=\"orders\"} 3");
  }

  @Test
  void messagesInPerSecondIsAnExponentiallyWeightedRate() {
    AtomicLong clock = new AtomicLong(0);
    Metrics m = new Metrics(clock::get);
    for (int tick = 0; tick < 60; tick++) { // 5 minutes at 5 s per tick
      m.recordAppend("t", 5_000, 0); // 1,000 records/s
      clock.addAndGet(5_000);
      m.tickRates();
    }
    assertThat(m.messagesInPerSec()).isBetween(950.0, 1_001.0);
  }

  @Test
  void labelValuesAreEscaped() {
    Metrics m = new Metrics(() -> 0L);
    m.recordAppend("a\"b\\c", 1, 1);
    assertThat(m.render()).contains("logforge_messages_in_total{topic=\"a\\\"b\\\\c\"} 1");
  }
}
