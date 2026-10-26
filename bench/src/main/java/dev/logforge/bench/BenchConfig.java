package dev.logforge.bench;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One benchmark configuration. Every published number carries all of these fields (record size,
 * batch size, acks, partitions, replication factor, fsync policy) plus the hardware block.
 *
 * @param fsync the durability setting under test, described for the report (broker policy for
 *     LogForge, {@code flush.messages}/{@code flush.ms} topic config for Kafka)
 * @param topicConfigs extra topic configs used to apply {@code fsync} on Kafka
 * @param ratePerSec 0 for "as fast as possible" (throughput runs); a fixed rate for latency runs
 */
record BenchConfig(
    String label,
    String mode,
    int recordSize,
    String acks,
    int partitions,
    int replicationFactor,
    int batchSize,
    int lingerMs,
    String fsync,
    Map<String, String> topicConfigs,
    int warmupSeconds,
    int measureSeconds,
    int ratePerSec) {

  Map<String, Object> describe() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("label", label);
    m.put("mode", mode);
    m.put("recordSizeBytes", recordSize);
    m.put("acks", acks);
    m.put("partitions", partitions);
    m.put("replicationFactor", replicationFactor);
    m.put("batchSizeBytes", batchSize);
    m.put("lingerMs", lingerMs);
    m.put("fsync", fsync);
    m.put("compression", "none");
    m.put("producerBufferBytes", 8 * 1024 * 1024);
    m.put("idempotentProducer", "all".equals(acks));
    m.put("warmupSeconds", warmupSeconds);
    m.put("measureSeconds", measureSeconds);
    m.put("targetRatePerSec", ratePerSec);
    return m;
  }
}
