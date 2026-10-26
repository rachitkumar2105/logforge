package dev.logforge.bench;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** The same workload driven against LogForge or Apache Kafka through their own Java clients. */
interface SystemUnderTest extends AutoCloseable {

  /** A producer: asynchronous send with a completion callback (offset, or -1 for acks=0). */
  interface AsyncProducer extends AutoCloseable {
    void send(byte[] key, byte[] value, BiConsumer<Long, Exception> callback);

    void flush();

    @Override
    void close();
  }

  /** A consumer reading every partition of one topic from the beginning, outside any group. */
  interface PollingConsumer extends AutoCloseable {
    List<byte[]> poll(long timeoutMs);

    @Override
    void close();
  }

  String name();

  String version();

  void createTopic(String name, int partitions, int replicationFactor, Map<String, String> configs)
      throws Exception;

  AsyncProducer producer(BenchConfig config, String topic);

  PollingConsumer consumer(String topic, int partitions);

  @Override
  void close();
}
