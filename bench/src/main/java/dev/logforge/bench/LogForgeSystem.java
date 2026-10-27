package dev.logforge.bench;

import dev.logforge.TopicPartition;
import dev.logforge.client.Acks;
import dev.logforge.client.Admin;
import dev.logforge.client.Consumer;
import dev.logforge.client.ConsumerConfig;
import dev.logforge.client.ConsumerRecord;
import dev.logforge.client.Producer;
import dev.logforge.client.ProducerConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

final class LogForgeSystem implements SystemUnderTest {
  private final String bootstrap;

  LogForgeSystem(String bootstrap) {
    this.bootstrap = bootstrap;
  }

  @Override
  public String name() {
    return "logforge";
  }

  @Override
  public String version() {
    return "1.0.0-SNAPSHOT";
  }

  @Override
  public void createTopic(String name, int partitions, int rf, Map<String, String> configs) {
    try (Admin admin = new Admin(bootstrap, 60_000, Main.clusterToken())) {
      admin.createTopic(name, partitions, rf, configs);
    }
  }

  static Acks acks(String a) {
    return switch (a) {
      case "0" -> Acks.NONE;
      case "1" -> Acks.LEADER;
      default -> Acks.ALL;
    };
  }

  @Override
  public AsyncProducer producer(BenchConfig c, String topic) {
    Producer p =
        new Producer(
            ProducerConfig.builder(bootstrap)
                .acks(acks(c.acks()))
                .batchSize(c.batchSize())
                .lingerMs(c.lingerMs())
                .idempotent("all".equals(c.acks()))
                .bufferMemory(8L * 1024 * 1024)
                .requestTimeoutMs(30_000)
                .build());
    return new AsyncProducer() {
      @Override
      public void send(byte[] key, byte[] value, BiConsumer<Long, Exception> callback) {
        p.send(topic, key, value)
            .whenComplete(
                (md, err) ->
                    callback.accept(
                        md == null ? -1L : md.offset(), err == null ? null : new Exception(err)));
      }

      @Override
      public void flush() {
        p.flush();
      }

      @Override
      public void close() {
        p.close();
      }
    };
  }

  @Override
  public PollingConsumer consumer(String topic, int partitions) {
    Consumer c =
        new Consumer(
            ConsumerConfig.builder(bootstrap).maxPollRecords(10_000).fetchMaxWaitMs(100).build());
    List<TopicPartition> tps = new ArrayList<>();
    for (int p = 0; p < partitions; p++) {
      tps.add(new TopicPartition(topic, p));
    }
    c.assign(tps);
    return new PollingConsumer() {
      @Override
      public List<byte[]> poll(long timeoutMs) {
        List<byte[]> out = new ArrayList<>();
        for (ConsumerRecord r : c.poll(Duration.ofMillis(timeoutMs))) {
          out.add(r.value());
        }
        return out;
      }

      @Override
      public void close() {
        c.close();
      }
    };
  }

  @Override
  public void close() {}
}
