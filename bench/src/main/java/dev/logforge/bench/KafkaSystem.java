package dev.logforge.bench;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;

/** Apache Kafka through its official client: the comparison baseline. */
final class KafkaSystem implements SystemUnderTest {
  private final String bootstrap;

  KafkaSystem(String bootstrap) {
    this.bootstrap = bootstrap;
  }

  @Override
  public String name() {
    return "kafka";
  }

  @Override
  public String version() {
    return "client " + AppInfoParser.getVersion();
  }

  @Override
  public void createTopic(String name, int partitions, int rf, Map<String, String> configs)
      throws Exception {
    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    try (AdminClient admin = AdminClient.create(p)) {
      long deadline = System.currentTimeMillis() + 120_000;
      boolean created = false;
      while (System.currentTimeMillis() < deadline) {
        try {
          if (!created) {
            admin
                .createTopics(List.of(new NewTopic(name, partitions, (short) rf).configs(configs)))
                .all()
                .get();
            created = true;
          }
          // Wait until every partition has a leader; metadata can lag topic creation briefly.
          var d = admin.describeTopics(List.of(name)).allTopicNames().get().get(name);
          if (d.partitions().stream().allMatch(pi -> pi.leader() != null)) {
            Thread.sleep(500);
            return;
          }
        } catch (java.util.concurrent.ExecutionException e) {
          if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
            created = true;
          }
          // UnknownTopicOrPartition and similar while the cluster settles: retry
        }
        Thread.sleep(250);
      }
      throw new IllegalStateException("topic " + name + " not ready on Kafka");
    }
  }

  @Override
  public AsyncProducer producer(BenchConfig c, String topic) {
    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    p.put("acks", c.acks());
    p.put("batch.size", Integer.toString(c.batchSize()));
    p.put("linger.ms", Integer.toString(c.lingerMs()));
    p.put("compression.type", "none");
    p.put("enable.idempotence", Boolean.toString("all".equals(c.acks())));
    p.put("buffer.memory", Long.toString(8L * 1024 * 1024));
    p.put("key.serializer", ByteArraySerializer.class.getName());
    p.put("value.serializer", ByteArraySerializer.class.getName());
    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p);
    return new AsyncProducer() {
      @Override
      public void send(byte[] key, byte[] value, BiConsumer<Long, Exception> callback) {
        producer.send(
            new ProducerRecord<>(topic, key, value),
            (md, err) -> callback.accept(md == null ? -1L : md.offset(), err));
      }

      @Override
      public void flush() {
        producer.flush();
      }

      @Override
      public void close() {
        producer.close(Duration.ofSeconds(30));
      }
    };
  }

  @Override
  public PollingConsumer consumer(String topic, int partitions) {
    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    p.put("key.deserializer", ByteArrayDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    p.put("auto.offset.reset", "earliest");
    p.put("enable.auto.commit", "false");
    p.put("max.poll.records", "10000");
    p.put("fetch.max.wait.ms", "100");
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p);
    List<TopicPartition> tps = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      tps.add(new TopicPartition(topic, i));
    }
    consumer.assign(tps);
    consumer.seekToBeginning(tps);
    return new PollingConsumer() {
      @Override
      public List<byte[]> poll(long timeoutMs) {
        List<byte[]> out = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(timeoutMs))) {
          out.add(r.value());
        }
        return out;
      }

      @Override
      public void close() {
        consumer.close();
      }
    };
  }

  @Override
  public void close() {}
}
