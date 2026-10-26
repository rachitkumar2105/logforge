package dev.logforge;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.log.RecordBatch;
import dev.logforge.network.BlockingChannel;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.Topics;
import dev.logforge.util.JsonLog;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Writes a small sample topic into a fresh node. The hosted demo has no persistent disk, so every
 * restart starts empty; seeding gives the live view something real to show. It talks to the node
 * over its own wire protocol on loopback, exactly as any client would.
 */
public final class Seeder {
  private static final JsonLog LOG = JsonLog.get(Seeder.class);
  private static final int BATCH = 50;
  private static final long DEADLINE_MS = 30_000;
  private static final String[] ITEMS = {"keyboard", "monitor", "cable", "dock", "lamp", "chair"};

  /** {@code LOGFORGE_SEED_TOPIC=name:partitions:records}, e.g. {@code demo-orders:3:600}. */
  public record Spec(String topic, int partitions, int records) {
    public static Spec parse(String s) {
      String[] p = s.split(":");
      try {
        if (p.length == 3) {
          Spec spec = new Spec(p[0], Integer.parseInt(p[1]), Integer.parseInt(p[2]));
          if (TopicPartition.isValidTopicName(spec.topic)
              && !TopicPartition.isInternal(spec.topic)
              && spec.partitions >= 1
              && spec.partitions <= 64
              && spec.records >= 0
              && spec.records <= 1_000_000) {
            return spec;
          }
        }
      } catch (NumberFormatException e) {
        // reported below
      }
      throw new IllegalArgumentException(
          "LOGFORGE_SEED_TOPIC must be name:partitions:records (1-64 partitions, 0-1000000"
              + " records), got "
              + s);
    }
  }

  private Seeder() {}

  /**
   * Creates the topic with replication factor 1 and writes the records, unless the topic already
   * exists. Returns the number of records written.
   */
  public static int seed(String host, int port, Spec spec)
      throws IOException, InterruptedException {
    try (BlockingChannel ch = new BlockingChannel(host, port, "logforge-seeder", 10_000)) {
      Topics.CreateRequest create =
          new Topics.CreateRequest(
              10_000,
              List.of(
                  new Topics.NewTopic(
                      spec.topic(),
                      spec.partitions(),
                      (short) 1,
                      Map.of("min.insync.replicas", "1"))));
      Topics.CreateResult created =
          Topics.CreateResponse.read(ch.request(ApiKeys.CREATE_TOPICS, create::write))
              .topics()
              .get(0);
      Errors err = Errors.forCode(created.errorCode());
      if (err == Errors.TOPIC_ALREADY_EXISTS) {
        LOG.info("seed topic exists, nothing written", "topic", spec.topic());
        return 0;
      }
      if (err != Errors.NONE) {
        throw new IOException("creating seed topic failed: " + err);
      }
      int written = 0;
      long now = System.currentTimeMillis();
      for (int start = 0; start < spec.records(); start += BATCH) {
        int partition = (start / BATCH) % spec.partitions();
        RecordBatch.Builder b = RecordBatch.builder(now);
        for (int i = start; i < Math.min(start + BATCH, spec.records()); i++) {
          String value =
              "{\"order\":"
                  + i
                  + ",\"item\":\""
                  + ITEMS[i % ITEMS.length]
                  + "\",\"quantity\":"
                  + (1 + i % 4)
                  + "}";
          b.append(now, ("order-" + i).getBytes(UTF_8), value.getBytes(UTF_8));
        }
        written += produce(ch, spec.topic(), partition, b.build());
      }
      LOG.info("seed topic written", "topic", spec.topic(), "records", written);
      return written;
    }
  }

  /** Retries while the new partition's leadership is still propagating. */
  private static int produce(BlockingChannel ch, String topic, int partition, RecordBatch batch)
      throws IOException, InterruptedException {
    long deadline = System.currentTimeMillis() + DEADLINE_MS;
    while (true) {
      Produce.Request req =
          new Produce.Request(
              (short) -1,
              10_000,
              List.of(
                  new Produce.TopicData(
                      topic,
                      List.of(new Produce.PartitionData(partition, batch.buffer().duplicate())))));
      Errors err =
          Errors.forCode(
              Produce.Response.read(ch.request(ApiKeys.PRODUCE, req::write))
                  .topics()
                  .get(0)
                  .partitions()
                  .get(0)
                  .errorCode());
      if (err == Errors.NONE) {
        return batch.recordCount();
      }
      if (!err.retriable() || System.currentTimeMillis() > deadline) {
        throw new IOException("seeding " + topic + "-" + partition + " failed: " + err);
      }
      Thread.sleep(100);
    }
  }
}
