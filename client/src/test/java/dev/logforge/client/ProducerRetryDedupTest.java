package dev.logforge.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.Node;
import dev.logforge.NodeConfig;
import dev.logforge.TopicPartition;
import dev.logforge.log.FsyncPolicy;
import dev.logforge.log.LogConfig;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 5 gate: exactly-once-per-partition under a forced retry.
 *
 * <p>The broker advertises a proxy's address, so all client traffic goes through it. The proxy
 * delivers the first Produce request and then cuts the connection before the response arrives: the
 * broker has appended the batch but the producer sees a network error and retries it. With
 * idempotence the retry carries the same producer id and sequence, and the broker answers with the
 * original offset instead of appending again. The control run without idempotence shows the retry
 * really happened by producing the duplicate.
 */
@Tag("gate")
class ProducerRetryDedupTest {

  @TempDir Path dir;

  private static List<String> readAll(String bootstrap, String topic) {
    List<String> out = new ArrayList<>();
    try (Consumer c = new Consumer(ConsumerConfig.builder(bootstrap).build())) {
      c.assign(List.of(new TopicPartition(topic, 0)));
      int idle = 0;
      while (idle < 4) {
        var batch = c.poll(Duration.ofMillis(250));
        if (batch.isEmpty()) {
          idle++;
        }
        batch.forEach(r -> out.add(new String(r.value(), UTF_8)));
      }
    }
    return out;
  }

  private int run(boolean idempotent, String topic, Path dataDir) throws Exception {
    int proxyTarget;
    try (ServerSocket s = new ServerSocket(0)) {
      proxyTarget = s.getLocalPort();
    }
    try (DroppingProxy proxy = new DroppingProxy(proxyTarget);
        Node node =
            Node.start(
                NodeConfig.builder()
                    .nodeId(0)
                    .roles(EnumSet.of(NodeConfig.Role.BROKER, NodeConfig.Role.CONTROLLER))
                    .listen("127.0.0.1", proxyTarget)
                    .advertisedHost("127.0.0.1")
                    .advertisedPort(proxy.port())
                    .dataDir(dataDir)
                    .adminListen(null)
                    .heartbeatIntervalMs(50)
                    .defaultLogConfig(LogConfig.defaults().withFsync(new FsyncPolicy.Never()))
                    .build())) {
      while (!node.brokerRegistered()) {
        Thread.sleep(10);
      }
      String bootstrap = "127.0.0.1:" + proxy.port();
      try (Admin admin = new Admin(bootstrap)) {
        admin.createTopic(topic, 1, 1, Map.of());
      }
      try (Producer p =
          new Producer(
              ProducerConfig.builder(bootstrap)
                  .acks(Acks.ALL)
                  .idempotent(idempotent)
                  .retries(5)
                  .retryBackoffMs(50)
                  .build())) {
        RecordMetadata md = p.send(topic, null, "exactly-once".getBytes(UTF_8)).get();
        assertThat(md.offset()).isZero();
      }
      assertThat(proxy.connectionsCut()).as("the first response was really dropped").isEqualTo(1);
      assertThat(proxy.producesForwarded())
          .as("the producer really retried")
          .isGreaterThanOrEqualTo(2);
      List<String> values = readAll(bootstrap, topic);
      System.out.printf(
          "forced retry (idempotent=%s): produce requests delivered=%d, copies in log=%d%n",
          idempotent, proxy.producesForwarded(), values.size());
      return values.size();
    }
  }

  @Test
  void aRetriedBatchAfterANetworkTimeoutIsStoredExactlyOnce() throws Exception {
    assertThat(run(true, "dedupe", dir.resolve("a"))).isEqualTo(1);
  }

  @Test
  void withoutIdempotenceTheSameRetryCreatesADuplicate() throws Exception {
    assertThat(run(false, "dupes", dir.resolve("b"))).isEqualTo(2);
  }
}
