package dev.logforge;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.log.RecordBatch;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import dev.logforge.util.JsonLog;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SECURITY.md S4 (payload limits) and S19 (nothing sensitive in logs), checked against a broker.
 */
class SecurityLimitsTest {

  @TempDir Path dir;

  @AfterEach
  void restoreLogs() {
    JsonLog.resetOutput();
  }

  @Test
  void aFetchCannotMakeTheBrokerAllocateMoreThanTheServerCap() throws Exception {
    try (Node node = TestNodes.single(dir);
        TestClient c = new TestClient("127.0.0.1", node.port())) {
      c.createTopic("big", 1, 1, Map.of());
      TestNodes.awaitTrue(() -> node.isLeaderFor("big", 0), Duration.ofSeconds(10));
      String mb = "x".repeat(900 * 1024);
      for (int i = 0; i < 24; i++) { // about 21 MiB
        assertThat(c.produce("big", 0, (short) 1, TestClient.batch(mb)).errorCode())
            .isEqualTo(Errors.NONE.code());
      }
      Fetch.Request greedy =
          new Fetch.Request(
              -1,
              0,
              1,
              Integer.MAX_VALUE,
              List.of(
                  new Fetch.TopicData(
                      "big", List.of(new Fetch.PartitionData(0, -1, 0L, Integer.MAX_VALUE)))));
      Fetch.PartitionResult r =
          Fetch.Response.read(c.call(ApiKeys.FETCH, greedy::write))
              .topics()
              .get(0)
              .partitions()
              .get(0);
      assertThat(r.errorCode()).isEqualTo(Errors.NONE.code());
      assertThat(r.records().remaining())
          .as("capped by the server, not by the client's maxBytes")
          .isLessThanOrEqualTo(8 * 1024 * 1024);
      assertThat(RecordBatch.iterate(r.records())).isNotEmpty();
    }
  }

  @Test
  void recordContentsAndTokensNeverReachTheLogs() throws Exception {
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    JsonLog.setOutput(new PrintStream(captured, true, UTF_8));
    String secretValue = "SECRET-VALUE-7d1f";
    String secretKey = "SECRET-KEY-2b9c";
    try (Node node = TestNodes.single(dir);
        TestClient c = new TestClient("127.0.0.1", node.port())) {
      c.createTopic("audit", 1, 1, Map.of("max.batch.bytes", "2048"));
      TestNodes.awaitTrue(() -> node.isLeaderFor("audit", 0), Duration.ofSeconds(10));
      RecordBatch b =
          RecordBatch.builder(System.currentTimeMillis())
              .append(
                  System.currentTimeMillis(),
                  secretKey.getBytes(UTF_8),
                  secretValue.getBytes(UTF_8))
              .build();
      c.produce("audit", 0, (short) 1, b);
      // Errors are logged too: a corrupt batch and an oversized one.
      RecordBatch corrupt =
          RecordBatch.builder(System.currentTimeMillis())
              .append(
                  System.currentTimeMillis(),
                  secretKey.getBytes(UTF_8),
                  secretValue.getBytes(UTF_8))
              .build();
      var buf = corrupt.buffer();
      buf.put(buf.limit() - 3, (byte) (buf.get(buf.limit() - 3) ^ 1));
      c.produce("audit", 0, (short) 1, corrupt);
      c.produce("audit", 0, (short) 1, TestClient.batch(secretValue.repeat(200)));
      c.fetch("audit", 0, 0, 0);
    }
    String logs = captured.toString(UTF_8);
    assertThat(logs).isNotEmpty();
    assertThat(logs).doesNotContain(secretValue).doesNotContain(secretKey);
  }
}
