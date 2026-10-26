package dev.logforge.network;

import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.Node;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.Metadata;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.ProtocolWriter;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NioServerTest {

  @TempDir Path dir;
  Node node;

  @BeforeEach
  void start() throws Exception {
    node = TestNodes.single(dir);
  }

  @AfterEach
  void stop() throws Exception {
    node.close();
  }

  @Test
  void pipelinedRequestsGetResponsesInOrder() throws Exception {
    try (TestClient c = new TestClient("127.0.0.1", node.port())) {
      ByteBuffer all = ByteBuffer.allocate(64 * 1024);
      for (int i = 1; i <= 50; i++) {
        all.put(TestClient.frame(ApiKeys.METADATA, i, w -> new Metadata.Request(null).write(w)));
      }
      all.flip();
      byte[] bytes = new byte[all.remaining()];
      all.get(bytes);
      c.sendRaw(bytes); // 50 requests in one write, before reading anything
      for (int i = 1; i <= 50; i++) {
        assertThat(c.readResponseCorrelation()).isEqualTo(i);
      }
    }
  }

  @Test
  void malformedFramesCloseOnlyTheirConnection() throws Exception {
    // Negative size, size above the cap (a 2 GiB claim must not be allocated), size below the
    // header, unknown API key, and a body that does not parse.
    List<byte[]> bad =
        List.of(
            ByteBuffer.allocate(4).putInt(-1).array(),
            ByteBuffer.allocate(8).putInt(Integer.MAX_VALUE).putInt(0).array(),
            ByteBuffer.allocate(8).putInt(3).put(new byte[] {1, 2, 3}).array(),
            TestClient.frame((short) 999, 1, w -> w.writeInt(7)),
            TestClient.frame(
                ApiKeys.PRODUCE, 1, w -> w.writeShort((short) 1).writeInt(5).writeInt(1_000_000)));
    for (byte[] frame : bad) {
      try (TestClient c = new TestClient("127.0.0.1", node.port())) {
        c.sendRaw(frame);
        assertThat(c.isClosedByPeer()).as("connection closed after malformed frame").isTrue();
      }
    }
    // The broker is still healthy for everyone else.
    try (TestClient ok = new TestClient("127.0.0.1", node.port())) {
      ok.createTopic("still-up", 1, 1, Map.of());
      TestNodes.awaitTrue(() -> node.isLeaderFor("still-up", 0), java.time.Duration.ofSeconds(10));
      Produce.PartitionResult r = ok.produce("still-up", 0, (short) 1, TestClient.batch("alive"));
      assertThat(r.errorCode()).isEqualTo(Errors.NONE.code());
    }
  }

  @Test
  void aSlowConsumerDoesNotStallOtherConnections() throws Exception {
    try (TestClient admin = new TestClient("127.0.0.1", node.port())) {
      admin.createTopic("big", 1, 1, Map.of());
      TestNodes.awaitTrue(() -> node.isLeaderFor("big", 0), java.time.Duration.ofSeconds(10));
      String payload = "x".repeat(60_000);
      for (int i = 0; i < 80; i++) { // ~5 MB of data to fetch
        admin.produce("big", 0, (short) 1, TestClient.batch(payload));
      }
    }

    // The slow client floods fetch requests and never reads a byte of the responses.
    Socket slow = new Socket("127.0.0.1", node.port());
    slow.setReceiveBufferSize(4096);
    OutputStream slowOut = slow.getOutputStream();
    Thread flooder =
        new Thread(
            () -> {
              try {
                for (int i = 1; i <= 2_000; i++) {
                  slowOut.write(
                      TestClient.frame(
                          ApiKeys.FETCH,
                          i,
                          w ->
                              new Fetch.Request(
                                      -1,
                                      0,
                                      1,
                                      1 << 20,
                                      List.of(
                                          new Fetch.TopicData(
                                              "big",
                                              List.of(
                                                  new Fetch.PartitionData(0, -1, 0L, 1 << 20)))))
                                  .write(w)));
                }
              } catch (Exception e) {
                // Expected: the broker stops reading, our writes eventually block or fail.
              }
            });
    flooder.setDaemon(true);
    flooder.start();
    Thread.sleep(500);

    long start = System.nanoTime();
    try (TestClient fast = new TestClient("127.0.0.1", node.port())) {
      for (int i = 0; i < 100; i++) {
        Produce.PartitionResult r =
            fast.produce("big", 0, (short) 1, TestClient.batch("fast-" + i));
        assertThat(r.errorCode()).isEqualTo(Errors.NONE.code());
      }
      Fetch.PartitionResult f = fast.fetch("big", 0, 80, 0);
      assertThat(f.errorCode()).isEqualTo(Errors.NONE.code());
      assertThat(f.records().remaining()).isPositive();
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertThat(elapsedMs).as("fast client latency while a slow client is stuck").isLessThan(5_000);
    assertThat(node.mutedConnections())
        .as("the slow connection was muted by backpressure")
        .isGreaterThanOrEqualTo(1);
    slow.close();
  }

  @Test
  void unsupportedVersionGetsAnErrorResponse() throws Exception {
    try (TestClient c = new TestClient("127.0.0.1", node.port())) {
      ProtocolWriter w = new ProtocolWriter();
      w.writeInt(0);
      new dev.logforge.network.protocol.RequestHeader(ApiKeys.METADATA, (short) 7, 42, "x")
          .write(w);
      ByteBuffer b = w.toBuffer();
      b.putInt(0, b.remaining() - 4);
      byte[] bytes = new byte[b.remaining()];
      b.get(bytes);
      c.sendRaw(bytes);
      assertThat(c.readResponse(42).readShort()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
    }
  }
}
