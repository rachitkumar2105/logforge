package dev.logforge.network;

import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.Node;
import dev.logforge.NodeConfig;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SECURITY-AUDIT F2: one client must not be able to exhaust the broker's file descriptors. Excess
 * connections are closed at accept time and counted; everyone else keeps working.
 */
class ConnectionLimitsTest {
  @TempDir Path dir;

  private Node start(UnaryOperator<NodeConfig.Builder> limits) throws Exception {
    Node node =
        Node.start(
            limits
                .apply(
                    TestNodes.baseConfig(0, dir)
                        .roles(EnumSet.of(NodeConfig.Role.BROKER, NodeConfig.Role.CONTROLLER)))
                .build());
    TestNodes.awaitTrue(node::brokerRegistered, Duration.ofSeconds(10));
    return node;
  }

  private static boolean answers(TestClient c) {
    try {
      return c.metadata(null) != null;
    } catch (Exception e) {
      return false;
    }
  }

  @Test
  void connectionsBeyondThePerIpCapAreClosedAndCounted() throws Exception {
    try (Node node = start(b -> b.maxConnectionsPerIp(3).maxConnections(50))) {
      List<TestClient> open = new ArrayList<>();
      try {
        for (int i = 0; i < 3; i++) {
          TestClient c = new TestClient("127.0.0.1", node.port());
          open.add(c);
          assertThat(answers(c)).as("connection %d within the cap", i).isTrue();
        }
        try (TestClient extra = new TestClient("127.0.0.1", node.port())) {
          assertThat(extra.isClosedByPeer()).as("fourth connection from one address").isTrue();
        }
        TestNodes.awaitTrue(() -> node.server().refusedConnections() == 1, Duration.ofSeconds(5));
        for (TestClient c : open) {
          assertThat(answers(c)).as("existing connections unaffected").isTrue();
        }

        open.remove(0).close();
        TestNodes.awaitTrue(() -> node.server().openConnections() == 2, Duration.ofSeconds(5));
        try (TestClient again = new TestClient("127.0.0.1", node.port())) {
          assertThat(answers(again)).as("a freed slot can be reused").isTrue();
        }
      } finally {
        for (TestClient c : open) {
          c.close();
        }
      }
    }
  }

  @Test
  void connectionsBeyondTheTotalCapAreClosed() throws Exception {
    try (Node node = start(b -> b.maxConnectionsPerIp(1_000).maxConnections(2))) {
      try (TestClient a = new TestClient("127.0.0.1", node.port());
          TestClient b = new TestClient("127.0.0.1", node.port())) {
        assertThat(answers(a)).isTrue();
        assertThat(answers(b)).isTrue();
        try (TestClient c = new TestClient("127.0.0.1", node.port())) {
          assertThat(c.isClosedByPeer()).isTrue();
        }
      }
    }
  }
}
