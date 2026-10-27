package dev.logforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The hosted demo runs one node on a platform (Render) that hands the service its HTTP port in
 * {@code PORT} and keeps no disk across restarts, so the node seeds a small sample topic itself.
 */
class HostedModeTest {
  @TempDir Path dir;

  private Map<String, String> env(String... kv) {
    Map<String, String> m = new HashMap<>();
    m.put("LOGFORGE_NODE_ID", "1");
    m.put("LOGFORGE_DATA_DIR", dir.toString());
    m.put("LOGFORGE_ADMIN_TOKEN", "a".repeat(32));
    m.put("LOGFORGE_CLUSTER_TOKEN", TestNodes.CLUSTER_TOKEN);
    m.put("LOGFORGE_READ_TOKEN", "r".repeat(32));
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  @Test
  void thePlatformPortBecomesTheAdminPortWhenNoAdminListenIsGiven() {
    NodeConfig c = NodeConfig.fromEnv(env("PORT", "10000"));
    assertThat(c.adminHost).isEqualTo("0.0.0.0");
    assertThat(c.adminPort).isEqualTo(10000);
  }

  @Test
  void anExplicitAdminListenWinsOverThePlatformPort() {
    NodeConfig c =
        NodeConfig.fromEnv(env("PORT", "10000", "LOGFORGE_ADMIN_LISTEN", "0.0.0.0:8081"));
    assertThat(c.adminPort).isEqualTo(8081);
  }

  @Test
  void withoutEitherTheAdminPortStays8080() {
    assertThat(NodeConfig.fromEnv(env()).adminPort).isEqualTo(8080);
  }

  @Test
  void aSeedSpecIsParsedAndABadOneFailsAtStartup() {
    NodeConfig c = NodeConfig.fromEnv(env("LOGFORGE_SEED_TOPIC", "demo-orders:3:600"));
    assertThat(c.seed).isEqualTo(new Seeder.Spec("demo-orders", 3, 600));
    assertThat(NodeConfig.fromEnv(env()).seed).isNull();
    for (String bad : List.of("demo", "demo:0:10", "demo:3:-1", "bad name!:3:10", "demo:x:10")) {
      assertThatThrownBy(() -> NodeConfig.fromEnv(env("LOGFORGE_SEED_TOPIC", bad)))
          .as(bad)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("LOGFORGE_SEED_TOPIC");
    }
  }

  @Test
  void theSeederCreatesTheTopicAndWritesTheRecordsExactlyOnce() throws Exception {
    try (Node node = TestNodes.single(dir)) {
      Seeder.Spec spec = new Seeder.Spec("demo-orders", 3, 600);
      assertThat(Seeder.seed("127.0.0.1", node.port(), spec, TestNodes.CLUSTER_TOKEN))
          .isEqualTo(600);
      // A second run (a restart that kept its disk) finds the topic and writes nothing.
      assertThat(Seeder.seed("127.0.0.1", node.port(), spec, TestNodes.CLUSTER_TOKEN)).isZero();

      long total = 0;
      try (TestClient c = new TestClient("127.0.0.1", node.port())) {
        var md = c.metadata(List.of("demo-orders"));
        assertThat(md.topics().get(0).partitions()).hasSize(3);
      }
      for (int p = 0; p < 3; p++) {
        int partition = p;
        TestNodes.awaitTrue(
            () -> node.logEndOffset("demo-orders", partition) > 0, java.time.Duration.ofSeconds(5));
        total += node.logEndOffset("demo-orders", p);
      }
      assertThat(total).isEqualTo(600);
    }
  }
}
