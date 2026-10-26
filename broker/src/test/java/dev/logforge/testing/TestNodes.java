package dev.logforge.testing;

import dev.logforge.Node;
import dev.logforge.NodeConfig;
import dev.logforge.log.FsyncPolicy;
import dev.logforge.log.LogConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumSet;
import java.util.function.BooleanSupplier;

/** Starts in-process nodes for tests. */
public final class TestNodes {
  private TestNodes() {}

  public static NodeConfig.Builder baseConfig(int nodeId, Path dataDir) {
    return NodeConfig.builder()
        .nodeId(nodeId)
        .listen("127.0.0.1", 0)
        .advertisedHost("127.0.0.1")
        .dataDir(dataDir.resolve("node-" + nodeId))
        .adminListen(null)
        .heartbeatIntervalMs(50)
        .brokerSessionTimeoutMs(1_500)
        .replicaLagTimeMaxMs(1_000)
        .defaultLogConfig(LogConfig.defaults().withFsync(new FsyncPolicy.Never()));
  }

  /** One process acting as both controller and broker. */
  public static Node single(Path dataDir) throws Exception {
    Node node =
        Node.start(
            baseConfig(0, dataDir)
                .roles(EnumSet.of(NodeConfig.Role.BROKER, NodeConfig.Role.CONTROLLER))
                .build());
    awaitTrue(() -> node.brokerRegistered(), Duration.ofSeconds(10));
    return node;
  }

  public static void awaitTrue(BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("condition not met within " + timeout);
      }
      Thread.sleep(10);
    }
  }
}
