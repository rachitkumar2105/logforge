package dev.logforge.client;

import dev.logforge.Node;
import dev.logforge.NodeConfig;
import dev.logforge.log.FsyncPolicy;
import dev.logforge.log.LogConfig;
import java.nio.file.Path;
import java.util.EnumSet;

/** A single in-process broker+controller for client tests. */
final class EmbeddedNode {
  private EmbeddedNode() {}

  static Node start(Path dataDir) throws Exception {
    Node node =
        Node.start(
            NodeConfig.builder()
                .clusterToken(TestToken.VALUE)
                .nodeId(0)
                .roles(EnumSet.of(NodeConfig.Role.BROKER, NodeConfig.Role.CONTROLLER))
                .listen("127.0.0.1", 0)
                .advertisedHost("127.0.0.1")
                .dataDir(dataDir)
                .adminListen(null)
                .heartbeatIntervalMs(50)
                .defaultLogConfig(LogConfig.defaults().withFsync(new FsyncPolicy.Never()))
                .build());
    long deadline = System.nanoTime() + 10_000_000_000L;
    while (!node.brokerRegistered()) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException("broker did not register");
      }
      Thread.sleep(10);
    }
    return node;
  }

  static String bootstrap(Node node) {
    return "127.0.0.1:" + node.port();
  }
}
