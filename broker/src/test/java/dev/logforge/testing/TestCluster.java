package dev.logforge.testing;

import dev.logforge.Node;
import dev.logforge.NodeConfig;
import dev.logforge.TopicPartition;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.replication.Partition;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An in-process cluster: one controller-only node (id 0) and N brokers (ids 1..N), each with its
 * own data directory and port. Brokers can be halted (crash-like stop), restarted on the same data
 * and port, and frozen (heartbeats and replication suspended, like SIGSTOP).
 */
public final class TestCluster implements AutoCloseable {
  private final Path dir;
  private final Node controller;
  private final Node[] brokers;
  private final int[] ports;
  private final boolean unclean;

  public TestCluster(Path dir, int brokers) throws Exception {
    this(dir, brokers, false);
  }

  public TestCluster(Path dir, int brokers, boolean uncleanElection) throws Exception {
    this.dir = dir;
    this.unclean = uncleanElection;
    this.controller =
        Node.start(
            TestNodes.baseConfig(0, dir)
                .roles(EnumSet.of(NodeConfig.Role.CONTROLLER))
                .uncleanLeaderElection(uncleanElection)
                .build());
    this.brokers = new Node[brokers + 1];
    this.ports = new int[brokers + 1];
    for (int i = 1; i <= brokers; i++) {
      startBroker(i, 0);
    }
    TestNodes.awaitTrue(this::allRegistered, Duration.ofSeconds(15));
  }

  private void startBroker(int id, int port) throws IOException {
    Node n =
        Node.start(
            TestNodes.baseConfig(id, dir)
                .roles(EnumSet.of(NodeConfig.Role.BROKER))
                .listen("127.0.0.1", port)
                .controller("127.0.0.1", controller.port())
                .uncleanLeaderElection(unclean)
                .build());
    brokers[id] = n;
    ports[id] = n.port();
  }

  private boolean allRegistered() {
    for (int i = 1; i < brokers.length; i++) {
      if (brokers[i] == null || !brokers[i].brokerRegistered()) {
        return false;
      }
    }
    return true;
  }

  public Node controller() {
    return controller;
  }

  public Node broker(int id) {
    return brokers[id];
  }

  public int size() {
    return brokers.length - 1;
  }

  public TestClient clientFor(int brokerId) throws IOException {
    return new TestClient("127.0.0.1", ports[brokerId]);
  }

  public TestClient controllerClient() throws IOException {
    return new TestClient("127.0.0.1", controller.port());
  }

  /** The controller's current view of a partition. */
  public ControllerApis.PartitionInfo partitionInfo(String topic, int partition) {
    for (ControllerApis.TopicInfo t : controller.controller().snapshot().topics()) {
      if (t.name().equals(topic)) {
        return t.partitions().get(partition);
      }
    }
    return null;
  }

  public int leaderOf(String topic, int partition) {
    ControllerApis.PartitionInfo p = partitionInfo(topic, partition);
    return p == null ? -1 : p.leader();
  }

  public List<Integer> isrOf(String topic, int partition) {
    ControllerApis.PartitionInfo p = partitionInfo(topic, partition);
    return p == null ? List.of() : p.isr();
  }

  /** Waits until the controller's leader is serving the partition. */
  public int awaitLeader(String topic, int partition, Duration timeout)
      throws InterruptedException {
    int[] out = {-1};
    TestNodes.awaitTrue(
        () -> {
          int l = leaderOf(topic, partition);
          if (l > 0 && brokers[l] != null && brokers[l].isLeaderFor(topic, partition)) {
            out[0] = l;
            return true;
          }
          return false;
        },
        timeout);
    return out[0];
  }

  public void awaitIsr(String topic, int partition, Set<Integer> expected, Duration timeout)
      throws InterruptedException {
    TestNodes.awaitTrue(() -> Set.copyOf(isrOf(topic, partition)).equals(expected), timeout);
  }

  public void createTopic(String name, int partitions, int rf, Map<String, String> configs)
      throws Exception {
    try (TestClient c = controllerClient()) {
      short err = c.createTopic(name, partitions, rf, configs).topics().get(0).errorCode();
      if (err != 0) {
        throw new IllegalStateException("create topic failed: " + err);
      }
    }
    for (int p = 0; p < partitions; p++) {
      awaitLeader(name, p, Duration.ofSeconds(15));
    }
  }

  /** Crash-like stop: no clean shutdown of logs, no leave. The data directory stays. */
  public void halt(int id) {
    brokers[id].halt();
  }

  /** Restarts a halted broker on the same port and data directory (recovery runs). */
  public void restart(int id) throws IOException {
    startBroker(id, ports[id]);
  }

  public void freeze(int id) {
    brokers[id].freeze();
  }

  public void unfreeze(int id) {
    brokers[id].unfreeze();
  }

  public Partition replica(int brokerId, String topic, int partition) {
    return brokers[brokerId].replicaManager().partition(new TopicPartition(topic, partition));
  }

  public List<Integer> aliveBrokers() {
    List<Integer> out = new ArrayList<>();
    for (int i = 1; i < brokers.length; i++) {
      if (brokers[i] != null && !brokers[i].isHalted()) {
        out.add(i);
      }
    }
    return out;
  }

  @Override
  public void close() throws Exception {
    for (int i = 1; i < brokers.length; i++) {
      if (brokers[i] != null) {
        brokers[i].close();
      }
    }
    controller.close();
  }
}
