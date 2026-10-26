package dev.logforge.replication;

import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestCluster;
import dev.logforge.testing.TestNodes;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A broker whose disk fails (for example, full) must stop leading: the controller fences it and
 * moves leadership to an in-sync replica, so producers recover instead of retrying a broker that
 * can no longer write. The real disk-full case runs in chaos/ on a size-limited tmpfs.
 */
class StorageFailureTest {

  @TempDir Path dir;

  @Test
  void aBrokerReportingStorageFailureIsFencedAndLosesLeadership() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 3)) {
      cluster.createTopic("disk", 1, 3, Map.of("min.insync.replicas", "2"));
      int leader = cluster.awaitLeader("disk", 0, Duration.ofSeconds(20));
      try (TestClient c = cluster.clientFor(leader)) {
        assertThat(c.produce("disk", 0, (short) -1, TestClient.batch("before")).errorCode())
            .isEqualTo(Errors.NONE.code());
      }

      cluster
          .broker(leader)
          .replicaManager()
          .markStorageFailed("simulated: no space left on device");

      TestNodes.awaitTrue(
          () -> cluster.leaderOf("disk", 0) > 0 && cluster.leaderOf("disk", 0) != leader,
          Duration.ofSeconds(20));
      boolean fencedInMetadata =
          cluster.controller().controller().snapshot().brokers().stream()
              .filter(b -> b.nodeId() == leader)
              .noneMatch(ControllerApis.BrokerInfo::alive);
      assertThat(fencedInMetadata).as("fenced broker shown as not alive").isTrue();
      int newLeader = cluster.awaitLeader("disk", 0, Duration.ofSeconds(20));
      try (TestClient c = cluster.clientFor(newLeader)) {
        assertThat(c.produce("disk", 0, (short) -1, TestClient.batch("after")).errorCode())
            .isEqualTo(Errors.NONE.code());
      }
      // Still fenced a few heartbeats later: a failed disk does not heal by itself.
      Thread.sleep(1_000);
      assertThat(cluster.leaderOf("disk", 0)).isNotEqualTo(leader);
    }
  }
}
