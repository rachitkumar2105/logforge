package dev.logforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Authenticate;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.Replication;
import dev.logforge.network.protocol.Topics;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SECURITY-AUDIT F1: the cluster's own operations on the binary protocol (topic administration,
 * broker registration, ISR changes, follower replication) need the cluster token. Producing and
 * consuming do not.
 */
class ClusterAuthTest {
  @TempDir Path dir;

  private static short authenticate(TestClient c, String token) throws Exception {
    return Authenticate.Response.read(
            c.call(ApiKeys.AUTHENTICATE, new Authenticate.Request(token)::write))
        .errorCode();
  }

  private static short create(TestClient c, String topic) throws Exception {
    Topics.CreateRequest req =
        new Topics.CreateRequest(
            10_000, List.of(new Topics.NewTopic(topic, 1, (short) 1, Map.of())));
    return Topics.CreateResponse.read(c.call(ApiKeys.CREATE_TOPICS, req::write))
        .topics()
        .get(0)
        .errorCode();
  }

  @Test
  void anAnonymousConnectionCannotCreateOrDeleteTopics() throws Exception {
    try (Node node = TestNodes.single(dir);
        TestClient anon = TestClient.anonymous("127.0.0.1", node.port());
        TestClient admin = new TestClient("127.0.0.1", node.port())) {
      assertThat(create(anon, "t")).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
      assertThat(admin.metadata(List.of("t")).topics().get(0).errorCode())
          .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());

      admin.createTopic("t", 1, 1, Map.of());
      Topics.DeleteRequest del = new Topics.DeleteRequest(10_000, List.of("t"));
      short err =
          Topics.DeleteResponse.read(anon.call(ApiKeys.DELETE_TOPICS, del::write))
              .topics()
              .get(0)
              .errorCode();
      assertThat(err).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
      assertThat(admin.metadata(List.of("t")).topics().get(0).errorCode())
          .isEqualTo(Errors.NONE.code());
    }
  }

  @Test
  void aWrongTokenIsRefusedAndTheRightOneUnlocksTheConnection() throws Exception {
    try (Node node = TestNodes.single(dir);
        TestClient c = TestClient.anonymous("127.0.0.1", node.port())) {
      assertThat(authenticate(c, "x".repeat(32)))
          .isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
      assertThat(create(c, "t")).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
      assertThat(authenticate(c, TestNodes.CLUSTER_TOKEN)).isEqualTo(Errors.NONE.code());
      assertThat(create(c, "t")).isEqualTo(Errors.NONE.code());
    }
  }

  @Test
  void anAnonymousConnectionCannotPoseAsABrokerOrChangeAnIsr() throws Exception {
    try (Node node = TestNodes.single(dir);
        TestClient anon = TestClient.anonymous("127.0.0.1", node.port());
        TestClient admin = new TestClient("127.0.0.1", node.port())) {
      admin.createTopic("t", 1, 1, Map.of());
      ControllerApis.HeartbeatRequest hb =
          new ControllerApis.HeartbeatRequest(99, 1, "evil.example", 9092, -1, null);
      ControllerApis.HeartbeatResponse hr =
          ControllerApis.HeartbeatResponse.read(anon.call(ApiKeys.BROKER_HEARTBEAT, hb::write));
      assertThat(hr.errorCode()).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
      assertThat(admin.metadata(null).brokers()).noneMatch(b -> b.nodeId() == 99);

      ControllerApis.AlterIsrRequest alter =
          new ControllerApis.AlterIsrRequest(
              0,
              1,
              List.of(
                  new ControllerApis.AlterTopic(
                      "t", List.of(new ControllerApis.AlterPartition(0, 0, 0, List.of())))));
      short err =
          ControllerApis.AlterIsrResponse.read(anon.call(ApiKeys.ALTER_ISR, alter::write))
              .topics()
              .get(0)
              .partitions()
              .get(0)
              .errorCode();
      assertThat(err).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
    }
  }

  @Test
  void anAnonymousConnectionCannotFetchAsAFollowerButProducersAndConsumersWork() throws Exception {
    try (Node node = TestNodes.single(dir);
        TestClient anon = TestClient.anonymous("127.0.0.1", node.port());
        TestClient admin = new TestClient("127.0.0.1", node.port())) {
      admin.createTopic("t", 1, 1, Map.of());
      TestNodes.awaitTrue(() -> node.isLeaderFor("t", 0), java.time.Duration.ofSeconds(10));
      assertThat(anon.produce("t", 0, (short) 1, TestClient.batch("a")).errorCode())
          .isEqualTo(Errors.NONE.code());
      Fetch.PartitionResult consumer = anon.fetch("t", 0, 0, 0);
      assertThat(consumer.errorCode()).isEqualTo(Errors.NONE.code());
      assertThat(consumer.highWatermark()).isEqualTo(1);

      Fetch.PartitionResult follower = anon.fetch(5, 0, "t", 0, 0, 0);
      assertThat(follower.errorCode()).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
      assertThat(follower.records().remaining()).isZero();

      Replication.OffsetForLeaderEpochRequest epochs =
          new Replication.OffsetForLeaderEpochRequest(
              5,
              List.of(
                  new Replication.EpochTopic(
                      "t", List.of(new Replication.EpochPartition(0, 0, 0)))));
      short err =
          Replication.OffsetForLeaderEpochResponse.read(
                  anon.call(ApiKeys.OFFSET_FOR_LEADER_EPOCH, epochs::write))
              .topics()
              .get(0)
              .partitions()
              .get(0)
              .errorCode();
      assertThat(err).isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
    }
  }

  @Test
  void aNodeRefusesToStartWithoutAStrongClusterToken() {
    Map<String, String> env = new HashMap<>();
    env.put("LOGFORGE_NODE_ID", "1");
    env.put("LOGFORGE_DATA_DIR", dir.toString());
    env.put("LOGFORGE_ADMIN_LISTEN", "off");
    assertThatThrownBy(() -> NodeConfig.fromEnv(env))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("LOGFORGE_CLUSTER_TOKEN");
    env.put("LOGFORGE_CLUSTER_TOKEN", "short-secret-value");
    assertThatThrownBy(() -> NodeConfig.fromEnv(env))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("LOGFORGE_CLUSTER_TOKEN")
        .hasMessageNotContaining("short-secret-value");
    env.put("LOGFORGE_CLUSTER_TOKEN", "c".repeat(32));
    assertThat(NodeConfig.fromEnv(env).clusterToken).isEqualTo("c".repeat(32));
  }
}
