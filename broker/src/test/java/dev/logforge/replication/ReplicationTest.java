package dev.logforge.replication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.log.Log;
import dev.logforge.log.Record;
import dev.logforge.log.RecordBatch;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.Produce;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestCluster;
import dev.logforge.testing.TestNodes;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Replication and ISR behaviour on an in-process cluster: one controller and three brokers. "Halt"
 * stops a broker without a clean shutdown; "freeze" suspends its heartbeats and replication (what
 * SIGSTOP does to a process). Real process kills are in the client module's LeaderKillGateTest and
 * in chaos/.
 */
class ReplicationTest {

  static final Duration WAIT = Duration.ofSeconds(20);

  @TempDir Path dir;

  private static List<String> values(Log log, long from, long to) {
    List<String> out = new ArrayList<>();
    long next = from;
    while (next < to) {
      ByteBuffer buf = log.read(next, 1 << 20, to);
      List<RecordBatch> batches = RecordBatch.iterate(buf);
      if (batches.isEmpty()) {
        break;
      }
      for (RecordBatch b : batches) {
        for (Record r : b.records()) {
          if (r.offset() >= next && r.offset() < to) {
            out.add(new String(r.value(), UTF_8));
          }
        }
        next = b.lastOffset() + 1;
      }
    }
    return out;
  }

  private static Produce.PartitionResult produce(
      TestClient c, String topic, short acks, String value) throws Exception {
    return c.produce(topic, 0, acks, TestClient.batch(value));
  }

  @Test
  void followersPullEverythingAndTheHighWatermarkAdvances() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 3)) {
      cluster.createTopic("rep", 1, 3, Map.of("min.insync.replicas", "2"));
      int leader = cluster.awaitLeader("rep", 0, WAIT);
      try (TestClient c = cluster.clientFor(leader)) {
        for (int i = 0; i < 50; i++) {
          Produce.PartitionResult r = produce(c, "rep", (short) -1, "v" + i);
          assertThat(r.errorCode()).as("acks=all #%d", i).isEqualTo(Errors.NONE.code());
          assertThat(r.baseOffset()).isEqualTo(i);
        }
      }
      for (int b = 1; b <= 3; b++) {
        int id = b;
        TestNodes.awaitTrue(() -> cluster.replica(id, "rep", 0).logEndOffset() == 50, WAIT);
        assertThat(values(cluster.replica(id, "rep", 0).log(), 0, 50))
            .hasSize(50)
            .first()
            .isEqualTo("v0");
      }
      assertThat(cluster.replica(leader, "rep", 0).highWatermark()).isEqualTo(50);
      assertThat(cluster.isrOf("rep", 0)).containsExactlyInAnyOrder(1, 2, 3);
    }
  }

  @Test
  void consumersOnlySeeRecordsBelowTheHighWatermark() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 3)) {
      cluster.createTopic("hw", 1, 3, Map.of());
      int leader = cluster.awaitLeader("hw", 0, WAIT);
      try (TestClient c = cluster.clientFor(leader)) {
        produce(c, "hw", (short) -1, "committed");
        for (int b = 1; b <= 3; b++) {
          if (b != leader) {
            cluster.broker(b).pauseReplication();
          }
        }
        produce(c, "hw", (short) 1, "not-yet-replicated");
        Partition p = cluster.replica(leader, "hw", 0);
        assertThat(p.logEndOffset()).isEqualTo(2);
        assertThat(p.highWatermark()).isEqualTo(1);
        Fetch.PartitionResult f = c.fetch("hw", 0, 0, 0);
        List<String> seen = new ArrayList<>();
        RecordBatch.iterate(f.records())
            .forEach(b -> b.records().forEach(r -> seen.add(new String(r.value(), UTF_8))));
        assertThat(seen).containsExactly("committed");
        assertThat(f.highWatermark()).isEqualTo(1);
        for (int b = 1; b <= 3; b++) {
          if (b != leader) {
            cluster.broker(b).resumeReplication();
          }
        }
        TestNodes.awaitTrue(() -> p.highWatermark() == 2, WAIT);
      }
    }
  }

  @Test
  void killingTheLeaderElectsAnIsrMemberAndLosesNoAcknowledgedRecord() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 3)) {
      cluster.createTopic("fail", 1, 3, Map.of("min.insync.replicas", "2"));
      int leader = cluster.awaitLeader("fail", 0, WAIT);
      List<String> acked = new ArrayList<>();
      try (TestClient c = cluster.clientFor(leader)) {
        for (int i = 0; i < 100; i++) {
          if (produce(c, "fail", (short) -1, "a" + i).errorCode() == Errors.NONE.code()) {
            acked.add("a" + i);
          }
        }
      }
      assertThat(acked).hasSize(100);
      int epochBefore = cluster.partitionInfo("fail", 0).leaderEpoch();
      List<Integer> isrBefore = cluster.isrOf("fail", 0);

      cluster.halt(leader);
      TestNodes.awaitTrue(
          () -> cluster.leaderOf("fail", 0) > 0 && cluster.leaderOf("fail", 0) != leader, WAIT);
      int newLeader = cluster.awaitLeader("fail", 0, WAIT);

      assertThat(isrBefore).contains(newLeader);
      assertThat(cluster.partitionInfo("fail", 0).leaderEpoch()).isGreaterThan(epochBefore);
      assertThat(cluster.isrOf("fail", 0)).doesNotContain(leader);
      assertThat(values(cluster.replica(newLeader, "fail", 0).log(), 0, 100)).isEqualTo(acked);
      try (TestClient c = cluster.clientFor(newLeader)) {
        assertThat(produce(c, "fail", (short) -1, "after").errorCode())
            .isEqualTo(Errors.NONE.code());
      }
    }
  }

  @Test
  void aPausedFollowerLeavesTheIsrWhileTheHighWatermarkKeepsMovingThenRejoins() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 3)) {
      cluster.createTopic("pause", 1, 3, Map.of("min.insync.replicas", "2"));
      int leader = cluster.awaitLeader("pause", 0, WAIT);
      int frozen = leader == 1 ? 2 : 1;
      cluster.freeze(frozen);
      try (TestClient c = cluster.clientFor(leader)) {
        // acks=all blocks until the ISR shrinks; then it completes with the remaining ISR.
        for (int i = 0; i < 20; i++) {
          Produce.PartitionResult r = produce(c, "pause", (short) -1, "p" + i);
          if (r.errorCode() != Errors.NONE.code()) {
            i--; // timed out while the ISR was still shrinking; retry the same record
          }
        }
      }
      assertThat(cluster.isrOf("pause", 0)).doesNotContain(frozen).hasSize(2);
      assertThat(cluster.replica(leader, "pause", 0).highWatermark()).isGreaterThanOrEqualTo(20);

      cluster.unfreeze(frozen);
      cluster.awaitIsr("pause", 0, Set.of(1, 2, 3), WAIT);
      TestNodes.awaitTrue(
          () ->
              cluster.replica(frozen, "pause", 0).logEndOffset()
                  == cluster.replica(leader, "pause", 0).logEndOffset(),
          WAIT);
    }
  }

  @Test
  void acksOneLosesRecordsWhenTheLeaderDiesBeforeReplicationAndTheOldLeaderTruncatesOnReturn()
      throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 3)) {
      cluster.createTopic("div", 1, 3, Map.of());
      int leader = cluster.awaitLeader("div", 0, WAIT);
      try (TestClient c = cluster.clientFor(leader)) {
        for (int i = 0; i < 10; i++) {
          assertThat(produce(c, "div", (short) -1, "safe" + i).errorCode())
              .isEqualTo(Errors.NONE.code());
        }
        for (int b = 1; b <= 3; b++) {
          if (b != leader) {
            cluster.broker(b).pauseReplication();
          }
        }
        // Acknowledged by the leader alone.
        for (int i = 0; i < 3; i++) {
          assertThat(produce(c, "div", (short) 1, "lost" + i).errorCode())
              .isEqualTo(Errors.NONE.code());
        }
      }
      assertThat(cluster.replica(leader, "div", 0).logEndOffset()).isEqualTo(13);
      cluster.halt(leader);
      for (int b = 1; b <= 3; b++) {
        if (b != leader) {
          cluster.broker(b).resumeReplication();
        }
      }
      TestNodes.awaitTrue(
          () -> cluster.leaderOf("div", 0) > 0 && cluster.leaderOf("div", 0) != leader, WAIT);
      int newLeader = cluster.awaitLeader("div", 0, WAIT);
      try (TestClient c = cluster.clientFor(newLeader)) {
        for (int i = 0; i < 5; i++) {
          assertThat(produce(c, "div", (short) -1, "new" + i).errorCode())
              .isEqualTo(Errors.NONE.code());
        }
      }
      // The three acks=1 records are gone: the new leader never had them. This is the documented
      // cost of acks=1, measured rather than assumed.
      List<String> survivor = values(cluster.replica(newLeader, "div", 0).log(), 0, 15);
      long lost =
          List.of("lost0", "lost1", "lost2").stream().filter(v -> !survivor.contains(v)).count();
      System.out.printf("acks=1 leader kill: %d of 3 acknowledged records lost%n", lost);
      assertThat(lost).isEqualTo(3);

      // The old leader comes back as a follower, finds where its epoch diverged, drops its tail.
      cluster.restart(leader);
      TestNodes.awaitTrue(
          () ->
              cluster.replica(leader, "div", 0) != null
                  && cluster.replica(leader, "div", 0).logEndOffset() == 15,
          WAIT);
      assertThat(values(cluster.replica(leader, "div", 0).log(), 0, 15)).isEqualTo(survivor);
      cluster.awaitIsr("div", 0, Set.of(1, 2, 3), WAIT);
    }
  }

  @Test
  void uncleanElectionIsOffByDefault() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 2)) {
      cluster.createTopic("clean", 1, 2, Map.of());
      int leader = cluster.awaitLeader("clean", 0, WAIT);
      int follower = leader == 1 ? 2 : 1;
      cluster.freeze(follower);
      cluster.awaitIsr("clean", 0, Set.of(leader), WAIT);
      cluster.unfreeze(follower);
      cluster.broker(follower).pauseReplication(); // alive, but stays out of the ISR
      cluster.halt(leader);
      Thread.sleep(3_000);
      assertThat(cluster.leaderOf("clean", 0))
          .as("no out-of-sync replica is elected")
          .isEqualTo(-1);
    }
  }

  @Test
  void uncleanElectionWhenEnabledPicksAnOutOfSyncReplica() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 2, true)) {
      cluster.createTopic("unclean", 1, 2, Map.of());
      int leader = cluster.awaitLeader("unclean", 0, WAIT);
      int follower = leader == 1 ? 2 : 1;
      cluster.freeze(follower);
      cluster.awaitIsr("unclean", 0, Set.of(leader), WAIT);
      try (TestClient c = cluster.clientFor(leader)) {
        produce(c, "unclean", (short) -1, "only-on-old-leader");
      }
      cluster.unfreeze(follower);
      cluster.broker(follower).pauseReplication();
      cluster.halt(leader);
      TestNodes.awaitTrue(() -> cluster.leaderOf("unclean", 0) == follower, WAIT);
      cluster.broker(follower).resumeReplication();
      assertThat(cluster.replica(follower, "unclean", 0).logEndOffset())
          .as("acked record lost by unclean election")
          .isZero();
    }
  }

  @Test
  void staleOrFutureLeaderEpochsAreFenced() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 3)) {
      cluster.createTopic("epoch", 1, 3, Map.of());
      int leader = cluster.awaitLeader("epoch", 0, WAIT);
      int epoch = cluster.partitionInfo("epoch", 0).leaderEpoch();
      int follower = leader == 1 ? 2 : 1;
      try (TestClient c = cluster.clientFor(leader)) {
        assertThat(c.fetch(follower, epoch + 1, "epoch", 0, 0, 0).errorCode())
            .isEqualTo(Errors.UNKNOWN_LEADER_EPOCH.code());
        assertThat(c.fetch(follower, epoch, "epoch", 0, 0, 0).errorCode())
            .isEqualTo(Errors.NONE.code());
      }
      // The follower must know the partition, or it answers UNKNOWN_TOPIC_OR_PARTITION instead.
      TestNodes.awaitTrue(() -> cluster.broker(follower).hasPartition("epoch", 0), WAIT);
      try (TestClient f = cluster.clientFor(follower)) {
        assertThat(f.fetch("epoch", 0, 0, 0).errorCode())
            .isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
        assertThat(produce(f, "epoch", (short) 1, "x").errorCode())
            .isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
      }
    }
  }
}
