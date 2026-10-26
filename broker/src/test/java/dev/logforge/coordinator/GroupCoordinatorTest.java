package dev.logforge.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.Node;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Groups;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GroupCoordinatorTest {

  @TempDir Path dir;
  Node node;

  @BeforeEach
  void start() throws Exception {
    node = TestNodes.single(dir);
    try (TestClient c = new TestClient("127.0.0.1", node.port())) {
      c.createTopic("events", 4, 1, Map.of());
    }
    for (int p = 0; p < 4; p++) {
      int part = p;
      TestNodes.awaitTrue(() -> node.isLeaderFor("events", part), Duration.ofSeconds(10));
    }
  }

  @AfterEach
  void stop() throws Exception {
    node.close();
  }

  private TestClient client() throws Exception {
    return new TestClient("127.0.0.1", node.port());
  }

  static Groups.FindCoordinatorResponse findCoordinator(TestClient c, String group)
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    while (true) {
      Groups.FindCoordinatorResponse r =
          Groups.FindCoordinatorResponse.read(
              c.call(
                  ApiKeys.FIND_COORDINATOR,
                  w -> new Groups.FindCoordinatorRequest(group).write(w)));
      if (r.errorCode() == Errors.NONE.code() || System.nanoTime() > deadline) {
        return r;
      }
      Thread.sleep(50); // the offsets topic is created on first use
    }
  }

  static Groups.JoinResponse join(
      TestClient c, String group, String memberId, String assignor, int sessionMs)
      throws Exception {
    Groups.JoinRequest req =
        new Groups.JoinRequest(group, sessionMs, 5_000, memberId, assignor, List.of("events"));
    return Groups.JoinResponse.read(c.call(ApiKeys.JOIN_GROUP, req::write));
  }

  static short heartbeat(TestClient c, String group, int generation, String memberId)
      throws Exception {
    return Groups.ErrorResponse.read(
            c.call(
                ApiKeys.HEARTBEAT,
                w -> new Groups.HeartbeatRequest(group, generation, memberId).write(w)))
        .errorCode();
  }

  static Set<Integer> partitions(Groups.JoinResponse r) {
    Set<Integer> out = new HashSet<>();
    r.assignment().forEach(a -> out.addAll(a.partitions()));
    return out;
  }

  @Test
  void findCoordinatorCreatesTheOffsetsTopicAndPointsAtItsLeader() throws Exception {
    try (TestClient c = client()) {
      Groups.FindCoordinatorResponse r = findCoordinator(c, "g1");
      assertThat(r.errorCode()).isEqualTo(Errors.NONE.code());
      assertThat(r.nodeId()).isZero();
      assertThat(r.port()).isEqualTo(node.port());
    }
  }

  @Test
  void aSecondMemberTriggersARebalanceAndBothGetDisjointPartitions() throws Exception {
    try (TestClient a = client();
        TestClient b = client()) {
      findCoordinator(a, "g");
      Groups.JoinResponse first = join(a, "g", "", "range", 10_000);
      assertThat(first.errorCode()).isEqualTo(Errors.NONE.code());
      assertThat(first.memberId()).isNotEmpty();
      assertThat(partitions(first)).containsExactlyInAnyOrder(0, 1, 2, 3);
      assertThat(heartbeat(a, "g", first.generationId(), first.memberId()))
          .isEqualTo(Errors.NONE.code());

      CompletableFuture<Groups.JoinResponse> second =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return join(b, "g", "", "range", 10_000);
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      // The newcomer is held at the barrier; the existing member learns from its heartbeat.
      TestNodes.awaitTrue(
          () -> {
            try {
              return heartbeat(a, "g", first.generationId(), first.memberId())
                  == Errors.REBALANCE_IN_PROGRESS.code();
            } catch (Exception e) {
              return false;
            }
          },
          Duration.ofSeconds(10));
      assertThat(second).isNotDone();
      Groups.JoinResponse rejoin = join(a, "g", first.memberId(), "range", 10_000);
      Groups.JoinResponse b2 = second.get();

      assertThat(rejoin.generationId())
          .isEqualTo(first.generationId() + 1)
          .isEqualTo(b2.generationId());
      assertThat(rejoin.members()).hasSize(2);
      Set<Integer> pa = partitions(rejoin);
      Set<Integer> pb = partitions(b2);
      assertThat(pa).hasSize(2).doesNotContainAnyElementsOf(pb);
      Set<Integer> union = new HashSet<>(pa);
      union.addAll(pb);
      assertThat(union).containsExactlyInAnyOrder(0, 1, 2, 3);
      assertThat(heartbeat(a, "g", first.generationId(), first.memberId()))
          .isEqualTo(Errors.ILLEGAL_GENERATION.code());
    }
  }

  @Test
  void aMemberThatStopsHeartbeatingIsRemovedAfterItsSessionTimeout() throws Exception {
    try (TestClient a = client();
        TestClient b = client()) {
      findCoordinator(a, "s");
      Groups.JoinResponse ja = join(a, "s", "", "roundrobin", 1_000);
      CompletableFuture<Groups.JoinResponse> jb =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return join(b, "s", "", "roundrobin", 1_000);
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      TestNodes.awaitTrue(
          () -> {
            try {
              return heartbeat(a, "s", ja.generationId(), ja.memberId())
                  == Errors.REBALANCE_IN_PROGRESS.code();
            } catch (Exception e) {
              return false;
            }
          },
          Duration.ofSeconds(10));
      Groups.JoinResponse ja2 = join(a, "s", ja.memberId(), "roundrobin", 1_000);
      jb.get();
      assertThat(partitions(ja2)).hasSize(2);

      // b goes silent. a keeps heartbeating and is told to rejoin once b's session expires.
      long t0 = System.nanoTime();
      TestNodes.awaitTrue(
          () -> {
            try {
              return heartbeat(a, "s", ja2.generationId(), ja2.memberId())
                  == Errors.REBALANCE_IN_PROGRESS.code();
            } catch (Exception e) {
              return false;
            }
          },
          Duration.ofSeconds(10));
      long ms = (System.nanoTime() - t0) / 1_000_000;
      assertThat(ms).isGreaterThanOrEqualTo(500L);
      Groups.JoinResponse alone = join(a, "s", ja2.memberId(), "roundrobin", 1_000);
      assertThat(alone.members()).containsExactly(ja2.memberId());
      assertThat(partitions(alone)).containsExactlyInAnyOrder(0, 1, 2, 3);
    }
  }

  @Test
  void leavingTriggersARebalanceAndUnknownMembersAreRejected() throws Exception {
    try (TestClient a = client()) {
      findCoordinator(a, "l");
      Groups.JoinResponse j = join(a, "l", "", "range", 10_000);
      short leave =
          Groups.ErrorResponse.read(
                  a.call(
                      ApiKeys.LEAVE_GROUP,
                      w -> new Groups.LeaveRequest("l", j.memberId()).write(w)))
              .errorCode();
      assertThat(leave).isEqualTo(Errors.NONE.code());
      assertThat(heartbeat(a, "l", j.generationId(), j.memberId()))
          .isEqualTo(Errors.UNKNOWN_MEMBER_ID.code());
      Groups.JoinResponse mismatch = join(a, "l", "", "range", 10_000);
      try (TestClient b = client()) {
        Groups.JoinRequest other =
            new Groups.JoinRequest("l", 10_000, 1_000, "", "roundrobin", List.of("events"));
        Groups.JoinResponse r = Groups.JoinResponse.read(b.call(ApiKeys.JOIN_GROUP, other::write));
        assertThat(r.errorCode()).isEqualTo(Errors.INCONSISTENT_ASSIGNOR.code());
      }
      assertThat(mismatch.errorCode()).isEqualTo(Errors.NONE.code());
    }
  }

  @Test
  void committedOffsetsAreFencedByGenerationAndSurviveARestart() throws Exception {
    Groups.JoinResponse j;
    try (TestClient a = client()) {
      findCoordinator(a, "o");
      j = join(a, "o", "", "range", 30_000);
      List<Groups.CommitPartition> parts = new ArrayList<>();
      for (int p = 0; p < 4; p++) {
        parts.add(new Groups.CommitPartition(p, 100L + p, null));
      }
      Groups.OffsetCommitRequest ok =
          new Groups.OffsetCommitRequest(
              "o",
              j.generationId(),
              j.memberId(),
              List.of(new Groups.CommitTopic("events", parts)));
      Groups.OffsetCommitResponse resp =
          Groups.OffsetCommitResponse.read(a.call(ApiKeys.OFFSET_COMMIT, ok::write));
      assertThat(resp.topics().get(0).partitions())
          .allSatisfy(p -> assertThat(p.errorCode()).isEqualTo(Errors.NONE.code()));

      Groups.OffsetCommitRequest stale =
          new Groups.OffsetCommitRequest(
              "o",
              j.generationId() - 1,
              j.memberId(),
              List.of(new Groups.CommitTopic("events", parts)));
      Groups.OffsetCommitResponse staleResp =
          Groups.OffsetCommitResponse.read(a.call(ApiKeys.OFFSET_COMMIT, stale::write));
      assertThat(staleResp.topics().get(0).partitions().get(0).errorCode())
          .isEqualTo(Errors.ILLEGAL_GENERATION.code());

      Groups.OffsetCommitRequest stranger =
          new Groups.OffsetCommitRequest(
              "o", j.generationId(), "nobody", List.of(new Groups.CommitTopic("events", parts)));
      assertThat(
              Groups.OffsetCommitResponse.read(a.call(ApiKeys.OFFSET_COMMIT, stranger::write))
                  .topics()
                  .get(0)
                  .partitions()
                  .get(0)
                  .errorCode())
          .isEqualTo(Errors.UNKNOWN_MEMBER_ID.code());
    }
    node.close();
    node = TestNodes.single(dir);
    try (TestClient a = client()) {
      findCoordinator(a, "o");
      Groups.OffsetFetchResponse f =
          Groups.OffsetFetchResponse.read(
              a.call(
                  ApiKeys.OFFSET_FETCH,
                  w ->
                      new Groups.OffsetFetchRequest(
                              "o", List.of(new Groups.FetchTopic("events", List.of(0, 1, 2, 3))))
                          .write(w)));
      assertThat(f.errorCode()).isEqualTo(Errors.NONE.code());
      assertThat(f.topics().get(0).partitions())
          .extracting(Groups.FetchedOffset::offset)
          .containsExactly(100L, 101L, 102L, 103L);
      Groups.OffsetFetchResponse none =
          Groups.OffsetFetchResponse.read(
              a.call(
                  ApiKeys.OFFSET_FETCH,
                  w ->
                      new Groups.OffsetFetchRequest(
                              "never-committed",
                              List.of(new Groups.FetchTopic("events", List.of(0))))
                          .write(w)));
      assertThat(none.topics().get(0).partitions().get(0).offset()).isEqualTo(-1L);
    }
  }
}
