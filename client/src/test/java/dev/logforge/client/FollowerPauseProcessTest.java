package dev.logforge.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import dev.logforge.network.protocol.Metadata;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SIGSTOP a follower process: the ISR must shrink without it and acks=all writes must keep
 * completing (the high watermark advances on the remaining ISR); SIGCONT it and it must rejoin.
 * Needs POSIX signals, so it runs on Linux and macOS only (CI, and the Docker run in RUN-REPORT).
 */
@Tag("gate")
class FollowerPauseProcessTest {

  @TempDir Path dir;

  private static void signal(String sig, long pid) throws Exception {
    Process p = new ProcessBuilder("kill", "-" + sig, Long.toString(pid)).inheritIO().start();
    assertThat(p.waitFor()).isZero();
  }

  @Test
  void sigstoppedFollowerLeavesTheIsrAndRejoinsAfterSigcont() throws Exception {
    assumeFalse(
        System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"),
        "needs POSIX signals");
    try (ProcessCluster cluster = new ProcessCluster(dir, 3, Map.of())) {
      String bootstrap = cluster.bootstrap();
      Admin admin = LeaderKillGateTest.awaitAdmin(bootstrap);
      LeaderKillGateTest.awaitBrokers(admin, 3);
      admin.createTopic("stop", 1, 3, Map.of("min.insync.replicas", "2"));
      int leader = LeaderKillGateTest.leaderOf(admin, "stop");
      Metadata.PartitionMeta before = LeaderKillGateTest.partition(admin, "stop");
      int follower = before.replicas().stream().filter(r -> r != leader).findFirst().orElseThrow();

      try (Producer p =
          new Producer(
              ProducerConfig.builder(bootstrap).acks(Acks.ALL).requestTimeoutMs(10_000).build())) {
        p.send("stop", null, "before".getBytes(UTF_8)).get();
        long t0 = System.nanoTime();
        signal("STOP", cluster.pids().get(follower));
        for (int i = 0; i < 50; i++) {
          p.send("stop", null, ("during-" + i).getBytes(UTF_8)).get();
        }
        long ms = (System.nanoTime() - t0) / 1_000_000;
        Metadata.PartitionMeta during = LeaderKillGateTest.partition(admin, "stop");
        System.out.printf(
            "SIGSTOP follower %d: 50 acks=all writes completed in %d ms, ISR now %s%n",
            follower, ms, during.isr());
        assertThat(during.isr()).doesNotContain(follower).hasSize(2);

        signal("CONT", cluster.pids().get(follower));
        long t1 = System.nanoTime();
        LeaderKillGateTest.awaitIsrSize(admin, "stop", 3);
        System.out.printf(
            "SIGCONT follower %d: back in ISR after %d ms%n",
            follower, (System.nanoTime() - t1) / 1_000_000);
        assertThat(
                p.send("stop", null, "after".getBytes(UTF_8))
                    .get(30, java.util.concurrent.TimeUnit.SECONDS)
                    .offset())
            .isEqualTo(51);
      }
      admin.close();
      Thread.sleep(Duration.ofMillis(100).toMillis());
    }
  }
}
