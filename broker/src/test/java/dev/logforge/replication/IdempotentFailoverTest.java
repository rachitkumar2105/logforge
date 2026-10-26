package dev.logforge.replication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.log.RecordBatch;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.Replication;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestCluster;
import dev.logforge.testing.TestNodes;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A retry that arrives at a different broker after a failover is still deduplicated, because
 * followers rebuild the same producer state from the batches they replicate.
 */
class IdempotentFailoverTest {

  @TempDir Path dir;

  private static ByteBuffer copy(RecordBatch b) {
    ByteBuffer src = b.buffer();
    ByteBuffer out = ByteBuffer.allocate(src.remaining());
    out.put(src).flip();
    return out;
  }

  @Test
  void aRetriedBatchOnTheNewLeaderIsNotAppendedTwice() throws Exception {
    try (TestCluster cluster = new TestCluster(dir, 3)) {
      cluster.createTopic("idem", 1, 3, Map.of("min.insync.replicas", "2"));
      int leader = cluster.awaitLeader("idem", 0, Duration.ofSeconds(20));
      long pid;
      RecordBatch original =
          RecordBatch.builder(System.currentTimeMillis())
              .producer(0, (short) 0, 0) // pid filled in below
              .append(System.currentTimeMillis(), null, "once".getBytes(UTF_8))
              .build();
      try (TestClient c = cluster.clientFor(leader)) {
        pid =
            Replication.InitProducerIdResponse.read(
                    c.call(
                        ApiKeys.INIT_PRODUCER_ID,
                        w -> new Replication.InitProducerIdRequest(0).write(w)))
                .producerId();
        original =
            RecordBatch.builder(System.currentTimeMillis())
                .producer(pid, (short) 0, 0)
                .append(System.currentTimeMillis(), null, "once".getBytes(UTF_8))
                .build();
        ByteBuffer firstAttempt = copy(original);
        Produce.PartitionResult r =
            c.produce("idem", 0, (short) -1, RecordBatch.wrap(firstAttempt));
        assertThat(r.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(r.baseOffset()).isZero();
      }
      // The producer never saw that ack (say the connection dropped). The leader dies.
      cluster.halt(leader);
      TestNodes.awaitTrue(
          () -> cluster.leaderOf("idem", 0) > 0 && cluster.leaderOf("idem", 0) != leader,
          Duration.ofSeconds(20));
      int newLeader = cluster.awaitLeader("idem", 0, Duration.ofSeconds(20));
      try (TestClient c = cluster.clientFor(newLeader)) {
        Produce.PartitionResult retry =
            c.produce("idem", 0, (short) -1, RecordBatch.wrap(copy(original)));
        assertThat(retry.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(retry.baseOffset()).as("the original offset is returned").isZero();
        assertThat(cluster.replica(newLeader, "idem", 0).logEndOffset())
            .as("nothing appended")
            .isEqualTo(1);

        RecordBatch next =
            RecordBatch.builder(System.currentTimeMillis())
                .producer(pid, (short) 0, 1)
                .append(System.currentTimeMillis(), null, "twice?".getBytes(UTF_8))
                .build();
        assertThat(c.produce("idem", 0, (short) -1, next).baseOffset()).isEqualTo(1);
      }
    }
  }
}
