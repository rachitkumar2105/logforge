package dev.logforge.replication;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Regression for the follower-pause failures (RUN-REPORT, Phase 8): shutting a fetcher down used to
 * interrupt its thread. An interrupt that lands while that thread writes or flushes fetched records
 * closes the log's FileChannel (Java NIO closes an interruptible channel on interrupt), every later
 * flush fails, the broker reports a storage failure and is fenced, and a partition whose whole ISR
 * is fenced goes offline. A fetcher must stop through its own flag, never through an interrupt.
 */
class ReplicaFetcherShutdownTest {

  @Test
  void shutdownStopsTheFetcherWithoutInterruptingIt() throws Exception {
    ReplicaFetcher fetcher =
        new ReplicaFetcher(
            1, 2, null, id -> null, () -> false, 100, 1 << 20, "test-cluster-token-0123456789");
    fetcher.start();
    Thread.sleep(300); // idle: waiting for partitions to follow

    long start = System.nanoTime();
    fetcher.shutdown();
    fetcher.join(2_000);

    assertThat(fetcher.isAlive()).as("the fetcher stopped").isFalse();
    assertThat((System.nanoTime() - start) / 1_000_000).as("promptly").isLessThan(1_000);
    assertThat(fetcher.stoppedByInterrupt()).as("never by interrupting its thread").isFalse();
  }
}
