package dev.logforge.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PartitionerTest {

  @Test
  void aKeyAlwaysMapsToTheSamePartition() {
    Partitioner a = new Partitioner();
    Partitioner b = new Partitioner();
    for (int i = 0; i < 10_000; i++) {
      byte[] key = ("customer-" + i).getBytes(UTF_8);
      int p = a.partition(key, 12);
      assertThat(p).isBetween(0, 11);
      assertThat(b.partition(key, 12)).as("stable across instances").isEqualTo(p);
      assertThat(a.partition(key, 12)).as("stable across calls").isEqualTo(p);
    }
  }

  @Test
  void keysSpreadAcrossPartitions() {
    Partitioner p = new Partitioner();
    Map<Integer, Integer> counts = new HashMap<>();
    for (int i = 0; i < 12_000; i++) {
      counts.merge(p.partition(("k" + i).getBytes(UTF_8), 12), 1, Integer::sum);
    }
    assertThat(counts).hasSize(12);
    assertThat(counts.values()).allSatisfy(c -> assertThat(c).isBetween(700, 1300));
  }

  @Test
  void nullKeysRoundRobin() {
    Partitioner p = new Partitioner();
    Map<Integer, Integer> counts = new HashMap<>();
    int first = p.partition(null, 4);
    counts.merge(first, 1, Integer::sum);
    for (int i = 1; i < 400; i++) {
      int next = p.partition(null, 4);
      assertThat(next).isEqualTo((first + i) % 4);
      counts.merge(next, 1, Integer::sum);
    }
    assertThat(counts.values()).containsOnly(100);
  }
}
