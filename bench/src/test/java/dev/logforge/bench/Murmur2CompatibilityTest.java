package dev.logforge.bench;

import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.util.Murmur2;
import java.util.Random;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

/**
 * PROTOCOL.md 5.16 promises LogForge's partitioner hashes exactly like Kafka's. Checked here, the
 * only module allowed to see kafka-clients, against Kafka's own implementation.
 */
class Murmur2CompatibilityTest {

  @Test
  void matchesKafkaMurmur2OnRandomKeys() {
    Random rnd = new Random(42);
    for (int i = 0; i < 20_000; i++) {
      byte[] key = new byte[rnd.nextInt(64)];
      rnd.nextBytes(key);
      assertThat(Murmur2.hash(key)).isEqualTo(Utils.murmur2(key));
      assertThat(Murmur2.partitionFor(key, 7)).isEqualTo(Utils.toPositive(Utils.murmur2(key)) % 7);
    }
  }
}
