package dev.logforge.controller;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.network.protocol.ControllerApis;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SECURITY-AUDIT F3: a broker's status JSON is kept by the controller per broker, so its size is
 * capped. An oversized status is dropped; the heartbeat itself still keeps the broker alive.
 */
class ControllerStatusLimitTest {
  @TempDir Path dir;

  private static ControllerApis.HeartbeatRequest heartbeat(String status) {
    return new ControllerApis.HeartbeatRequest(
        1, 7, "broker1", 9092, -1, ByteBuffer.wrap(status.getBytes(UTF_8)));
  }

  @Test
  void anOversizedStatusIsDroppedButTheHeartbeatCounts() throws Exception {
    AtomicLong now = new AtomicLong(1_000);
    Controller controller = new Controller(0, "c", dir, now::get, 3_000, 1, 1, false);

    controller.heartbeat(heartbeat("{\"ok\":1}"));
    assertThat(controller.brokerStatuses().get(1)).isEqualTo("{\"ok\":1}");

    now.addAndGet(2_000);
    String huge = "{\"pad\":\"" + "x".repeat(Controller.MAX_STATUS_BYTES) + "\"}";
    controller.heartbeat(heartbeat(huge));
    assertThat(controller.brokerStatuses().get(1))
        .as("previous status kept")
        .isEqualTo("{\"ok\":1}");
    assertThat(controller.heartbeatAges().get(1)).as("heartbeat still counted").isZero();

    now.addAndGet(2_000);
    controller.tick();
    assertThat(controller.snapshot().brokers())
        .anySatisfy(b -> assertThat(b.nodeId() == 1 && b.alive()).isTrue());
  }
}
