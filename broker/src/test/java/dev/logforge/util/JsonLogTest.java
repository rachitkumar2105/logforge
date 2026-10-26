package dev.logforge.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.log.Record;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JsonLogTest {

  private final ByteArrayOutputStream captured = new ByteArrayOutputStream();

  @AfterEach
  void restore() {
    JsonLog.resetOutput();
    JsonLog.setCorrelationId(null);
  }

  private List<Map<String, Object>> lines() {
    return captured.toString(UTF_8).lines().map(Json::parseObject).toList();
  }

  @Test
  void writesOneJsonObjectPerLineWithTheCorrelationId() {
    JsonLog.setOutput(new PrintStream(captured, true, UTF_8));
    JsonLog log = JsonLog.get(JsonLogTest.class);
    JsonLog.setCorrelationId("7-42");
    log.info("appended", "topic", "orders", "partition", 3, "offset", 1200L);
    JsonLog.setCorrelationId(null);
    log.warn("no correlation here");
    log.error("failed", new IllegalStateException("disk full"), "partition", 1);

    List<Map<String, Object>> out = lines();
    assertThat(out).hasSize(3);
    assertThat(out.get(0))
        .containsEntry("level", "INFO")
        .containsEntry("logger", "JsonLogTest")
        .containsEntry("msg", "appended")
        .containsEntry("corr", "7-42")
        .containsEntry("topic", "orders")
        .containsEntry("partition", 3L)
        .containsKey("ts");
    assertThat(out.get(1)).doesNotContainKey("corr");
    assertThat(out.get(2))
        .containsEntry("error", "IllegalStateException")
        .containsEntry("errorMessage", "disk full");
    assertThat(captured.toString(UTF_8)).doesNotContain("\tat "); // no stack traces
  }

  @Test
  void escapesHostileStringsSoEveryLineStaysValidJson() {
    JsonLog.setOutput(new PrintStream(captured, true, UTF_8));
    JsonLog.get(JsonLogTest.class).info("weird", "topic", "a\"b\n{\"injected\":true}<script>");
    Map<String, Object> line = lines().get(0);
    assertThat(line)
        .containsEntry("topic", "a\"b\n{\"injected\":true}<script>")
        .doesNotContainKey("injected");
    assertThat(captured.toString(UTF_8)).doesNotContain("<script>");
  }

  @Test
  void recordsNeverPrintTheirKeyOrValue() {
    Record r =
        new Record(5, 0, "secret-key".getBytes(UTF_8), "secret-value".getBytes(UTF_8), List.of());
    assertThat(r.toString()).doesNotContain("secret");
  }
}
