package dev.logforge.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactorTest {

  @TempDir Path dir;
  private final AtomicLong clock = new AtomicLong(10_000_000L);

  private Log open(long deleteRetentionMs) throws Exception {
    LogConfig config =
        LogConfig.defaults()
            .withCompact(true)
            .withSegmentBytes(2048)
            .withDeleteRetentionMs(deleteRetentionMs)
            .withFsync(new FsyncPolicy.Never());
    return Log.open(dir.resolve("compacted-0"), config, clock::get);
  }

  private void put(Log log, String key, String value) {
    log.appendAsLeader(
        RecordBatch.builder(clock.get())
            .append(clock.get(), key.getBytes(UTF_8), value == null ? null : value.getBytes(UTF_8))
            .build(),
        0);
  }

  /** Reads the log front to back and folds it the way a consumer of a compacted topic would. */
  private static Map<String, String> materialize(Log log, Map<Long, String> offsets) {
    Map<String, String> state = new LinkedHashMap<>();
    long next = log.logStartOffset();
    while (next < log.logEndOffset()) {
      var batches = RecordBatch.iterate(log.read(next, 1 << 20, Long.MAX_VALUE));
      if (batches.isEmpty()) {
        break;
      }
      for (RecordBatch b : batches) {
        b.validate();
        for (Record r : b.records()) {
          String k = new String(r.key(), UTF_8);
          String v = r.value() == null ? null : new String(r.value(), UTF_8);
          offsets.put(r.offset(), k + "=" + v);
          if (v == null) {
            state.remove(k);
          } else {
            state.put(k, v);
          }
        }
        next = b.lastOffset() + 1;
      }
    }
    return state;
  }

  @Test
  void keepsOnlyTheLatestValuePerKeyAndDropsExpiredTombstones() throws Exception {
    Map<String, String> expected = new HashMap<>();
    try (Log log = open(0)) {
      for (int round = 0; round < 20; round++) {
        for (int k = 0; k < 10; k++) {
          String v = "k" + k + "-r" + round + "-" + "x".repeat(20);
          put(log, "k" + k, v);
          expected.put("k" + k, v);
        }
      }
      put(log, "k3", null); // delete k3
      expected.remove("k3");
      put(log, "k7", null);
      put(log, "k7", "reborn"); // delete then re-create
      expected.put("k7", "reborn");
      for (int i = 0; i < 30; i++) { // push the tombstones out of the active segment
        put(log, "filler" + (i % 3), "f" + i);
        expected.put("filler" + (i % 3), "f" + i);
      }
      long endBefore = log.logEndOffset();
      long sizeBefore = log.sizeInBytes();
      clock.addAndGet(1_000);

      Compactor.Result result = Compactor.compact(log, clock.get());

      assertThat(result.recordsRemoved()).isGreaterThan(150);
      assertThat(log.sizeInBytes()).isLessThan(sizeBefore / 2);
      assertThat(log.logEndOffset()).as("offsets never move").isEqualTo(endBefore);
      Map<Long, String> offsets = new HashMap<>();
      assertThat(materialize(log, offsets)).isEqualTo(expected);
      assertThat(offsets.values()).noneMatch(s -> s.startsWith("k3="));
    }
    // And after a restart the compacted segments load and recover cleanly.
    try (Log log = open(0)) {
      assertThat(materialize(log, new HashMap<>())).isEqualTo(expected);
    }
  }

  @Test
  void tombstonesSurviveUntilDeleteRetentionPasses() throws Exception {
    try (Log log = open(60_000)) {
      for (int i = 0; i < 40; i++) {
        put(log, "a", "v" + i + "-" + "y".repeat(40));
      }
      put(log, "a", null);
      for (int i = 0; i < 40; i++) {
        put(log, "b", "w" + i + "-" + "y".repeat(40));
      }
      clock.addAndGet(1_000);
      Compactor.compact(log, clock.get());
      Map<Long, String> offsets = new HashMap<>();
      materialize(log, offsets);
      assertThat(offsets.values())
          .as("tombstone kept within delete.retention.ms")
          .contains("a=null");

      clock.addAndGet(120_000);
      Compactor.compact(log, clock.get());
      Map<Long, String> after = new HashMap<>();
      Map<String, String> state = materialize(log, after);
      assertThat(after.values()).noneMatch(s -> s.startsWith("a="));
      assertThat(state).containsOnlyKeys("b");
    }
  }

  @Test
  void neverTouchesTheActiveSegmentAndLeavesNoTemporaryFiles() throws Exception {
    try (Log log = open(0)) {
      for (int i = 0; i < 5; i++) {
        put(log, "same", "v" + i);
      }
      assertThat(log.segmentInfos()).hasSize(1);
      Compactor.Result r = Compactor.compact(log, clock.get());
      assertThat(r.segmentsCleaned()).isZero();
      assertThat(log.logEndOffset()).isEqualTo(5);
      try (Stream<Path> files = Files.list(dir.resolve("compacted-0"))) {
        assertThat(files.map(p -> p.getFileName().toString()))
            .noneMatch(n -> n.endsWith(".cleaned"));
      }
    }
  }
}
