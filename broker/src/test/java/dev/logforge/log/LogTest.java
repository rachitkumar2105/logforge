package dev.logforge.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogTest {

  @TempDir Path dir;

  private final AtomicLong clock = new AtomicLong(1_000_000L);

  private Path partitionDir() {
    return dir.resolve("orders-0");
  }

  private Log open(LogConfig config) throws Exception {
    return Log.open(partitionDir(), config, clock::get);
  }

  private static RecordBatch single(long timestamp, String key, String value) {
    return RecordBatch.builder(timestamp)
        .append(
            timestamp,
            key == null ? null : key.getBytes(UTF_8),
            value == null ? null : value.getBytes(UTF_8))
        .build();
  }

  private static String valueFor(long offset) {
    return "v" + offset + "|" + Long.toHexString(offset * 0x9E3779B97F4A7C15L);
  }

  private static long segmentCount(Path p) throws Exception {
    try (Stream<Path> files = Files.list(p)) {
      return files.filter(f -> f.toString().endsWith(".log")).count();
    }
  }

  @Test
  void hundredThousandAppendsAcrossRollsReadBackExactly() throws Exception {
    LogConfig config =
        LogConfig.defaults().withSegmentBytes(256 * 1024).withFsync(new FsyncPolicy.Never());
    int n = 100_000;
    try (Log log = open(config)) {
      for (long i = 0; i < n; i++) {
        LogAppendInfo info = log.appendAsLeader(single(clock.get(), "k" + i, valueFor(i)), 0);
        assertThat(info.firstOffset()).isEqualTo(i);
      }
      assertThat(log.logEndOffset()).isEqualTo(n);
      assertThat(segmentCount(partitionDir())).isGreaterThan(10);
      assertReadsBackInOrder(log, n);
    }
    // And again after a clean reopen: sealed segments load from their index, the active one is
    // recovered by a full CRC scan.
    try (Log log = open(config)) {
      assertThat(log.logEndOffset()).isEqualTo(n);
      assertReadsBackInOrder(log, n);
      for (long target : new long[] {0, 1, 4_095, 50_000, 77_777, n - 1}) {
        RecordBatch b = RecordBatch.iterate(log.read(target, 1, Long.MAX_VALUE)).get(0);
        assertThat(b.baseOffset()).isEqualTo(target);
        assertThat(new String(b.records().get(0).value(), UTF_8)).isEqualTo(valueFor(target));
      }
    }
  }

  private static void assertReadsBackInOrder(Log log, int n) {
    long next = 0;
    while (next < n) {
      ByteBuffer chunk = log.read(next, 64 * 1024, Long.MAX_VALUE);
      List<RecordBatch> batches = RecordBatch.iterate(chunk);
      assertThat(batches).isNotEmpty();
      for (RecordBatch b : batches) {
        b.validate();
        for (Record r : b.records()) {
          assertThat(r.offset()).isEqualTo(next);
          assertThat(new String(r.key(), UTF_8)).isEqualTo("k" + next);
          assertThat(new String(r.value(), UTF_8)).isEqualTo(valueFor(next));
          next++;
        }
      }
    }
    assertThat(next).isEqualTo(n);
  }

  @Test
  void rollsOnSizeAndOnTimeAndEncodesBaseOffsetInFileNames() throws Exception {
    LogConfig config = LogConfig.defaults().withSegmentBytes(2048).withSegmentMs(60_000);
    try (Log log = open(config)) {
      for (int i = 0; i < 30; i++) {
        log.appendAsLeader(single(clock.get(), "k", "x".repeat(100)), 0);
      }
      long bySize = segmentCount(partitionDir());
      assertThat(bySize).isGreaterThan(1);
      for (LogSegmentInfo s : log.segmentInfos()) {
        assertThat(
                Files.exists(partitionDir().resolve(LogSegment.fileName(s.baseOffset(), ".log"))))
            .isTrue();
      }

      clock.addAndGet(61_000);
      long before = log.segmentInfos().size();
      log.appendAsLeader(single(clock.get(), "k", "late"), 0);
      assertThat(log.segmentInfos()).hasSize((int) before + 1);
      assertThat(log.segmentInfos().get(log.segmentInfos().size() - 1).baseOffset()).isEqualTo(30L);
    }
  }

  @Test
  void retentionByAgeDeletesOnlyEligibleSealedSegments() throws Exception {
    LogConfig config = LogConfig.defaults().withSegmentBytes(1024).withRetentionMs(10_000);
    try (Log log = open(config)) {
      long old = clock.get();
      for (int i = 0; i < 20; i++) {
        log.appendAsLeader(single(old, "k", "x".repeat(150)), 0);
      }
      clock.addAndGet(20_000);
      long fresh = clock.get();
      for (int i = 0; i < 20; i++) {
        log.appendAsLeader(single(fresh, "k", "y".repeat(150)), 0);
      }
      int segmentsBefore = log.segmentInfos().size();
      long oldEnd = 20;

      int deleted = log.deleteRetentionExpired(clock.get(), log.logEndOffset());

      assertThat(deleted).isGreaterThan(0);
      assertThat(log.segmentInfos()).hasSize(segmentsBefore - deleted);
      // Every remaining segment has a fresh record, or is the one that straddles old and fresh.
      assertThat(log.logStartOffset()).isLessThanOrEqualTo(oldEnd);
      assertThat(log.logStartOffset()).isGreaterThan(0L);
      assertThatThrownBy(() -> log.read(0, 1024, Long.MAX_VALUE))
          .isInstanceOf(OffsetOutOfRangeException.class);
      RecordBatch first =
          RecordBatch.iterate(log.read(log.logStartOffset(), 1, Long.MAX_VALUE)).get(0);
      assertThat(first.baseOffset()).isEqualTo(log.logStartOffset());
    }
  }

  @Test
  void retentionNeverDeletesTheActiveSegmentOrDataAboveTheHighWatermark() throws Exception {
    LogConfig config = LogConfig.defaults().withSegmentBytes(1024).withRetentionMs(1);
    try (Log log = open(config)) {
      for (int i = 0; i < 30; i++) {
        log.appendAsLeader(single(clock.get(), "k", "x".repeat(150)), 0);
      }
      clock.addAndGet(1_000_000);

      assertThat(log.deleteRetentionExpired(clock.get(), 0L)).isZero();

      int deleted = log.deleteRetentionExpired(clock.get(), log.logEndOffset());
      assertThat(deleted).isGreaterThan(0);
      assertThat(log.segmentInfos()).hasSize(1);
      assertThat(log.segmentInfos().get(0).active()).isTrue();
      assertThat(log.logEndOffset()).isEqualTo(30L);
    }
  }

  @Test
  void retentionBySizeKeepsTheNewestBytes() throws Exception {
    LogConfig config = LogConfig.defaults().withSegmentBytes(1024).withRetentionBytes(3000);
    try (Log log = open(config)) {
      for (int i = 0; i < 60; i++) {
        log.appendAsLeader(single(clock.get(), "k", "x".repeat(150)), 0);
      }
      long totalBefore = log.sizeInBytes();
      assertThat(totalBefore).isGreaterThan(3000);

      log.deleteRetentionExpired(clock.get(), log.logEndOffset());

      assertThat(log.sizeInBytes()).isLessThanOrEqualTo(3000 + 1024);
      assertThat(log.sizeInBytes()).isGreaterThanOrEqualTo(3000 - 1024);
      assertThat(log.logEndOffset()).isEqualTo(60L);
    }
  }

  @Test
  void compactedTopicsAreNotTouchedByTimeRetention() throws Exception {
    LogConfig config =
        LogConfig.defaults().withSegmentBytes(1024).withRetentionMs(1).withCompact(true);
    try (Log log = open(config)) {
      for (int i = 0; i < 30; i++) {
        log.appendAsLeader(single(clock.get(), "k" + i, "x".repeat(150)), 0);
      }
      clock.addAndGet(1_000_000);
      assertThat(log.deleteRetentionExpired(clock.get(), log.logEndOffset())).isZero();
    }
  }

  @Test
  void recoveryTruncatesATornTailInTheActiveSegment() throws Exception {
    LogConfig config = LogConfig.defaults().withSegmentBytes(4096);
    try (Log log = open(config)) {
      for (int i = 0; i < 40; i++) {
        log.appendAsLeader(single(clock.get(), "k", "x".repeat(150)), 0);
      }
    }
    Path active;
    try (Stream<Path> files = Files.list(partitionDir())) {
      active =
          files
              .filter(f -> f.toString().endsWith(".log"))
              .sorted()
              .reduce((a, b) -> b)
              .orElseThrow();
    }
    try (RandomAccessFile raf = new RandomAccessFile(active.toFile(), "rw")) {
      raf.setLength(raf.length() - 30);
    }
    try (Log log = open(config)) {
      assertThat(log.lastRecovery().truncatedBytes()).isGreaterThan(0);
      assertThat(log.logEndOffset()).isEqualTo(39L);
      RecordBatch b = RecordBatch.iterate(log.read(38, 1, Long.MAX_VALUE)).get(0);
      b.validate();
      LogAppendInfo info = log.appendAsLeader(single(clock.get(), "k", "after"), 0);
      assertThat(info.firstOffset()).isEqualTo(39L);
    }
  }

  @Test
  void rebuildsAShortIndexOnASealedSegment() throws Exception {
    LogConfig config = LogConfig.defaults().withSegmentBytes(64 * 1024).withIndexIntervalBytes(256);
    try (Log log = open(config)) {
      for (int i = 0; i < 1000; i++) {
        log.appendAsLeader(single(clock.get(), "k", "x".repeat(100)), 0);
      }
    }
    Path firstIndex = partitionDir().resolve(LogSegment.fileName(0, ".index"));
    long fullSize = Files.size(firstIndex);
    try (RandomAccessFile raf = new RandomAccessFile(firstIndex.toFile(), "rw")) {
      raf.setLength(16); // keep two entries
    }
    try (Log log = open(config)) {
      assertThat(Files.size(firstIndex)).isEqualTo(fullSize);
      RecordBatch b = RecordBatch.iterate(log.read(300, 1, Long.MAX_VALUE)).get(0);
      assertThat(b.baseOffset()).isEqualTo(300L);
    }
  }

  @Test
  void truncateToRemovesLaterSegmentsAndAppendsContinue() throws Exception {
    LogConfig config = LogConfig.defaults().withSegmentBytes(1024);
    try (Log log = open(config)) {
      for (int i = 0; i < 50; i++) {
        log.appendAsLeader(single(clock.get(), "k", "x".repeat(150)), 0);
      }
      int before = log.segmentInfos().size();
      log.truncateTo(12);
      assertThat(log.logEndOffset()).isEqualTo(12L);
      assertThat(log.segmentInfos().size()).isLessThan(before);
      assertThat(log.read(12, 1024, Long.MAX_VALUE).remaining()).isZero();
      for (int i = 0; i < 10; i++) {
        assertThat(log.appendAsLeader(single(clock.get(), "k", "z"), 1).firstOffset())
            .isEqualTo(12L + i);
      }
    }
    try (Log log = open(config)) {
      assertThat(log.logEndOffset()).isEqualTo(22L);
    }
  }

  @Test
  void truncateFullyAndStartAtMovesTheLogStart() throws Exception {
    try (Log log = open(LogConfig.defaults())) {
      for (int i = 0; i < 5; i++) {
        log.appendAsLeader(single(clock.get(), "k", "v"), 0);
      }
      log.truncateFullyAndStartAt(1000L);
      assertThat(log.logStartOffset()).isEqualTo(1000L);
      assertThat(log.logEndOffset()).isEqualTo(1000L);
      assertThat(log.appendAsLeader(single(clock.get(), "k", "v"), 0).firstOffset())
          .isEqualTo(1000L);
    }
  }

  @Test
  void readsAreCappedAtMaxOffsetAndRejectOutOfRange() throws Exception {
    try (Log log = open(LogConfig.defaults())) {
      for (int i = 0; i < 10; i++) {
        log.appendAsLeader(single(clock.get(), "k", "v" + i), 0);
      }
      List<RecordBatch> capped = RecordBatch.iterate(log.read(0, 1 << 20, 4));
      assertThat(capped).extracting(RecordBatch::baseOffset).containsExactly(0L, 1L, 2L, 3L);
      assertThat(log.read(10, 1024, Long.MAX_VALUE).remaining()).isZero();
      assertThatThrownBy(() -> log.read(11, 1024, Long.MAX_VALUE))
          .isInstanceOf(OffsetOutOfRangeException.class);
      assertThatThrownBy(() -> log.read(-1, 1024, Long.MAX_VALUE))
          .isInstanceOf(OffsetOutOfRangeException.class);
    }
  }

  @Test
  void rejectsOversizedAndCorruptBatchesWithoutAppending() throws Exception {
    try (Log log = open(LogConfig.defaults().withMaxBatchBytes(512))) {
      assertThatThrownBy(() -> log.appendAsLeader(single(clock.get(), "k", "x".repeat(600)), 0))
          .isInstanceOf(RecordTooLargeException.class);
      RecordBatch corrupt = single(clock.get(), "k", "value");
      ByteBuffer bytes = corrupt.buffer(); // shares content with the batch
      bytes.put(bytes.limit() - 2, (byte) (bytes.get(bytes.limit() - 2) ^ 0x01));
      assertThatThrownBy(() -> log.appendAsLeader(corrupt, 0))
          .isInstanceOf(CorruptRecordException.class);
      assertThat(log.logEndOffset()).isZero();
    }
  }

  @Test
  void leaderAppendStampsEpochsAndTheEpochCacheAnswersEndOffsets() throws Exception {
    try (Log log = open(LogConfig.defaults())) {
      for (int i = 0; i < 5; i++) {
        log.appendAsLeader(single(clock.get(), "k", "e0"), 0);
      }
      for (int i = 0; i < 3; i++) {
        log.appendAsLeader(single(clock.get(), "k", "e2"), 2);
      }
      RecordBatch b = RecordBatch.iterate(log.read(6, 1, Long.MAX_VALUE)).get(0);
      assertThat(b.partitionLeaderEpoch()).isEqualTo(2);

      LeaderEpochCache cache = log.epochCache();
      assertThat(cache.latestEpoch()).isEqualTo(2);
      assertThat(cache.endOffsetFor(0, log.logEndOffset())).isEqualTo(new EpochEndOffset(0, 5L));
      assertThat(cache.endOffsetFor(1, log.logEndOffset())).isEqualTo(new EpochEndOffset(0, 5L));
      assertThat(cache.endOffsetFor(2, log.logEndOffset())).isEqualTo(new EpochEndOffset(2, 8L));
      assertThat(cache.endOffsetFor(9, log.logEndOffset())).isEqualTo(new EpochEndOffset(2, 8L));
    }
    try (Log log = open(LogConfig.defaults())) {
      assertThat(log.epochCache().endOffsetFor(0, log.logEndOffset()))
          .isEqualTo(new EpochEndOffset(0, 5L));
      log.truncateTo(6);
      assertThat(log.epochCache().endOffsetFor(2, log.logEndOffset()))
          .isEqualTo(new EpochEndOffset(2, 6L));
    }
  }

  @Test
  void followerAppendKeepsLeaderOffsetsAndEpochs() throws Exception {
    try (Log leader = Log.open(dir.resolve("l-0"), LogConfig.defaults(), clock::get);
        Log follower = Log.open(dir.resolve("f-0"), LogConfig.defaults(), clock::get)) {
      for (int i = 0; i < 5; i++) {
        leader.appendAsLeader(single(clock.get(), "k", "v" + i), 3);
      }
      for (RecordBatch b : RecordBatch.iterate(leader.read(0, 1 << 20, Long.MAX_VALUE))) {
        follower.appendAsFollower(b);
      }
      assertThat(follower.logEndOffset()).isEqualTo(5L);
      assertThat(follower.epochCache().latestEpoch()).isEqualTo(3);
      RecordBatch again = RecordBatch.iterate(leader.read(2, 1, Long.MAX_VALUE)).get(0);
      assertThatThrownBy(() -> follower.appendAsFollower(again))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void idempotentProducerDuplicatesAreDroppedAndStateSurvivesRestart() throws Exception {
    try (Log log = open(LogConfig.defaults())) {
      RecordBatch first = idempotent(77L, (short) 0, 0, "a", "b");
      LogAppendInfo a = log.appendAsLeader(first, 0);
      assertThat(a.duplicate()).isFalse();
      assertThat(a.firstOffset()).isEqualTo(0L);

      LogAppendInfo retry = log.appendAsLeader(idempotent(77L, (short) 0, 0, "a", "b"), 0);
      assertThat(retry.duplicate()).isTrue();
      assertThat(retry.firstOffset()).isEqualTo(0L);
      assertThat(log.logEndOffset()).isEqualTo(2L);

      assertThatThrownBy(() -> log.appendAsLeader(idempotent(77L, (short) 0, 5, "x"), 0))
          .isInstanceOf(OutOfOrderSequenceException.class);
      assertThat(log.appendAsLeader(idempotent(77L, (short) 0, 2, "c"), 0).firstOffset())
          .isEqualTo(2L);
    }
    try (Log log = open(LogConfig.defaults())) {
      LogAppendInfo retryAfterRestart = log.appendAsLeader(idempotent(77L, (short) 0, 2, "c"), 0);
      assertThat(retryAfterRestart.duplicate()).isTrue();
      assertThat(retryAfterRestart.firstOffset()).isEqualTo(2L);
      assertThatThrownBy(() -> log.appendAsLeader(idempotent(77L, (short) 0, 9, "y"), 0))
          .isInstanceOf(OutOfOrderSequenceException.class);
      assertThat(log.appendAsLeader(idempotent(77L, (short) 1, 0, "new epoch"), 0).duplicate())
          .isFalse();
      assertThatThrownBy(() -> log.appendAsLeader(idempotent(77L, (short) 0, 3, "zombie"), 0))
          .isInstanceOf(InvalidProducerEpochException.class);
    }
  }

  private RecordBatch idempotent(long pid, short epoch, int seq, String... values) {
    RecordBatch.Builder b = RecordBatch.builder(clock.get()).producer(pid, epoch, seq);
    for (String v : values) {
      b.append(clock.get(), null, v.getBytes(UTF_8));
    }
    return b.build();
  }

  @Test
  void fsyncPolicyParsesAndDecides() {
    assertThat(FsyncPolicy.parse("never")).isEqualTo(new FsyncPolicy.Never());
    assertThat(FsyncPolicy.parse("records:5")).isEqualTo(new FsyncPolicy.EveryRecords(5));
    assertThat(FsyncPolicy.parse("MS:250")).isEqualTo(new FsyncPolicy.EveryMs(250));
    assertThat(FsyncPolicy.defaultPolicy()).isEqualTo(new FsyncPolicy.EveryMs(1000));
    assertThatThrownBy(() -> FsyncPolicy.parse("sometimes"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> FsyncPolicy.parse("records:0"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(new FsyncPolicy.Never().shouldFlush(1_000_000, 1_000_000)).isFalse();
    assertThat(new FsyncPolicy.EveryRecords(3).shouldFlush(2, 0)).isFalse();
    assertThat(new FsyncPolicy.EveryRecords(3).shouldFlush(3, 0)).isTrue();
    assertThat(new FsyncPolicy.EveryMs(100).shouldFlush(1, 99)).isFalse();
    assertThat(new FsyncPolicy.EveryMs(100).shouldFlush(1, 100)).isTrue();
    assertThat(new FsyncPolicy.EveryMs(100).shouldFlush(0, 500)).isFalse();
  }

  @Test
  void fsyncEveryRecordCountsUnflushedRecords() throws Exception {
    try (Log log = open(LogConfig.defaults().withFsync(new FsyncPolicy.EveryRecords(3)))) {
      log.appendAsLeader(single(clock.get(), "k", "1"), 0);
      log.appendAsLeader(single(clock.get(), "k", "2"), 0);
      assertThat(log.unflushedRecords()).isEqualTo(2);
      log.appendAsLeader(single(clock.get(), "k", "3"), 0);
      assertThat(log.unflushedRecords()).isZero();
      assertThat(log.flushCount()).isEqualTo(1);
    }
    try (Log log = open(LogConfig.defaults().withFsync(new FsyncPolicy.EveryMs(100)))) {
      log.appendAsLeader(single(clock.get(), "k", "1"), 0);
      log.maybeFlushOnTimer();
      long flushesBefore = log.flushCount();
      clock.addAndGet(150);
      log.maybeFlushOnTimer();
      assertThat(log.flushCount()).isEqualTo(flushesBefore + 1);
    }
  }

  @Test
  void topicConfigsMapOntoLogConfig() {
    LogConfig c =
        LogConfig.defaults()
            .withTopicConfigs(
                Map.of(
                    "cleanup.policy", "compact",
                    "segment.bytes", "4096",
                    "retention.ms", "-1",
                    "min.insync.replicas", "2",
                    "delete.retention.ms", "0"));
    assertThat(c.compact()).isTrue();
    assertThat(c.segmentBytes()).isEqualTo(4096);
    assertThat(c.retentionMs()).isEqualTo(-1L);
    assertThat(c.minInsyncReplicas()).isEqualTo(2);
    assertThat(c.deleteRetentionMs()).isZero();
    assertThat(LogConfig.defaults().withTopicConfigs(c.toTopicConfigs())).isEqualTo(c);
    assertThatThrownBy(() -> LogConfig.defaults().withTopicConfigs(Map.of("bogus", "1")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> LogConfig.defaults().withTopicConfigs(Map.of("cleanup.policy", "archive")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LogConfig.defaults().withTopicConfigs(Map.of("segment.bytes", "10")))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
