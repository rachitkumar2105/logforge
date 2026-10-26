package dev.logforge.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogSegmentTest {

  @TempDir Path dir;

  private final LogConfig config = LogConfig.defaults().withIndexIntervalBytes(512);

  private static RecordBatch batch(long offset, String value) {
    RecordBatch b =
        RecordBatch.builder(offset)
            .append(offset, ("k" + offset).getBytes(UTF_8), value.getBytes(UTF_8))
            .build();
    b.setBaseOffset(offset);
    b.setPartitionLeaderEpoch(0);
    return b;
  }

  private static String payload(long i) {
    return "value-" + i + "-" + "x".repeat(100);
  }

  @Test
  void fileNamesEncodeTheBaseOffset() throws Exception {
    try (LogSegment seg = LogSegment.create(dir, 4211L, config)) {
      assertThat(seg.logFile().getFileName().toString()).isEqualTo("00000000000000004211.log");
      assertThat(seg.indexFile().getFileName().toString()).isEqualTo("00000000000000004211.index");
    }
  }

  @Test
  void indexLookupResolvesFirstMiddleAndLastOffsets() throws Exception {
    try (LogSegment seg = LogSegment.create(dir, 0L, config)) {
      for (long i = 0; i < 1000; i++) {
        seg.append(batch(i, payload(i)));
      }
      assertThat(seg.index().entryCount()).isGreaterThan(100);
      assertThat(seg.nextOffset()).isEqualTo(1000L);

      for (long target : new long[] {0L, 500L, 999L}) {
        IndexEntry floor = seg.index().lookup(target);
        assertThat(floor.offset()).isLessThanOrEqualTo(target);

        ByteBuffer read = seg.read(target, 1);
        RecordBatch found = RecordBatch.iterate(read).get(0);
        assertThat(found.baseOffset()).isEqualTo(target);
        assertThat(new String(found.records().get(0).value(), UTF_8)).isEqualTo(payload(target));
      }
    }
  }

  @Test
  void readReturnsWholeBatchesUpToMaxBytesButAlwaysAtLeastOne() throws Exception {
    try (LogSegment seg = LogSegment.create(dir, 0L, config)) {
      for (long i = 0; i < 10; i++) {
        seg.append(batch(i, payload(i)));
      }
      int oneBatch = batch(0, payload(0)).sizeInBytes();
      List<RecordBatch> three = RecordBatch.iterate(seg.read(4L, oneBatch * 3 + 10));
      assertThat(three).extracting(RecordBatch::baseOffset).containsExactly(4L, 5L, 6L);
      assertThat(RecordBatch.iterate(seg.read(4L, 1))).hasSize(1);
      assertThat(seg.read(10L, 4096).remaining()).isZero();
    }
  }

  @Test
  void recoveryDetectsAndTruncatesATornFinalRecord() throws Exception {
    long sizeAfterNine;
    try (LogSegment seg = LogSegment.create(dir, 0L, config)) {
      for (long i = 0; i < 9; i++) {
        seg.append(batch(i, payload(i)));
      }
      sizeAfterNine = seg.sizeInBytes();
      seg.append(batch(9, payload(9)));
    }
    Path log = dir.resolve("00000000000000000000.log");
    try (RandomAccessFile raf = new RandomAccessFile(log.toFile(), "rw")) {
      raf.setLength(raf.length() - 7); // tear the last batch
    }

    try (LogSegment seg = LogSegment.open(dir, 0L, config)) {
      LogSegment.RecoveryResult result = seg.recover();
      assertThat(result.truncatedBytes()).isGreaterThan(0);
      assertThat(result.validBatches()).isEqualTo(9);
      assertThat(seg.sizeInBytes()).isEqualTo(sizeAfterNine);
      assertThat(seg.nextOffset()).isEqualTo(9L);
      assertThat(Files.size(log)).isEqualTo(sizeAfterNine);
    }
  }

  @Test
  void recoveryTruncatesAtTheFirstCorruptBatchEvenWhenLaterBytesLookFine() throws Exception {
    long goodSize;
    try (LogSegment seg = LogSegment.create(dir, 0L, config)) {
      for (long i = 0; i < 5; i++) {
        seg.append(batch(i, payload(i)));
      }
      goodSize = seg.sizeInBytes();
      for (long i = 5; i < 10; i++) {
        seg.append(batch(i, payload(i)));
      }
    }
    Path log = dir.resolve("00000000000000000000.log");
    try (RandomAccessFile raf = new RandomAccessFile(log.toFile(), "rw")) {
      raf.seek(goodSize + 70); // inside batch 5's records
      int x = raf.read();
      raf.seek(goodSize + 70);
      raf.write(x ^ 0xFF);
    }

    try (LogSegment seg = LogSegment.open(dir, 0L, config)) {
      LogSegment.RecoveryResult result = seg.recover();
      assertThat(result.validBatches()).isEqualTo(5);
      assertThat(seg.nextOffset()).isEqualTo(5L);
      assertThat(seg.sizeInBytes()).isEqualTo(goodSize);
    }
  }

  @Test
  void recoveryRebuildsAMissingIndex() throws Exception {
    try (LogSegment seg = LogSegment.create(dir, 0L, config)) {
      for (long i = 0; i < 200; i++) {
        seg.append(batch(i, payload(i)));
      }
    }
    Files.delete(dir.resolve("00000000000000000000.index"));

    try (LogSegment seg = LogSegment.open(dir, 0L, config)) {
      seg.recover();
      assertThat(seg.index().entryCount()).isGreaterThan(10);
      RecordBatch found = RecordBatch.iterate(seg.read(150L, 1)).get(0);
      assertThat(found.baseOffset()).isEqualTo(150L);
    }
  }

  @Test
  void truncateToDropsBatchesAtOrAfterTheOffset() throws Exception {
    try (LogSegment seg = LogSegment.create(dir, 0L, config)) {
      for (long i = 0; i < 50; i++) {
        seg.append(batch(i, payload(i)));
      }
      seg.truncateTo(20L);
      assertThat(seg.nextOffset()).isEqualTo(20L);
      assertThat(seg.read(20L, 4096).remaining()).isZero();
      assertThat(RecordBatch.iterate(seg.read(19L, 1)).get(0).baseOffset()).isEqualTo(19L);
      seg.append(batch(20, "after truncation"));
      assertThat(RecordBatch.iterate(seg.read(20L, 1)).get(0).records().get(0).value())
          .isEqualTo("after truncation".getBytes(UTF_8));
    }
  }
}
