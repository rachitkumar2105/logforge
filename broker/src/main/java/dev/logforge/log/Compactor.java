package dev.logforge.log;

import dev.logforge.util.JsonLog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Key-based log compaction for {@code cleanup.policy=compact}: keep only the newest record for each
 * key, so the partition becomes a changelog of current state rather than full history.
 *
 * <ol>
 *   <li>Scan the log (below {@code maxOffsetExclusive}, the high watermark) and record, per key,
 *       the offset of its newest record.
 *   <li>Rewrite each sealed segment, oldest first, keeping a record only if it is the newest for
 *       its key. Offsets never change: a rewritten batch keeps its base and last offset and its
 *       records keep their offset deltas, so consumers see gaps, never renumbering.
 *   <li>Swap the rewritten file in atomically (write {@code .cleaned}, rename over {@code .log}).
 * </ol>
 *
 * <h2>Why tombstones</h2>
 *
 * Deleting a key cannot just mean "drop its records": compaction would then resurface whatever
 * older value of that key survives elsewhere, and a consumer rebuilding state from the start would
 * never learn that the key was deleted. A tombstone (null value) is the newest record for the key,
 * so it shadows every older value; it is kept for {@code delete.retention.ms} so that consumers
 * that are behind can still see the delete, and only then removed.
 *
 * <p>The active segment is never rewritten, and nothing at or above the high watermark is used to
 * decide what to delete: an uncommitted newer value may yet be truncated by a failover, and must
 * not erase the committed older one.
 */
public final class Compactor {
  private static final JsonLog LOG = JsonLog.get(Compactor.class);

  private Compactor() {}

  /** Outcome of one pass. */
  public record Result(
      int segmentsCleaned, long recordsRemoved, long bytesBefore, long bytesAfter) {
    static final Result NOTHING = new Result(0, 0, 0, 0);
  }

  public static Result compact(Log log, long nowMs) throws IOException {
    return compact(log, nowMs, Long.MAX_VALUE);
  }

  public static Result compact(Log log, long nowMs, long maxOffsetExclusive) throws IOException {
    List<LogSegment> segments = log.segmentsSnapshot();
    if (segments.size() <= 1) {
      return Result.NOTHING;
    }
    Map<ByteBuffer, Long> latest = new HashMap<>();
    for (LogSegment seg : segments) {
      seg.forEachBatch(
          (batch, position) -> {
            for (Record r : batch.records()) {
              if (r.offset() >= maxOffsetExclusive) {
                return false;
              }
              if (r.key() != null) {
                latest.put(ByteBuffer.wrap(r.key()), r.offset());
              }
            }
            return true;
          });
    }
    long deleteRetentionMs = log.config().deleteRetentionMs();
    int cleaned = 0;
    long removed = 0;
    long before = 0;
    long after = 0;
    for (LogSegment seg : segments.subList(0, segments.size() - 1)) {
      if (seg.nextOffset() > maxOffsetExclusive) {
        break;
      }
      Path tmp = seg.logFile().resolveSibling(seg.logFile().getFileName() + ".cleaned");
      long[] removedHere = {0};
      try (FileChannel out =
          FileChannel.open(
              tmp,
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE)) {
        seg.forEachBatch(
            (batch, position) -> {
              List<Record> records = batch.records();
              List<Record> keep = new ArrayList<>(records.size());
              for (Record r : records) {
                Long newestOffset = r.key() == null ? null : latest.get(ByteBuffer.wrap(r.key()));
                boolean newest = newestOffset == null || newestOffset == r.offset();
                boolean expiredTombstone =
                    r.isTombstone() && nowMs - r.timestamp() > deleteRetentionMs;
                if (newest && !expiredTombstone) {
                  keep.add(r);
                }
              }
              removedHere[0] += records.size() - keep.size();
              if (keep.isEmpty()) {
                return true;
              }
              ByteBuffer bytes =
                  keep.size() == records.size() ? batch.buffer() : rebuild(batch, keep).buffer();
              while (bytes.hasRemaining()) {
                out.write(bytes);
              }
              return true;
            });
        out.force(true);
      }
      if (removedHere[0] == 0) {
        Files.deleteIfExists(tmp);
        continue;
      }
      long oldSize = seg.sizeInBytes();
      LogSegment replacement = log.swapSegment(seg, tmp);
      if (replacement == null) {
        Files.deleteIfExists(tmp); // the log changed under us (truncation); try next pass
        continue;
      }
      cleaned++;
      removed += removedHere[0];
      before += oldSize;
      after += replacement.sizeInBytes();
    }
    if (cleaned > 0) {
      log.rebuildDerivedStateAfterCompaction();
      LOG.info(
          "compacted",
          "dir",
          log.dir().getFileName().toString(),
          "segments",
          cleaned,
          "recordsRemoved",
          removed,
          "bytesBefore",
          before,
          "bytesAfter",
          after);
    }
    return new Result(cleaned, removed, before, after);
  }

  /** Same header (offsets, epoch, producer, timestamps), fewer records, offset deltas kept. */
  private static RecordBatch rebuild(RecordBatch original, List<Record> keep) {
    RecordBatch.Builder b =
        RecordBatch.builder(original.baseTimestamp())
            .baseOffset(original.baseOffset())
            .partitionLeaderEpoch(original.partitionLeaderEpoch())
            .producer(original.producerId(), original.producerEpoch(), original.baseSequence())
            .lastOffset(original.lastOffset());
    for (Record r : keep) {
      b.appendWithOffset(r.offset(), r.timestamp(), r.key(), r.value(), r.headers());
    }
    return b.build();
  }
}
