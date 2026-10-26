package dev.logforge.bench;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.log.FsyncPolicy;
import dev.logforge.log.IndexEntry;
import dev.logforge.log.Log;
import dev.logforge.log.LogConfig;
import dev.logforge.log.LogSegment;
import dev.logforge.log.LogSegmentInfo;
import dev.logforge.log.RecordBatch;
import dev.logforge.util.Json;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Writes a small real partition with the real {@link Log} code and dumps its physical layout: every
 * segment, its sparse index entries, and every batch's offsets and byte position. The website's
 * segment explorer runs its lookup (segment, then index, then scan) over this file.
 */
final class SegmentSample {
  private SegmentSample() {}

  static void write(Path out) throws Exception {
    Path dir = Files.createTempDirectory("logforge-sample").resolve("orders-0");
    LogConfig config =
        LogConfig.defaults()
            .withSegmentBytes(48 * 1024)
            .withIndexIntervalBytes(4096)
            .withFsync(new FsyncPolicy.Never());
    Random rnd = new Random(2026);
    List<Object> segments = new ArrayList<>();
    List<LogSegmentInfo> infos;
    try (Log log = Log.open(dir, config)) {
      long ts = 1_761_436_800_000L;
      for (int i = 0; i < 260; i++) {
        RecordBatch.Builder b = RecordBatch.builder(ts);
        int records = 1 + rnd.nextInt(8);
        for (int r = 0; r < records; r++) {
          byte[] value = new byte[60 + rnd.nextInt(500)];
          rnd.nextBytes(value);
          b.append(ts, ("order-" + rnd.nextInt(5000)).getBytes(UTF_8), value);
        }
        log.appendAsLeader(b.build(), 0);
        ts += 37;
      }
      log.flush();
      infos = log.segmentInfos();
    }
    // Reopen each segment read-only after the log is closed (one mapping per file at a time).
    {
      for (LogSegmentInfo info : infos) {
        LogSegment seg =
            info.active()
                ? LogSegment.open(dir, info.baseOffset(), config)
                : LogSegment.openSealed(dir, info.baseOffset(), config);
        try {
          List<Object> index = new ArrayList<>();
          for (int e = 0; e < seg.index().entryCount(); e++) {
            IndexEntry ie = seg.index().entry(e);
            index.add(Json.obj("offset", ie.offset(), "position", ie.position()));
          }
          List<Object> batches = new ArrayList<>();
          for (RecordBatch b : seg.batchesFrom(0)) {
            batches.add(
                Json.obj(
                    "baseOffset", b.baseOffset(),
                    "lastOffset", b.lastOffset(),
                    "records", b.recordCount(),
                    "sizeBytes", b.sizeInBytes()));
          }
          long pos = 0;
          for (Object o : batches) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) o;
            m.put("position", pos);
            pos += ((Number) m.get("sizeBytes")).longValue();
          }
          segments.add(
              Json.obj(
                  "baseOffset", info.baseOffset(),
                  "nextOffset", info.nextOffset(),
                  "sizeBytes", info.sizeBytes(),
                  "active", info.active(),
                  "fileName", LogSegment.fileName(info.baseOffset(), ".log"),
                  "index", index,
                  "batches", batches));
        } finally {
          seg.close();
        }
      }
    }
    Map<String, Object> doc =
        Json.obj(
            "topic",
            "orders",
            "partition",
            0,
            "generatedBy",
            "bench SegmentSample: real Log, LogSegment and OffsetIndex code",
            "segmentBytes",
            config.segmentBytes(),
            "indexIntervalBytes",
            config.indexIntervalBytes(),
            "segments",
            segments);
    Files.createDirectories(out.getParent());
    Files.writeString(out, Json.write(doc) + "\n", UTF_8);
    System.out.println("wrote " + out + " with " + segments.size() + " segments");
  }
}
