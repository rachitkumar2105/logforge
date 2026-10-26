package dev.logforge.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Durability under a hard process kill.
 *
 * <p>A child JVM appends to a real {@link Log} and prints an ACK after every append returns. The
 * test kills it with {@link Process#destroyForcibly()} at a random moment: {@code SIGKILL} on Linux
 * and macOS, {@code TerminateProcess} on Windows. Neither runs shutdown hooks or finalizers, so the
 * log is never closed cleanly. The test then reopens the log (running recovery) and checks:
 *
 * <ul>
 *   <li>every acknowledged offset is present with exactly the bytes that were written;
 *   <li>the recovered log contains only valid batches, contiguous from offset 0;
 *   <li>any torn tail was truncated (counted and printed, because whether a kill lands inside a
 *       write is timing-dependent).
 * </ul>
 *
 * <p>Ten kill cycles run against the same directory, so recovery also runs on logs that were
 * themselves recovered.
 */
class LogCrashRecoveryTest {

  static final int CYCLES = 10;

  @TempDir Path dir;

  @Test
  void everyAcknowledgedRecordSurvivesSigkillAcrossTenCycles() throws Exception {
    Path partition = dir.resolve("crash-0");
    long tornTails = 0;
    long truncatedBytesTotal = 0;
    long lastAckEver = -1;

    for (int cycle = 1; cycle <= CYCLES; cycle++) {
      AtomicLong lastAck = new AtomicLong(-1);
      AtomicLong ready = new AtomicLong(-1);
      Process child = startWriter(partition);
      Thread reader = readAcks(child, lastAck, ready);

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (ready.get() < 0 && System.nanoTime() < deadline && child.isAlive()) {
        Thread.sleep(5);
      }
      assertThat(ready.get()).as("child started (cycle %d)", cycle).isGreaterThanOrEqualTo(0);
      Thread.sleep(ThreadLocalRandom.current().nextInt(150, 900));

      child.destroyForcibly();
      assertThat(child.waitFor(30, TimeUnit.SECONDS)).isTrue();
      reader.join(10_000);

      long acked = lastAck.get();
      assertThat(acked)
          .as("child acknowledged something in cycle %d", cycle)
          .isGreaterThan(lastAckEver);
      lastAckEver = acked;

      try (Log log = Log.open(partition, CrashWriter.config(), System::currentTimeMillis)) {
        LogSegment.RecoveryResult rec = log.lastRecovery();
        if (rec.truncatedBytes() > 0) {
          tornTails++;
          truncatedBytesTotal += rec.truncatedBytes();
        }
        assertThat(log.logEndOffset()).as("log end after cycle %d", cycle).isGreaterThan(acked);
        verifyContiguousAndExact(log, acked);
      }
      System.out.printf(
          "cycle %d: lastAck=%d, recovered torn tail bytes so far=%d (torn tails=%d)%n",
          cycle, acked, truncatedBytesTotal, tornTails);
    }
    System.out.printf(
        "SIGKILL recovery: %d cycles, %d torn tails truncated (%d bytes), last ack %d%n",
        CYCLES, tornTails, truncatedBytesTotal, lastAckEver);
  }

  private static void verifyContiguousAndExact(Log log, long acked) {
    long next = 0;
    long end = log.logEndOffset();
    while (next < end) {
      ByteBuffer chunk = log.read(next, 1 << 20, Long.MAX_VALUE);
      List<RecordBatch> batches = RecordBatch.iterate(chunk);
      assertThat(batches).as("data at offset %d", next).isNotEmpty();
      for (RecordBatch b : batches) {
        b.validate();
        assertThat(b.baseOffset()).isEqualTo(next);
        for (Record r : b.records()) {
          assertThat(r.offset()).isEqualTo(next);
          assertThat(new String(r.value(), UTF_8)).isEqualTo(CrashWriter.valueFor(next));
          next++;
        }
      }
    }
    assertThat(next).isGreaterThan(acked);
  }

  private static Process startWriter(Path partition) throws Exception {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    ProcessBuilder pb =
        new ProcessBuilder(
            java,
            "-Xmx256m",
            "-XX:+UseSerialGC",
            "-cp",
            System.getProperty("java.class.path"),
            CrashWriter.class.getName(),
            partition.toString());
    pb.redirectError(ProcessBuilder.Redirect.INHERIT);
    return pb.start();
  }

  private static Thread readAcks(Process child, AtomicLong lastAck, AtomicLong ready) {
    Thread t =
        new Thread(
            () -> {
              try (BufferedReader r =
                  new BufferedReader(new InputStreamReader(child.getInputStream(), UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                  if (line.startsWith("ACK ")) {
                    lastAck.set(Long.parseLong(line.substring(4).trim()));
                  } else if (line.startsWith("READY ")) {
                    ready.set(Long.parseLong(line.substring(6).trim()));
                  }
                }
              } catch (Exception e) {
                // The pipe breaks when the child is killed; everything read so far stands.
              }
            },
            "ack-reader");
    t.setDaemon(true);
    t.start();
    return t;
  }
}
