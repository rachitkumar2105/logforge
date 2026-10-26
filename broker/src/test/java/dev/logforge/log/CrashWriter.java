package dev.logforge.log;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintStream;
import java.nio.file.Path;

/**
 * Child-process main for {@link LogCrashRecoveryTest}. Appends batches forever and prints {@code
 * ACK <lastOffset>} after each append returns. The parent kills this process with no warning.
 *
 * <p>Batches are deliberately large (64 records of ~1 KB, about 17 pages) so a kill can land inside
 * the kernel's copy of one write, which is how a real torn tail happens.
 */
public final class CrashWriter {
  static final int RECORDS_PER_BATCH = 64;

  private CrashWriter() {}

  static LogConfig config() {
    return LogConfig.defaults()
        .withSegmentBytes(4 * 1024 * 1024)
        .withFsync(new FsyncPolicy.Never());
  }

  static String valueFor(long offset) {
    StringBuilder sb = new StringBuilder(1024);
    sb.append("offset=").append(offset).append(';');
    long x = offset * 0x9E3779B97F4A7C15L;
    while (sb.length() < 1000) {
      sb.append(Long.toHexString(x));
      x = x * 6364136223846793005L + 1442695040888963407L;
    }
    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    Path dir = Path.of(args[0]);
    PrintStream out = new PrintStream(System.out, false, UTF_8);
    try (Log log = Log.open(dir, config(), System::currentTimeMillis)) {
      out.println("READY " + log.logEndOffset());
      out.flush();
      while (true) {
        long next = log.logEndOffset();
        RecordBatch.Builder b = RecordBatch.builder(System.currentTimeMillis());
        for (int i = 0; i < RECORDS_PER_BATCH; i++) {
          b.append(
              System.currentTimeMillis(),
              ("k" + (next + i)).getBytes(UTF_8),
              valueFor(next + i).getBytes(UTF_8));
        }
        LogAppendInfo info = log.appendAsLeader(b.build(), 0);
        out.println("ACK " + info.lastOffset());
        out.flush();
      }
    }
  }
}
