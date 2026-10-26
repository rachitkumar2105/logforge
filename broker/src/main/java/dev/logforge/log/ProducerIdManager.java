package dev.logforge.log;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Hands out producer ids that are unique across the cluster and never reused after a restart.
 *
 * <p>An id is {@code (brokerId << 40) | counter}. The broker persists the end of the current block
 * of 1000 counters before handing out the first id in it, so a crash can skip ids but never repeat
 * one. A repeated id would let a new producer's sequence numbers be mistaken for an old one's.
 */
public final class ProducerIdManager {
  static final long BLOCK = 1000;

  private final Path file;
  private final long brokerPrefix;
  private long next;
  private long blockEnd;

  public ProducerIdManager(Path dataDir, int brokerId) throws IOException {
    this.file = dataDir.resolve("producer-ids");
    this.brokerPrefix = ((long) brokerId) << 40;
    long persisted = 0;
    if (Files.exists(file)) {
      persisted = Long.parseLong(Files.readString(file, UTF_8).trim());
    }
    this.next = persisted;
    this.blockEnd = persisted;
  }

  public synchronized long nextProducerId() throws IOException {
    if (next >= blockEnd) {
      blockEnd = next + BLOCK;
      Path tmp = file.resolveSibling("producer-ids.tmp");
      Files.writeString(tmp, Long.toString(blockEnd), UTF_8);
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
    return brokerPrefix | next++;
  }
}
