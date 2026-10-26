package dev.logforge.log;

/** A record batch failed validation: bad length, magic, CRC, or record framing. */
public final class CorruptRecordException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public CorruptRecordException(String message) {
    super(message);
  }
}
