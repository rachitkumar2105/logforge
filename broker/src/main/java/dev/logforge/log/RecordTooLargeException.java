package dev.logforge.log;

/** A batch is larger than {@code max.batch.bytes}. */
public final class RecordTooLargeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public RecordTooLargeException(String message) {
    super(message);
  }

  public RecordTooLargeException(String message, Throwable cause) {
    super(message, cause);
  }
}
