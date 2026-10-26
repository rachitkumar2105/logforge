package dev.logforge.log;

/** An idempotent batch's sequence does not follow the last one appended for its producer. */
public final class OutOfOrderSequenceException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public OutOfOrderSequenceException(String message) {
    super(message);
  }

  public OutOfOrderSequenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
