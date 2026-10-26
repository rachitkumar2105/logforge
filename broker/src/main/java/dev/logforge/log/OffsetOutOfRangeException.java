package dev.logforge.log;

/** A fetch or truncation offset is outside [logStartOffset, logEndOffset]. */
public final class OffsetOutOfRangeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public OffsetOutOfRangeException(String message) {
    super(message);
  }

  public OffsetOutOfRangeException(String message, Throwable cause) {
    super(message, cause);
  }
}
