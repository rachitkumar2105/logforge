package dev.logforge.log;

/** A disk write failed. Nothing was appended; the log is still valid. */
public final class LogStorageException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public LogStorageException(String message) {
    super(message);
  }

  public LogStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
