package dev.logforge.log;

/**
 * An idempotent batch carries an older producer epoch than the broker has seen (a zombie producer).
 */
public final class InvalidProducerEpochException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public InvalidProducerEpochException(String message) {
    super(message);
  }

  public InvalidProducerEpochException(String message, Throwable cause) {
    super(message, cause);
  }
}
