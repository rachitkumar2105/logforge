package dev.logforge.network.protocol;

/** A request could not be parsed. The broker closes the connection that sent it. */
public final class InvalidRequestException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public InvalidRequestException(String message) {
    super(message);
  }
}
