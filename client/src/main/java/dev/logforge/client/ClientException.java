package dev.logforge.client;

import dev.logforge.network.protocol.Errors;

/** A request failed with a protocol error code or could not reach the cluster. */
public class ClientException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final transient Errors error;

  public ClientException(Errors error, String message) {
    super(message + " (" + error.name() + ")");
    this.error = error;
  }

  public ClientException(String message, Throwable cause) {
    super(message, cause);
    this.error = Errors.UNKNOWN_SERVER_ERROR;
  }

  public Errors error() {
    return error;
  }
}
