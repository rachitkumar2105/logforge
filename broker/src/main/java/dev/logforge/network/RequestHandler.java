package dev.logforge.network;

import dev.logforge.network.protocol.ProtocolReader;
import dev.logforge.network.protocol.RequestHeader;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Turns one parsed request into a response body.
 *
 * <p>Called on a request-handler thread, never on an IO thread, so it may touch the disk. Requests
 * from one connection are handed over strictly in arrival order and each call's synchronous part
 * finishes before the next one starts, so a producer's batches are appended in the order it sent
 * them. The returned future may complete later (acks=all waiting for the ISR, long-poll fetch)
 * without blocking the next request on the connection: that is what makes pipelining useful.
 */
public interface RequestHandler {

  /** Identifies the request for logging and carries the connection's {@link Session}. */
  record Context(long connectionId, String clientAddress, RequestHeader header, Session session) {
    public Context(long connectionId, String clientAddress, RequestHeader header) {
      this(connectionId, clientAddress, header, new Session());
    }

    public String correlationTag() {
      return connectionId + "-" + header.correlationId();
    }
  }

  /**
   * @return the response body (everything after the correlation id), or a future completing with
   *     {@code null} when no response must be sent (Produce with acks=0)
   * @throws dev.logforge.network.protocol.InvalidRequestException if the body cannot be parsed; the
   *     server then closes the connection
   */
  CompletableFuture<ByteBuffer> handle(Context context, ProtocolReader body);
}
