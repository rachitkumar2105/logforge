package dev.logforge.network;

import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Authenticate;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.ProtocolReader;
import dev.logforge.network.protocol.ProtocolWriter;
import dev.logforge.network.protocol.RequestHeader;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * A blocking request/response connection: send one framed request, read its response. Used for
 * broker-to-controller and broker-to-broker calls and by the Java clients. Thread-safe (one request
 * at a time). After any IO error the socket is dropped and the next call reconnects.
 */
public final class BlockingChannel implements Closeable {
  private final String host;
  private final int port;
  private final String clientId;
  private final int timeoutMs;
  private final String clusterToken;
  private volatile Socket socket;
  private volatile boolean closed;
  private DataInputStream in;
  private DataOutputStream out;
  private int correlation;

  public BlockingChannel(String host, int port, String clientId, int timeoutMs) {
    this(host, port, clientId, timeoutMs, null);
  }

  /**
   * @param clusterToken if not null, the channel sends Authenticate with it right after each
   *     (re)connect, so cluster operations work on it (SECURITY-AUDIT F1)
   */
  public BlockingChannel(
      String host, int port, String clientId, int timeoutMs, String clusterToken) {
    this.clusterToken = clusterToken;
    this.host = host;
    this.port = port;
    this.clientId = clientId;
    this.timeoutMs = timeoutMs;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  private void ensureConnected() throws IOException {
    if (closed) {
      throw new IOException("channel closed");
    }
    if (socket != null && !socket.isClosed()) {
      return;
    }
    Socket s = new Socket();
    s.connect(new InetSocketAddress(host, port), Math.min(timeoutMs, 5_000));
    s.setSoTimeout(timeoutMs);
    s.setTcpNoDelay(true);
    socket = s;
    in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 64 * 1024));
    out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 64 * 1024));
    if (clusterToken != null) {
      short err =
          Authenticate.Response.read(
                  exchange(ApiKeys.AUTHENTICATE, new Authenticate.Request(clusterToken)::write))
              .errorCode();
      if (err != Errors.NONE.code()) {
        disconnect();
        throw new IOException("cluster authentication refused by " + host + ":" + port);
      }
    }
  }

  private int writeFrame(short apiKey, Consumer<ProtocolWriter> body) throws IOException {
    int corr = ++correlation;
    ProtocolWriter w = new ProtocolWriter(512);
    w.writeInt(0);
    new RequestHeader(apiKey, (short) 0, corr, clientId).write(w);
    body.accept(w);
    ByteBuffer frame = w.toBuffer();
    frame.putInt(0, frame.remaining() - 4);
    out.write(frame.array(), frame.arrayOffset() + frame.position(), frame.remaining());
    out.flush();
    return corr;
  }

  /** Sends a request and waits for its response; returns a reader positioned at the body. */
  public synchronized ProtocolReader request(short apiKey, Consumer<ProtocolWriter> body)
      throws IOException {
    try {
      ensureConnected();
      return exchange(apiKey, body);
    } catch (IOException e) {
      disconnect();
      throw e;
    }
  }

  /** Writes one request on the open socket and reads its response. */
  private ProtocolReader exchange(short apiKey, Consumer<ProtocolWriter> body) throws IOException {
    int corr = writeFrame(apiKey, body);
    int size = in.readInt();
    if (size < 4 || size > 256 * 1024 * 1024) {
      throw new IOException("bad response size " + size);
    }
    byte[] bytes = new byte[size];
    in.readFully(bytes);
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int got = buf.getInt();
    if (got != corr) {
      throw new IOException("correlation mismatch: sent " + corr + ", got " + got);
    }
    return new ProtocolReader(buf);
  }

  /** Like {@link #request} but wraps IO failures in {@link UncheckedIOException}. */
  public ProtocolReader requestUnchecked(short apiKey, Consumer<ProtocolWriter> body) {
    try {
      return request(apiKey, body);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Sends a request that gets no response (Produce with acks=0). */
  public synchronized void sendOneWay(short apiKey, Consumer<ProtocolWriter> body)
      throws IOException {
    try {
      ensureConnected();
      writeFrame(apiKey, body);
    } catch (IOException e) {
      disconnect();
      throw e;
    }
  }

  public synchronized void disconnect() {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException e) {
        // already broken
      }
      socket = null;
    }
  }

  /**
   * Closes the channel for good. Deliberately not synchronized: another thread may be blocked in
   * {@link #request} waiting for a peer that never answers (a paused broker), and waiting for its
   * monitor would block the caller until that read times out. Closing the socket instead wakes the
   * blocked reader with an IOException at once.
   */
  @Override
  public void close() {
    closed = true;
    Socket s = socket;
    if (s != null) {
      try {
        s.close();
      } catch (IOException e) {
        // already broken
      }
    }
  }
}
