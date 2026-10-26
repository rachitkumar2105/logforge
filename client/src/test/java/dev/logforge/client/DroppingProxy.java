package dev.logforge.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP proxy in front of one broker that simulates a network timeout at the worst moment: the
 * first Produce request is delivered to the broker (so it is appended), and then the connection is
 * cut before the response can reach the client. The client cannot tell whether the write landed and
 * must retry.
 */
final class DroppingProxy implements AutoCloseable {
  private final ServerSocket server;
  private final int targetPort;
  private final AtomicBoolean dropNextProduce = new AtomicBoolean(true);
  private final AtomicInteger producesForwarded = new AtomicInteger();
  private final AtomicInteger connectionsCut = new AtomicInteger();
  private volatile boolean running = true;

  DroppingProxy(int targetPort) throws IOException {
    this.server = new ServerSocket(0);
    this.targetPort = targetPort;
    Thread acceptor = new Thread(this::acceptLoop, "proxy-accept");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  int port() {
    return server.getLocalPort();
  }

  int producesForwarded() {
    return producesForwarded.get();
  }

  int connectionsCut() {
    return connectionsCut.get();
  }

  private void acceptLoop() {
    while (running) {
      try {
        Socket client = server.accept();
        Socket broker = new Socket("127.0.0.1", targetPort);
        AtomicBoolean cut = new AtomicBoolean(false);
        start(() -> clientToBroker(client, broker, cut));
        start(() -> brokerToClient(broker, client, cut));
      } catch (IOException e) {
        return;
      }
    }
  }

  private static void start(Runnable r) {
    Thread t = new Thread(r, "proxy-pump");
    t.setDaemon(true);
    t.start();
  }

  /** Frame-aware pump so we can recognise Produce (api key 0). */
  private void clientToBroker(Socket client, Socket broker, AtomicBoolean cut) {
    try (DataInputStream in = new DataInputStream(client.getInputStream())) {
      OutputStream out = broker.getOutputStream();
      while (true) {
        int size = in.readInt();
        byte[] body = new byte[size];
        in.readFully(body);
        short apiKey = (short) (((body[0] & 0xff) << 8) | (body[1] & 0xff));
        out.write(
            new byte[] {
              (byte) (size >>> 24), (byte) (size >>> 16), (byte) (size >>> 8), (byte) size
            });
        out.write(body);
        out.flush();
        if (apiKey == 0) {
          producesForwarded.incrementAndGet();
          if (dropNextProduce.compareAndSet(true, false)) {
            cut.set(true);
            Thread.sleep(300); // let the broker append and answer, then lose the answer
            connectionsCut.incrementAndGet();
            client.close();
            broker.close();
            return;
          }
        }
      }
    } catch (IOException | InterruptedException e) {
      closeQuietly(client);
      closeQuietly(broker);
    }
  }

  private static void brokerToClient(Socket broker, Socket client, AtomicBoolean cut) {
    byte[] buf = new byte[64 * 1024];
    try {
      InputStream in = broker.getInputStream();
      OutputStream out = client.getOutputStream();
      int n;
      while ((n = in.read(buf)) >= 0) {
        if (cut.get()) {
          continue; // swallow the response to the dropped produce
        }
        out.write(buf, 0, n);
        out.flush();
      }
    } catch (IOException e) {
      // connection cut
    } finally {
      closeQuietly(client);
      closeQuietly(broker);
    }
  }

  private static void closeQuietly(Socket s) {
    try {
      s.close();
    } catch (IOException e) {
      // already closed
    }
  }

  @Override
  public void close() throws IOException {
    running = false;
    server.close();
  }
}
