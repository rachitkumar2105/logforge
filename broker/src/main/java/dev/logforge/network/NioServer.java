package dev.logforge.network;

import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.InvalidRequestException;
import dev.logforge.network.protocol.ProtocolReader;
import dev.logforge.network.protocol.RequestHeader;
import dev.logforge.util.JsonLog;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * The binary protocol server.
 *
 * <ul>
 *   <li>One <b>acceptor</b> thread accepts sockets and hands each to an IO thread round-robin.
 *   <li>N <b>IO threads</b> ("processors"), each with its own {@link Selector}, own their
 *       connections for life: they read length-prefixed frames, parse the 10-byte header, and write
 *       responses. They never touch the disk.
 *   <li>Requests run on the shared handler pool through a per-connection {@link SerialExecutor}.
 * </ul>
 *
 * <p><b>Framing and limits.</b> The 4-byte size is checked against {@code maxRequestBytes} before
 * any buffer is allocated, so a hostile length prefix costs nothing. A bad size, a header that does
 * not parse, an unknown API or a body that does not parse closes that connection only.
 *
 * <p><b>Pipelining.</b> A client may send many requests without waiting. Each gets a slot in a
 * FIFO; responses are written strictly in slot order even when later requests finish first.
 *
 * <p><b>Backpressure.</b> If a client stops reading, its responses pile up in its FIFO. Once the
 * FIFO holds {@code maxQueuedResponses} entries the processor stops reading from that socket
 * (clears {@code OP_READ}), so the client's further requests wait in its own TCP buffers and the
 * broker's memory for it stays bounded. Other connections are on non-blocking sockets and are
 * unaffected. Reading resumes when the FIFO drains below half.
 */
public final class NioServer implements Closeable {
  private static final JsonLog LOG = JsonLog.get(NioServer.class);
  private static final int MIN_FRAME = RequestHeader.FIXED_SIZE + 2;

  private final String host;
  private final int requestedPort;
  private final int maxRequestBytes;
  private final int maxQueuedResponses;
  private final RequestHandler handler;
  private final ExecutorService handlerPool;
  private final Processor[] processors;
  private final AtomicLong connectionIds = new AtomicLong();
  private final AtomicInteger nextProcessor = new AtomicInteger();
  private final LongAdder bytesIn = new LongAdder();
  private final LongAdder bytesOut = new LongAdder();
  private final AtomicInteger openConnections = new AtomicInteger();
  private final AtomicInteger mutedConnections = new AtomicInteger();
  private final AtomicInteger everMuted = new AtomicInteger();

  private ServerSocketChannel serverChannel;
  private Thread acceptor;
  private volatile boolean running;

  public NioServer(
      String host,
      int port,
      int ioThreads,
      int maxRequestBytes,
      int maxQueuedResponses,
      RequestHandler handler,
      ExecutorService handlerPool) {
    this.host = host;
    this.requestedPort = port;
    this.maxRequestBytes = maxRequestBytes;
    this.maxQueuedResponses = maxQueuedResponses;
    this.handler = handler;
    this.handlerPool = handlerPool;
    this.processors = new Processor[ioThreads];
  }

  public void start() throws IOException {
    serverChannel = ServerSocketChannel.open();
    serverChannel.bind(new InetSocketAddress(host, requestedPort), 512);
    running = true;
    for (int i = 0; i < processors.length; i++) {
      processors[i] = new Processor(i);
      processors[i].start();
    }
    acceptor = new Thread(this::acceptLoop, "logforge-acceptor");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  public int port() {
    return serverChannel.socket().getLocalPort();
  }

  public long bytesIn() {
    return bytesIn.sum();
  }

  public long bytesOut() {
    return bytesOut.sum();
  }

  public int openConnections() {
    return openConnections.get();
  }

  /** Connections that have been muted by backpressure at least once. */
  public int everMutedConnections() {
    return everMuted.get();
  }

  public int mutedConnections() {
    return mutedConnections.get();
  }

  private void acceptLoop() {
    while (running) {
      try {
        SocketChannel ch = serverChannel.accept();
        ch.configureBlocking(false);
        ch.socket().setTcpNoDelay(true);
        int idx = Math.floorMod(nextProcessor.getAndIncrement(), processors.length);
        processors[idx].assign(ch);
      } catch (ClosedChannelException e) {
        return;
      } catch (IOException e) {
        if (running) {
          LOG.warn("accept failed", "error", e.getClass().getSimpleName());
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    running = false;
    if (serverChannel != null) {
      serverChannel.close();
    }
    for (Processor p : processors) {
      if (p != null) {
        p.shutdown();
      }
    }
    if (acceptor != null) {
      try {
        acceptor.join(2_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** One pending request on a connection, in arrival order. */
  private static final class Slot {
    final int correlationId;
    final CompletableFuture<ByteBuffer> future;

    Slot(int correlationId, CompletableFuture<ByteBuffer> future) {
      this.correlationId = correlationId;
      this.future = future;
    }
  }

  private final class Connection {
    final long id;
    final SocketChannel channel;
    final String address;
    final SerialExecutor serial;
    final ByteBuffer sizeBuf = ByteBuffer.allocate(4);
    final ArrayDeque<Slot> slots = new ArrayDeque<>();
    ByteBuffer payload;
    ByteBuffer writing;
    SelectionKey key;
    boolean muted;
    final Session session = new Session();

    Connection(long id, SocketChannel channel, String address) {
      this.id = id;
      this.channel = channel;
      this.address = address;
      this.serial = new SerialExecutor(handlerPool);
    }
  }

  private final class Processor extends Thread {
    private final Selector selector;
    private final Queue<SocketChannel> newConnections = new ConcurrentLinkedQueue<>();
    private final Queue<Connection> ready = new ConcurrentLinkedQueue<>();
    private final List<Connection> connections = new ArrayList<>();

    Processor(int index) throws IOException {
      super("logforge-io-" + index);
      setDaemon(true);
      this.selector = Selector.open();
    }

    void assign(SocketChannel ch) {
      newConnections.add(ch);
      selector.wakeup();
    }

    void notifyReady(Connection c) {
      ready.add(c);
      selector.wakeup();
    }

    void shutdown() {
      selector.wakeup();
      try {
        join(2_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void run() {
      try {
        while (running) {
          selector.select(300);
          registerNew();
          Connection c;
          while ((c = ready.poll()) != null) {
            if (c.channel.isOpen()) {
              flush(c);
            }
          }
          Iterator<SelectionKey> it = selector.selectedKeys().iterator();
          while (it.hasNext()) {
            SelectionKey k = it.next();
            it.remove();
            Connection conn = (Connection) k.attachment();
            try {
              if (k.isValid() && k.isReadable()) {
                read(conn);
              }
              if (k.isValid() && k.isWritable()) {
                flush(conn);
              }
            } catch (IOException | RuntimeException e) {
              close(conn, e.getClass().getSimpleName());
            }
          }
        }
      } catch (IOException e) {
        LOG.error("processor crashed", e);
      } finally {
        for (Connection c : List.copyOf(connections)) {
          close(c, "shutdown");
        }
        try {
          selector.close();
        } catch (IOException e) {
          // closing anyway
        }
      }
    }

    private void registerNew() {
      SocketChannel ch;
      while ((ch = newConnections.poll()) != null) {
        try {
          String addr = String.valueOf(ch.getRemoteAddress());
          Connection c = new Connection(connectionIds.incrementAndGet(), ch, addr);
          c.key = ch.register(selector, SelectionKey.OP_READ, c);
          connections.add(c);
          openConnections.incrementAndGet();
        } catch (IOException e) {
          try {
            ch.close();
          } catch (IOException ignored) {
            // already broken
          }
        }
      }
    }

    private void read(Connection c) throws IOException {
      while (!c.muted) {
        if (c.payload == null) {
          int n = c.channel.read(c.sizeBuf);
          if (n < 0) {
            close(c, "eof");
            return;
          }
          bytesIn.add(Math.max(n, 0));
          if (c.sizeBuf.hasRemaining()) {
            return;
          }
          int size = c.sizeBuf.getInt(0);
          c.sizeBuf.clear();
          if (size < MIN_FRAME || size > maxRequestBytes) {
            // Checked before allocating: a hostile length prefix never reaches the heap.
            close(c, "bad frame size " + size);
            return;
          }
          c.payload = ByteBuffer.allocate(size);
        }
        int n = c.channel.read(c.payload);
        if (n < 0) {
          close(c, "eof");
          return;
        }
        bytesIn.add(Math.max(n, 0));
        if (c.payload.hasRemaining()) {
          return;
        }
        ByteBuffer frame = c.payload;
        c.payload = null;
        frame.flip();
        if (!dispatch(c, frame)) {
          return;
        }
        if (c.slots.size() >= maxQueuedResponses) {
          mute(c);
        }
      }
    }

    /** Parses the header and queues the request. Returns false if the connection was closed. */
    private boolean dispatch(Connection c, ByteBuffer frame) {
      ProtocolReader reader = new ProtocolReader(frame);
      RequestHeader header;
      try {
        header = RequestHeader.read(reader);
      } catch (InvalidRequestException e) {
        close(c, "bad header");
        return false;
      }
      if (ApiKeys.name(header.apiKey()).startsWith("Unknown")) {
        close(c, "unknown api " + header.apiKey());
        return false;
      }
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      c.slots.add(new Slot(header.correlationId(), result));
      if (header.apiVersion() != 0) {
        ByteBuffer err = ByteBuffer.allocate(2).putShort(0, Errors.UNSUPPORTED_VERSION.code());
        result.complete(err);
        notifyReady(c);
        return true;
      }
      RequestHandler.Context ctx = new RequestHandler.Context(c.id, c.address, header, c.session);
      c.serial.execute(
          () -> {
            String prev = JsonLog.setCorrelationId(ctx.correlationTag());
            try {
              handler
                  .handle(ctx, reader)
                  .whenComplete(
                      (body, err) -> {
                        if (err != null) {
                          result.completeExceptionally(err);
                        } else {
                          result.complete(body);
                        }
                        notifyReady(c);
                      });
            } catch (RuntimeException e) {
              result.completeExceptionally(e);
              notifyReady(c);
            } finally {
              JsonLog.setCorrelationId(prev);
            }
          });
      return true;
    }

    /** Writes completed responses in request order. */
    private void flush(Connection c) {
      try {
        while (true) {
          if (c.writing == null) {
            Slot head = c.slots.peek();
            if (head == null || !head.future.isDone()) {
              break;
            }
            c.slots.poll();
            if (head.future.isCompletedExceptionally()) {
              Throwable cause = head.future.exceptionNow();
              if (!(cause instanceof InvalidRequestException)) {
                LOG.error("request failed", cause, "connection", c.id);
              }
              close(c, "request failed: " + cause.getClass().getSimpleName());
              return;
            }
            ByteBuffer body = head.future.resultNow();
            if (body == null) {
              continue; // acks=0: no response at all
            }
            ByteBuffer frame = ByteBuffer.allocate(8 + body.remaining());
            frame
                .putInt(4 + body.remaining())
                .putInt(head.correlationId)
                .put(body.duplicate())
                .flip();
            c.writing = frame;
          }
          int n = c.channel.write(c.writing);
          bytesOut.add(Math.max(n, 0));
          if (c.writing.hasRemaining()) {
            break;
          }
          c.writing = null;
        }
        boolean wantWrite = c.writing != null;
        if (c.key.isValid()) {
          int ops = c.key.interestOps();
          c.key.interestOps(wantWrite ? ops | SelectionKey.OP_WRITE : ops & ~SelectionKey.OP_WRITE);
        }
        if (c.muted && c.slots.size() <= maxQueuedResponses / 2) {
          unmute(c);
        }
      } catch (IOException e) {
        close(c, "write failed");
      }
    }

    private void mute(Connection c) {
      if (!c.muted && c.key.isValid()) {
        c.muted = true;
        mutedConnections.incrementAndGet();
        everMuted.incrementAndGet();
        c.key.interestOps(c.key.interestOps() & ~SelectionKey.OP_READ);
      }
    }

    private void unmute(Connection c) {
      if (c.muted && c.key.isValid()) {
        c.muted = false;
        mutedConnections.decrementAndGet();
        c.key.interestOps(c.key.interestOps() | SelectionKey.OP_READ);
        try {
          read(c); // bytes may already be buffered in the socket
        } catch (IOException | RuntimeException e) {
          close(c, "read failed");
        }
      }
    }

    private void close(Connection c, String reason) {
      if (!connections.remove(c)) {
        return;
      }
      openConnections.decrementAndGet();
      if (c.muted) {
        mutedConnections.decrementAndGet();
      }
      if (c.key != null) {
        c.key.cancel();
      }
      try {
        c.channel.close();
      } catch (IOException e) {
        // nothing more to do
      }
      LOG.debug("connection closed", "connection", c.id, "reason", reason);
    }
  }
}
