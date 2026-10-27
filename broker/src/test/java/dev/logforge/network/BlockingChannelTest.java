package dev.logforge.network;

import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Metadata;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Regression for the follower-pause anomaly (RUN-REPORT, Phase 8): when a leader is paused
 * (SIGSTOP), a follower's fetcher waits inside {@code request()} for a reply that never comes. The
 * metadata update that moves leadership elsewhere shuts that fetcher down from the heartbeat
 * thread; {@code close()} used to wait for the read to time out (up to the fetch wait plus 10 s),
 * so the healthy follower missed its own heartbeats and was declared dead too.
 */
class BlockingChannelTest {

  @Test
  void closeReturnsAtOnceAndWakesAThreadWaitingForAReply() throws Exception {
    List<Socket> held = new CopyOnWriteArrayList<>();
    try (ServerSocket blackHole = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      Thread acceptor =
          new Thread(
              () -> {
                try {
                  while (true) {
                    held.add(blackHole.accept()); // accept, never answer: a paused process
                  }
                } catch (IOException e) {
                  // server closed
                }
              });
      acceptor.setDaemon(true);
      acceptor.start();

      BlockingChannel ch =
          new BlockingChannel("127.0.0.1", blackHole.getLocalPort(), "test", 30_000);
      CompletableFuture<Throwable> waiter =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  ch.request(ApiKeys.METADATA, w -> new Metadata.Request(null).write(w));
                  return null;
                } catch (Throwable t) {
                  return t;
                }
              });
      long deadline = System.currentTimeMillis() + 5_000;
      while (held.isEmpty() && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      Thread.sleep(200); // the request is written and the waiter is blocked in the read

      long start = System.nanoTime();
      ch.close();
      long closeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

      assertThat(closeMs).as("close() must not wait for the blocked read").isLessThan(1_000);
      assertThat(waiter.get(2, TimeUnit.SECONDS))
          .as("the waiting thread is released with an error")
          .isInstanceOf(IOException.class);
    } finally {
      for (Socket s : held) {
        s.close();
      }
    }
  }
}
