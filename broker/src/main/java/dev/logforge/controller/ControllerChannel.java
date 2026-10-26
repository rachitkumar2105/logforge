package dev.logforge.controller;

import dev.logforge.network.BlockingChannel;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Topics;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * How a broker talks to the controller: in-process when both roles share a JVM, over the binary
 * protocol otherwise. Calls are asynchronous so a slow controller never blocks a request thread.
 */
public interface ControllerChannel extends Closeable {
  CompletableFuture<ControllerApis.HeartbeatResponse> heartbeat(
      ControllerApis.HeartbeatRequest req);

  CompletableFuture<ControllerApis.AlterIsrResponse> alterIsr(ControllerApis.AlterIsrRequest req);

  CompletableFuture<Topics.CreateResponse> createTopics(Topics.CreateRequest req);

  /** Same JVM: direct calls on a separate thread (keeps callers off the controller's monitor). */
  static ControllerChannel local(Controller controller) {
    ExecutorService exec =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "logforge-controller-channel");
              t.setDaemon(true);
              return t;
            });
    return new ControllerChannel() {
      @Override
      public CompletableFuture<ControllerApis.HeartbeatResponse> heartbeat(
          ControllerApis.HeartbeatRequest req) {
        return CompletableFuture.supplyAsync(() -> controller.heartbeat(req), exec);
      }

      @Override
      public CompletableFuture<ControllerApis.AlterIsrResponse> alterIsr(
          ControllerApis.AlterIsrRequest req) {
        return CompletableFuture.supplyAsync(() -> controller.alterIsr(req), exec);
      }

      @Override
      public CompletableFuture<Topics.CreateResponse> createTopics(Topics.CreateRequest req) {
        return CompletableFuture.supplyAsync(() -> controller.createTopics(req), exec);
      }

      @Override
      public void close() {
        exec.shutdownNow();
      }
    };
  }

  /**
   * Over the network. Heartbeats and the other calls use separate connections and threads, and
   * every call times out within {@code timeoutMs}, so a caller that gives up never leaves a backlog
   * of stale requests queued behind a hung one.
   */
  static ControllerChannel remote(String host, int port, int brokerId, int timeoutMs) {
    BlockingChannel heartbeats =
        new BlockingChannel(host, port, "broker-" + brokerId + "-hb", timeoutMs);
    BlockingChannel control = new BlockingChannel(host, port, "broker-" + brokerId, timeoutMs);
    ExecutorService hbExec = Executors.newSingleThreadExecutor(daemon("logforge-controller-hb"));
    ExecutorService ctlExec =
        Executors.newSingleThreadExecutor(daemon("logforge-controller-client"));
    return new ControllerChannel() {
      @Override
      public CompletableFuture<ControllerApis.HeartbeatResponse> heartbeat(
          ControllerApis.HeartbeatRequest req) {
        return CompletableFuture.supplyAsync(
            () ->
                ControllerApis.HeartbeatResponse.read(
                    heartbeats.requestUnchecked(ApiKeys.BROKER_HEARTBEAT, req::write)),
            hbExec);
      }

      @Override
      public CompletableFuture<ControllerApis.AlterIsrResponse> alterIsr(
          ControllerApis.AlterIsrRequest req) {
        return CompletableFuture.supplyAsync(
            () ->
                ControllerApis.AlterIsrResponse.read(
                    control.requestUnchecked(ApiKeys.ALTER_ISR, req::write)),
            ctlExec);
      }

      @Override
      public CompletableFuture<Topics.CreateResponse> createTopics(Topics.CreateRequest req) {
        return CompletableFuture.supplyAsync(
            () ->
                Topics.CreateResponse.read(
                    control.requestUnchecked(ApiKeys.CREATE_TOPICS, req::write)),
            ctlExec);
      }

      @Override
      public void close() throws IOException {
        hbExec.shutdownNow();
        ctlExec.shutdownNow();
        heartbeats.close();
        control.close();
      }
    };
  }

  private static java.util.concurrent.ThreadFactory daemon(String name) {
    return r -> {
      Thread t = new Thread(r, name);
      t.setDaemon(true);
      return t;
    };
  }
}
