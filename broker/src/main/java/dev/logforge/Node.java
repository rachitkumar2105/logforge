package dev.logforge;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.controller.Controller;
import dev.logforge.controller.ControllerChannel;
import dev.logforge.log.LogManager;
import dev.logforge.log.ProducerIdManager;
import dev.logforge.network.NioServer;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.replication.DelayedOperations;
import dev.logforge.replication.IsrManager;
import dev.logforge.replication.Partition;
import dev.logforge.replication.ReplicaManager;
import dev.logforge.util.Json;
import dev.logforge.util.JsonLog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One LogForge process. Wires the roles it was configured with:
 *
 * <ul>
 *   <li><b>controller</b>: cluster metadata and leadership decisions;
 *   <li><b>broker</b>: logs, replication, client APIs, and a heartbeat loop that registers with the
 *       controller and pulls metadata.
 * </ul>
 */
public final class Node implements AutoCloseable {
  private static final JsonLog LOG = JsonLog.get(Node.class);

  private final NodeConfig config;
  private final long brokerEpoch = System.currentTimeMillis();
  private Controller controller;
  private ControllerChannel controllerChannel;
  private LogManager logManager;
  private ReplicaManager replicaManager;
  private ProducerIdManager producerIds;
  private NioServer server;
  private ExecutorService handlerPool;
  private ScheduledExecutorService scheduler;
  private ScheduledExecutorService heartbeatThread;
  private BrokerApis apis;
  private volatile ControllerApis.Snapshot metadata;
  private volatile long lastStatusSentMs;
  private volatile boolean closed;

  private Node(NodeConfig config) {
    this.config = config;
  }

  public static Node start(NodeConfig config) throws IOException {
    Node node = new Node(config);
    try {
      node.startup();
    } catch (IOException | RuntimeException e) {
      node.close();
      throw e;
    }
    return node;
  }

  private void startup() throws IOException {
    JsonLog.setNodeId(config.nodeId);
    Files.createDirectories(config.dataDir);
    scheduler = Executors.newScheduledThreadPool(2, daemonFactory("logforge-scheduler"));
    if (config.isController()) {
      controller =
          new Controller(
              config.nodeId,
              config.clusterId,
              config.dataDir.resolve("controller"),
              System::currentTimeMillis,
              config.brokerSessionTimeoutMs,
              config.offsetsTopicPartitions,
              config.offsetsTopicReplicationFactor);
    }
    if (config.isBroker()) {
      controllerChannel =
          controller != null
              ? ControllerChannel.local(controller)
              : ControllerChannel.remote(
                  config.controllerHost,
                  config.controllerPort,
                  config.nodeId,
                  (int) config.brokerSessionTimeoutMs);
      logManager = new LogManager(config.dataDir, System::currentTimeMillis);
      producerIds = new ProducerIdManager(config.dataDir, config.nodeId);
      replicaManager =
          new ReplicaManager(
              config.nodeId,
              logManager,
              config.defaultLogConfig,
              new DelayedOperations(),
              alterIsrSender(),
              System::currentTimeMillis);
    }
    apis = new BrokerApis(controller, replicaManager, () -> metadata, producerIds);
    handlerPool =
        Executors.newFixedThreadPool(config.handlerThreads, daemonFactory("logforge-handler"));
    server =
        new NioServer(
            config.listenHost,
            config.listenPort,
            config.ioThreads,
            config.maxRequestBytes,
            config.maxQueuedResponsesPerConnection,
            apis,
            handlerPool);
    server.start();
    if (controller != null) {
      controller.setSelfAddress(config.advertisedHost, advertisedPort());
      scheduler.scheduleWithFixedDelay(
          () -> runSafely("controller tick", controller::tick),
          config.heartbeatIntervalMs,
          config.heartbeatIntervalMs,
          TimeUnit.MILLISECONDS);
    }
    if (replicaManager != null) {
      // Own thread: a heartbeat can block for a session timeout when the controller is slow, and
      // must never delay the fsync, ISR or retention ticks.
      heartbeatThread =
          Executors.newSingleThreadScheduledExecutor(daemonFactory("logforge-heartbeat"));
      heartbeatThread.scheduleWithFixedDelay(
          () -> runSafely("heartbeat", this::heartbeat),
          0,
          config.heartbeatIntervalMs,
          TimeUnit.MILLISECONDS);
      scheduler.scheduleWithFixedDelay(
          () -> runSafely("flush", logManager::flushTick), 100, 100, TimeUnit.MILLISECONDS);
      long isrPeriod = Math.max(50, config.replicaLagTimeMaxMs / 4);
      scheduler.scheduleWithFixedDelay(
          () -> runSafely("isr", () -> replicaManager.isrShrinkTick(config.replicaLagTimeMaxMs)),
          isrPeriod,
          isrPeriod,
          TimeUnit.MILLISECONDS);
      scheduler.scheduleWithFixedDelay(
          () ->
              runSafely(
                  "retention", () -> logManager.retentionTick(replicaManager::retentionBound)),
          config.retentionCheckIntervalMs,
          config.retentionCheckIntervalMs,
          TimeUnit.MILLISECONDS);
    }
    LOG.info(
        "node started",
        "roles",
        config.roles.toString(),
        "port",
        server.port(),
        "dataDir",
        config.dataDir.toString(),
        "fsync",
        config.defaultLogConfig.fsync().toString());
  }

  private static java.util.concurrent.ThreadFactory daemonFactory(String prefix) {
    AtomicInteger n = new AtomicInteger();
    return r -> {
      Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
  }

  private static void runSafely(String what, Runnable r) {
    try {
      r.run();
    } catch (RuntimeException e) {
      LOG.error(what + " failed", e);
    }
  }

  private int advertisedPort() {
    return config.advertisedPort > 0 ? config.advertisedPort : server.port();
  }

  private IsrManager.AlterIsrSender alterIsrSender() {
    return (tp, leaderEpoch, partitionEpoch, newIsr, callback) -> {
      ControllerApis.AlterIsrRequest req =
          new ControllerApis.AlterIsrRequest(
              config.nodeId,
              brokerEpoch,
              List.of(
                  new ControllerApis.AlterTopic(
                      tp.topic(),
                      List.of(
                          new ControllerApis.AlterPartition(
                              tp.partition(), leaderEpoch, partitionEpoch, newIsr)))));
      controllerChannel
          .alterIsr(req)
          .whenComplete(
              (resp, err) -> {
                if (err != null) {
                  callback.accept(
                      new IsrManager.AlterResult(Errors.BROKER_NOT_AVAILABLE, -1, -1, List.of()));
                  return;
                }
                ControllerApis.AlterResult r = resp.topics().get(0).partitions().get(0);
                callback.accept(
                    new IsrManager.AlterResult(
                        Errors.forCode(r.errorCode()),
                        r.leaderEpoch(),
                        r.partitionEpoch(),
                        r.isr()));
              });
    };
  }

  /** Registers with the controller, reports status, and applies newer metadata. */
  private void heartbeat() {
    if (closed) {
      return;
    }
    long now = System.currentTimeMillis();
    ByteBuffer status = null;
    if (now - lastStatusSentMs >= 1_000) {
      status = ByteBuffer.wrap(Json.write(statusJson()).getBytes(UTF_8));
      lastStatusSentMs = now;
    }
    ControllerApis.Snapshot current = metadata;
    ControllerApis.HeartbeatRequest req =
        new ControllerApis.HeartbeatRequest(
            config.nodeId,
            brokerEpoch,
            config.advertisedHost,
            advertisedPort(),
            current == null ? -1 : current.version(),
            status);
    try {
      ControllerApis.HeartbeatResponse resp =
          controllerChannel
              .heartbeat(req)
              .get(config.brokerSessionTimeoutMs + 1_000, TimeUnit.MILLISECONDS);
      if (resp.snapshot() != null) {
        ControllerApis.Snapshot snap = ControllerApis.Snapshot.decode(resp.snapshot());
        replicaManager.applyMetadata(snap);
        metadata = snap;
      }
    } catch (Exception e) {
      if (!closed) {
        LOG.warn("heartbeat failed", "error", e.getClass().getSimpleName());
      }
    }
  }

  /** What this broker reports about itself; also served at /api/v1/local. */
  public Map<String, Object> statusJson() {
    return Json.obj(
        "nodeId", config.nodeId,
        "partitions", replicaManager == null ? List.of() : replicaManager.status(),
        "messagesIn", replicaManager == null ? 0 : replicaManager.messagesIn(),
        "bytesIn", replicaManager == null ? 0 : replicaManager.bytesIn(),
        "bytesOut", replicaManager == null ? 0 : replicaManager.bytesOut(),
        "connections", server == null ? 0 : server.openConnections());
  }

  // ------------------------------------------------------------------ accessors (tests, admin)

  public NodeConfig config() {
    return config;
  }

  public int port() {
    return server.port();
  }

  public Controller controller() {
    return controller;
  }

  public ReplicaManager replicaManager() {
    return replicaManager;
  }

  public NioServer server() {
    return server;
  }

  public ControllerApis.Snapshot metadata() {
    return metadata;
  }

  /** True once the controller lists this broker as alive in metadata the broker has applied. */
  public boolean brokerRegistered() {
    ControllerApis.Snapshot s = metadata;
    if (s == null) {
      return false;
    }
    for (ControllerApis.BrokerInfo b : s.brokers()) {
      if (b.nodeId() == config.nodeId && b.alive()) {
        return true;
      }
    }
    return false;
  }

  public boolean hasPartition(String topic, int partition) {
    return replicaManager != null
        && replicaManager.partition(new TopicPartition(topic, partition)) != null;
  }

  public boolean isLeaderFor(String topic, int partition) {
    if (replicaManager == null) {
      return false;
    }
    Partition p = replicaManager.partition(new TopicPartition(topic, partition));
    return p != null && p.isLeader() && p.log() != null;
  }

  public long logEndOffset(String topic, int partition) {
    Partition p =
        replicaManager == null
            ? null
            : replicaManager.partition(new TopicPartition(topic, partition));
    return p == null ? -1 : p.logEndOffset();
  }

  public int mutedConnections() {
    return server == null ? 0 : server.mutedConnections();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    if (heartbeatThread != null) {
      heartbeatThread.shutdownNow();
    }
    if (server != null) {
      server.close();
    }
    if (replicaManager != null) {
      replicaManager.close();
    }
    if (handlerPool != null) {
      handlerPool.shutdownNow();
    }
    if (logManager != null) {
      logManager.close();
    }
    if (controllerChannel != null) {
      controllerChannel.close();
    }
    LOG.info("node stopped");
  }
}
