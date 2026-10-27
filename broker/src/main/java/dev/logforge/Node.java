package dev.logforge;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.admin.AdminHttpServer;
import dev.logforge.admin.Metrics;
import dev.logforge.controller.Controller;
import dev.logforge.controller.ControllerChannel;
import dev.logforge.coordinator.GroupCoordinator;
import dev.logforge.log.Compactor;
import dev.logforge.log.LogManager;
import dev.logforge.log.ProducerIdManager;
import dev.logforge.network.NioServer;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.replication.DelayedOperations;
import dev.logforge.replication.IsrManager;
import dev.logforge.replication.Partition;
import dev.logforge.replication.ReplicaFetcherManager;
import dev.logforge.replication.ReplicaManager;
import dev.logforge.util.Json;
import dev.logforge.util.JsonLog;
import java.io.IOException;
import java.net.InetSocketAddress;
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
  private volatile boolean halted;
  private volatile boolean frozen;
  private volatile boolean replicationPaused;
  private ReplicaFetcherManager fetchers;
  private GroupCoordinator groupCoordinator;
  private final Metrics metrics = new Metrics(System::currentTimeMillis);
  private final long startedAtMs = System.currentTimeMillis();
  private AdminHttpServer admin;

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
              config.offsetsTopicReplicationFactor,
              config.uncleanLeaderElection);
    }
    if (config.isBroker()) {
      controllerChannel =
          controller != null
              ? ControllerChannel.local(controller)
              : ControllerChannel.remote(
                  config.controllerHost,
                  config.controllerPort,
                  config.nodeId,
                  (int) config.brokerSessionTimeoutMs,
                  config.clusterToken);
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
      fetchers =
          new ReplicaFetcherManager(
              config.nodeId,
              replicaManager,
              this::brokerAddress,
              config.replicaFetchWaitMs,
              8 * 1024 * 1024,
              config.clusterToken);
      replicaManager.setFetcherControl(fetchers);
      replicaManager.setLatencyObserver(metrics);
      registerGauges();
      groupCoordinator =
          new GroupCoordinator(
              config.nodeId,
              replicaManager,
              () -> metadata,
              controllerChannel,
              System::currentTimeMillis);
    }
    apis =
        new BrokerApis(
            controller, replicaManager, () -> metadata, producerIds, config.clusterToken);
    if (groupCoordinator != null) {
      apis.setGroupApis(groupCoordinator::handle);
    }
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
    server.setConnectionLimits(config.maxConnections, config.maxConnectionsPerIp);
    server.start();
    if (controller != null) {
      controller.setSelfAddress(config.advertisedHost, advertisedPort());
      scheduler.scheduleWithFixedDelay(
          () -> runSafely("controller tick", controller::tick),
          config.heartbeatIntervalMs,
          config.heartbeatIntervalMs,
          TimeUnit.MILLISECONDS);
    }
    scheduler.scheduleWithFixedDelay(
        () -> runSafely("rates", metrics::tickRates), 5, 5, TimeUnit.SECONDS);
    if (config.adminEnabled()) {
      admin =
          new AdminHttpServer(
              config,
              AdminHttpServer.source(
                  config.nodeId,
                  config.roles.stream()
                      .map(r -> r.name().toLowerCase(java.util.Locale.ROOT))
                      .toList(),
                  () -> System.currentTimeMillis() - startedAtMs,
                  () -> controller,
                  this::statusJson,
                  metrics::render));
      admin.start();
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
      scheduler.scheduleWithFixedDelay(
          () -> runSafely("groups", groupCoordinator::tick), 100, 100, TimeUnit.MILLISECONDS);
      scheduler.scheduleWithFixedDelay(
          () -> runSafely("compaction", this::compactNow),
          config.compactionIntervalMs,
          config.compactionIntervalMs,
          TimeUnit.MILLISECONDS);
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
    if (closed || frozen) {
      return;
    }
    long now = System.currentTimeMillis();
    ByteBuffer status = null;
    if (now - lastStatusSentMs >= 1_000 || replicaManager.storageFailure() != null) {
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

  /**
   * Compacts every {@code cleanup.policy=compact} partition this broker holds, below its high
   * watermark. Runs on a timer; public so tests can trigger a pass.
   *
   * @return records removed
   */
  public long compactNow() {
    long removed = 0;
    for (Partition p : replicaManager.partitions().values()) {
      if (p.log() == null || p.config() == null || !p.config().compact()) {
        continue;
      }
      try {
        removed +=
            Compactor.compact(p.log(), System.currentTimeMillis(), p.highWatermark())
                .recordsRemoved();
      } catch (IOException | RuntimeException e) {
        LOG.error("compaction failed", e, "partition", p.topicPartition().toString());
      }
    }
    return removed;
  }

  /** What this broker reports about itself; also served at /api/v1/local. */
  public Map<String, Object> statusJson() {
    return Json.obj(
        "nodeId", config.nodeId,
        "storageFailed", replicaManager != null && replicaManager.storageFailure() != null,
        "partitions", replicaManager == null ? List.of() : replicaManager.status(),
        "messagesIn", replicaManager == null ? 0 : replicaManager.messagesIn(),
        "bytesIn", replicaManager == null ? 0 : replicaManager.bytesIn(),
        "bytesOut", replicaManager == null ? 0 : replicaManager.bytesOut(),
        "connections", server == null ? 0 : server.openConnections(),
        "groups", groupCoordinator == null ? List.of() : groupCoordinator.status(),
        "rates",
            Json.obj(
                "messagesInPerSec", metrics.messagesInPerSec(),
                "bytesInPerSec", metrics.bytesInPerSec(),
                "bytesOutPerSec", metrics.bytesOutPerSec()));
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

  /** Address of a broker from the latest metadata, for replica fetchers. */
  private InetSocketAddress brokerAddress(int id) {
    ControllerApis.Snapshot s = metadata;
    if (s == null) {
      return null;
    }
    for (ControllerApis.BrokerInfo b : s.brokers()) {
      if (b.nodeId() == id) {
        return InetSocketAddress.createUnresolved(b.host(), b.port());
      }
    }
    return null;
  }

  /**
   * Test hook: stops the node abruptly, keeping its data directory. In-process this cannot be a
   * real crash (files are closed so the same JVM can reopen them); real kills are done on separate
   * processes in LeaderKillGateTest and chaos/.
   */
  public void halt() {
    halted = true;
    try {
      close();
    } catch (IOException e) {
      LOG.warn("halt: close failed", "error", e.getClass().getSimpleName());
    }
  }

  public boolean isHalted() {
    return halted;
  }

  /** Test hook modelling SIGSTOP: no heartbeats, no replication, until {@link #unfreeze}. */
  public void freeze() {
    frozen = true;
    applyFetchPause();
  }

  public void unfreeze() {
    frozen = false;
    applyFetchPause();
  }

  /** Test hook: stop pulling from leaders while staying alive (a slow follower). */
  public void pauseReplication() {
    replicationPaused = true;
    applyFetchPause();
  }

  public void resumeReplication() {
    replicationPaused = false;
    applyFetchPause();
  }

  private void applyFetchPause() {
    if (fetchers != null) {
      fetchers.setPaused(frozen || replicationPaused);
    }
  }

  /** Port of the admin HTTP server, or -1 if it is disabled. */
  public int adminPort() {
    return admin == null ? -1 : admin.port();
  }

  public Metrics metrics() {
    return metrics;
  }

  /** Scrape-time gauges computed from live partition and group state. */
  private void registerGauges() {
    metrics.gauge(
        "logforge_connections_refused_total",
        "Connections closed at accept time because a connection limit was reached.",
        () ->
            List.of(
                new Metrics.Sample(Map.of(), server == null ? 0 : server.refusedConnections())));
    metrics.gauge(
        "logforge_isr_size",
        "In-sync replicas per partition this broker leads.",
        () -> {
          List<Metrics.Sample> out = new java.util.ArrayList<>();
          replicaManager
              .partitions()
              .forEach(
                  (tp, p) -> {
                    if (p.isLeader()) {
                      out.add(new Metrics.Sample(labels(tp), p.inSyncReplicas().size()));
                    }
                  });
          return out;
        });
    metrics.gauge(
        "logforge_under_replicated_partitions",
        "Partitions led here whose ISR is smaller than their replica set.",
        () -> {
          long n =
              replicaManager.partitions().values().stream()
                  .filter(p -> p.isLeader() && p.inSyncReplicas().size() < p.replicas().size())
                  .count();
          return List.of(new Metrics.Sample(Map.of(), n));
        });
    metrics.gauge(
        "logforge_log_end_offset",
        "Log end offset of every replica on this broker.",
        () -> {
          List<Metrics.Sample> out = new java.util.ArrayList<>();
          replicaManager
              .partitions()
              .forEach((tp, p) -> out.add(new Metrics.Sample(labels(tp), p.logEndOffset())));
          return out;
        });
    metrics.gauge(
        "logforge_high_watermark",
        "High watermark of every replica on this broker.",
        () -> {
          List<Metrics.Sample> out = new java.util.ArrayList<>();
          replicaManager
              .partitions()
              .forEach((tp, p) -> out.add(new Metrics.Sample(labels(tp), p.highWatermark())));
          return out;
        });
    metrics.gauge(
        "logforge_consumer_group_lag",
        "High watermark minus committed offset, for groups coordinated here and partitions led here.",
        this::groupLagSamples);
  }

  private static Map<String, String> labels(TopicPartition tp) {
    return Map.of("topic", tp.topic(), "partition", Integer.toString(tp.partition()));
  }

  @SuppressWarnings("unchecked")
  private List<Metrics.Sample> groupLagSamples() {
    List<Metrics.Sample> out = new java.util.ArrayList<>();
    if (groupCoordinator == null) {
      return out;
    }
    for (Map<String, Object> g : groupCoordinator.status()) {
      for (Object o : (List<Object>) g.get("committed")) {
        Map<String, Object> c = (Map<String, Object>) o;
        TopicPartition tp =
            new TopicPartition((String) c.get("topic"), ((Number) c.get("partition")).intValue());
        Partition p = replicaManager.partition(tp);
        if (p != null && p.isLeader()) {
          long lag = Math.max(0, p.highWatermark() - ((Number) c.get("committed")).longValue());
          out.add(
              new Metrics.Sample(
                  Map.of(
                      "group",
                      (String) g.get("groupId"),
                      "topic",
                      tp.topic(),
                      "partition",
                      Integer.toString(tp.partition())),
                  lag));
        }
      }
    }
    return out;
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
    if (admin != null) {
      admin.close();
    }
    if (fetchers != null) {
      fetchers.close();
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
