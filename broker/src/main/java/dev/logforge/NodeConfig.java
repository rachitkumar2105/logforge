package dev.logforge;

import dev.logforge.log.FsyncPolicy;
import dev.logforge.log.LogConfig;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Configuration for one node. Built from environment variables in production ({@link #fromEnv}) and
 * from the builder in tests. Every variable is listed in ARCHITECTURE.md section 9.
 */
public final class NodeConfig {
  public enum Role {
    BROKER,
    CONTROLLER
  }

  public final int nodeId;
  public final Set<Role> roles;
  public final String listenHost;
  public final int listenPort;
  public final String advertisedHost;
  public final int advertisedPort;
  public final String controllerHost;
  public final int controllerPort;
  public final Path dataDir;
  public final String adminHost;
  public final int adminPort;
  public final String adminToken;
  public final String readToken;
  public final List<String> corsOrigins;
  public final double ratePerIp;
  public final double ratePerToken;
  public final int ioThreads;
  public final int handlerThreads;
  public final int maxRequestBytes;
  public final int maxQueuedResponsesPerConnection;
  public final long heartbeatIntervalMs;
  public final long brokerSessionTimeoutMs;
  public final long replicaLagTimeMaxMs;
  public final int replicaFetchWaitMs;
  public final boolean uncleanLeaderElection;
  public final LogConfig defaultLogConfig;
  public final int offsetsTopicPartitions;
  public final int offsetsTopicReplicationFactor;
  public final long retentionCheckIntervalMs;
  public final long compactionIntervalMs;
  public final String clusterId;

  /** Sample topic to write at startup (hosted demo), or null. */
  public final Seeder.Spec seed;

  private NodeConfig(Builder b) {
    this.nodeId = b.nodeId;
    this.seed = b.seed;
    this.roles = EnumSet.copyOf(b.roles);
    this.listenHost = b.listenHost;
    this.listenPort = b.listenPort;
    this.advertisedHost = b.advertisedHost;
    this.advertisedPort = b.advertisedPort;
    this.controllerHost = b.controllerHost;
    this.controllerPort = b.controllerPort;
    this.dataDir = b.dataDir;
    this.adminHost = b.adminHost;
    this.adminPort = b.adminPort;
    this.adminToken = b.adminToken;
    this.readToken = b.readToken;
    this.corsOrigins = List.copyOf(b.corsOrigins);
    this.ratePerIp = b.ratePerIp;
    this.ratePerToken = b.ratePerToken;
    this.ioThreads = b.ioThreads;
    this.handlerThreads = b.handlerThreads;
    this.maxRequestBytes = b.maxRequestBytes;
    this.maxQueuedResponsesPerConnection = b.maxQueuedResponsesPerConnection;
    this.heartbeatIntervalMs = b.heartbeatIntervalMs;
    this.brokerSessionTimeoutMs = b.brokerSessionTimeoutMs;
    this.replicaLagTimeMaxMs = b.replicaLagTimeMaxMs;
    this.replicaFetchWaitMs = b.replicaFetchWaitMs;
    this.uncleanLeaderElection = b.uncleanLeaderElection;
    this.defaultLogConfig = b.defaultLogConfig;
    this.offsetsTopicPartitions = b.offsetsTopicPartitions;
    this.offsetsTopicReplicationFactor = b.offsetsTopicReplicationFactor;
    this.retentionCheckIntervalMs = b.retentionCheckIntervalMs;
    this.compactionIntervalMs = b.compactionIntervalMs;
    this.clusterId = b.clusterId;
  }

  public boolean isBroker() {
    return roles.contains(Role.BROKER);
  }

  public boolean isController() {
    return roles.contains(Role.CONTROLLER);
  }

  public boolean adminEnabled() {
    return adminHost != null;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Reads configuration from environment variables. Fails fast on anything invalid. */
  public static NodeConfig fromEnv(Map<String, String> env) {
    Builder b = builder();
    String id = env.get("LOGFORGE_NODE_ID");
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("LOGFORGE_NODE_ID is required");
    }
    b.nodeId(Integer.parseInt(id.trim()));
    String roles = env.getOrDefault("LOGFORGE_ROLES", "broker,controller");
    EnumSet<Role> rs = EnumSet.noneOf(Role.class);
    for (String r : roles.split(",")) {
      if (!r.isBlank()) {
        rs.add(Role.valueOf(r.trim().toUpperCase(Locale.ROOT)));
      }
    }
    if (rs.isEmpty()) {
      throw new IllegalArgumentException("LOGFORGE_ROLES must name broker and/or controller");
    }
    b.roles(rs);
    String[] listen = hostPort(env.getOrDefault("LOGFORGE_LISTEN", "0.0.0.0:9092"));
    b.listen(listen[0], Integer.parseInt(listen[1]));
    String advertised = env.get("LOGFORGE_ADVERTISED");
    if (advertised != null) {
      String[] adv = hostPort(advertised);
      b.advertisedHost(adv[0]).advertisedPort(Integer.parseInt(adv[1]));
    } else {
      b.advertisedHost("localhost");
    }
    String controller = env.get("LOGFORGE_CONTROLLER");
    if (controller != null && !controller.isBlank()) {
      String[] c = hostPort(controller);
      b.controller(c[0], Integer.parseInt(c[1]));
    } else if (!rs.contains(Role.CONTROLLER)) {
      throw new IllegalArgumentException("LOGFORGE_CONTROLLER is required for a broker-only node");
    }
    String dataDir = env.get("LOGFORGE_DATA_DIR");
    b.dataDir(
        dataDir != null
            ? Path.of(dataDir)
            : Path.of(System.getProperty("user.home"), "logforge-data", "node-" + id.trim()));
    // Platforms such as Render hand a web service its one public port in PORT.
    String platformPort = env.get("PORT");
    String admin =
        env.getOrDefault(
            "LOGFORGE_ADMIN_LISTEN",
            platformPort != null && !platformPort.isBlank()
                ? "0.0.0.0:" + platformPort.trim()
                : "0.0.0.0:8080");
    if (admin.equalsIgnoreCase("off")) {
      b.adminListen(null);
    } else {
      String[] a = hostPort(admin);
      b.adminListen(a[0]).adminPort(Integer.parseInt(a[1]));
      // Tokens are only read when the admin server runs; AdminHttpServer refuses to start without
      // them.
      b.adminToken(env.get("LOGFORGE_ADMIN_TOKEN")).readToken(env.get("LOGFORGE_READ_TOKEN"));
    }
    String cors = env.getOrDefault("LOGFORGE_CORS_ORIGINS", "http://localhost:3000");
    List<String> origins = new ArrayList<>();
    for (String o : cors.split(",")) {
      if (!o.isBlank()) {
        origins.add(o.trim());
      }
    }
    b.corsOrigins(origins);
    b.ratePerIp(Double.parseDouble(env.getOrDefault("LOGFORGE_RATE_PER_IP", "20")));
    b.ratePerToken(Double.parseDouble(env.getOrDefault("LOGFORGE_RATE_PER_TOKEN", "50")));
    LogConfig logConfig =
        LogConfig.defaults()
            .withFsync(FsyncPolicy.parse(env.getOrDefault("LOGFORGE_FSYNC", "ms:1000")));
    b.defaultLogConfig(logConfig);
    b.uncleanLeaderElection(
        Boolean.parseBoolean(env.getOrDefault("LOGFORGE_UNCLEAN_ELECTION", "false")));
    if (env.containsKey("LOGFORGE_IO_THREADS")) {
      b.ioThreads(Integer.parseInt(env.get("LOGFORGE_IO_THREADS")));
    }
    if (env.containsKey("LOGFORGE_HANDLER_THREADS")) {
      b.handlerThreads(Integer.parseInt(env.get("LOGFORGE_HANDLER_THREADS")));
    }
    if (env.containsKey("LOGFORGE_HEARTBEAT_MS")) {
      b.heartbeatIntervalMs(Long.parseLong(env.get("LOGFORGE_HEARTBEAT_MS")));
    }
    if (env.containsKey("LOGFORGE_SESSION_TIMEOUT_MS")) {
      b.brokerSessionTimeoutMs(Long.parseLong(env.get("LOGFORGE_SESSION_TIMEOUT_MS")));
    }
    if (env.containsKey("LOGFORGE_REPLICA_LAG_MS")) {
      b.replicaLagTimeMaxMs(Long.parseLong(env.get("LOGFORGE_REPLICA_LAG_MS")));
    }
    if (env.containsKey("LOGFORGE_CLUSTER_ID")) {
      b.clusterId(env.get("LOGFORGE_CLUSTER_ID"));
    }
    String seed = env.get("LOGFORGE_SEED_TOPIC");
    if (seed != null && !seed.isBlank()) {
      b.seed(Seeder.Spec.parse(seed.trim()));
    }
    return b.build();
  }

  private static String[] hostPort(String s) {
    int colon = s.lastIndexOf(':');
    if (colon <= 0 || colon == s.length() - 1) {
      throw new IllegalArgumentException("expected host:port, got " + s);
    }
    return new String[] {s.substring(0, colon), s.substring(colon + 1)};
  }

  /** Mutable builder; all fields have defaults except {@code nodeId} and {@code dataDir}. */
  public static final class Builder {
    private int nodeId = -1;
    private Set<Role> roles = EnumSet.of(Role.BROKER, Role.CONTROLLER);
    private String listenHost = "0.0.0.0";
    private int listenPort = 9092;
    private String advertisedHost = "localhost";
    private int advertisedPort = -1;
    private String controllerHost;
    private int controllerPort = -1;
    private Path dataDir;
    private String adminHost;
    private int adminPort = 8080;
    private String adminToken;
    private String readToken;
    private List<String> corsOrigins = List.of("http://localhost:3000");
    private double ratePerIp = 20;
    private double ratePerToken = 50;
    private int ioThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
    private int handlerThreads = Math.max(4, Runtime.getRuntime().availableProcessors());
    private int maxRequestBytes = 16 * 1024 * 1024;
    private int maxQueuedResponsesPerConnection = 16;
    private long heartbeatIntervalMs = 250;
    private long brokerSessionTimeoutMs = 3_000;
    private long replicaLagTimeMaxMs = 10_000;
    private int replicaFetchWaitMs = 200;
    private boolean uncleanLeaderElection = false;
    private LogConfig defaultLogConfig = LogConfig.defaults();
    private int offsetsTopicPartitions = 3;
    private int offsetsTopicReplicationFactor = 3;
    private Seeder.Spec seed;
    private long retentionCheckIntervalMs = 30_000;
    private long compactionIntervalMs = 15_000;
    private String clusterId = "logforge";

    public Builder nodeId(int v) {
      nodeId = v;
      return this;
    }

    public Builder roles(Set<Role> v) {
      roles = v;
      return this;
    }

    public Builder listen(String host, int port) {
      listenHost = host;
      listenPort = port;
      return this;
    }

    public Builder advertisedHost(String v) {
      advertisedHost = v;
      return this;
    }

    public Builder advertisedPort(int v) {
      advertisedPort = v;
      return this;
    }

    public Builder controller(String host, int port) {
      controllerHost = host;
      controllerPort = port;
      return this;
    }

    public Builder dataDir(Path v) {
      dataDir = v;
      return this;
    }

    /** Host for the admin HTTP server, or null to disable it. */
    public Builder adminListen(String v) {
      adminHost = v;
      return this;
    }

    public Builder adminPort(int v) {
      adminPort = v;
      return this;
    }

    public Builder adminToken(String v) {
      adminToken = v;
      return this;
    }

    public Builder readToken(String v) {
      readToken = v;
      return this;
    }

    public Builder corsOrigins(List<String> v) {
      corsOrigins = v;
      return this;
    }

    public Builder ratePerIp(double v) {
      ratePerIp = v;
      return this;
    }

    public Builder ratePerToken(double v) {
      ratePerToken = v;
      return this;
    }

    public Builder ioThreads(int v) {
      ioThreads = v;
      return this;
    }

    public Builder handlerThreads(int v) {
      handlerThreads = v;
      return this;
    }

    public Builder maxRequestBytes(int v) {
      maxRequestBytes = v;
      return this;
    }

    public Builder maxQueuedResponsesPerConnection(int v) {
      maxQueuedResponsesPerConnection = v;
      return this;
    }

    public Builder heartbeatIntervalMs(long v) {
      heartbeatIntervalMs = v;
      return this;
    }

    public Builder brokerSessionTimeoutMs(long v) {
      brokerSessionTimeoutMs = v;
      return this;
    }

    public Builder replicaLagTimeMaxMs(long v) {
      replicaLagTimeMaxMs = v;
      return this;
    }

    public Builder replicaFetchWaitMs(int v) {
      replicaFetchWaitMs = v;
      return this;
    }

    public Builder uncleanLeaderElection(boolean v) {
      uncleanLeaderElection = v;
      return this;
    }

    public Builder defaultLogConfig(LogConfig v) {
      defaultLogConfig = v;
      return this;
    }

    public Builder offsetsTopicPartitions(int v) {
      offsetsTopicPartitions = v;
      return this;
    }

    public Builder seed(Seeder.Spec v) {
      seed = v;
      return this;
    }

    public Builder offsetsTopicReplicationFactor(int v) {
      offsetsTopicReplicationFactor = v;
      return this;
    }

    public Builder retentionCheckIntervalMs(long v) {
      retentionCheckIntervalMs = v;
      return this;
    }

    public Builder compactionIntervalMs(long v) {
      compactionIntervalMs = v;
      return this;
    }

    public Builder clusterId(String v) {
      clusterId = v;
      return this;
    }

    public NodeConfig build() {
      if (nodeId < 0) {
        throw new IllegalArgumentException("nodeId must be >= 0");
      }
      if (dataDir == null) {
        throw new IllegalArgumentException("dataDir is required");
      }
      if (roles.isEmpty()) {
        throw new IllegalArgumentException("at least one role");
      }
      if (!roles.contains(Role.CONTROLLER) && controllerHost == null) {
        throw new IllegalArgumentException("a broker-only node needs the controller address");
      }
      if (maxRequestBytes < 1024) {
        throw new IllegalArgumentException("maxRequestBytes too small");
      }
      return new NodeConfig(this);
    }
  }
}
