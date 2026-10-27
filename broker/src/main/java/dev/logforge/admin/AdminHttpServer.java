package dev.logforge.admin;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.logforge.NodeConfig;
import dev.logforge.TopicPartition;
import dev.logforge.controller.Controller;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Topics;
import dev.logforge.util.Json;
import dev.logforge.util.JsonLog;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * The admin HTTP API (docs/API.md): what the showcase site and Prometheus read.
 *
 * <p>Security model (SECURITY.md): two bearer tokens from the environment, required at startup,
 * compared in constant time, never logged or echoed. The read token can only GET; mutations need
 * the admin token. Deny by default: every path not explicitly public needs a token. Per-IP and
 * per-token rate limits answer 429 with Retry-After. CORS echoes an origin only on exact match with
 * the allowlist. Request bodies are capped. Errors are short codes, never stack traces.
 */
public final class AdminHttpServer implements Closeable {
  private static final JsonLog LOG = JsonLog.get(AdminHttpServer.class);
  static final int MAX_BODY = 64 * 1024;
  static final int MIN_TOKEN_LENGTH = 24;

  /** Everything the API reads from the node. */
  public interface Source {
    Controller controller();

    Map<String, Object> localStatus();

    String prometheus();
  }

  private enum Access {
    NONE,
    READ,
    ADMIN
  }

  private final byte[] adminToken;
  private final byte[] readToken;
  private final List<String> corsOrigins;
  private final RateLimiter perIp;
  private final RateLimiter perToken;
  private final Source source;
  private final HttpServer server;
  private final ExecutorService executor;

  /**
   * Validates the tokens and binds. Fails fast (before binding) if a token is missing, short, or
   * the two are equal; the message names the variable, never the value.
   */
  public AdminHttpServer(NodeConfig config, Source source) throws IOException {
    requireToken("LOGFORGE_ADMIN_TOKEN", config.adminToken);
    requireToken("LOGFORGE_READ_TOKEN", config.readToken);
    if (config.adminToken.equals(config.readToken)) {
      throw new IllegalStateException("LOGFORGE_ADMIN_TOKEN and LOGFORGE_READ_TOKEN must differ");
    }
    this.adminToken = config.adminToken.getBytes(UTF_8);
    this.readToken = config.readToken.getBytes(UTF_8);
    this.corsOrigins = List.copyOf(config.corsOrigins);
    for (String o : corsOrigins) {
      if (o.contains("*")) {
        throw new IllegalStateException("LOGFORGE_CORS_ORIGINS may not contain a wildcard");
      }
    }
    this.perIp = new RateLimiter(config.ratePerIp, System::nanoTime);
    this.perToken = new RateLimiter(config.ratePerToken, System::nanoTime);
    this.source = source;
    this.server = HttpServer.create(new InetSocketAddress(config.adminHost, config.adminPort), 64);
    this.executor =
        Executors.newFixedThreadPool(
            4,
            r -> {
              Thread t = new Thread(r, "logforge-admin");
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(executor);
    server.createContext("/", this::handle);
  }

  private static void requireToken(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          name + " is not set; the admin API refuses to start without it");
    }
    if (value.length() < MIN_TOKEN_LENGTH) {
      throw new IllegalStateException(
          name + " must be at least " + MIN_TOKEN_LENGTH + " characters");
    }
  }

  public void start() {
    server.start();
  }

  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }

  // ------------------------------------------------------------------ request pipeline

  private void handle(HttpExchange ex) throws IOException {
    try {
      String origin = ex.getRequestHeaders().getFirst("Origin");
      boolean corsOk = origin != null && corsOrigins.contains(origin);
      if (corsOk) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", origin);
        ex.getResponseHeaders().set("Vary", "Origin");
      }
      String method = ex.getRequestMethod();
      String path = ex.getRequestURI().getPath();
      if ("OPTIONS".equals(method)) {
        if (corsOk) {
          ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
          ex.getResponseHeaders()
              .set("Access-Control-Allow-Headers", "Authorization, Content-Type");
          ex.getResponseHeaders().set("Access-Control-Max-Age", "600");
        }
        send(ex, 204, null, null);
        return;
      }
      String ip = ex.getRemoteAddress().getAddress().getHostAddress();
      long wait = perIp.acquire(ip);
      if (wait > 0) {
        rateLimited(ex, wait);
        return;
      }
      // Public, so it says only that the node is up (SECURITY-AUDIT F5). HEAD for uptime monitors.
      if ("/health".equals(path) && ("GET".equals(method) || "HEAD".equals(method))) {
        if ("HEAD".equals(method)) {
          send(ex, 200, "application/json; charset=utf-8", null);
        } else {
          sendJson(ex, 200, Json.obj("status", "ok"));
        }
        return;
      }
      Access access = authenticate(ex);
      if (access == Access.NONE) {
        error(ex, 401, "unauthorized");
        return;
      }
      wait = perToken.acquire(access.name());
      if (wait > 0) {
        rateLimited(ex, wait);
        return;
      }
      route(ex, method, path, access);
    } catch (RuntimeException e) {
      LOG.error("admin request failed", e);
      error(ex, 500, "internal");
    } finally {
      ex.close();
    }
  }

  /** Constant-time comparison against both tokens; the result says which one matched. */
  private Access authenticate(HttpExchange ex) {
    String h = ex.getRequestHeaders().getFirst("Authorization");
    if (h == null || !h.startsWith("Bearer ")) {
      return Access.NONE;
    }
    byte[] presented = h.substring(7).trim().getBytes(UTF_8);
    boolean admin = MessageDigest.isEqual(presented, adminToken);
    boolean read = MessageDigest.isEqual(presented, readToken);
    return admin ? Access.ADMIN : read ? Access.READ : Access.NONE;
  }

  private void route(HttpExchange ex, String method, String path, Access access)
      throws IOException {
    String[] parts = path.split("/", -1); // "", "api", "v1", ...
    boolean isGet = "GET".equals(method);
    if ("/metrics".equals(path)) {
      if (!isGet) {
        error(ex, 405, "method_not_allowed");
        return;
      }
      send(
          ex, 200, "text/plain; version=0.0.4; charset=utf-8", source.prometheus().getBytes(UTF_8));
      return;
    }
    if ("/api/v1/local".equals(path) && isGet) {
      sendJson(ex, 200, source.localStatus());
      return;
    }
    Controller controller = source.controller();
    if (parts.length < 4 || !"api".equals(parts[1]) || !"v1".equals(parts[2])) {
      error(ex, 404, "not_found");
      return;
    }
    if (controller == null) {
      error(ex, 404, "not_controller");
      return;
    }
    String resource = parts[3];
    switch (resource) {
      case "cluster" -> {
        if (!isGet || parts.length != 4) {
          error(ex, isGet ? 404 : 405, isGet ? "not_found" : "method_not_allowed");
          return;
        }
        sendJson(ex, 200, ClusterView.build(controller));
      }
      case "groups" -> {
        if (!isGet) {
          error(ex, 405, "method_not_allowed");
          return;
        }
        sendJson(ex, 200, Json.obj("groups", ClusterView.build(controller).get("groups")));
      }
      case "segments" -> {
        if (!isGet || parts.length != 6) {
          error(ex, 404, "not_found");
          return;
        }
        int partition;
        try {
          partition = Integer.parseInt(parts[5]);
        } catch (NumberFormatException e) {
          error(ex, 400, "invalid_partition");
          return;
        }
        Map<String, Object> segs = ClusterView.segments(controller, parts[4], partition);
        if (segs == null) {
          error(ex, 404, "not_found");
        } else {
          sendJson(ex, 200, segs);
        }
      }
      case "topics" -> topics(ex, method, parts, access, controller);
      default -> error(ex, 404, "not_found");
    }
  }

  private void topics(
      HttpExchange ex, String method, String[] parts, Access access, Controller controller)
      throws IOException {
    if ("GET".equals(method) && parts.length == 5) {
      Map<String, Object> t = ClusterView.topic(controller, parts[4]);
      if (t == null) {
        error(ex, 404, "not_found");
      } else {
        sendJson(ex, 200, t);
      }
      return;
    }
    boolean mutation =
        ("POST".equals(method) && parts.length == 4)
            || ("DELETE".equals(method) && parts.length == 5);
    if (!mutation) {
      error(ex, 405, "method_not_allowed");
      return;
    }
    if (access != Access.ADMIN) {
      error(ex, 403, "forbidden"); // the read token can never mutate
      return;
    }
    if ("DELETE".equals(method)) {
      String name = parts[4];
      if (TopicPartition.isInternal(name)) {
        error(ex, 400, "internal_topic");
        return;
      }
      Topics.DeleteResult r =
          controller.deleteTopics(new Topics.DeleteRequest(10_000, List.of(name))).topics().get(0);
      Errors e = Errors.forCode(r.errorCode());
      if (e == Errors.NONE) {
        LOG.info("topic deleted via admin API", "topic", name);
        sendJson(ex, 200, Json.obj("deleted", name));
      } else {
        error(
            ex,
            e == Errors.UNKNOWN_TOPIC_OR_PARTITION ? 404 : 400,
            e.name().toLowerCase(java.util.Locale.ROOT));
      }
      return;
    }
    byte[] body = readBody(ex);
    if (body == null) {
      error(ex, 413, "payload_too_large");
      return;
    }
    Topics.NewTopic nt;
    try {
      nt = parseNewTopic(new String(body, UTF_8));
    } catch (RuntimeException e) {
      error(ex, 400, "invalid_body");
      return;
    }
    Topics.CreateResult r =
        controller.createTopics(new Topics.CreateRequest(10_000, List.of(nt))).topics().get(0);
    Errors e = Errors.forCode(r.errorCode());
    switch (e) {
      case NONE -> {
        LOG.info(
            "topic created via admin API", "topic", nt.name(), "partitions", nt.numPartitions());
        sendJson(ex, 201, Json.obj("name", nt.name()));
      }
      case TOPIC_ALREADY_EXISTS -> error(ex, 409, "topic_exists");
      case INVALID_TOPIC -> error(ex, 400, "invalid_topic");
      case INVALID_PARTITIONS -> error(ex, 400, "invalid_partitions");
      case INVALID_REPLICATION_FACTOR -> error(ex, 400, "invalid_replication_factor");
      default -> error(ex, 400, "invalid_body");
    }
  }

  @SuppressWarnings("unchecked")
  private static Topics.NewTopic parseNewTopic(String json) {
    Map<String, Object> m = Json.parseObject(json);
    String name = (String) m.get("name");
    int partitions = ((Number) m.get("partitions")).intValue();
    int rf = ((Number) m.get("replicationFactor")).intValue();
    if (rf < 1 || rf > Short.MAX_VALUE) {
      throw new IllegalArgumentException("replicationFactor");
    }
    Map<String, String> configs = new HashMap<>();
    Object c = m.get("configs");
    if (c != null) {
      ((Map<String, Object>) c).forEach((k, v) -> configs.put(k, String.valueOf(v)));
    }
    if (name == null) {
      throw new IllegalArgumentException("name");
    }
    return new Topics.NewTopic(name, partitions, (short) rf, configs);
  }

  /** Reads at most MAX_BODY bytes; returns null if the body is larger. */
  private static byte[] readBody(HttpExchange ex) throws IOException {
    try (InputStream in = ex.getRequestBody()) {
      byte[] buf = in.readNBytes(MAX_BODY + 1);
      return buf.length > MAX_BODY ? null : buf;
    }
  }

  private void rateLimited(HttpExchange ex, long retryAfterSeconds) throws IOException {
    ex.getResponseHeaders().set("Retry-After", Long.toString(retryAfterSeconds));
    error(ex, 429, "rate_limited");
  }

  private static void error(HttpExchange ex, int status, String code) throws IOException {
    sendJson(ex, status, Json.obj("error", code));
  }

  private static void sendJson(HttpExchange ex, int status, Object body) throws IOException {
    send(ex, status, "application/json; charset=utf-8", Json.write(body).getBytes(UTF_8));
  }

  private static void send(HttpExchange ex, int status, String contentType, byte[] body)
      throws IOException {
    var h = ex.getResponseHeaders();
    h.set("X-Content-Type-Options", "nosniff");
    h.set("Cache-Control", "no-store");
    h.set("Referrer-Policy", "no-referrer");
    h.set("X-Frame-Options", "DENY");
    // TLS ends at the hosting platform's edge; browsers must not fall back to plain HTTP (F6).
    h.set("Strict-Transport-Security", "max-age=63072000; includeSubDomains");
    if (contentType != null) {
      h.set("Content-Type", contentType);
    }
    if (body == null || status == 204) {
      ex.sendResponseHeaders(status, -1);
      return;
    }
    ex.sendResponseHeaders(status, body.length);
    try (OutputStream out = ex.getResponseBody()) {
      out.write(body);
    }
  }

  // ------------------------------------------------------------------ cluster view

  /** Builds the JSON documents in API.md from controller state and broker heartbeat statuses. */
  static final class ClusterView {
    private ClusterView() {}

    @SuppressWarnings("unchecked")
    private static Map<Integer, Map<String, Object>> statuses(Controller c) {
      Map<Integer, Map<String, Object>> out = new HashMap<>();
      c.brokerStatuses()
          .forEach(
              (id, json) -> {
                try {
                  out.put(id, Json.parseObject(json));
                } catch (RuntimeException e) {
                  out.put(id, Map.of());
                }
              });
      return out;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> partitionStatus(
        Map<String, Object> brokerStatus, String topic, int partition) {
      Object ps = brokerStatus.get("partitions");
      if (!(ps instanceof List<?> list)) {
        return null;
      }
      for (Object o : list) {
        Map<String, Object> p = (Map<String, Object>) o;
        if (topic.equals(p.get("topic")) && ((Number) p.get("partition")).intValue() == partition) {
          return p;
        }
      }
      return null;
    }

    static Map<String, Object> build(Controller c) {
      ControllerApis.Snapshot s = c.snapshot();
      Map<Integer, Map<String, Object>> statuses = statuses(c);
      Map<Integer, Long> ages = c.heartbeatAges();
      List<Object> brokers = new ArrayList<>();
      for (ControllerApis.BrokerInfo b : s.brokers()) {
        Map<String, Object> bm = new LinkedHashMap<>();
        bm.put("id", b.nodeId());
        bm.put("host", b.host());
        bm.put("port", b.port());
        bm.put("alive", b.alive());
        bm.put("controller", b.nodeId() == s.controllerId());
        bm.put("lastHeartbeatMsAgo", ages.getOrDefault(b.nodeId(), 0L));
        brokers.add(bm);
      }
      Map<String, Long> highWatermarks = new HashMap<>();
      List<Object> topics = new ArrayList<>();
      for (ControllerApis.TopicInfo t : s.topics()) {
        Map<String, Object> tm = topicJson(t, statuses, highWatermarks);
        topics.add(tm);
      }
      double msgRate = 0;
      double inRate = 0;
      double outRate = 0;
      List<Object> groups = new ArrayList<>();
      for (Map<String, Object> st : statuses.values()) {
        if (st.get("rates") instanceof Map<?, ?> r) {
          msgRate += num(r.get("messagesInPerSec"));
          inRate += num(r.get("bytesInPerSec"));
          outRate += num(r.get("bytesOutPerSec"));
        }
        if (st.get("groups") instanceof List<?> gl) {
          for (Object go : gl) {
            groups.add(groupJson(go, highWatermarks));
          }
        }
      }
      Map<String, Object> out = new LinkedHashMap<>();
      out.put("clusterId", s.clusterId());
      out.put("controllerId", s.controllerId());
      out.put("metadataVersion", s.version());
      out.put("capturedAtMs", System.currentTimeMillis());
      out.put("brokers", brokers);
      out.put("topics", topics);
      out.put("groups", groups);
      out.put(
          "rates",
          Json.obj(
              "messagesInPerSec", msgRate, "bytesInPerSec", inRate, "bytesOutPerSec", outRate));
      return out;
    }

    private static double num(Object o) {
      return o instanceof Number n ? n.doubleValue() : 0;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> groupJson(Object go, Map<String, Long> highWatermarks) {
      Map<String, Object> g = new LinkedHashMap<>((Map<String, Object>) go);
      List<Object> lag = new ArrayList<>();
      Object committed = g.remove("committed");
      if (committed instanceof List<?> cl) {
        for (Object co : cl) {
          Map<String, Object> cm = (Map<String, Object>) co;
          String key = cm.get("topic") + "-" + cm.get("partition");
          long off = ((Number) cm.get("committed")).longValue();
          Long hw = highWatermarks.get(key);
          Map<String, Object> lm = new LinkedHashMap<>(cm);
          lm.put("highWatermark", hw == null ? -1 : hw);
          lm.put("lag", hw == null ? -1 : Math.max(0, hw - off));
          lag.add(lm);
        }
      }
      g.put("lag", lag);
      return g;
    }

    private static Map<String, Object> topicJson(
        ControllerApis.TopicInfo t,
        Map<Integer, Map<String, Object>> statuses,
        Map<String, Long> highWatermarks) {
      Map<String, Object> tm = new LinkedHashMap<>();
      tm.put("name", t.name());
      tm.put("internal", TopicPartition.isInternal(t.name()));
      tm.put("cleanupPolicy", t.configs().getOrDefault("cleanup.policy", "delete"));
      List<Object> parts = new ArrayList<>();
      for (ControllerApis.PartitionInfo p : t.partitions()) {
        Map<String, Object> pm = new LinkedHashMap<>();
        pm.put("partition", p.partition());
        pm.put("leader", p.leader());
        pm.put("leaderEpoch", p.leaderEpoch());
        pm.put("replicas", p.replicas());
        pm.put("isr", p.isr());
        pm.put("underReplicated", p.leader() >= 0 && p.isr().size() < p.replicas().size());
        pm.put("offline", p.leader() < 0);
        List<Object> rs = new ArrayList<>();
        for (int r : p.replicas()) {
          Map<String, Object> st =
              partitionStatus(statuses.getOrDefault(r, Map.of()), t.name(), p.partition());
          Map<String, Object> rm = new LinkedHashMap<>();
          rm.put("brokerId", r);
          rm.put("logEndOffset", st == null ? -1 : st.get("logEndOffset"));
          rm.put("highWatermark", st == null ? -1 : st.get("highWatermark"));
          rs.add(rm);
          if (r == p.leader() && st != null && st.get("highWatermark") instanceof Number hw) {
            highWatermarks.put(t.name() + "-" + p.partition(), hw.longValue());
          }
        }
        pm.put("replicaState", rs);
        parts.add(pm);
      }
      tm.put("partitions", parts);
      return tm;
    }

    static Map<String, Object> topic(Controller c, String name) {
      for (ControllerApis.TopicInfo t : c.snapshot().topics()) {
        if (t.name().equals(name)) {
          Map<String, Object> tm = topicJson(t, statuses(c), new HashMap<>());
          tm.put("configs", new java.util.TreeMap<>(t.configs()));
          return tm;
        }
      }
      return null;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> segments(Controller c, String topic, int partition) {
      ControllerApis.PartitionInfo info = null;
      for (ControllerApis.TopicInfo t : c.snapshot().topics()) {
        if (t.name().equals(topic) && partition >= 0 && partition < t.partitions().size()) {
          info = t.partitions().get(partition);
        }
      }
      if (info == null || info.leader() < 0) {
        return null;
      }
      Map<String, Object> st =
          partitionStatus(statuses(c).getOrDefault(info.leader(), Map.of()), topic, partition);
      if (st == null) {
        return null;
      }
      Map<String, Object> out = new LinkedHashMap<>();
      out.put("topic", topic);
      out.put("partition", partition);
      out.put("reportedBy", info.leader());
      out.put("logStartOffset", st.get("logStartOffset"));
      out.put("logEndOffset", st.get("logEndOffset"));
      out.put("highWatermark", st.get("highWatermark"));
      out.put("segments", st.getOrDefault("segments", List.of()));
      return out;
    }
  }

  /** Convenience for wiring: a Source from lambdas. */
  public static Source source(
      Supplier<Controller> controller,
      Supplier<Map<String, Object>> local,
      Supplier<String> prometheus) {
    return new Source() {
      @Override
      public Controller controller() {
        return controller.get();
      }

      @Override
      public Map<String, Object> localStatus() {
        return local.get();
      }

      @Override
      public String prometheus() {
        return prometheus.get();
      }
    };
  }
}
