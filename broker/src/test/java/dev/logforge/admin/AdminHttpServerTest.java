package dev.logforge.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.logforge.Node;
import dev.logforge.NodeConfig;
import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import dev.logforge.util.Json;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdminHttpServerTest {

  static final String ADMIN = "admin-token-0123456789abcdef";
  static final String READ = "read-token-0123456789abcdefgh";

  @TempDir Path dir;
  Node node;
  HttpClient http = HttpClient.newHttpClient();
  String base;

  private NodeConfig.Builder config(Path data) {
    return TestNodes.baseConfig(0, data)
        .roles(EnumSet.of(NodeConfig.Role.BROKER, NodeConfig.Role.CONTROLLER))
        .adminListen("127.0.0.1")
        .adminPort(0)
        .adminToken(ADMIN)
        .readToken(READ)
        .corsOrigins(List.of("https://logforge.example", "http://localhost:3000"))
        .ratePerIp(1_000)
        .ratePerToken(1_000);
  }

  @BeforeEach
  void start() throws Exception {
    node = Node.start(config(dir).build());
    TestNodes.awaitTrue(node::brokerRegistered, Duration.ofSeconds(10));
    base = "http://127.0.0.1:" + node.adminPort();
  }

  @AfterEach
  void stop() throws Exception {
    node.close();
  }

  private HttpResponse<String> get(String path, String token) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).GET();
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> send(String method, String path, String token, String body)
      throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create(base + path))
            .method(
                method,
                body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body))
            .header("Content-Type", "application/json");
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void refusesToStartWithoutStrongDistinctTokens() {
    Path d = dir.resolve("other");
    assertThatThrownBy(() -> Node.start(config(d).adminToken(null).build()))
        .hasMessageContaining("LOGFORGE_ADMIN_TOKEN")
        .hasMessageNotContaining(READ);
    assertThatThrownBy(() -> Node.start(config(d).readToken("short").build()))
        .hasMessageContaining("LOGFORGE_READ_TOKEN");
    assertThatThrownBy(() -> Node.start(config(d).readToken(ADMIN).build()))
        .hasMessageContaining("must differ")
        .hasMessageNotContaining(ADMIN);
  }

  @Test
  void healthIsPublicEverythingElseNeedsAToken() throws Exception {
    HttpResponse<String> health = get("/health", null);
    assertThat(health.statusCode()).isEqualTo(200);
    assertThat(Json.parseObject(health.body())).containsEntry("status", "ok");

    assertThat(get("/api/v1/cluster", null).statusCode()).isEqualTo(401);
    assertThat(get("/api/v1/cluster", "wrong-token-wrong-token-xx").statusCode()).isEqualTo(401);
    assertThat(get("/metrics", null).statusCode()).isEqualTo(401);
    HttpResponse<String> unauth = get("/api/v1/cluster", "wrong-token-wrong-token-xx");
    assertThat(unauth.body())
        .doesNotContain("wrong-token")
        .doesNotContain(READ)
        .doesNotContain(ADMIN);
    assertThat(unauth.headers().firstValue("X-Content-Type-Options")).hasValue("nosniff");
    assertThat(get("/nope", READ).statusCode()).isEqualTo(404);
  }

  @Test
  void theReadTokenCannotMutateButTheAdminTokenCan() throws Exception {
    String body =
        "{\"name\":\"made-by-api\",\"partitions\":2,\"replicationFactor\":1,\"configs\":{\"retention.ms\":\"60000\"}}";
    HttpResponse<String> denied = send("POST", "/api/v1/topics", READ, body);
    assertThat(denied.statusCode()).isEqualTo(403);
    assertThat(send("DELETE", "/api/v1/topics/anything", READ, null).statusCode()).isEqualTo(403);

    HttpResponse<String> created = send("POST", "/api/v1/topics", ADMIN, body);
    assertThat(created.statusCode()).isEqualTo(201);
    assertThat(send("POST", "/api/v1/topics", ADMIN, body).statusCode()).isEqualTo(409);
    assertThat(
            send(
                    "POST",
                    "/api/v1/topics",
                    ADMIN,
                    "{\"name\":\"bad name\",\"partitions\":1,\"replicationFactor\":1}")
                .statusCode())
        .isEqualTo(400);
    assertThat(send("POST", "/api/v1/topics", ADMIN, "not json").statusCode()).isEqualTo(400);

    TestNodes.awaitTrue(() -> node.isLeaderFor("made-by-api", 1), Duration.ofSeconds(10));
    HttpResponse<String> detail = get("/api/v1/topics/made-by-api", READ);
    assertThat(detail.statusCode()).isEqualTo(200);
    Map<String, Object> topic = Json.parseObject(detail.body());
    assertThat(topic).containsEntry("name", "made-by-api");
    assertThat((Map<?, ?>) topic.get("configs")).containsEntry("retention.ms", "60000");

    assertThat(send("DELETE", "/api/v1/topics/made-by-api", ADMIN, null).statusCode())
        .isEqualTo(200);
    assertThat(send("DELETE", "/api/v1/topics/made-by-api", ADMIN, null).statusCode())
        .isEqualTo(404);
    assertThat(send("DELETE", "/api/v1/topics/__consumer_offsets", ADMIN, null).statusCode())
        .isEqualTo(400);
  }

  @Test
  @SuppressWarnings("unchecked")
  void clusterViewShowsBrokersPartitionsReplicaStateAndSegments() throws Exception {
    try (TestClient c = new TestClient("127.0.0.1", node.port())) {
      c.createTopic("orders", 2, 1, Map.of());
      TestNodes.awaitTrue(() -> node.isLeaderFor("orders", 1), Duration.ofSeconds(10));
      for (int i = 0; i < 20; i++) {
        c.produce("orders", i % 2, (short) 1, TestClient.batch("v" + i));
      }
    }
    // Broker status reaches the controller with the next status heartbeat (about 1 s).
    TestNodes.awaitTrue(
        () -> {
          try {
            return get("/api/v1/cluster", READ).body().contains("\"logEndOffset\":10");
          } catch (Exception e) {
            return false;
          }
        },
        Duration.ofSeconds(10));
    Map<String, Object> cluster = Json.parseObject(get("/api/v1/cluster", READ).body());
    assertThat(cluster).containsEntry("controllerId", 0L);
    List<Object> brokers = (List<Object>) cluster.get("brokers");
    assertThat(brokers).hasSize(1);
    assertThat((Map<String, Object>) brokers.get(0)).containsEntry("alive", true);
    Map<String, Object> orders =
        ((List<Object>) cluster.get("topics"))
            .stream()
                .map(t -> (Map<String, Object>) t)
                .filter(t -> t.get("name").equals("orders"))
                .findFirst()
                .orElseThrow();
    Map<String, Object> p0 = (Map<String, Object>) ((List<Object>) orders.get("partitions")).get(0);
    assertThat(p0)
        .containsEntry("leader", 0L)
        .containsEntry("underReplicated", false)
        .containsEntry("offline", false);
    Map<String, Object> rs = (Map<String, Object>) ((List<Object>) p0.get("replicaState")).get(0);
    assertThat(rs).containsEntry("logEndOffset", 10L).containsEntry("highWatermark", 10L);
    assertThat(cluster).containsKey("rates");

    Map<String, Object> segs = Json.parseObject(get("/api/v1/segments/orders/0", READ).body());
    assertThat(segs).containsEntry("topic", "orders").containsEntry("logEndOffset", 10L);
    assertThat((List<Object>) segs.get("segments")).isNotEmpty();
    assertThat(get("/api/v1/segments/orders/9", READ).statusCode()).isEqualTo(404);

    HttpResponse<String> metrics = get("/metrics", READ);
    assertThat(metrics.statusCode()).isEqualTo(200);
    assertThat(metrics.headers().firstValue("Content-Type").orElse("")).startsWith("text/plain");
    assertThat(metrics.body())
        .contains("logforge_messages_in_total{topic=\"orders\"} 20")
        .contains("logforge_log_end_offset{partition=\"0\",topic=\"orders\"} 10")
        .contains("logforge_high_watermark{partition=\"1\",topic=\"orders\"} 10")
        .contains("logforge_isr_size{partition=\"0\",topic=\"orders\"} 1")
        .contains("logforge_under_replicated_partitions 0")
        .contains("logforge_produce_latency_seconds_count{acks=\"1\"} 20");
  }

  @Test
  void corsAllowsOnlyListedOriginsAndNeverAWildcard() throws Exception {
    HttpRequest allowed =
        HttpRequest.newBuilder(URI.create(base + "/api/v1/cluster"))
            .header("Origin", "https://logforge.example")
            .header("Authorization", "Bearer " + READ)
            .GET()
            .build();
    HttpResponse<String> ok = http.send(allowed, HttpResponse.BodyHandlers.ofString());
    assertThat(ok.headers().firstValue("Access-Control-Allow-Origin"))
        .hasValue("https://logforge.example");

    HttpRequest evil =
        HttpRequest.newBuilder(URI.create(base + "/api/v1/cluster"))
            .header("Origin", "https://evil.example")
            .header("Authorization", "Bearer " + READ)
            .GET()
            .build();
    HttpResponse<String> no = http.send(evil, HttpResponse.BodyHandlers.ofString());
    assertThat(no.headers().firstValue("Access-Control-Allow-Origin")).isEmpty();

    HttpRequest preflight =
        HttpRequest.newBuilder(URI.create(base + "/api/v1/cluster"))
            .header("Origin", "http://localhost:3000")
            .header("Access-Control-Request-Method", "GET")
            .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
            .build();
    HttpResponse<String> pre = http.send(preflight, HttpResponse.BodyHandlers.ofString());
    assertThat(pre.statusCode()).isEqualTo(204);
    assertThat(pre.headers().firstValue("Access-Control-Allow-Origin"))
        .hasValue("http://localhost:3000");
    assertThat(pre.headers().allValues("Access-Control-Allow-Origin")).doesNotContain("*");
  }

  @Test
  void oversizedBodiesAreRejected() throws Exception {
    String huge = "{\"name\":\"" + "x".repeat(70_000) + "\"}";
    assertThat(send("POST", "/api/v1/topics", ADMIN, huge).statusCode()).isEqualTo(413);
  }

  @Test
  void rateLimitsReturn429WithRetryAfter() throws Exception {
    node.close();
    node = Node.start(config(dir).ratePerIp(2).ratePerToken(1_000).build());
    base = "http://127.0.0.1:" + node.adminPort();
    int limited = 0;
    HttpResponse<String> last = null;
    for (int i = 0; i < 20; i++) {
      HttpResponse<String> r = get("/api/v1/cluster", READ);
      if (r.statusCode() == 429) {
        limited++;
        last = r;
      }
    }
    assertThat(limited).isGreaterThan(0);
    assertThat(last.headers().firstValue("Retry-After")).isPresent();
    assertThat(Integer.parseInt(last.headers().firstValue("Retry-After").orElseThrow()))
        .isPositive();
    assertThat(Json.parseObject(last.body())).containsEntry("error", "rate_limited");
  }
}
