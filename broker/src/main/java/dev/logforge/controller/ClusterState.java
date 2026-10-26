package dev.logforge.controller;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.util.Json;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The controller's authoritative view: topics, partition assignments, leaders, ISRs and epochs,
 * plus the live-broker registry. Topics and epochs are persisted to {@code metadata.json} with
 * write-temp, fsync, rename, so a controller restart never moves epochs backwards. Broker liveness
 * is not persisted: brokers re-register by heartbeating.
 *
 * <p>Not thread-safe; {@link Controller} serialises access.
 */
final class ClusterState {

  static final class BrokerRegistration {
    final int id;
    String host;
    int port;
    long epoch;
    long lastHeartbeatMs;
    boolean alive;

    /** Reported a storage failure: not eligible for leadership until it restarts. */
    boolean fenced;

    boolean usable() {
      return alive && !fenced;
    }

    String status = "{}";

    BrokerRegistration(int id) {
      this.id = id;
    }
  }

  static final class PartitionState {
    final int partition;
    final List<Integer> replicas;
    int leader;
    int leaderEpoch;
    int partitionEpoch;
    List<Integer> isr;

    PartitionState(
        int partition,
        List<Integer> replicas,
        int leader,
        int leaderEpoch,
        int partitionEpoch,
        List<Integer> isr) {
      this.partition = partition;
      this.replicas = List.copyOf(replicas);
      this.leader = leader;
      this.leaderEpoch = leaderEpoch;
      this.partitionEpoch = partitionEpoch;
      this.isr = List.copyOf(isr);
    }
  }

  static final class TopicState {
    final String name;
    final Map<String, String> configs;
    final List<PartitionState> partitions = new ArrayList<>();

    TopicState(String name, Map<String, String> configs) {
      this.name = name;
      this.configs = new LinkedHashMap<>(configs);
    }
  }

  final String clusterId;
  final Map<Integer, BrokerRegistration> brokers = new TreeMap<>();
  final Map<String, TopicState> topics = new TreeMap<>();
  long version;
  private final Path file;

  ClusterState(String clusterId, Path dir) throws IOException {
    this.clusterId = clusterId;
    Files.createDirectories(dir);
    this.file = dir.resolve("metadata.json");
    load();
  }

  @SuppressWarnings("unchecked")
  private void load() throws IOException {
    if (!Files.exists(file)) {
      version = 1;
      return;
    }
    Map<String, Object> root = Json.parseObject(Files.readString(file, UTF_8));
    // Start above anything a broker may already hold, so every broker re-pulls the snapshot.
    version = ((Number) root.get("version")).longValue() + 1;
    for (Object t : (List<Object>) root.get("topics")) {
      Map<String, Object> tm = (Map<String, Object>) t;
      Map<String, String> configs = new LinkedHashMap<>();
      ((Map<String, Object>) tm.get("configs"))
          .forEach((k, v) -> configs.put(k, String.valueOf(v)));
      TopicState ts = new TopicState((String) tm.get("name"), configs);
      for (Object p : (List<Object>) tm.get("partitions")) {
        Map<String, Object> pm = (Map<String, Object>) p;
        ts.partitions.add(
            new PartitionState(
                num(pm.get("partition")),
                ints(pm.get("replicas")),
                num(pm.get("leader")),
                num(pm.get("leaderEpoch")),
                num(pm.get("partitionEpoch")),
                ints(pm.get("isr"))));
      }
      topics.put(ts.name, ts);
    }
  }

  private static int num(Object o) {
    return ((Number) o).intValue();
  }

  @SuppressWarnings("unchecked")
  private static List<Integer> ints(Object o) {
    List<Integer> out = new ArrayList<>();
    for (Object x : (List<Object>) o) {
      out.add(((Number) x).intValue());
    }
    return out;
  }

  /** Bumps the version and persists topics atomically. */
  void commit() throws IOException {
    version++;
    List<Object> ts = new ArrayList<>();
    for (TopicState t : topics.values()) {
      List<Object> ps = new ArrayList<>();
      for (PartitionState p : t.partitions) {
        ps.add(
            Json.obj(
                "partition", p.partition,
                "replicas", p.replicas,
                "leader", p.leader,
                "leaderEpoch", p.leaderEpoch,
                "partitionEpoch", p.partitionEpoch,
                "isr", p.isr));
      }
      ts.add(Json.obj("name", t.name, "configs", t.configs, "partitions", ps));
    }
    String json = Json.write(Json.obj("clusterId", clusterId, "version", version, "topics", ts));
    Path tmp = file.resolveSibling("metadata.json.tmp");
    Files.writeString(tmp, json, UTF_8);
    try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
      ch.force(true);
    }
    Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  /** Version bump for changes that are not persisted (broker liveness). */
  void bumpVersion() {
    version++;
  }

  List<Integer> liveBrokerIds() {
    List<Integer> ids = new ArrayList<>();
    brokers.forEach(
        (id, b) -> {
          if (b.usable()) {
            ids.add(id);
          }
        });
    return ids;
  }

  boolean isAlive(int id) {
    BrokerRegistration b = brokers.get(id);
    return b != null && b.usable();
  }

  ControllerApis.Snapshot snapshot(int controllerId, String controllerHost, int controllerPort) {
    List<ControllerApis.BrokerInfo> bs = new ArrayList<>();
    for (BrokerRegistration b : brokers.values()) {
      bs.add(new ControllerApis.BrokerInfo(b.id, b.host, b.port, b.usable()));
    }
    if (!brokers.containsKey(controllerId) && controllerHost != null) {
      bs.add(new ControllerApis.BrokerInfo(controllerId, controllerHost, controllerPort, true));
    }
    List<ControllerApis.TopicInfo> ts = new ArrayList<>();
    for (TopicState t : topics.values()) {
      List<ControllerApis.PartitionInfo> ps = new ArrayList<>();
      for (PartitionState p : t.partitions) {
        ps.add(
            new ControllerApis.PartitionInfo(
                p.partition, p.leader, p.leaderEpoch, p.partitionEpoch, p.replicas, p.isr));
      }
      ts.add(new ControllerApis.TopicInfo(t.name, Map.copyOf(t.configs), ps));
    }
    return new ControllerApis.Snapshot(version, clusterId, controllerId, bs, ts);
  }
}
