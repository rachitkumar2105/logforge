package dev.logforge.controller;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.TopicPartition;
import dev.logforge.log.LogConfig;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Topics;
import dev.logforge.util.JsonLog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * The single, fixed controller. Owns cluster metadata and decides partition leadership.
 *
 * <p>Known limitation (by design, see DECISIONS.md B6): there is exactly one controller and no
 * consensus protocol. If the controller process is down, brokers keep serving with the metadata
 * they last pulled, but no leader election, ISR change or topic creation can happen until it
 * returns. Its state survives restarts through {@code metadata.json}.
 *
 * <p>All public methods are synchronized: requests arrive from many handler threads and the
 * liveness ticker, and decisions must be made on one consistent view.
 */
public final class Controller {
  private static final JsonLog LOG = JsonLog.get(Controller.class);

  private final int nodeId;
  private final ClusterState state;
  private final LongSupplier clock;
  private final long sessionTimeoutMs;
  private final int offsetsPartitions;
  private final int offsetsReplicationFactor;
  private final boolean uncleanElection;
  private volatile String selfHost;
  private volatile int selfPort;

  public Controller(
      int nodeId,
      String clusterId,
      Path dir,
      LongSupplier clock,
      long sessionTimeoutMs,
      int offsetsPartitions,
      int offsetsReplicationFactor,
      boolean uncleanElection)
      throws IOException {
    this.nodeId = nodeId;
    this.state = new ClusterState(clusterId, dir);
    this.clock = clock;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.offsetsPartitions = offsetsPartitions;
    this.offsetsReplicationFactor = offsetsReplicationFactor;
    this.uncleanElection = uncleanElection;
  }

  /** The address clients use to reach the controller (published in metadata). */
  public void setSelfAddress(String host, int port) {
    this.selfHost = host;
    this.selfPort = port;
  }

  public int nodeId() {
    return nodeId;
  }

  public synchronized ControllerApis.Snapshot snapshot() {
    return state.snapshot(nodeId, selfHost, selfPort);
  }

  public synchronized long version() {
    return state.version;
  }

  // ------------------------------------------------------------------ brokers

  /** Registers or refreshes a broker and returns metadata if the broker's copy is stale. */
  public synchronized ControllerApis.HeartbeatResponse heartbeat(
      ControllerApis.HeartbeatRequest req) {
    long now = clock.getAsLong();
    ClusterState.BrokerRegistration b = state.brokers.get(req.brokerId());
    boolean changed = false;
    if (b == null) {
      b = new ClusterState.BrokerRegistration(req.brokerId());
      state.brokers.put(req.brokerId(), b);
      changed = true;
      LOG.info(
          "broker registered", "broker", req.brokerId(), "host", req.host(), "port", req.port());
    }
    if (b.epoch != req.brokerEpoch()) {
      if (b.alive && b.epoch != 0) {
        // Restarted faster than the session timeout: treat it as a failure first, so every
        // partition it led gets a new leader epoch and its old tail is truncated on return.
        LOG.warn("broker restarted", "broker", b.id);
        b.alive = false;
        onBrokersDied(List.of(b.id));
      }
      b.epoch = req.brokerEpoch();
      changed = true;
    }
    if (!b.alive) {
      b.alive = true;
      changed = true;
      LOG.info("broker alive", "broker", b.id);
    }
    if (!req.host().equals(b.host) || req.port() != b.port) {
      b.host = req.host();
      b.port = req.port();
      changed = true;
    }
    b.lastHeartbeatMs = now;
    if (req.status() != null) {
      ByteBuffer s = req.status().duplicate();
      byte[] bytes = new byte[s.remaining()];
      s.get(bytes);
      b.status = new String(bytes, UTF_8);
    }
    if (changed) {
      onBrokerChange(b.id);
      state.bumpVersion();
    }
    ByteBuffer snapshot = req.metadataVersion() < state.version ? snapshot().encode() : null;
    return new ControllerApis.HeartbeatResponse(Errors.NONE.code(), state.version, snapshot);
  }

  /**
   * A broker came (back) to life. Offline partitions whose last ISR includes it get it as leader.
   * With unclean election enabled, an offline partition takes any live replica.
   */
  private void onBrokerChange(int brokerId) {
    boolean changed = false;
    for (ClusterState.TopicState t : state.topics.values()) {
      for (ClusterState.PartitionState p : t.partitions) {
        if (p.leader >= 0 || !p.replicas.contains(brokerId)) {
          continue;
        }
        changed |= elect(t.name, p);
      }
    }
    if (changed) {
      commitQuietly();
    }
  }

  /**
   * Picks a leader for {@code p} from its live ISR members, in replica order. If none is alive the
   * partition goes offline (leader -1) and keeps its last ISR, so that when one of those brokers
   * returns it is elected and no committed record is lost. Only with unclean election enabled may
   * an out-of-sync replica be chosen: that replica lacks some committed records, which are then
   * lost for good (and consumers may see offsets reused with different data).
   *
   * @return true if the partition state changed
   */
  private boolean elect(String topic, ClusterState.PartitionState p) {
    List<Integer> liveIsr = new ArrayList<>();
    for (int r : p.replicas) {
      if (p.isr.contains(r) && state.isAlive(r)) {
        liveIsr.add(r);
      }
    }
    int oldLeader = p.leader;
    if (!liveIsr.isEmpty()) {
      p.leader = liveIsr.get(0);
      p.isr = List.copyOf(liveIsr);
    } else if (uncleanElection) {
      int chosen = -1;
      for (int r : p.replicas) {
        if (state.isAlive(r)) {
          chosen = r;
          break;
        }
      }
      if (chosen < 0) {
        if (oldLeader == -1) {
          return false;
        }
        p.leader = -1;
      } else {
        p.leader = chosen;
        p.isr = List.of(chosen);
        LOG.warn(
            "UNCLEAN leader election: committed records may be lost",
            "topic",
            topic,
            "partition",
            p.partition,
            "leader",
            chosen);
      }
    } else {
      if (oldLeader == -1) {
        return false; // still offline; keep the last ISR
      }
      p.leader = -1;
    }
    p.leaderEpoch++;
    p.partitionEpoch++;
    LOG.info(
        "leader elected",
        "topic",
        topic,
        "partition",
        p.partition,
        "leader",
        p.leader,
        "previousLeader",
        oldLeader,
        "leaderEpoch",
        p.leaderEpoch,
        "isr",
        p.isr);
    return true;
  }

  private void commitQuietly() {
    try {
      state.commit();
    } catch (IOException e) {
      LOG.error("could not persist metadata", e);
    }
  }

  /** Marks brokers dead after a missed session. Called periodically. */
  public synchronized void tick() {
    long now = clock.getAsLong();
    List<Integer> died = new ArrayList<>();
    for (ClusterState.BrokerRegistration b : state.brokers.values()) {
      if (b.alive && now - b.lastHeartbeatMs > sessionTimeoutMs) {
        b.alive = false;
        died.add(b.id);
        LOG.warn("broker session expired", "broker", b.id, "silentMs", now - b.lastHeartbeatMs);
      }
    }
    if (!died.isEmpty()) {
      onBrokersDied(died);
      state.bumpVersion();
    }
  }

  /**
   * Brokers stopped heartbeating. Partitions they led get a new leader from the ISR; partitions
   * where they were in-sync followers drop them from the ISR (so acks=all stops waiting on them).
   */
  private void onBrokersDied(List<Integer> died) {
    boolean changed = false;
    for (ClusterState.TopicState t : state.topics.values()) {
      for (ClusterState.PartitionState p : t.partitions) {
        if (died.contains(p.leader)) {
          changed |= elect(t.name, p);
        } else if (p.leader >= 0 && p.isr.stream().anyMatch(died::contains)) {
          List<Integer> isr = new ArrayList<>(p.isr);
          isr.removeAll(died);
          p.isr = List.copyOf(isr);
          p.partitionEpoch++;
          changed = true;
          LOG.info(
              "ISR shrunk by controller", "topic", t.name, "partition", p.partition, "isr", p.isr);
        }
      }
    }
    if (changed) {
      commitQuietly();
    }
  }

  /** Last status JSON each broker reported, for the admin API. */
  public synchronized Map<Integer, String> brokerStatuses() {
    Map<Integer, String> out = new LinkedHashMap<>();
    state.brokers.forEach((id, b) -> out.put(id, b.status));
    return out;
  }

  /** {@code id -> ms since its last heartbeat}. */
  public synchronized Map<Integer, Long> heartbeatAges() {
    long now = clock.getAsLong();
    Map<Integer, Long> out = new LinkedHashMap<>();
    state.brokers.forEach((id, b) -> out.put(id, now - b.lastHeartbeatMs));
    return out;
  }

  // ------------------------------------------------------------------ topics

  public synchronized Topics.CreateResponse createTopics(Topics.CreateRequest req) {
    List<Topics.CreateResult> results = new ArrayList<>();
    for (Topics.NewTopic t : req.topics()) {
      results.add(createTopic(t));
    }
    return new Topics.CreateResponse(results);
  }

  private Topics.CreateResult createTopic(Topics.NewTopic t) {
    String name = t.name();
    int partitions = t.numPartitions();
    int rf = t.replicationFactor();
    Map<String, String> configs = t.configs();
    if (TopicPartition.OFFSETS_TOPIC.equals(name)) {
      // Internal topic: the controller decides its shape regardless of what was asked.
      partitions = offsetsPartitions;
      rf = Math.max(1, Math.min(offsetsReplicationFactor, state.liveBrokerIds().size()));
      configs =
          Map.of("cleanup.policy", "compact", "segment.bytes", Integer.toString(16 * 1024 * 1024));
    } else if (!TopicPartition.isValidTopicName(name) || TopicPartition.isInternal(name)) {
      return result(
          name,
          Errors.INVALID_TOPIC,
          "topic names are [A-Za-z0-9._-]{1,200} and may not start with __");
    }
    if (state.topics.containsKey(name)) {
      return result(name, Errors.TOPIC_ALREADY_EXISTS, null);
    }
    if (partitions < 1 || partitions > 1024) {
      return result(name, Errors.INVALID_PARTITIONS, "partitions must be 1..1024");
    }
    List<Integer> live = state.liveBrokerIds();
    if (rf < 1 || rf > live.size()) {
      return result(
          name,
          Errors.INVALID_REPLICATION_FACTOR,
          "replication factor " + rf + " with " + live.size() + " live brokers");
    }
    try {
      LogConfig.defaults().withTopicConfigs(configs);
    } catch (IllegalArgumentException e) {
      return result(name, Errors.INVALID_REQUEST, e.getMessage());
    }
    ClusterState.TopicState ts = new ClusterState.TopicState(name, configs);
    int start = ThreadLocalRandom.current().nextInt(live.size());
    for (int p = 0; p < partitions; p++) {
      List<Integer> replicas = new ArrayList<>();
      for (int i = 0; i < rf; i++) {
        replicas.add(live.get((start + p + i) % live.size()));
      }
      ts.partitions.add(
          new ClusterState.PartitionState(p, replicas, replicas.get(0), 0, 0, replicas));
    }
    state.topics.put(name, ts);
    try {
      state.commit();
    } catch (IOException e) {
      state.topics.remove(name);
      LOG.error("could not persist metadata", e);
      return result(name, Errors.UNKNOWN_SERVER_ERROR, null);
    }
    LOG.info("topic created", "topic", name, "partitions", partitions, "replicationFactor", rf);
    return result(name, Errors.NONE, null);
  }

  private static Topics.CreateResult result(String name, Errors e, String message) {
    return new Topics.CreateResult(name, e.code(), message);
  }

  public synchronized Topics.DeleteResponse deleteTopics(Topics.DeleteRequest req) {
    List<Topics.DeleteResult> results = new ArrayList<>();
    boolean changed = false;
    for (String name : req.names()) {
      if (TopicPartition.isInternal(name)) {
        results.add(new Topics.DeleteResult(name, Errors.INVALID_TOPIC.code()));
      } else if (state.topics.remove(name) == null) {
        results.add(new Topics.DeleteResult(name, Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
      } else {
        changed = true;
        results.add(new Topics.DeleteResult(name, Errors.NONE.code()));
        LOG.info("topic deleted", "topic", name);
      }
    }
    if (changed) {
      try {
        state.commit();
      } catch (IOException e) {
        LOG.error("could not persist metadata", e);
      }
    }
    return new Topics.DeleteResponse(results);
  }

  // ------------------------------------------------------------------ ISR

  /**
   * Accepts an ISR change from a partition leader if the leader is current (same leader epoch and
   * partition epoch), the new ISR includes the leader, contains only assigned replicas, and every
   * added member is alive.
   */
  public synchronized ControllerApis.AlterIsrResponse alterIsr(ControllerApis.AlterIsrRequest req) {
    List<ControllerApis.AlterTopicResult> topics = new ArrayList<>();
    boolean changed = false;
    for (ControllerApis.AlterTopic t : req.topics()) {
      List<ControllerApis.AlterResult> parts = new ArrayList<>();
      ClusterState.TopicState ts = state.topics.get(t.name());
      for (ControllerApis.AlterPartition ap : t.partitions()) {
        ClusterState.PartitionState ps =
            ts == null || ap.partition() < 0 || ap.partition() >= ts.partitions.size()
                ? null
                : ts.partitions.get(ap.partition());
        Errors err = validateAlter(req.brokerId(), ap, ps);
        if (err == Errors.NONE) {
          ps.isr = List.copyOf(ap.newIsr());
          ps.partitionEpoch++;
          changed = true;
          LOG.info(
              "ISR updated",
              "topic",
              t.name(),
              "partition",
              ap.partition(),
              "isr",
              ps.isr,
              "partitionEpoch",
              ps.partitionEpoch);
        }
        parts.add(
            ps == null
                ? new ControllerApis.AlterResult(ap.partition(), err.code(), -1, -1, List.of())
                : new ControllerApis.AlterResult(
                    ap.partition(), err.code(), ps.leaderEpoch, ps.partitionEpoch, ps.isr));
      }
      topics.add(new ControllerApis.AlterTopicResult(t.name(), parts));
    }
    if (changed) {
      try {
        state.commit();
      } catch (IOException e) {
        LOG.error("could not persist metadata", e);
      }
    }
    return new ControllerApis.AlterIsrResponse(topics);
  }

  private Errors validateAlter(
      int brokerId, ControllerApis.AlterPartition ap, ClusterState.PartitionState ps) {
    if (ps == null) {
      return Errors.UNKNOWN_TOPIC_OR_PARTITION;
    }
    if (ps.leader != brokerId) {
      return Errors.NOT_LEADER_OR_FOLLOWER;
    }
    if (ap.leaderEpoch() != ps.leaderEpoch || ap.partitionEpoch() != ps.partitionEpoch) {
      return Errors.FENCED_LEADER_EPOCH;
    }
    Set<Integer> proposed = new HashSet<>(ap.newIsr());
    if (!proposed.contains(brokerId) || !ps.replicas.containsAll(proposed)) {
      return Errors.INVALID_REQUEST;
    }
    for (int id : proposed) {
      if (!ps.isr.contains(id) && !state.isAlive(id)) {
        return Errors.INVALID_REQUEST;
      }
    }
    return Errors.NONE;
  }
}
