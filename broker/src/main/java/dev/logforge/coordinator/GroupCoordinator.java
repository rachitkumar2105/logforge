package dev.logforge.coordinator;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.TopicPartition;
import dev.logforge.controller.ControllerChannel;
import dev.logforge.log.Log;
import dev.logforge.log.Record;
import dev.logforge.log.RecordBatch;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Groups;
import dev.logforge.network.protocol.InvalidRequestException;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.ProtocolReader;
import dev.logforge.network.protocol.ProtocolWriter;
import dev.logforge.network.protocol.Topics;
import dev.logforge.replication.Partition;
import dev.logforge.replication.ReplicaManager;
import dev.logforge.util.JsonLog;
import dev.logforge.util.Murmur2;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Consumer group coordinator for the groups whose offsets partition this broker leads.
 *
 * <h2>Where a group lives</h2>
 *
 * Group {@code g} belongs to partition {@code murmur2(g) % N} of {@code __consumer_offsets}; the
 * leader of that partition is its coordinator. Committed offsets are records in that compacted
 * partition, keyed {@code group \0 topic \0 partition}, so they are replicated like any data and a
 * new coordinator rebuilds them by reading the partition after a failover. Membership itself is
 * kept in memory: after a coordinator move, members get UNKNOWN_MEMBER_ID and rejoin.
 *
 * <h2>Rebalance protocol</h2>
 *
 * A join, leave or session expiry moves the group to <em>preparing</em>. Existing members learn
 * through their heartbeat (REBALANCE_IN_PROGRESS) and rejoin; the coordinator holds every JoinGroup
 * response until all known members have rejoined or the rebalance timeout passes (stragglers are
 * removed). It then bumps the generation, computes the assignment and answers all members at once.
 * Commits carry the generation, so a member that missed a rebalance cannot commit over partitions
 * that now belong to someone else.
 *
 * <h2>Rebalance storms</h2>
 *
 * Every membership change stops the whole group for one round. Mitigations here: one barrier per
 * round (late joiners wait for the next, they do not restart the current one), session timeouts
 * well above heartbeat intervals, and members that are blocked in JoinGroup are never expired for
 * missing heartbeats.
 */
public final class GroupCoordinator {
  private static final JsonLog LOG = JsonLog.get(GroupCoordinator.class);

  private enum State {
    EMPTY,
    PREPARING,
    STABLE
  }

  private static final class Member {
    final String id;
    List<String> topics;
    int sessionTimeoutMs;
    int rebalanceTimeoutMs;
    long lastHeartbeatMs;
    CompletableFuture<Groups.JoinResponse> pendingJoin;

    Member(String id) {
      this.id = id;
    }
  }

  private static final class Group {
    final String id;
    final int offsetsPartition;
    String assignor;
    int generation;
    State state = State.EMPTY;
    long rebalanceDeadlineMs;
    final Map<String, Member> members = new LinkedHashMap<>();
    Map<String, List<TopicPartition>> assignment = Map.of();

    Group(String id, int offsetsPartition) {
      this.id = id;
      this.offsetsPartition = offsetsPartition;
    }
  }

  private final int brokerId;
  private final ReplicaManager replicaManager;
  private final Supplier<ControllerApis.Snapshot> metadata;
  private final ControllerChannel controller;
  private final LongSupplier clock;
  private final Map<String, Group> groups = new ConcurrentHashMap<>();
  private final Map<String, Map<TopicPartition, Long>> offsets = new ConcurrentHashMap<>();
  private final Map<Integer, Integer> loadedEpoch = new ConcurrentHashMap<>();
  private final AtomicBoolean offsetsTopicRequested = new AtomicBoolean();

  public GroupCoordinator(
      int brokerId,
      ReplicaManager replicaManager,
      Supplier<ControllerApis.Snapshot> metadata,
      ControllerChannel controller,
      LongSupplier clock) {
    this.brokerId = brokerId;
    this.replicaManager = replicaManager;
    this.metadata = metadata;
    this.controller = controller;
    this.clock = clock;
  }

  // ------------------------------------------------------------------ routing

  private static ByteBuffer encode(Consumer<ProtocolWriter> body) {
    ProtocolWriter w = new ProtocolWriter(256);
    body.accept(w);
    return w.toBuffer();
  }

  private static <T> T parse(
      ProtocolReader r, java.util.function.Function<ProtocolReader, T> reader) {
    T t = reader.apply(r);
    r.expectEnd();
    return t;
  }

  /** Entry point from the request handler. */
  public CompletableFuture<ByteBuffer> handle(short apiKey, ProtocolReader body) {
    return switch (apiKey) {
      case ApiKeys.FIND_COORDINATOR ->
          CompletableFuture.completedFuture(
              encode(findCoordinator(parse(body, Groups.FindCoordinatorRequest::read))::write));
      case ApiKeys.JOIN_GROUP ->
          join(parse(body, Groups.JoinRequest::read)).thenApply(r -> encode(r::write));
      case ApiKeys.HEARTBEAT ->
          CompletableFuture.completedFuture(
              encode(
                  new Groups.ErrorResponse(
                          heartbeat(parse(body, Groups.HeartbeatRequest::read)).code())
                      ::write));
      case ApiKeys.LEAVE_GROUP ->
          CompletableFuture.completedFuture(
              encode(
                  new Groups.ErrorResponse(leave(parse(body, Groups.LeaveRequest::read)).code())
                      ::write));
      case ApiKeys.OFFSET_COMMIT ->
          commit(parse(body, Groups.OffsetCommitRequest::read)).thenApply(r -> encode(r::write));
      case ApiKeys.OFFSET_FETCH ->
          CompletableFuture.completedFuture(
              encode(fetchOffsets(parse(body, Groups.OffsetFetchRequest::read))::write));
      default -> throw new InvalidRequestException("not a group api: " + apiKey);
    };
  }

  private int offsetsPartitionCount() {
    ControllerApis.Snapshot s = metadata.get();
    if (s != null) {
      for (ControllerApis.TopicInfo t : s.topics()) {
        if (t.name().equals(TopicPartition.OFFSETS_TOPIC)) {
          return t.partitions().size();
        }
      }
    }
    return -1;
  }

  private int partitionFor(String groupId, int count) {
    return Murmur2.partitionFor(groupId.getBytes(UTF_8), count);
  }

  /**
   * Returns the offsets partition if this broker leads it and has loaded it, otherwise null. Loads
   * (or reloads after a leadership change) on first use.
   */
  private Partition coordinatorPartition(String groupId) {
    int count = offsetsPartitionCount();
    if (count <= 0) {
      return null;
    }
    int p = partitionFor(groupId, count);
    Partition partition =
        replicaManager.partition(new TopicPartition(TopicPartition.OFFSETS_TOPIC, p));
    if (partition == null || !partition.isLeader() || partition.log() == null) {
      return null;
    }
    ensureLoaded(p, partition);
    return partition;
  }

  /** Rebuilds the offset cache for one offsets partition from its log. */
  private synchronized void ensureLoaded(int p, Partition partition) {
    Integer epoch = loadedEpoch.get(p);
    if (epoch != null && epoch == partition.leaderEpoch()) {
      return;
    }
    int count = offsetsPartitionCount();
    groups.values().removeIf(g -> g.offsetsPartition == p);
    offsets.keySet().removeIf(g -> partitionFor(g, count) == p);
    Log log = partition.log();
    long next = log.logStartOffset();
    long end = log.logEndOffset();
    int records = 0;
    while (next < end) {
      List<RecordBatch> batches = RecordBatch.iterate(log.read(next, 1 << 20, end));
      if (batches.isEmpty()) {
        break;
      }
      for (RecordBatch b : batches) {
        for (Record r : b.records()) {
          if (r.key() == null) {
            continue;
          }
          String[] parts = new String(r.key(), UTF_8).split("\u0000", -1);
          if (parts.length != 3) {
            continue;
          }
          TopicPartition tp = new TopicPartition(parts[1], Integer.parseInt(parts[2]));
          if (r.value() == null) {
            Map<TopicPartition, Long> m = offsets.get(parts[0]);
            if (m != null) {
              m.remove(tp);
            }
          } else if (r.value().length == 8) {
            offsets
                .computeIfAbsent(parts[0], k -> new ConcurrentHashMap<>())
                .put(tp, ByteBuffer.wrap(r.value()).getLong());
          }
          records++;
        }
        next = b.lastOffset() + 1;
      }
    }
    loadedEpoch.put(p, partition.leaderEpoch());
    LOG.info(
        "loaded consumer offsets",
        "partition",
        p,
        "records",
        records,
        "leaderEpoch",
        partition.leaderEpoch());
  }

  private Group group(String id) {
    int count = offsetsPartitionCount();
    return groups.computeIfAbsent(id, g -> new Group(g, partitionFor(g, count)));
  }

  // ------------------------------------------------------------------ FindCoordinator

  private Groups.FindCoordinatorResponse findCoordinator(Groups.FindCoordinatorRequest req) {
    ControllerApis.Snapshot s = metadata.get();
    short unavailable = Errors.COORDINATOR_NOT_AVAILABLE.code();
    int count = offsetsPartitionCount();
    if (s == null || count <= 0) {
      if (controller != null && offsetsTopicRequested.compareAndSet(false, true)) {
        controller
            .createTopics(
                new Topics.CreateRequest(
                    10_000,
                    List.of(
                        new Topics.NewTopic(TopicPartition.OFFSETS_TOPIC, 0, (short) 0, Map.of()))))
            .whenComplete((r, e) -> offsetsTopicRequested.set(false));
      }
      return new Groups.FindCoordinatorResponse(unavailable, -1, null, -1);
    }
    int p = partitionFor(req.groupId(), count);
    int leader = -1;
    for (ControllerApis.TopicInfo t : s.topics()) {
      if (t.name().equals(TopicPartition.OFFSETS_TOPIC)) {
        leader = t.partitions().get(p).leader();
      }
    }
    for (ControllerApis.BrokerInfo b : s.brokers()) {
      if (b.nodeId() == leader && b.alive()) {
        return new Groups.FindCoordinatorResponse(
            Errors.NONE.code(), b.nodeId(), b.host(), b.port());
      }
    }
    return new Groups.FindCoordinatorResponse(unavailable, -1, null, -1);
  }

  // ------------------------------------------------------------------ membership

  /** Group ids become part of offset-record keys; keep them printable and separator-free. */
  static boolean isValidGroupId(String id) {
    if (id == null || id.isEmpty() || id.length() > 255) {
      return false;
    }
    for (int i = 0; i < id.length(); i++) {
      if (Character.isISOControl(id.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private CompletableFuture<Groups.JoinResponse> join(Groups.JoinRequest req) {
    if (!isValidGroupId(req.groupId())) {
      return CompletableFuture.completedFuture(
          Groups.JoinResponse.error(Errors.INVALID_REQUEST.code(), req.memberId()));
    }
    if (!Assignors.isKnown(req.assignor())) {
      return CompletableFuture.completedFuture(
          Groups.JoinResponse.error(Errors.INVALID_REQUEST.code(), req.memberId()));
    }
    if (coordinatorPartition(req.groupId()) == null) {
      return CompletableFuture.completedFuture(
          Groups.JoinResponse.error(Errors.NOT_COORDINATOR.code(), req.memberId()));
    }
    Group g = group(req.groupId());
    synchronized (g) {
      if (g.assignor != null && !g.members.isEmpty() && !g.assignor.equals(req.assignor())) {
        return CompletableFuture.completedFuture(
            Groups.JoinResponse.error(Errors.INCONSISTENT_ASSIGNOR.code(), req.memberId()));
      }
      String memberId = req.memberId();
      if (!memberId.isEmpty() && !g.members.containsKey(memberId)) {
        return CompletableFuture.completedFuture(
            Groups.JoinResponse.error(Errors.UNKNOWN_MEMBER_ID.code(), memberId));
      }
      if (memberId.isEmpty()) {
        memberId = "member-" + UUID.randomUUID();
      }
      Member m = g.members.computeIfAbsent(memberId, Member::new);
      m.topics = List.copyOf(req.topics());
      m.sessionTimeoutMs = Math.max(100, req.sessionTimeoutMs());
      m.rebalanceTimeoutMs = Math.max(100, req.rebalanceTimeoutMs());
      m.lastHeartbeatMs = clock.getAsLong();
      if (m.pendingJoin != null && !m.pendingJoin.isDone()) {
        m.pendingJoin.complete(
            Groups.JoinResponse.error(Errors.REBALANCE_IN_PROGRESS.code(), memberId));
      }
      CompletableFuture<Groups.JoinResponse> future = new CompletableFuture<>();
      m.pendingJoin = future;
      if (g.members.size() == 1) {
        g.assignor = req.assignor();
      }
      if (g.state != State.PREPARING) {
        startRebalance(g, "member " + memberId + " joined");
      }
      tryCompleteJoin(g);
      return future;
    }
  }

  /** Caller holds the group's monitor. */
  private void startRebalance(Group g, String reason) {
    g.state = State.PREPARING;
    int timeout = 0;
    for (Member m : g.members.values()) {
      timeout = Math.max(timeout, m.rebalanceTimeoutMs);
    }
    g.rebalanceDeadlineMs = clock.getAsLong() + timeout;
    LOG.info("rebalance started", "group", g.id, "generation", g.generation, "reason", reason);
  }

  private void tryCompleteJoin(Group g) {
    if (g.state != State.PREPARING) {
      return;
    }
    for (Member m : g.members.values()) {
      if (m.pendingJoin == null) {
        return;
      }
    }
    completeJoin(g);
  }

  private void completeJoin(Group g) {
    if (g.members.isEmpty()) {
      g.state = State.EMPTY;
      return;
    }
    g.generation++;
    g.state = State.STABLE;
    Map<String, List<String>> subs = new LinkedHashMap<>();
    g.members.values().forEach(m -> subs.put(m.id, m.topics));
    g.assignment = Assignors.assign(g.assignor, subs, partitionCounts());
    String leaderId = g.members.keySet().iterator().next();
    List<String> memberIds = List.copyOf(g.members.keySet());
    long now = clock.getAsLong();
    for (Member m : g.members.values()) {
      Map<String, List<Integer>> byTopic = new LinkedHashMap<>();
      for (TopicPartition tp : g.assignment.getOrDefault(m.id, List.of())) {
        byTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition());
      }
      List<Groups.TopicAssignment> assignment = new ArrayList<>();
      byTopic.forEach((t, ps) -> assignment.add(new Groups.TopicAssignment(t, ps)));
      m.lastHeartbeatMs = now;
      CompletableFuture<Groups.JoinResponse> f = m.pendingJoin;
      m.pendingJoin = null;
      f.complete(
          new Groups.JoinResponse(
              Errors.NONE.code(), g.generation, g.assignor, leaderId, m.id, memberIds, assignment));
    }
    LOG.info(
        "rebalance complete",
        "group",
        g.id,
        "generation",
        g.generation,
        "members",
        memberIds.size());
  }

  private Map<String, Integer> partitionCounts() {
    Map<String, Integer> out = new HashMap<>();
    ControllerApis.Snapshot s = metadata.get();
    if (s != null) {
      s.topics().forEach(t -> out.put(t.name(), t.partitions().size()));
    }
    return out;
  }

  private Errors heartbeat(Groups.HeartbeatRequest req) {
    if (coordinatorPartition(req.groupId()) == null) {
      return Errors.NOT_COORDINATOR;
    }
    Group g = groups.get(req.groupId());
    if (g == null) {
      return Errors.UNKNOWN_MEMBER_ID;
    }
    synchronized (g) {
      Member m = g.members.get(req.memberId());
      if (m == null) {
        return Errors.UNKNOWN_MEMBER_ID;
      }
      m.lastHeartbeatMs = clock.getAsLong();
      if (req.generationId() != g.generation) {
        return Errors.ILLEGAL_GENERATION;
      }
      return g.state == State.PREPARING ? Errors.REBALANCE_IN_PROGRESS : Errors.NONE;
    }
  }

  private Errors leave(Groups.LeaveRequest req) {
    if (coordinatorPartition(req.groupId()) == null) {
      return Errors.NOT_COORDINATOR;
    }
    Group g = groups.get(req.groupId());
    if (g == null) {
      return Errors.UNKNOWN_MEMBER_ID;
    }
    synchronized (g) {
      Member m = g.members.remove(req.memberId());
      if (m == null) {
        return Errors.UNKNOWN_MEMBER_ID;
      }
      if (m.pendingJoin != null && !m.pendingJoin.isDone()) {
        m.pendingJoin.complete(Groups.JoinResponse.error(Errors.UNKNOWN_MEMBER_ID.code(), m.id));
      }
      LOG.info("member left", "group", g.id, "member", m.id);
      if (g.members.isEmpty()) {
        g.state = State.EMPTY;
      } else {
        if (g.state != State.PREPARING) {
          startRebalance(g, "member left");
        }
        tryCompleteJoin(g);
      }
      return Errors.NONE;
    }
  }

  /** Expires silent members and finishes rebalances whose timeout passed. Called every ~100 ms. */
  public void tick() {
    long now = clock.getAsLong();
    // Forget groups with no members; their committed offsets stay in the offsets map and log.
    groups
        .values()
        .removeIf(
            g -> {
              synchronized (g) {
                return g.state == State.EMPTY && g.members.isEmpty();
              }
            });
    for (Group g : groups.values()) {
      synchronized (g) {
        List<String> expired = new ArrayList<>();
        for (Member m : g.members.values()) {
          // A member blocked in JoinGroup cannot heartbeat; it is bounded by the rebalance timeout.
          if (m.pendingJoin == null && now - m.lastHeartbeatMs > m.sessionTimeoutMs) {
            expired.add(m.id);
          }
        }
        for (String id : expired) {
          g.members.remove(id);
          LOG.info("member session expired", "group", g.id, "member", id);
        }
        if (!expired.isEmpty()) {
          if (g.members.isEmpty()) {
            g.state = State.EMPTY;
          } else if (g.state != State.PREPARING) {
            startRebalance(g, "session expired");
          }
        }
        if (g.state == State.PREPARING && now >= g.rebalanceDeadlineMs) {
          g.members.values().removeIf(m -> m.pendingJoin == null);
          completeJoin(g);
        } else {
          tryCompleteJoin(g);
        }
      }
    }
  }

  // ------------------------------------------------------------------ offsets

  private static byte[] offsetKey(String group, TopicPartition tp) {
    return (group + "\u0000" + tp.topic() + "\u0000" + tp.partition()).getBytes(UTF_8);
  }

  private CompletableFuture<Groups.OffsetCommitResponse> commit(Groups.OffsetCommitRequest req) {
    if (!isValidGroupId(req.groupId())) {
      return CompletableFuture.completedFuture(uniform(req, Errors.INVALID_REQUEST));
    }
    Partition partition = coordinatorPartition(req.groupId());
    Errors check = partition == null ? Errors.NOT_COORDINATOR : validateCommit(req);
    if (check != Errors.NONE) {
      return CompletableFuture.completedFuture(uniform(req, check));
    }
    RecordBatch.Builder b = RecordBatch.builder(clock.getAsLong());
    Map<TopicPartition, Long> pending = new LinkedHashMap<>();
    for (Groups.CommitTopic t : req.topics()) {
      for (Groups.CommitPartition p : t.partitions()) {
        TopicPartition tp = new TopicPartition(t.name(), p.partition());
        pending.put(tp, p.offset());
        b.append(
            clock.getAsLong(),
            offsetKey(req.groupId(), tp),
            ByteBuffer.allocate(8).putLong(0, p.offset()).array());
      }
    }
    if (pending.isEmpty()) {
      return CompletableFuture.completedFuture(new Groups.OffsetCommitResponse(List.of()));
    }
    TopicPartition offsetsTp = partition.topicPartition();
    Produce.Request produce =
        new Produce.Request(
            (short) -1,
            10_000,
            List.of(
                new Produce.TopicData(
                    offsetsTp.topic(),
                    List.of(
                        new Produce.PartitionData(offsetsTp.partition(), b.build().buffer())))));
    return replicaManager
        .appendRecords(produce)
        .thenApply(
            resp -> {
              short code = resp.topics().get(0).partitions().get(0).errorCode();
              Errors e = Errors.forCode(code);
              if (e == Errors.NONE) {
                offsets
                    .computeIfAbsent(req.groupId(), k -> new ConcurrentHashMap<>())
                    .putAll(pending);
                return uniform(req, Errors.NONE);
              }
              Errors mapped =
                  e == Errors.NOT_LEADER_OR_FOLLOWER
                      ? Errors.NOT_COORDINATOR
                      : Errors.COORDINATOR_NOT_AVAILABLE;
              return uniform(req, mapped);
            });
  }

  private Errors validateCommit(Groups.OffsetCommitRequest req) {
    Group g = groups.get(req.groupId());
    if (req.generationId() < 0) {
      // A consumer that manages its own assignment. Allowed while no group membership exists.
      if (g == null) {
        return Errors.NONE;
      }
      synchronized (g) {
        return g.members.isEmpty() ? Errors.NONE : Errors.ILLEGAL_GENERATION;
      }
    }
    if (g == null) {
      return Errors.UNKNOWN_MEMBER_ID;
    }
    synchronized (g) {
      if (!g.members.containsKey(req.memberId())) {
        return Errors.UNKNOWN_MEMBER_ID;
      }
      // Commits from the current generation are accepted even while a rebalance is preparing:
      // the member still owns its partitions until it rejoins, and committing now avoids
      // re-delivering what it already processed.
      return req.generationId() == g.generation ? Errors.NONE : Errors.ILLEGAL_GENERATION;
    }
  }

  private static Groups.OffsetCommitResponse uniform(Groups.OffsetCommitRequest req, Errors e) {
    List<Groups.TopicErrors> topics = new ArrayList<>();
    for (Groups.CommitTopic t : req.topics()) {
      List<Groups.PartitionError> ps = new ArrayList<>();
      t.partitions().forEach(p -> ps.add(new Groups.PartitionError(p.partition(), e.code())));
      topics.add(new Groups.TopicErrors(t.name(), ps));
    }
    return new Groups.OffsetCommitResponse(topics);
  }

  private Groups.OffsetFetchResponse fetchOffsets(Groups.OffsetFetchRequest req) {
    if (coordinatorPartition(req.groupId()) == null) {
      return new Groups.OffsetFetchResponse(Errors.NOT_COORDINATOR.code(), List.of());
    }
    Map<TopicPartition, Long> committed = offsets.getOrDefault(req.groupId(), Map.of());
    List<Groups.FetchedTopic> out = new ArrayList<>();
    if (req.topics() == null) {
      Map<String, List<Groups.FetchedOffset>> byTopic = new LinkedHashMap<>();
      committed.forEach(
          (tp, off) ->
              byTopic
                  .computeIfAbsent(tp.topic(), k -> new ArrayList<>())
                  .add(new Groups.FetchedOffset(tp.partition(), off, Errors.NONE.code())));
      byTopic.forEach((t, ps) -> out.add(new Groups.FetchedTopic(t, ps)));
    } else {
      for (Groups.FetchTopic t : req.topics()) {
        List<Groups.FetchedOffset> ps = new ArrayList<>();
        for (int p : t.partitions()) {
          Long off = committed.get(new TopicPartition(t.name(), p));
          ps.add(new Groups.FetchedOffset(p, off == null ? -1L : off, Errors.NONE.code()));
        }
        out.add(new Groups.FetchedTopic(t.name(), ps));
      }
    }
    return new Groups.OffsetFetchResponse(Errors.NONE.code(), out);
  }

  // ------------------------------------------------------------------ admin view

  /** Groups this broker coordinates, with committed offsets, for the admin API. */
  public List<Map<String, Object>> status() {
    List<Map<String, Object>> out = new ArrayList<>();
    for (Group g : groups.values()) {
      Map<String, Object> m = new LinkedHashMap<>();
      synchronized (g) {
        m.put("groupId", g.id);
        m.put("coordinator", brokerId);
        m.put("state", g.state.name().toLowerCase(java.util.Locale.ROOT));
        m.put("generation", g.generation);
        m.put("members", g.members.size());
        m.put("assignor", g.assignor);
      }
      List<Map<String, Object>> committed = new ArrayList<>();
      offsets
          .getOrDefault(g.id, Map.of())
          .forEach(
              (tp, off) -> {
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("topic", tp.topic());
                c.put("partition", tp.partition());
                c.put("committed", off);
                committed.add(c);
              });
      m.put("committed", committed);
      out.add(m);
    }
    return out;
  }
}
