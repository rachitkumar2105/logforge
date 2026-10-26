package dev.logforge.client;

import dev.logforge.TopicPartition;
import dev.logforge.network.BlockingChannel;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Groups;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A consumer's membership in a group: finds the coordinator, joins (the coordinator holds the join
 * until the whole group has rejoined), heartbeats on a background thread, and commits offsets
 * tagged with the generation it was assigned in.
 *
 * <p>The heartbeat thread only raises a flag; the rejoin happens on the polling thread inside
 * {@link Consumer#poll}, so the assignment never changes underneath records being processed. Before
 * rejoining, the member commits what it has consumed so far, which keeps re-delivery after a
 * rebalance to the records that were fetched but not yet handed out.
 */
final class GroupMembership {
  private final Consumer consumer;
  private final ConsumerConfig config;
  private final List<String> topics;
  private volatile String memberId = "";
  private volatile int generation = -1;
  private volatile boolean rejoinNeeded = true;
  private volatile boolean joining;
  private volatile boolean stopped;
  private BlockingChannel coordinator;
  private BlockingChannel heartbeatChannel;
  private Thread heartbeatThread;

  GroupMembership(Consumer consumer, List<String> topics) {
    this.consumer = consumer;
    this.config = consumer.config();
    this.topics = List.copyOf(topics);
  }

  String memberId() {
    return memberId;
  }

  int generation() {
    return generation;
  }

  /** Joins (or rejoins) until the member holds a current assignment. */
  void ensureActive() {
    long deadline =
        System.currentTimeMillis() + config.rebalanceTimeoutMs() + config.requestTimeoutMs();
    while (rejoinNeeded && !stopped) {
      if (System.currentTimeMillis() > deadline) {
        throw new ClientException(
            Errors.REQUEST_TIMED_OUT, "could not join group " + config.groupId());
      }
      joinOnce();
    }
  }

  /** Called at the start of every poll. */
  void poll() {
    if (rejoinNeeded && !stopped) {
      ensureActive();
    }
  }

  private synchronized BlockingChannel coordinator() {
    if (coordinator != null) {
      return coordinator;
    }
    long deadline = System.currentTimeMillis() + config.requestTimeoutMs();
    while (System.currentTimeMillis() < deadline && !stopped) {
      try {
        Groups.FindCoordinatorResponse r =
            Groups.FindCoordinatorResponse.read(
                consumer
                    .metadata()
                    .anyChannel()
                    .request(
                        ApiKeys.FIND_COORDINATOR,
                        w -> new Groups.FindCoordinatorRequest(config.groupId()).write(w)));
        if (r.errorCode() == Errors.NONE.code()) {
          int timeout = config.rebalanceTimeoutMs() + config.requestTimeoutMs();
          coordinator = new BlockingChannel(r.host(), r.port(), config.clientId(), timeout);
          heartbeatChannel =
              new BlockingChannel(
                  r.host(), r.port(), config.clientId() + "-hb", config.requestTimeoutMs());
          return coordinator;
        }
      } catch (IOException | RuntimeException e) {
        try {
          consumer.metadata().refresh();
        } catch (RuntimeException ignored) {
          // retry below
        }
      }
      sleep(100);
    }
    throw new ClientException(
        Errors.COORDINATOR_NOT_AVAILABLE, "no coordinator for " + config.groupId());
  }

  private synchronized void coordinatorLost() {
    if (coordinator != null) {
      coordinator.close();
      coordinator = null;
    }
    if (heartbeatChannel != null) {
      heartbeatChannel.close();
      heartbeatChannel = null;
    }
  }

  private void joinOnce() {
    if (generation >= 0) {
      try {
        commit(consumer.positions()); // hand over cleanly: commit before giving partitions up
      } catch (RuntimeException e) {
        // best effort; the partitions' new owner re-reads from the last successful commit
      }
    }
    Groups.JoinResponse r;
    joining = true;
    try {
      Groups.JoinRequest req =
          new Groups.JoinRequest(
              config.groupId(),
              config.sessionTimeoutMs(),
              config.rebalanceTimeoutMs(),
              memberId,
              config.assignor(),
              topics);
      r = Groups.JoinResponse.read(coordinator().request(ApiKeys.JOIN_GROUP, req::write));
    } catch (IOException | RuntimeException e) {
      coordinatorLost();
      sleep(100);
      return;
    } finally {
      joining = false;
    }
    Errors err = Errors.forCode(r.errorCode());
    switch (err) {
      case NONE -> {
        memberId = r.memberId();
        generation = r.generationId();
        List<TopicPartition> assigned = new ArrayList<>();
        for (Groups.TopicAssignment a : r.assignment()) {
          a.partitions().forEach(p -> assigned.add(new TopicPartition(a.topic(), p)));
        }
        consumer.setAssignment(assigned);
        rejoinNeeded = false;
        startHeartbeats();
      }
      case UNKNOWN_MEMBER_ID -> {
        memberId = "";
        generation = -1;
      }
      case NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE -> {
        coordinatorLost();
        sleep(100);
      }
      case REBALANCE_IN_PROGRESS -> sleep(20);
      default -> throw new ClientException(err, "join group " + config.groupId());
    }
  }

  private synchronized void startHeartbeats() {
    if (heartbeatThread != null) {
      return;
    }
    heartbeatThread = new Thread(this::heartbeatLoop, "logforge-consumer-heartbeat");
    heartbeatThread.setDaemon(true);
    heartbeatThread.start();
  }

  private void heartbeatLoop() {
    while (!stopped) {
      sleep(config.heartbeatIntervalMs());
      if (stopped || joining || rejoinNeeded) {
        continue;
      }
      int gen = generation;
      BlockingChannel ch;
      synchronized (this) {
        ch = heartbeatChannel;
      }
      if (ch == null) {
        continue;
      }
      try {
        short code =
            Groups.ErrorResponse.read(
                    ch.request(
                        ApiKeys.HEARTBEAT,
                        w -> new Groups.HeartbeatRequest(config.groupId(), gen, memberId).write(w)))
                .errorCode();
        Errors err = Errors.forCode(code);
        if (gen != generation) {
          continue; // answer to a generation we already left
        }
        switch (err) {
          case NONE -> {}
          case REBALANCE_IN_PROGRESS, ILLEGAL_GENERATION -> rejoinNeeded = true;
          case UNKNOWN_MEMBER_ID -> {
            memberId = "";
            generation = -1;
            rejoinNeeded = true;
          }
          case NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE -> {
            coordinatorLost();
            rejoinNeeded = true;
          }
          default -> {}
        }
      } catch (IOException | RuntimeException e) {
        if (!stopped) {
          coordinatorLost();
          rejoinNeeded = true;
        }
      }
    }
  }

  /** Commits offsets; throws {@link ClientException} if the group moved on. */
  void commit(Map<TopicPartition, Long> positions) {
    if (positions.isEmpty() || generation < 0) {
      return;
    }
    Map<String, List<Groups.CommitPartition>> byTopic = new LinkedHashMap<>();
    positions.forEach(
        (tp, off) ->
            byTopic
                .computeIfAbsent(tp.topic(), k -> new ArrayList<>())
                .add(new Groups.CommitPartition(tp.partition(), off, null)));
    List<Groups.CommitTopic> ts = new ArrayList<>();
    byTopic.forEach((t, ps) -> ts.add(new Groups.CommitTopic(t, ps)));
    Groups.OffsetCommitRequest req =
        new Groups.OffsetCommitRequest(config.groupId(), generation, memberId, ts);
    Groups.OffsetCommitResponse resp;
    try {
      resp =
          Groups.OffsetCommitResponse.read(
              coordinator().request(ApiKeys.OFFSET_COMMIT, req::write));
    } catch (IOException e) {
      coordinatorLost();
      throw new ClientException("commit failed", e);
    }
    for (Groups.TopicErrors t : resp.topics()) {
      for (Groups.PartitionError p : t.partitions()) {
        Errors err = Errors.forCode(p.errorCode());
        if (err != Errors.NONE) {
          if (err == Errors.ILLEGAL_GENERATION
              || err == Errors.UNKNOWN_MEMBER_ID
              || err == Errors.REBALANCE_IN_PROGRESS) {
            rejoinNeeded = true;
          } else if (err == Errors.NOT_COORDINATOR || err == Errors.COORDINATOR_NOT_AVAILABLE) {
            coordinatorLost();
          }
          throw new ClientException(err, "commit for " + t.name() + "-" + p.partition());
        }
      }
    }
  }

  /** Committed offsets for {@code partitions}; -1 where nothing is committed. */
  Map<TopicPartition, Long> fetchCommitted(List<TopicPartition> partitions) {
    Map<String, List<Integer>> byTopic = new LinkedHashMap<>();
    partitions.forEach(
        tp -> byTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition()));
    List<Groups.FetchTopic> ts = new ArrayList<>();
    byTopic.forEach((t, ps) -> ts.add(new Groups.FetchTopic(t, ps)));
    Map<TopicPartition, Long> out = new HashMap<>();
    try {
      Groups.OffsetFetchResponse r =
          Groups.OffsetFetchResponse.read(
              coordinator()
                  .request(
                      ApiKeys.OFFSET_FETCH,
                      w -> new Groups.OffsetFetchRequest(config.groupId(), ts).write(w)));
      if (r.errorCode() != Errors.NONE.code()) {
        coordinatorLost();
        return out;
      }
      for (Groups.FetchedTopic t : r.topics()) {
        for (Groups.FetchedOffset p : t.partitions()) {
          out.put(new TopicPartition(t.name(), p.partition()), p.offset());
        }
      }
    } catch (IOException | RuntimeException e) {
      coordinatorLost();
    }
    return out;
  }

  /** Leaves the group so its partitions move immediately instead of after a session timeout. */
  void leave() {
    stopped = true;
    try {
      BlockingChannel ch;
      synchronized (this) {
        ch = coordinator;
      }
      if (ch != null && !memberId.isEmpty()) {
        ch.request(
            ApiKeys.LEAVE_GROUP, w -> new Groups.LeaveRequest(config.groupId(), memberId).write(w));
      }
    } catch (IOException | RuntimeException e) {
      // the coordinator will expire us instead
    }
    coordinatorLost();
  }

  /** Stops heartbeating without leaving: what a crash looks like to the coordinator. */
  void abandon() {
    stopped = true;
    coordinatorLost();
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
