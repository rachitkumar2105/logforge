package dev.logforge;

import dev.logforge.controller.Controller;
import dev.logforge.log.ProducerIdManager;
import dev.logforge.network.RequestHandler;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Authenticate;
import dev.logforge.network.protocol.ControllerApis;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.Groups;
import dev.logforge.network.protocol.InvalidRequestException;
import dev.logforge.network.protocol.ListOffsets;
import dev.logforge.network.protocol.Metadata;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.ProtocolReader;
import dev.logforge.network.protocol.ProtocolWriter;
import dev.logforge.network.protocol.Replication;
import dev.logforge.network.protocol.Topics;
import dev.logforge.replication.ReplicaManager;
import dev.logforge.util.JsonLog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Routes each request to the component that owns it. Parsing failures surface as {@link
 * InvalidRequestException}, which makes the server close the connection; every other problem is an
 * error code in the response.
 */
final class BrokerApis implements RequestHandler {
  private static final JsonLog LOG = JsonLog.get(BrokerApis.class);

  /** Group-protocol requests; implemented by the coordinator. */
  interface GroupApis {
    CompletableFuture<ByteBuffer> handle(short apiKey, ProtocolReader body);
  }

  private final Controller controller;
  private final ReplicaManager replicaManager;
  private final Supplier<ControllerApis.Snapshot> metadata;
  private final ProducerIdManager producerIds;
  private final byte[] clusterToken;
  private volatile GroupApis groups;

  BrokerApis(
      Controller controller,
      ReplicaManager replicaManager,
      Supplier<ControllerApis.Snapshot> metadata,
      ProducerIdManager producerIds,
      String clusterToken) {
    this.clusterToken = clusterToken.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    this.controller = controller;
    this.replicaManager = replicaManager;
    this.metadata = metadata;
    this.producerIds = producerIds;
  }

  void setGroupApis(GroupApis groups) {
    this.groups = groups;
  }

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

  @Override
  public CompletableFuture<ByteBuffer> handle(Context ctx, ProtocolReader body) {
    short key = ctx.header().apiKey();
    // SECURITY-AUDIT F1: the cluster's own operations need a connection that presented the token.
    boolean member = ctx.session().isClusterMember();
    return switch (key) {
      case ApiKeys.AUTHENTICATE -> authenticate(ctx, parse(body, Authenticate.Request::read));
      case ApiKeys.PRODUCE -> produce(parse(body, Produce.Request::read));
      case ApiKeys.FETCH -> {
        Fetch.Request req = parse(body, Fetch.Request::read);
        yield req.isFromFollower() && !member ? done(refuseFetch(req)::write) : fetch(req);
      }
      case ApiKeys.LIST_OFFSETS -> listOffsets(parse(body, ListOffsets.Request::read));
      case ApiKeys.METADATA -> done(metadata(parse(body, Metadata.Request::read))::write);
      case ApiKeys.CREATE_TOPICS -> {
        Topics.CreateRequest req = parse(body, Topics.CreateRequest::read);
        yield member ? createTopics(req) : done(refuseCreate(req, UNAUTHORIZED)::write);
      }
      case ApiKeys.DELETE_TOPICS -> {
        Topics.DeleteRequest req = parse(body, Topics.DeleteRequest::read);
        yield member ? deleteTopics(req) : done(refuseDelete(req, UNAUTHORIZED)::write);
      }
      case ApiKeys.INIT_PRODUCER_ID ->
          initProducerId(parse(body, Replication.InitProducerIdRequest::read));
      case ApiKeys.OFFSET_FOR_LEADER_EPOCH -> {
        Replication.OffsetForLeaderEpochRequest req =
            parse(body, Replication.OffsetForLeaderEpochRequest::read);
        yield member ? offsetForLeaderEpoch(req) : done(refuseEpochs(req)::write);
      }
      case ApiKeys.BROKER_HEARTBEAT -> {
        ControllerApis.HeartbeatRequest req = parse(body, ControllerApis.HeartbeatRequest::read);
        yield member
            ? brokerHeartbeat(req)
            : done(new ControllerApis.HeartbeatResponse(UNAUTHORIZED.code(), -1, null)::write);
      }
      case ApiKeys.ALTER_ISR -> {
        ControllerApis.AlterIsrRequest req = parse(body, ControllerApis.AlterIsrRequest::read);
        yield member ? alterIsr(req) : done(refuseAlterIsr(req, UNAUTHORIZED)::write);
      }
      case ApiKeys.FIND_COORDINATOR,
              ApiKeys.JOIN_GROUP,
              ApiKeys.HEARTBEAT,
              ApiKeys.LEAVE_GROUP,
              ApiKeys.OFFSET_COMMIT,
              ApiKeys.OFFSET_FETCH ->
          groupRequest(key, body);
      default -> throw new InvalidRequestException("unknown api " + key);
    };
  }

  private static final Errors UNAUTHORIZED = Errors.CLUSTER_AUTHORIZATION_FAILED;

  /** Wrong tokens tolerated per connection; the next one closes it. */
  private static final int MAX_AUTH_FAILURES = 3;

  private final java.util.concurrent.atomic.AtomicLong authRefusals =
      new java.util.concurrent.atomic.AtomicLong();
  private volatile long lastAuthWarnMs;

  private CompletableFuture<ByteBuffer> authenticate(Context ctx, Authenticate.Request req) {
    byte[] given = req.token().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    if (java.security.MessageDigest.isEqual(given, clusterToken)) {
      ctx.session().markClusterMember();
      return done(new Authenticate.Response(Errors.NONE.code())::write);
    }
    long refusedTotal = authRefusals.incrementAndGet();
    long now = System.currentTimeMillis();
    if (now - lastAuthWarnMs > 10_000) {
      // At most one line per 10 s, and never the presented value.
      lastAuthWarnMs = now;
      LOG.warn(
          "cluster authentication refused", "client", ctx.clientAddress(), "total", refusedTotal);
    }
    if (ctx.session().recordAuthFailure() >= MAX_AUTH_FAILURES) {
      throw new InvalidRequestException("too many failed authentications");
    }
    return done(new Authenticate.Response(UNAUTHORIZED.code())::write);
  }

  private static Topics.CreateResponse refuseCreate(Topics.CreateRequest req, Errors err) {
    List<Topics.CreateResult> rs = new ArrayList<>();
    req.topics().forEach(t -> rs.add(new Topics.CreateResult(t.name(), err.code(), null)));
    return new Topics.CreateResponse(rs);
  }

  private static Topics.DeleteResponse refuseDelete(Topics.DeleteRequest req, Errors err) {
    List<Topics.DeleteResult> rs = new ArrayList<>();
    req.names().forEach(n -> rs.add(new Topics.DeleteResult(n, err.code())));
    return new Topics.DeleteResponse(rs);
  }

  private static ControllerApis.AlterIsrResponse refuseAlterIsr(
      ControllerApis.AlterIsrRequest req, Errors err) {
    List<ControllerApis.AlterTopicResult> ts = new ArrayList<>();
    for (ControllerApis.AlterTopic t : req.topics()) {
      List<ControllerApis.AlterResult> ps = new ArrayList<>();
      t.partitions()
          .forEach(
              p ->
                  ps.add(
                      new ControllerApis.AlterResult(
                          p.partition(), err.code(), -1, -1, List.of())));
      ts.add(new ControllerApis.AlterTopicResult(t.name(), ps));
    }
    return new ControllerApis.AlterIsrResponse(ts);
  }

  private static Fetch.Response refuseFetch(Fetch.Request req) {
    List<Fetch.TopicResult> topics = new ArrayList<>();
    for (Fetch.TopicData t : req.topics()) {
      List<Fetch.PartitionResult> ps = new ArrayList<>();
      t.partitions()
          .forEach(
              p ->
                  ps.add(
                      new Fetch.PartitionResult(
                          p.partition(), UNAUTHORIZED.code(), -1, -1, -1, ByteBuffer.allocate(0))));
      topics.add(new Fetch.TopicResult(t.name(), ps));
    }
    return new Fetch.Response(topics);
  }

  private static Replication.OffsetForLeaderEpochResponse refuseEpochs(
      Replication.OffsetForLeaderEpochRequest req) {
    List<Replication.EpochTopicResult> ts = new ArrayList<>();
    for (Replication.EpochTopic t : req.topics()) {
      List<Replication.EpochEnd> ps = new ArrayList<>();
      t.partitions()
          .forEach(
              p -> ps.add(new Replication.EpochEnd(p.partition(), UNAUTHORIZED.code(), -1, -1)));
      ts.add(new Replication.EpochTopicResult(t.name(), ps));
    }
    return new Replication.OffsetForLeaderEpochResponse(ts);
  }

  private static CompletableFuture<ByteBuffer> done(Consumer<ProtocolWriter> body) {
    return CompletableFuture.completedFuture(encode(body));
  }

  private CompletableFuture<ByteBuffer> produce(Produce.Request req) {
    if (replicaManager == null) {
      List<Produce.TopicResult> topics = new ArrayList<>();
      for (Produce.TopicData t : req.topics()) {
        List<Produce.PartitionResult> ps = new ArrayList<>();
        t.partitions()
            .forEach(
                p ->
                    ps.add(
                        new Produce.PartitionResult(
                            p.partition(), Errors.NOT_LEADER_OR_FOLLOWER.code(), -1, -1)));
        topics.add(new Produce.TopicResult(t.name(), ps));
      }
      return req.acks() == 0
          ? CompletableFuture.completedFuture(null)
          : done(new Produce.Response(topics)::write);
    }
    return replicaManager.appendRecords(req).thenApply(r -> r == null ? null : encode(r::write));
  }

  private CompletableFuture<ByteBuffer> fetch(Fetch.Request req) {
    if (replicaManager == null) {
      List<Fetch.TopicResult> topics = new ArrayList<>();
      for (Fetch.TopicData t : req.topics()) {
        List<Fetch.PartitionResult> ps = new ArrayList<>();
        t.partitions()
            .forEach(
                p ->
                    ps.add(
                        new Fetch.PartitionResult(
                            p.partition(),
                            Errors.NOT_LEADER_OR_FOLLOWER.code(),
                            -1,
                            -1,
                            -1,
                            ByteBuffer.allocate(0))));
        topics.add(new Fetch.TopicResult(t.name(), ps));
      }
      return done(new Fetch.Response(topics)::write);
    }
    return replicaManager.fetch(req).thenApply(r -> encode(r::write));
  }

  private CompletableFuture<ByteBuffer> listOffsets(ListOffsets.Request req) {
    if (replicaManager == null) {
      return done(new ListOffsets.Response(List.of())::write);
    }
    return done(replicaManager.listOffsets(req)::write);
  }

  private CompletableFuture<ByteBuffer> offsetForLeaderEpoch(
      Replication.OffsetForLeaderEpochRequest req) {
    if (replicaManager == null) {
      return done(new Replication.OffsetForLeaderEpochResponse(List.of())::write);
    }
    return done(replicaManager.offsetForLeaderEpoch(req)::write);
  }

  /** Builds a Metadata response from this node's view of the cluster. */
  Metadata.Response metadata(Metadata.Request req) {
    ControllerApis.Snapshot s = controller != null ? controller.snapshot() : metadata.get();
    if (s == null) {
      return new Metadata.Response(null, -1, List.of(), List.of());
    }
    List<Metadata.Broker> brokers = new ArrayList<>();
    for (ControllerApis.BrokerInfo b : s.brokers()) {
      brokers.add(new Metadata.Broker(b.nodeId(), b.host(), b.port(), b.alive()));
    }
    Map<String, ControllerApis.TopicInfo> byName = new HashMap<>();
    s.topics().forEach(t -> byName.put(t.name(), t));
    List<String> wanted =
        req.topics() == null
            ? s.topics().stream().map(ControllerApis.TopicInfo::name).toList()
            : req.topics();
    List<Metadata.TopicMeta> topics = new ArrayList<>();
    for (String name : wanted) {
      ControllerApis.TopicInfo t = byName.get(name);
      if (t == null) {
        topics.add(
            new Metadata.TopicMeta(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), name, List.of()));
        continue;
      }
      List<Metadata.PartitionMeta> ps = new ArrayList<>();
      for (ControllerApis.PartitionInfo p : t.partitions()) {
        short err = p.leader() < 0 ? Errors.LEADER_NOT_AVAILABLE.code() : Errors.NONE.code();
        ps.add(
            new Metadata.PartitionMeta(
                err, p.partition(), p.leader(), p.leaderEpoch(), p.replicas(), p.isr()));
      }
      topics.add(new Metadata.TopicMeta(Errors.NONE.code(), name, ps));
    }
    return new Metadata.Response(s.clusterId(), s.controllerId(), brokers, topics);
  }

  private CompletableFuture<ByteBuffer> createTopics(Topics.CreateRequest req) {
    if (controller == null) {
      return done(refuseCreate(req, Errors.NOT_CONTROLLER)::write);
    }
    return done(controller.createTopics(req)::write);
  }

  private CompletableFuture<ByteBuffer> deleteTopics(Topics.DeleteRequest req) {
    if (controller == null) {
      return done(refuseDelete(req, Errors.NOT_CONTROLLER)::write);
    }
    return done(controller.deleteTopics(req)::write);
  }

  private CompletableFuture<ByteBuffer> brokerHeartbeat(ControllerApis.HeartbeatRequest req) {
    if (controller == null) {
      return done(
          new ControllerApis.HeartbeatResponse(Errors.NOT_CONTROLLER.code(), -1, null)::write);
    }
    return done(controller.heartbeat(req)::write);
  }

  private CompletableFuture<ByteBuffer> alterIsr(ControllerApis.AlterIsrRequest req) {
    if (controller == null) {
      return done(refuseAlterIsr(req, Errors.NOT_CONTROLLER)::write);
    }
    return done(controller.alterIsr(req)::write);
  }

  private CompletableFuture<ByteBuffer> initProducerId(Replication.InitProducerIdRequest req) {
    if (producerIds == null) {
      return done(
          new Replication.InitProducerIdResponse(Errors.BROKER_NOT_AVAILABLE.code(), -1, (short) -1)
              ::write);
    }
    try {
      long id = producerIds.nextProducerId();
      return done(new Replication.InitProducerIdResponse(Errors.NONE.code(), id, (short) 0)::write);
    } catch (IOException e) {
      LOG.error("producer id allocation failed", e);
      return done(
          new Replication.InitProducerIdResponse(Errors.STORAGE_ERROR.code(), -1, (short) -1)
              ::write);
    }
  }

  private CompletableFuture<ByteBuffer> groupRequest(short key, ProtocolReader body) {
    GroupApis g = groups;
    if (g != null) {
      return g.handle(key, body);
    }
    short err = Errors.COORDINATOR_NOT_AVAILABLE.code();
    return switch (key) {
      case ApiKeys.FIND_COORDINATOR -> {
        parse(body, Groups.FindCoordinatorRequest::read);
        yield done(new Groups.FindCoordinatorResponse(err, -1, null, -1)::write);
      }
      case ApiKeys.JOIN_GROUP -> {
        Groups.JoinRequest j = parse(body, Groups.JoinRequest::read);
        yield done(Groups.JoinResponse.error(err, j.memberId())::write);
      }
      case ApiKeys.HEARTBEAT -> {
        parse(body, Groups.HeartbeatRequest::read);
        yield done(new Groups.ErrorResponse(err)::write);
      }
      case ApiKeys.LEAVE_GROUP -> {
        parse(body, Groups.LeaveRequest::read);
        yield done(new Groups.ErrorResponse(err)::write);
      }
      case ApiKeys.OFFSET_COMMIT -> {
        parse(body, Groups.OffsetCommitRequest::read);
        yield done(new Groups.OffsetCommitResponse(List.of())::write);
      }
      default -> {
        parse(body, Groups.OffsetFetchRequest::read);
        yield done(new Groups.OffsetFetchResponse(err, List.of())::write);
      }
    };
  }
}
