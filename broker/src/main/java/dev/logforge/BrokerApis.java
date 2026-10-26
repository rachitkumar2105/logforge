package dev.logforge;

import dev.logforge.controller.Controller;
import dev.logforge.log.ProducerIdManager;
import dev.logforge.network.RequestHandler;
import dev.logforge.network.protocol.ApiKeys;
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
  private volatile GroupApis groups;

  BrokerApis(
      Controller controller,
      ReplicaManager replicaManager,
      Supplier<ControllerApis.Snapshot> metadata,
      ProducerIdManager producerIds) {
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
    return switch (key) {
      case ApiKeys.PRODUCE -> produce(parse(body, Produce.Request::read));
      case ApiKeys.FETCH -> fetch(parse(body, Fetch.Request::read));
      case ApiKeys.LIST_OFFSETS -> listOffsets(parse(body, ListOffsets.Request::read));
      case ApiKeys.METADATA -> done(metadata(parse(body, Metadata.Request::read))::write);
      case ApiKeys.CREATE_TOPICS -> createTopics(parse(body, Topics.CreateRequest::read));
      case ApiKeys.DELETE_TOPICS -> deleteTopics(parse(body, Topics.DeleteRequest::read));
      case ApiKeys.INIT_PRODUCER_ID ->
          initProducerId(parse(body, Replication.InitProducerIdRequest::read));
      case ApiKeys.OFFSET_FOR_LEADER_EPOCH ->
          offsetForLeaderEpoch(parse(body, Replication.OffsetForLeaderEpochRequest::read));
      case ApiKeys.BROKER_HEARTBEAT ->
          brokerHeartbeat(parse(body, ControllerApis.HeartbeatRequest::read));
      case ApiKeys.ALTER_ISR -> alterIsr(parse(body, ControllerApis.AlterIsrRequest::read));
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
      List<Topics.CreateResult> rs = new ArrayList<>();
      req.topics()
          .forEach(
              t -> rs.add(new Topics.CreateResult(t.name(), Errors.NOT_CONTROLLER.code(), null)));
      return done(new Topics.CreateResponse(rs)::write);
    }
    return done(controller.createTopics(req)::write);
  }

  private CompletableFuture<ByteBuffer> deleteTopics(Topics.DeleteRequest req) {
    if (controller == null) {
      List<Topics.DeleteResult> rs = new ArrayList<>();
      req.names().forEach(n -> rs.add(new Topics.DeleteResult(n, Errors.NOT_CONTROLLER.code())));
      return done(new Topics.DeleteResponse(rs)::write);
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
      List<ControllerApis.AlterTopicResult> ts = new ArrayList<>();
      for (ControllerApis.AlterTopic t : req.topics()) {
        List<ControllerApis.AlterResult> ps = new ArrayList<>();
        t.partitions()
            .forEach(
                p ->
                    ps.add(
                        new ControllerApis.AlterResult(
                            p.partition(), Errors.NOT_CONTROLLER.code(), -1, -1, List.of())));
        ts.add(new ControllerApis.AlterTopicResult(t.name(), ps));
      }
      return done(new ControllerApis.AlterIsrResponse(ts)::write);
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
