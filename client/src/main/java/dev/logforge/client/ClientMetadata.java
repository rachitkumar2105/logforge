package dev.logforge.client;

import dev.logforge.TopicPartition;
import dev.logforge.network.BlockingChannel;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Metadata;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The client's view of the cluster (brokers, partition leaders) plus one connection per broker.
 * Refreshed on demand: at start, for unknown topics, and after NOT_LEADER-style errors.
 */
final class ClientMetadata implements Closeable {
  record Address(String host, int port) {}

  private final List<Address> bootstrap = new ArrayList<>();
  private final String clientId;
  private final int requestTimeoutMs;
  private final Map<Integer, Address> brokers = new ConcurrentHashMap<>();
  private final Map<String, List<Metadata.PartitionMeta>> topics = new ConcurrentHashMap<>();
  private final Map<Integer, BlockingChannel> channels = new ConcurrentHashMap<>();
  private volatile int controllerId = -1;

  ClientMetadata(String bootstrapServers, String clientId, int requestTimeoutMs) {
    for (String s : bootstrapServers.split(",")) {
      String hp = s.trim();
      if (hp.isEmpty()) {
        continue;
      }
      int colon = hp.lastIndexOf(':');
      if (colon <= 0) {
        throw new IllegalArgumentException("bootstrap entries are host:port, got " + hp);
      }
      bootstrap.add(new Address(hp.substring(0, colon), Integer.parseInt(hp.substring(colon + 1))));
    }
    if (bootstrap.isEmpty()) {
      throw new IllegalArgumentException("no bootstrap servers");
    }
    this.clientId = clientId;
    this.requestTimeoutMs = requestTimeoutMs;
  }

  /** Fetches full metadata from any reachable broker (known brokers first, then bootstrap). */
  synchronized Metadata.Response refresh() {
    List<Address> candidates = new ArrayList<>(brokers.values());
    candidates.addAll(bootstrap);
    IOException last = null;
    for (Address a : candidates) {
      BlockingChannel ch = new BlockingChannel(a.host(), a.port(), clientId, requestTimeoutMs);
      try {
        Metadata.Response md =
            Metadata.Response.read(
                ch.request(ApiKeys.METADATA, w -> new Metadata.Request(null).write(w)));
        if (md.brokers().isEmpty()) {
          continue; // a broker that has not received metadata yet
        }
        apply(md);
        return md;
      } catch (IOException e) {
        last = e;
      } finally {
        ch.close();
      }
    }
    throw new ClientException("no broker reachable", last);
  }

  private void apply(Metadata.Response md) {
    Map<Integer, Address> bs = new HashMap<>();
    for (Metadata.Broker b : md.brokers()) {
      bs.put(b.nodeId(), new Address(b.host(), b.port()));
    }
    brokers.keySet().retainAll(bs.keySet());
    brokers.putAll(bs);
    topics.clear();
    for (Metadata.TopicMeta t : md.topics()) {
      if (t.errorCode() == Errors.NONE.code()) {
        topics.put(t.name(), List.copyOf(t.partitions()));
      }
    }
    controllerId = md.controllerId();
  }

  /** Partition count for a topic, refreshing once if unknown; -1 if the topic does not exist. */
  int partitionCount(String topic) {
    List<Metadata.PartitionMeta> ps = topics.get(topic);
    if (ps == null) {
      refresh();
      ps = topics.get(topic);
    }
    return ps == null ? -1 : ps.size();
  }

  /** Leader node id, or -1 if unknown/offline. */
  int leaderFor(TopicPartition tp) {
    List<Metadata.PartitionMeta> ps = topics.get(tp.topic());
    if (ps == null) {
      return -1;
    }
    for (Metadata.PartitionMeta p : ps) {
      if (p.partition() == tp.partition()) {
        return p.leader();
      }
    }
    return -1;
  }

  List<Metadata.PartitionMeta> partitions(String topic) {
    return topics.getOrDefault(topic, List.of());
  }

  /** Current address of {@code nodeId}, or null if unknown. */
  Address address(int nodeId) {
    return brokers.get(nodeId);
  }

  int controllerId() {
    return controllerId;
  }

  /** The connection to {@code nodeId}. Callers share it; it serialises requests. */
  BlockingChannel channel(int nodeId) {
    Address a = brokers.get(nodeId);
    if (a == null) {
      refresh();
      a = brokers.get(nodeId);
      if (a == null) {
        throw new ClientException(Errors.BROKER_NOT_AVAILABLE, "unknown broker " + nodeId);
      }
    }
    Address addr = a;
    BlockingChannel ch = channels.get(nodeId);
    if (ch != null && (!ch.host().equals(addr.host()) || ch.port() != addr.port())) {
      channels.remove(nodeId, ch);
      ch.close();
      ch = null;
    }
    if (ch == null) {
      ch =
          channels.computeIfAbsent(
              nodeId,
              id -> new BlockingChannel(addr.host(), addr.port(), clientId, requestTimeoutMs));
    }
    return ch;
  }

  /** A fresh, unshared connection to {@code nodeId} (for long-poll fetches running in parallel). */
  BlockingChannel newChannel(int nodeId) {
    Address a = brokers.get(nodeId);
    if (a == null) {
      refresh();
      a = brokers.get(nodeId);
    }
    if (a == null) {
      throw new ClientException(Errors.BROKER_NOT_AVAILABLE, "unknown broker " + nodeId);
    }
    return new BlockingChannel(a.host(), a.port(), clientId, requestTimeoutMs);
  }

  /** Any broker, for requests that any broker can answer. */
  BlockingChannel anyChannel() {
    if (brokers.isEmpty()) {
      refresh();
    }
    return channel(brokers.keySet().iterator().next());
  }

  @Override
  public void close() {
    channels.values().forEach(BlockingChannel::close);
    channels.clear();
  }
}
