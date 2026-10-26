package dev.logforge.client;

import dev.logforge.TopicPartition;
import dev.logforge.log.Header;
import dev.logforge.log.RecordBatch;
import dev.logforge.network.BlockingChannel;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Errors;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.Replication;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A batching producer.
 *
 * <p><b>Batching.</b> {@link #send} appends to the open batch for the record's partition. A batch
 * is sent when it reaches {@code batchSize}, when it has waited {@code lingerMs}, or on {@link
 * #flush}. Bigger batches mean fewer requests and one sequential write per batch on the broker.
 *
 * <p><b>Ordering and in-flight.</b> At most one batch per partition is in flight. A retry is the
 * exact same bytes with the exact same sequence number, so ordering is preserved and, with
 * idempotence, the broker drops the duplicate if the first attempt actually landed.
 *
 * <p><b>Backpressure.</b> Unsent bytes are capped at {@code bufferMemory}; {@code send} blocks when
 * the cap is reached, so a fast caller cannot exhaust the heap.
 *
 * <p>Thread model: callers append under the producer's monitor; one sender thread drains ready
 * batches and dispatches one request per broker on an IO pool (requests to different brokers run in
 * parallel).
 */
public final class Producer implements AutoCloseable {
  private final ProducerConfig config;
  private final ClientMetadata metadata;
  private final Partitioner partitioner = new Partitioner();
  private final Map<TopicPartition, ArrayDeque<Batch>> queues = new LinkedHashMap<>();
  private final Set<TopicPartition> inFlight = new HashSet<>();
  private final Map<TopicPartition, Integer> nextSequence = new HashMap<>();
  private final ExecutorService io;
  private final Thread sender;
  private final AtomicInteger outstanding = new AtomicInteger();
  private long bufferedBytes;
  private boolean flushRequested;
  private volatile boolean closed;
  private volatile boolean closedHard;
  private long producerId = RecordBatch.NO_PRODUCER_ID;
  private short producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;

  /** One batch for one partition, with the callers waiting on it. */
  private static final class Batch {
    final TopicPartition tp;
    final RecordBatch.Builder builder;
    final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
    final long createdNanos = System.nanoTime();
    final long deadlineNanos;
    ByteBuffer bytes;
    int attempts;
    long retryAtNanos;
    int estimatedSize;
    long bufferedSize;

    Batch(TopicPartition tp, long deadlineNanos) {
      this.tp = tp;
      this.builder = RecordBatch.builder(System.currentTimeMillis());
      this.deadlineNanos = deadlineNanos;
    }
  }

  public Producer(ProducerConfig config) {
    this.config = config;
    this.metadata =
        new ClientMetadata(config.bootstrapServers(), config.clientId(), config.requestTimeoutMs());
    this.metadata.refresh();
    if (config.idempotent()) {
      initProducerId();
    }
    this.io =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "logforge-producer-io");
              t.setDaemon(true);
              return t;
            });
    this.sender = new Thread(this::runSender, "logforge-producer-sender");
    this.sender.setDaemon(true);
    this.sender.start();
  }

  private void initProducerId() {
    try {
      Replication.InitProducerIdResponse r =
          Replication.InitProducerIdResponse.read(
              metadata
                  .anyChannel()
                  .request(
                      ApiKeys.INIT_PRODUCER_ID,
                      w -> new Replication.InitProducerIdRequest(0).write(w)));
      if (r.errorCode() != Errors.NONE.code()) {
        throw new ClientException(Errors.forCode(r.errorCode()), "InitProducerId failed");
      }
      synchronized (this) {
        producerId = r.producerId();
        producerEpoch = r.producerEpoch();
        nextSequence.clear();
      }
    } catch (IOException e) {
      throw new ClientException("InitProducerId failed", e);
    }
  }

  public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) {
    return send(topic, key, value, List.of());
  }

  /** Queues a record. The future completes when the broker acknowledges it (per {@code acks}). */
  public CompletableFuture<RecordMetadata> send(
      String topic, byte[] key, byte[] value, List<Header> headers) {
    if (closed) {
      throw new IllegalStateException("producer closed");
    }
    int partitions = metadata.partitionCount(topic);
    if (partitions <= 0) {
      return CompletableFuture.failedFuture(
          new ClientException(Errors.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic " + topic));
    }
    TopicPartition tp = new TopicPartition(topic, partitioner.partition(key, partitions));
    int size = 32 + (key == null ? 0 : key.length) + (value == null ? 0 : value.length);
    CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
    synchronized (this) {
      while (bufferedBytes + size > config.bufferMemory() && bufferedBytes > 0 && !closed) {
        try {
          wait(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return CompletableFuture.failedFuture(e);
        }
      }
      ArrayDeque<Batch> q = queues.computeIfAbsent(tp, k -> new ArrayDeque<>());
      Batch open = q.peekLast();
      if (open == null || open.bytes != null || open.estimatedSize + size > config.batchSize()) {
        open =
            new Batch(
                tp, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.deliveryTimeoutMs()));
        q.addLast(open);
      }
      open.builder.append(System.currentTimeMillis(), key, value, headers.toArray(new Header[0]));
      open.futures.add(future);
      open.estimatedSize = open.builder.estimatedSizeInBytes();
      open.bufferedSize += size;
      bufferedBytes += size;
      outstanding.incrementAndGet();
      if (open.estimatedSize >= config.batchSize()) {
        notifyAll();
      }
    }
    return future;
  }

  /** Sends everything queued and waits until every record is acknowledged or failed. */
  public void flush() {
    synchronized (this) {
      flushRequested = true;
      notifyAll();
      while (outstanding.get() > 0) {
        try {
          wait(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
      flushRequested = false;
    }
  }

  private void runSender() {
    while (!closed || outstanding.get() > 0) {
      Map<Integer, List<Batch>> byBroker;
      synchronized (this) {
        byBroker = drainReady();
        if (byBroker.isEmpty()) {
          try {
            wait(Math.max(1, config.lingerMs()));
          } catch (InterruptedException e) {
            return;
          }
          continue;
        }
      }
      for (Map.Entry<Integer, List<Batch>> e : byBroker.entrySet()) {
        int broker = e.getKey();
        List<Batch> batches = e.getValue();
        io.execute(() -> sendToBroker(broker, batches));
      }
    }
  }

  /** Picks the head batch of every partition that is ready, not in flight, and has a leader. */
  private Map<Integer, List<Batch>> drainReady() {
    Map<Integer, List<Batch>> out = new HashMap<>();
    long now = System.nanoTime();
    long lingerNanos = TimeUnit.MILLISECONDS.toNanos(config.lingerMs());
    boolean refreshNeeded = false;
    for (Map.Entry<TopicPartition, ArrayDeque<Batch>> e : queues.entrySet()) {
      TopicPartition tp = e.getKey();
      Batch head = e.getValue().peekFirst();
      if (head == null || inFlight.contains(tp) || head.retryAtNanos > now) {
        continue;
      }
      boolean full = e.getValue().size() > 1 || head.estimatedSize >= config.batchSize();
      boolean lingered = now - head.createdNanos >= lingerNanos;
      if (!(full || lingered || flushRequested || closed || head.bytes != null)) {
        continue;
      }
      if (now > head.deadlineNanos) {
        e.getValue().pollFirst();
        fail(head, new ClientException(Errors.REQUEST_TIMED_OUT, "delivery timeout for " + tp));
        continue;
      }
      int leader = metadata.leaderFor(tp);
      if (leader < 0) {
        refreshNeeded = true;
        head.retryAtNanos = now + TimeUnit.MILLISECONDS.toNanos(config.retryBackoffMs());
        continue;
      }
      if (head.bytes == null) {
        seal(head);
      }
      inFlight.add(tp);
      out.computeIfAbsent(leader, k -> new ArrayList<>()).add(head);
    }
    if (refreshNeeded) {
      io.execute(this::refreshQuietly);
    }
    return out;
  }

  /** Builds the batch bytes once; retries resend these exact bytes. */
  private void seal(Batch b) {
    if (config.idempotent()) {
      int seq = nextSequence.getOrDefault(b.tp, 0);
      b.builder.producer(producerId, producerEpoch, seq);
      nextSequence.put(b.tp, seq + b.builder.recordCount());
    }
    b.bytes = b.builder.build().buffer();
  }

  private void refreshQuietly() {
    try {
      metadata.refresh();
    } catch (RuntimeException e) {
      // Retried on the next drain.
    }
  }

  private void sendToBroker(int broker, List<Batch> batches) {
    Map<String, List<Produce.PartitionData>> byTopic = new LinkedHashMap<>();
    for (Batch b : batches) {
      byTopic
          .computeIfAbsent(b.tp.topic(), k -> new ArrayList<>())
          .add(new Produce.PartitionData(b.tp.partition(), b.bytes.duplicate()));
    }
    List<Produce.TopicData> topics = new ArrayList<>();
    byTopic.forEach((t, ps) -> topics.add(new Produce.TopicData(t, ps)));
    Produce.Request req =
        new Produce.Request(config.acks().wire(), config.requestTimeoutMs(), topics);
    try {
      BlockingChannel ch = metadata.channel(broker);
      if (config.acks() == Acks.NONE) {
        ch.sendOneWay(ApiKeys.PRODUCE, req::write);
        for (Batch b : batches) {
          complete(b, RecordMetadata.UNKNOWN_OFFSET);
        }
        return;
      }
      Produce.Response resp = Produce.Response.read(ch.request(ApiKeys.PRODUCE, req::write));
      Map<TopicPartition, Produce.PartitionResult> results = new HashMap<>();
      for (Produce.TopicResult t : resp.topics()) {
        for (Produce.PartitionResult p : t.partitions()) {
          results.put(new TopicPartition(t.name(), p.partition()), p);
        }
      }
      for (Batch b : batches) {
        Produce.PartitionResult r = results.get(b.tp);
        Errors err = r == null ? Errors.UNKNOWN_SERVER_ERROR : Errors.forCode(r.errorCode());
        if (err == Errors.NONE) {
          complete(b, r.baseOffset());
        } else {
          retryOrFail(b, err, null);
        }
      }
    } catch (IOException | RuntimeException e) {
      for (Batch b : batches) {
        retryOrFail(b, Errors.BROKER_NOT_AVAILABLE, e);
      }
    }
  }

  private void complete(Batch b, long baseOffset) {
    synchronized (this) {
      queues.get(b.tp).remove(b);
      inFlight.remove(b.tp);
      release(b);
    }
    for (int i = 0; i < b.futures.size(); i++) {
      long offset = baseOffset < 0 ? RecordMetadata.UNKNOWN_OFFSET : baseOffset + i;
      b.futures.get(i).complete(new RecordMetadata(b.tp.topic(), b.tp.partition(), offset));
    }
  }

  private void retryOrFail(Batch b, Errors err, Exception cause) {
    boolean retriable = err.retriable() || cause != null;
    synchronized (this) {
      inFlight.remove(b.tp);
      if (retriable
          && b.attempts < config.retries()
          && System.nanoTime() < b.deadlineNanos
          && !closedHard) {
        b.attempts++;
        b.retryAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.retryBackoffMs());
        io.execute(this::refreshQuietly);
        notifyAll();
        return;
      }
      queues.get(b.tp).remove(b);
    }
    fail(
        b,
        cause != null
            ? new ClientException("send failed after " + b.attempts + " retries", cause)
            : new ClientException(err, "send to " + b.tp + " failed"));
    if (config.idempotent()) {
      // The sequence numbers of the failed batch may or may not be on the broker. Start a fresh
      // producer id so the next batch cannot be rejected as out of order.
      try {
        initProducerId();
      } catch (RuntimeException e) {
        // Next send will surface connectivity problems.
      }
    }
  }

  private void fail(Batch b, Exception e) {
    synchronized (this) {
      release(b);
    }
    for (CompletableFuture<RecordMetadata> f : b.futures) {
      f.completeExceptionally(e);
    }
  }

  /** Caller holds the monitor. */
  private void release(Batch b) {
    bufferedBytes -= b.bufferedSize;
    outstanding.addAndGet(-b.futures.size());
    notifyAll();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    flush();
    closed = true;
    synchronized (this) {
      notifyAll();
    }
    try {
      sender.join(5_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    closedHard = true;
    io.shutdownNow();
    metadata.close();
  }
}
