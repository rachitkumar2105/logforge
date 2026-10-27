package dev.logforge.testing;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.logforge.log.RecordBatch;
import dev.logforge.network.protocol.ApiKeys;
import dev.logforge.network.protocol.Authenticate;
import dev.logforge.network.protocol.Fetch;
import dev.logforge.network.protocol.ListOffsets;
import dev.logforge.network.protocol.Metadata;
import dev.logforge.network.protocol.Produce;
import dev.logforge.network.protocol.ProtocolReader;
import dev.logforge.network.protocol.ProtocolWriter;
import dev.logforge.network.protocol.RequestHeader;
import dev.logforge.network.protocol.Topics;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** A blocking, one-request-at-a-time client built only from the protocol codecs. For tests. */
public final class TestClient implements Closeable {
  private final Socket socket;
  private final DataInputStream in;
  private final OutputStream out;
  private int correlation = 0;
  private final String clusterToken;
  private boolean authenticated;

  /**
   * A client that authenticates with {@link TestNodes#CLUSTER_TOKEN} before topic administration,
   * as the cluster's own tools do.
   */
  public TestClient(String host, int port) throws IOException {
    this(host, port, TestNodes.CLUSTER_TOKEN);
  }

  /** A client that never authenticates: what anyone on the network can do. */
  public static TestClient anonymous(String host, int port) throws IOException {
    return new TestClient(host, port, null);
  }

  private TestClient(String host, int port, String clusterToken) throws IOException {
    this.clusterToken = clusterToken;
    socket = new Socket();
    socket.connect(new InetSocketAddress(host, port), 5_000);
    socket.setSoTimeout(30_000);
    socket.setTcpNoDelay(true);
    in = new DataInputStream(socket.getInputStream());
    out = socket.getOutputStream();
  }

  public Socket socket() {
    return socket;
  }

  public static byte[] frame(short apiKey, int correlationId, Consumer<ProtocolWriter> body) {
    ProtocolWriter w = new ProtocolWriter();
    w.writeInt(0);
    new RequestHeader(apiKey, (short) 0, correlationId, "test-client").write(w);
    body.accept(w);
    ByteBuffer b = w.toBuffer();
    b.putInt(0, b.remaining() - 4);
    byte[] bytes = new byte[b.remaining()];
    b.get(bytes);
    return bytes;
  }

  public void sendRaw(byte[] bytes) throws IOException {
    out.write(bytes);
    out.flush();
  }

  /** Reads one response frame; returns a reader positioned after the correlation id. */
  public ProtocolReader readResponse(int expectedCorrelation) throws IOException {
    int size = in.readInt();
    byte[] body = new byte[size];
    in.readFully(body);
    ByteBuffer buf = ByteBuffer.wrap(body);
    int corr = buf.getInt();
    if (corr != expectedCorrelation) {
      throw new IOException("correlation " + corr + " != " + expectedCorrelation);
    }
    return new ProtocolReader(buf);
  }

  public int readResponseCorrelation() throws IOException {
    int size = in.readInt();
    byte[] body = new byte[size];
    in.readFully(body);
    return ByteBuffer.wrap(body).getInt();
  }

  public ProtocolReader call(short apiKey, Consumer<ProtocolWriter> body) throws IOException {
    int corr = ++correlation;
    sendRaw(frame(apiKey, corr, body));
    return readResponse(corr);
  }

  /** True if the broker closed the connection (EOF) within the socket timeout. */
  public boolean isClosedByPeer() {
    try {
      int b = in.read();
      return b < 0;
    } catch (EOFException e) {
      return true;
    } catch (IOException e) {
      return true;
    }
  }

  public Metadata.Response metadata(List<String> topics) throws IOException {
    return Metadata.Response.read(
        call(ApiKeys.METADATA, w -> new Metadata.Request(topics).write(w)));
  }

  private void authenticateOnce() throws IOException {
    if (clusterToken == null || authenticated) {
      return;
    }
    short err =
        Authenticate.Response.read(
                call(ApiKeys.AUTHENTICATE, new Authenticate.Request(clusterToken)::write))
            .errorCode();
    if (err != 0) {
      throw new IOException("authentication refused: " + err);
    }
    authenticated = true;
  }

  public Topics.CreateResponse createTopic(
      String name, int partitions, int rf, Map<String, String> configs) throws IOException {
    authenticateOnce();
    Topics.CreateRequest req =
        new Topics.CreateRequest(
            10_000, List.of(new Topics.NewTopic(name, partitions, (short) rf, configs)));
    return Topics.CreateResponse.read(call(ApiKeys.CREATE_TOPICS, req::write));
  }

  public static RecordBatch batch(String... values) {
    RecordBatch.Builder b = RecordBatch.builder(System.currentTimeMillis());
    for (String v : values) {
      b.append(System.currentTimeMillis(), null, v.getBytes(UTF_8));
    }
    return b.build();
  }

  public Produce.PartitionResult produce(String topic, int partition, short acks, RecordBatch batch)
      throws IOException {
    Produce.Request req =
        new Produce.Request(
            acks,
            10_000,
            List.of(
                new Produce.TopicData(
                    topic, List.of(new Produce.PartitionData(partition, batch.buffer())))));
    if (acks == 0) {
      sendRaw(frame(ApiKeys.PRODUCE, ++correlation, req::write));
      return null;
    }
    Produce.Response resp = Produce.Response.read(call(ApiKeys.PRODUCE, req::write));
    return resp.topics().get(0).partitions().get(0);
  }

  public Fetch.PartitionResult fetch(String topic, int partition, long offset, int maxWaitMs)
      throws IOException {
    return fetch(-1, -1, topic, partition, offset, maxWaitMs);
  }

  public Fetch.PartitionResult fetch(
      int replicaId, int leaderEpoch, String topic, int partition, long offset, int maxWaitMs)
      throws IOException {
    if (replicaId >= 0) {
      authenticateOnce(); // a follower fetch is a cluster operation (F1)
    }
    Fetch.Request req =
        new Fetch.Request(
            replicaId,
            maxWaitMs,
            1,
            1 << 20,
            List.of(
                new Fetch.TopicData(
                    topic,
                    List.of(new Fetch.PartitionData(partition, leaderEpoch, offset, 1 << 20)))));
    return Fetch.Response.read(call(ApiKeys.FETCH, req::write)).topics().get(0).partitions().get(0);
  }

  public ListOffsets.PartitionResult listOffset(String topic, int partition, long timestamp)
      throws IOException {
    ListOffsets.Request req =
        new ListOffsets.Request(
            -1,
            List.of(
                new ListOffsets.TopicData(
                    topic, List.of(new ListOffsets.PartitionData(partition, timestamp)))));
    return ListOffsets.Response.read(call(ApiKeys.LIST_OFFSETS, req::write))
        .topics()
        .get(0)
        .partitions()
        .get(0);
  }

  @Override
  public void close() throws IOException {
    socket.close();
  }
}
