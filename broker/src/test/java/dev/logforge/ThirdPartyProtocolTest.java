package dev.logforge;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.testing.TestClient;
import dev.logforge.testing.TestNodes;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Speaks the protocol using nothing but docs/PROTOCOL.md: no LogForge codec classes. Every byte is
 * written by hand with {@link DataOutputStream} in the order the document gives. This is the
 * evidence for the Phase 0 gate that a third party could implement a client from the document.
 */
class ThirdPartyProtocolTest {

  @TempDir Path dir;

  @Test
  void produceAndFetchFromHandWrittenBytes() throws Exception {
    try (Node node = TestNodes.single(dir)) {
      try (TestClient admin = new TestClient("127.0.0.1", node.port())) {
        admin.createTopic("raw", 1, 1, Map.of());
      }
      TestNodes.awaitTrue(() -> node.isLeaderFor("raw", 0), Duration.ofSeconds(10));

      try (Socket s = new Socket("127.0.0.1", node.port())) {
        DataOutputStream out = new DataOutputStream(s.getOutputStream());
        DataInputStream in = new DataInputStream(s.getInputStream());

        // ---- Section 2: one record batch, key "k", value "hello", no headers.
        ByteArrayOutputStream rec = new ByteArrayOutputStream();
        rec.write(0); // attributes
        rec.write(0); // timestampDelta = 0 (zig-zag)
        rec.write(0); // offsetDelta = 0
        rec.write(2); // keyLength = 1 -> zig-zag 2
        rec.write('k');
        rec.write(10); // valueLength = 5 -> zig-zag 10
        rec.write("hello".getBytes(UTF_8));
        rec.write(0); // headerCount
        byte[] body = rec.toByteArray();
        ByteArrayOutputStream records = new ByteArrayOutputStream();
        records.write(body.length * 2); // record length, zig-zag (fits in one byte)
        records.write(body);

        ByteBuffer afterCrc = ByteBuffer.allocate(2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + records.size());
        afterCrc.putShort((short) 0); // attributes
        afterCrc.putInt(0); // lastOffsetDelta
        afterCrc.putLong(1_700_000_000_000L); // baseTimestamp
        afterCrc.putLong(1_700_000_000_000L); // maxTimestamp
        afterCrc.putLong(-1L); // producerId
        afterCrc.putShort((short) -1); // producerEpoch
        afterCrc.putInt(-1); // baseSequence
        afterCrc.putInt(1); // recordCount
        afterCrc.put(records.toByteArray());
        CRC32C crc = new CRC32C();
        crc.update(afterCrc.array());

        ByteArrayOutputStream batchBytes = new ByteArrayOutputStream();
        DataOutputStream batch = new DataOutputStream(batchBytes);
        int lengthAfterField = 8 + 4 + 1 + 4 + afterCrc.capacity();
        batch.writeInt(lengthAfterField);
        batch.writeLong(0L); // baseOffset
        batch.writeInt(-1); // partitionLeaderEpoch
        batch.writeByte(1); // magic
        batch.writeInt((int) crc.getValue());
        batch.write(afterCrc.array());

        // ---- Sections 3 and 5.0: Produce request frame.
        ByteArrayOutputStream req = new ByteArrayOutputStream();
        DataOutputStream r = new DataOutputStream(req);
        r.writeShort(0); // apiKey Produce
        r.writeShort(0); // apiVersion
        r.writeInt(77); // correlationId
        writeString(r, "hand-rolled");
        r.writeShort(1); // acks
        r.writeInt(5_000); // timeoutMs
        r.writeInt(1); // topics
        writeString(r, "raw");
        r.writeInt(1); // partitions
        r.writeInt(0); // partition
        r.writeInt(batchBytes.size()); // records bytes length
        r.write(batchBytes.toByteArray());
        out.writeInt(req.size());
        out.write(req.toByteArray());
        out.flush();

        int size = in.readInt();
        assertThat(size).isPositive();
        assertThat(in.readInt()).isEqualTo(77); // correlationId
        assertThat(in.readInt()).isEqualTo(1); // topics
        assertThat(readString(in)).isEqualTo("raw");
        assertThat(in.readInt()).isEqualTo(1); // partitions
        assertThat(in.readInt()).isEqualTo(0); // partition
        assertThat(in.readShort()).isEqualTo((short) 0); // errorCode NONE
        assertThat(in.readLong()).isEqualTo(0L); // baseOffset
        in.readLong(); // logStartOffset

        // ---- Section 5.1: Fetch it back.
        ByteArrayOutputStream fq = new ByteArrayOutputStream();
        DataOutputStream f = new DataOutputStream(fq);
        f.writeShort(1);
        f.writeShort(0);
        f.writeInt(78);
        writeString(f, "hand-rolled");
        f.writeInt(-1); // replicaId
        f.writeInt(0); // maxWaitMs
        f.writeInt(1); // minBytes
        f.writeInt(1 << 20); // maxBytes
        f.writeInt(1);
        writeString(f, "raw");
        f.writeInt(1);
        f.writeInt(0); // partition
        f.writeInt(-1); // currentLeaderEpoch
        f.writeLong(0L); // fetchOffset
        f.writeInt(1 << 20); // partitionMaxBytes
        out.writeInt(fq.size());
        out.write(fq.toByteArray());
        out.flush();

        in.readInt();
        assertThat(in.readInt()).isEqualTo(78);
        assertThat(in.readInt()).isEqualTo(1);
        assertThat(readString(in)).isEqualTo("raw");
        assertThat(in.readInt()).isEqualTo(1);
        assertThat(in.readInt()).isEqualTo(0);
        assertThat(in.readShort()).isEqualTo((short) 0);
        assertThat(in.readLong()).isEqualTo(1L); // highWatermark
        in.readLong(); // logStartOffset
        in.readLong(); // logEndOffset
        int recLen = in.readInt();
        byte[] fetched = new byte[recLen];
        in.readFully(fetched);
        ByteBuffer fb = ByteBuffer.wrap(fetched);
        assertThat(fb.getInt(0) + 4).isEqualTo(recLen); // exactly our one batch
        assertThat(fb.getLong(4)).isZero(); // baseOffset assigned by the leader
        assertThat(fb.get(16)).isEqualTo((byte) 1); // magic
        // The leader stamped offset and epoch outside the CRC: our checksum is unchanged.
        assertThat(fb.getInt(17)).isEqualTo((int) crc.getValue());
        String tail = new String(fetched, fetched.length - 6, 5, UTF_8);
        assertThat(tail).isEqualTo("hello");
      }
    }
  }

  private static void writeString(DataOutputStream out, String s) throws Exception {
    byte[] b = s.getBytes(UTF_8);
    out.writeShort(b.length);
    out.write(b);
  }

  private static String readString(DataInputStream in) throws Exception {
    byte[] b = new byte[in.readShort()];
    in.readFully(b);
    return new String(b, UTF_8);
  }
}
