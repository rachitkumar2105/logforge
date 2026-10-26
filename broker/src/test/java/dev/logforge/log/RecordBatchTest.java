package dev.logforge.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecordBatchTest {

  private static byte[] b(String s) {
    return s.getBytes(UTF_8);
  }

  @Test
  void roundTripsKeysValuesHeadersAndTimestamps() {
    RecordBatch batch =
        RecordBatch.builder(1_700_000_000_000L)
            .append(1_700_000_000_000L, b("k1"), b("v1"))
            .append(1_700_000_000_005L, b("k2"), b("v2"), new Header("trace", b("abc")))
            .append(1_699_999_999_990L, null, b("no key"))
            .build();

    RecordBatch decoded = RecordBatch.wrap(ByteBuffer.wrap(toBytes(batch)));
    decoded.validate();

    assertThat(decoded.recordCount()).isEqualTo(3);
    assertThat(decoded.lastOffsetDelta()).isEqualTo(2);
    assertThat(decoded.maxTimestamp()).isEqualTo(1_700_000_000_005L);
    List<Record> records = decoded.records();
    assertThat(records).hasSize(3);
    assertThat(records.get(0).key()).isEqualTo(b("k1"));
    assertThat(records.get(0).value()).isEqualTo(b("v1"));
    assertThat(records.get(0).offset()).isEqualTo(0);
    assertThat(records.get(1).timestamp()).isEqualTo(1_700_000_000_005L);
    assertThat(records.get(1).headers()).containsExactly(new Header("trace", b("abc")));
    assertThat(records.get(2).key()).isNull();
    assertThat(records.get(2).timestamp()).isEqualTo(1_699_999_999_990L);
  }

  @Test
  void distinguishesEmptyKeyFromNullKeyAndNullValue() {
    RecordBatch batch =
        RecordBatch.builder(0L)
            .append(0L, new byte[0], b("v"))
            .append(0L, null, null)
            .append(0L, b("k"), new byte[0])
            .build();

    List<Record> records = RecordBatch.wrap(ByteBuffer.wrap(toBytes(batch))).records();

    assertThat(records.get(0).key()).isNotNull().isEmpty();
    assertThat(records.get(1).key()).isNull();
    assertThat(records.get(1).value()).isNull();
    assertThat(records.get(1).isTombstone()).isTrue();
    assertThat(records.get(2).value()).isNotNull().isEmpty();
    assertThat(records.get(2).isTombstone()).isFalse();
  }

  @Test
  void roundTripsMultiByteUtf8() {
    String key = "ключ-鍵-🔑";
    String value = "значение — 価値 — ✓ — 𝄞";
    RecordBatch batch =
        RecordBatch.builder(0L).append(0L, b(key), b(value), new Header("ヘッダー", b("値"))).build();

    Record r = RecordBatch.wrap(ByteBuffer.wrap(toBytes(batch))).records().get(0);

    assertThat(new String(r.key(), UTF_8)).isEqualTo(key);
    assertThat(new String(r.value(), UTF_8)).isEqualTo(value);
    assertThat(r.headers().get(0).key()).isEqualTo("ヘッダー");
    assertThat(new String(r.headers().get(0).value(), UTF_8)).isEqualTo("値");
  }

  @Test
  void stampingOffsetAndEpochKeepsTheProducerChecksumValid() {
    RecordBatch batch = RecordBatch.builder(0L).append(0L, b("k"), b("v")).build();
    long crcBefore = batch.checksum();

    batch.setBaseOffset(4211L);
    batch.setPartitionLeaderEpoch(7);

    batch.validate();
    assertThat(batch.checksum()).isEqualTo(crcBefore);
    assertThat(batch.baseOffset()).isEqualTo(4211L);
    assertThat(batch.lastOffset()).isEqualTo(4211L);
    assertThat(batch.partitionLeaderEpoch()).isEqualTo(7);
    assertThat(batch.records().get(0).offset()).isEqualTo(4211L);
  }

  @Test
  void detectsAndRejectsCrcMismatch() {
    RecordBatch batch = RecordBatch.builder(0L).append(0L, b("key"), b("value")).build();
    byte[] bytes = toBytes(batch);
    bytes[bytes.length - 3] ^= 0x01; // flip one bit inside the value

    RecordBatch corrupted = RecordBatch.wrap(ByteBuffer.wrap(bytes));

    assertThat(corrupted.isValid()).isFalse();
    assertThatThrownBy(corrupted::validate)
        .isInstanceOf(CorruptRecordException.class)
        .hasMessageContaining("crc");
  }

  @Test
  void detectsTruncatedFinalRecord() {
    RecordBatch batch =
        RecordBatch.builder(0L).append(0L, b("k1"), b("v1")).append(0L, b("k2"), b("v2")).build();
    byte[] full = toBytes(batch);
    byte[] cut = java.util.Arrays.copyOf(full, full.length - 2);

    assertThat(RecordBatch.sizeIfComplete(ByteBuffer.wrap(cut))).isEqualTo(-1);
    RecordBatch truncated = RecordBatch.wrap(ByteBuffer.wrap(cut));
    assertThat(truncated.isValid()).isFalse();
    assertThatThrownBy(truncated::validate).isInstanceOf(CorruptRecordException.class);
  }

  @Test
  void rejectsWrongMagicAndBadLength() {
    byte[] bytes = toBytes(RecordBatch.builder(0L).append(0L, b("k"), b("v")).build());
    byte[] wrongMagic = bytes.clone();
    wrongMagic[RecordBatch.MAGIC_OFFSET] = 9;
    assertThatThrownBy(() -> RecordBatch.wrap(ByteBuffer.wrap(wrongMagic)).validate())
        .isInstanceOf(CorruptRecordException.class)
        .hasMessageContaining("magic");

    byte[] shortLength = bytes.clone();
    ByteBuffer.wrap(shortLength).putInt(0, 10);
    assertThatThrownBy(() -> RecordBatch.wrap(ByteBuffer.wrap(shortLength)).validate())
        .isInstanceOf(CorruptRecordException.class);
  }

  @Test
  void carriesProducerIdentityAndSequences() {
    RecordBatch batch =
        RecordBatch.builder(0L)
            .producer(42L, (short) 3, 100)
            .append(0L, b("a"), b("1"))
            .append(0L, b("b"), b("2"))
            .append(0L, b("c"), b("3"))
            .build();
    RecordBatch decoded = RecordBatch.wrap(ByteBuffer.wrap(toBytes(batch)));

    assertThat(decoded.producerId()).isEqualTo(42L);
    assertThat(decoded.producerEpoch()).isEqualTo((short) 3);
    assertThat(decoded.baseSequence()).isEqualTo(100);
    assertThat(decoded.lastSequence()).isEqualTo(102);
    assertThat(decoded.hasProducerId()).isTrue();
  }

  @Test
  void keepsOffsetGapsWrittenByCompaction() {
    RecordBatch batch =
        RecordBatch.builder(0L)
            .baseOffset(100L)
            .appendWithOffset(100L, 0L, b("a"), b("1"), List.of())
            .appendWithOffset(105L, 0L, b("b"), b("2"), List.of())
            .lastOffset(109L)
            .build();
    RecordBatch decoded = RecordBatch.wrap(ByteBuffer.wrap(toBytes(batch)));
    decoded.validate();

    assertThat(decoded.lastOffset()).isEqualTo(109L);
    assertThat(decoded.records()).extracting(Record::offset).containsExactly(100L, 105L);
  }

  @Test
  void iteratesConcatenatedBatchesAndStopsAtPartialTail() {
    RecordBatch a = RecordBatch.builder(0L).append(0L, b("a"), b("1")).build();
    RecordBatch c = RecordBatch.builder(0L).append(0L, b("c"), b("3")).build();
    c.setBaseOffset(1);
    byte[] ab = toBytes(a);
    byte[] cb = toBytes(c);
    ByteBuffer buf = ByteBuffer.allocate(ab.length + cb.length - 5);
    buf.put(ab).put(cb, 0, cb.length - 5).flip();

    List<RecordBatch> batches = RecordBatch.iterate(buf);

    assertThat(batches).hasSize(1);
    assertThat(batches.get(0).records().get(0).value()).isEqualTo(b("1"));
  }

  @Test
  void varintsMatchTheExamplesInProtocolMd() {
    assertThat(encodeVarint(0)).containsExactly(0x00);
    assertThat(encodeVarint(-1)).containsExactly(0x01);
    assertThat(encodeVarint(1)).containsExactly(0x02);
    assertThat(encodeVarint(-2)).containsExactly(0x03);
    assertThat(encodeVarint(63)).containsExactly(0x7E);
    assertThat(encodeVarint(64)).containsExactly(0x80, 0x01);
    assertThat(encodeVarint(300)).containsExactly(0xD8, 0x04);
    for (int v : new int[] {Integer.MIN_VALUE, -65536, -1, 0, 1, 127, 128, Integer.MAX_VALUE}) {
      ByteBuffer buf = ByteBuffer.allocate(5);
      Varint.writeVarint(buf, v);
      buf.flip();
      assertThat(Varint.readVarint(buf)).isEqualTo(v);
      assertThat(Varint.sizeOfVarint(v)).isEqualTo(buf.position());
    }
    for (long v : new long[] {Long.MIN_VALUE, -1L, 0L, 1L << 40, Long.MAX_VALUE}) {
      ByteBuffer buf = ByteBuffer.allocate(10);
      Varint.writeVarlong(buf, v);
      buf.flip();
      assertThat(Varint.readVarlong(buf)).isEqualTo(v);
    }
  }

  @Test
  void rejectsOverlongVarint() {
    ByteBuffer buf =
        ByteBuffer.wrap(
            new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01});
    assertThatThrownBy(() -> Varint.readVarint(buf)).isInstanceOf(CorruptRecordException.class);
  }

  @Test
  void matchesTheWorkedExampleLayoutInProtocolMd() {
    RecordBatch batch = RecordBatch.builder(1_000L).append(1_000L, b("k"), b("v")).build();
    batch.setPartitionLeaderEpoch(0);
    byte[] bytes = toBytes(batch);
    ByteBuffer buf = ByteBuffer.wrap(bytes);

    assertThat(bytes).hasSize(70);
    assertThat(buf.getInt(0)).isEqualTo(66);
    assertThat(buf.getLong(4)).isEqualTo(0L);
    assertThat(buf.get(16)).isEqualTo((byte) 1);
    assertThat(buf.getLong(43)).isEqualTo(-1L);
    assertThat(buf.getInt(57)).isEqualTo(1);
    byte[] record = java.util.Arrays.copyOfRange(bytes, 61, 70);
    assertThat(record)
        .containsExactly(
            (byte) 0x10,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x02,
            (byte) 0x6B,
            (byte) 0x02,
            (byte) 0x76,
            (byte) 0x00);
  }

  private static int[] encodeVarint(int v) {
    ByteBuffer buf = ByteBuffer.allocate(5);
    Varint.writeVarint(buf, v);
    int[] out = new int[buf.position()];
    for (int i = 0; i < out.length; i++) {
      out[i] = buf.get(i) & 0xFF;
    }
    return out;
  }

  static byte[] toBytes(RecordBatch batch) {
    ByteBuffer buf = batch.buffer();
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
  }
}
