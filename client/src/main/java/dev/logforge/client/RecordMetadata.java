package dev.logforge.client;

/** Where a sent record landed. {@code offset} is {@link #UNKNOWN_OFFSET} for acks=0. */
public record RecordMetadata(String topic, int partition, long offset) {
  public static final long UNKNOWN_OFFSET = -1L;
}
