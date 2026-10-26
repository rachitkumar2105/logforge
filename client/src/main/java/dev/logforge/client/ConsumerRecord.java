package dev.logforge.client;

import dev.logforge.log.Header;
import java.util.List;

/** A record returned by {@link Consumer#poll}. */
public record ConsumerRecord(
    String topic,
    int partition,
    long offset,
    long timestamp,
    byte[] key,
    byte[] value,
    List<Header> headers) {
  @Override
  public String toString() {
    return "ConsumerRecord(" + topic + "-" + partition + "@" + offset + ")";
  }
}
