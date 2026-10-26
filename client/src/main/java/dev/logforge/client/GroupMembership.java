package dev.logforge.client;

import dev.logforge.TopicPartition;
import java.util.List;
import java.util.Map;

/** Consumer group membership for {@link Consumer#subscribe}. Implemented in Phase 5. */
final class GroupMembership {
  GroupMembership(Consumer consumer, List<String> topics) {
    throw new UnsupportedOperationException("consumer groups are not implemented yet (Phase 5)");
  }

  void ensureActive() {}

  void poll() {}

  void commit(Map<TopicPartition, Long> offsets) {}

  Map<TopicPartition, Long> fetchCommitted(List<TopicPartition> partitions) {
    return Map.of();
  }

  void leave() {}
}
