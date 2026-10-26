package dev.logforge.coordinator;

import dev.logforge.TopicPartition;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Partition assignment strategies, computed by the coordinator (see DECISIONS.md B10).
 *
 * <ul>
 *   <li><b>range</b>: per topic, members sorted by id each get a contiguous block; the first {@code
 *       partitions % members} get one extra. Co-partitioned topics land together, at the cost of
 *       imbalance when there are many topics with few partitions.
 *   <li><b>roundrobin</b>: every subscribed partition, sorted, dealt out one at a time. More even,
 *       but a member's partitions are scattered.
 * </ul>
 */
public final class Assignors {
  private Assignors() {}

  public static boolean isKnown(String strategy) {
    return "range".equals(strategy) || "roundrobin".equals(strategy);
  }

  /**
   * @param subscriptions member id to subscribed topics
   * @param partitionCounts topic to partition count; topics missing here are ignored
   * @return member id to assigned partitions (every member present, possibly with an empty list)
   */
  public static Map<String, List<TopicPartition>> assign(
      String strategy,
      Map<String, List<String>> subscriptions,
      Map<String, Integer> partitionCounts) {
    List<String> members = new ArrayList<>(new TreeSet<>(subscriptions.keySet()));
    Map<String, List<TopicPartition>> out = new LinkedHashMap<>();
    members.forEach(m -> out.put(m, new ArrayList<>()));
    TreeSet<String> topics = new TreeSet<>();
    subscriptions.values().forEach(topics::addAll);
    topics.removeIf(t -> !partitionCounts.containsKey(t));

    if ("roundrobin".equals(strategy)) {
      int next = 0;
      for (String topic : topics) {
        for (int p = 0; p < partitionCounts.get(topic); p++) {
          for (int tries = 0; tries < members.size(); tries++) {
            String m = members.get(next % members.size());
            next++;
            if (subscriptions.get(m).contains(topic)) {
              out.get(m).add(new TopicPartition(topic, p));
              break;
            }
          }
        }
      }
      return out;
    }

    for (String topic : topics) {
      List<String> subscribers = new ArrayList<>();
      for (String m : members) {
        if (subscriptions.get(m).contains(topic)) {
          subscribers.add(m);
        }
      }
      int count = partitionCounts.get(topic);
      int per = count / subscribers.size();
      int extra = count % subscribers.size();
      int p = 0;
      for (int i = 0; i < subscribers.size(); i++) {
        int take = per + (i < extra ? 1 : 0);
        for (int k = 0; k < take; k++) {
          out.get(subscribers.get(i)).add(new TopicPartition(topic, p++));
        }
      }
    }
    return out;
  }
}
