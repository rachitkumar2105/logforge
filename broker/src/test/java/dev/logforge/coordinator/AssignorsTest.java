package dev.logforge.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import dev.logforge.TopicPartition;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AssignorsTest {

  private static final Map<String, Integer> PARTITIONS = Map.of("a", 7, "b", 3);

  private static Set<TopicPartition> all(Map<String, List<TopicPartition>> assignment) {
    Set<TopicPartition> out = new HashSet<>();
    assignment.values().forEach(out::addAll);
    return out;
  }

  @Test
  void rangeGivesContiguousRangesPerTopicAndTheRemainderToTheFirstMembers() {
    Map<String, List<String>> subs =
        Map.of("m1", List.of("a", "b"), "m2", List.of("a", "b"), "m3", List.of("a", "b"));
    Map<String, List<TopicPartition>> a = Assignors.assign("range", subs, PARTITIONS);

    assertThat(a.get("m1"))
        .containsExactly(
            new TopicPartition("a", 0),
            new TopicPartition("a", 1),
            new TopicPartition("a", 2),
            new TopicPartition("b", 0));
    assertThat(a.get("m2"))
        .containsExactly(
            new TopicPartition("a", 3), new TopicPartition("a", 4), new TopicPartition("b", 1));
    assertThat(a.get("m3"))
        .containsExactly(
            new TopicPartition("a", 5), new TopicPartition("a", 6), new TopicPartition("b", 2));
    assertThat(all(a)).hasSize(10);
  }

  @Test
  void roundRobinInterleavesAllPartitions() {
    Map<String, List<String>> subs = Map.of("m1", List.of("a", "b"), "m2", List.of("a", "b"));
    Map<String, List<TopicPartition>> a = Assignors.assign("roundrobin", subs, PARTITIONS);

    assertThat(a.get("m1")).hasSize(5);
    assertThat(a.get("m2")).hasSize(5);
    assertThat(a.get("m1")).startsWith(new TopicPartition("a", 0), new TopicPartition("a", 2));
    assertThat(a.get("m2")).startsWith(new TopicPartition("a", 1), new TopicPartition("a", 3));
    assertThat(all(a)).hasSize(10);
  }

  @Test
  void membersOnlyGetTopicsTheySubscribeToAndExtraMembersGetNothing() {
    Map<String, List<String>> subs =
        Map.of("m1", List.of("b"), "m2", List.of("b"), "m3", List.of("b"), "m4", List.of("b"));
    for (String strategy : List.of("range", "roundrobin")) {
      Map<String, List<TopicPartition>> a = Assignors.assign(strategy, subs, PARTITIONS);
      assertThat(all(a)).as(strategy).hasSize(3).allMatch(tp -> tp.topic().equals("b"));
      assertThat(a.values().stream().filter(List::isEmpty).count()).as(strategy).isEqualTo(1);
    }
  }

  @Test
  void unknownTopicsAreIgnored() {
    Map<String, List<TopicPartition>> a =
        Assignors.assign("range", Map.of("m1", List.of("nope")), PARTITIONS);
    assertThat(a.get("m1")).isEmpty();
  }
}
