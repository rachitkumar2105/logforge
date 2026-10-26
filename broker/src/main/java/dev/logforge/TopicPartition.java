package dev.logforge;

import java.util.regex.Pattern;

/** A partition of a topic. Its directory on disk is {@code <topic>-<partition>}. */
public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {
  private static final Pattern VALID_TOPIC = Pattern.compile("[A-Za-z0-9._-]{1,200}");

  public static final String OFFSETS_TOPIC = "__consumer_offsets";

  public static boolean isValidTopicName(String name) {
    return name != null && VALID_TOPIC.matcher(name).matches();
  }

  public static boolean isInternal(String name) {
    return name.startsWith("__");
  }

  public String dirName() {
    return topic + "-" + partition;
  }

  /** Parses a directory name; the partition is after the last '-'. */
  public static TopicPartition fromDirName(String name) {
    int dash = name.lastIndexOf('-');
    if (dash <= 0) {
      throw new IllegalArgumentException("not a partition directory: " + name);
    }
    return new TopicPartition(name.substring(0, dash), Integer.parseInt(name.substring(dash + 1)));
  }

  @Override
  public int compareTo(TopicPartition o) {
    int c = topic.compareTo(o.topic);
    return c != 0 ? c : Integer.compare(partition, o.partition);
  }

  @Override
  public String toString() {
    return dirName();
  }
}
