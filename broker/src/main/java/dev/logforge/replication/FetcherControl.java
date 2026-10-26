package dev.logforge.replication;

import dev.logforge.TopicPartition;

/** Starts and stops follower replication for partitions. Implemented by the fetcher manager. */
public interface FetcherControl {
  /** Start (or redirect) pulling {@code tp} from {@code leaderId} at {@code leaderEpoch}. */
  void follow(TopicPartition tp, int leaderId, int leaderEpoch);

  /** Stop pulling {@code tp}. */
  void unfollow(TopicPartition tp);

  FetcherControl NONE =
      new FetcherControl() {
        @Override
        public void follow(TopicPartition tp, int leaderId, int leaderEpoch) {}

        @Override
        public void unfollow(TopicPartition tp) {}
      };
}
