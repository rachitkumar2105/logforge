# follower-pause study

The first follower-pause run (2026-10-26) moved leadership away from a leader that was never paused. Four repeats did not show it, which was not an explanation, so the scenario was repeated with every node's log and a 200 ms record of the controller's cluster view (`bash chaos/follower-pause-study.sh 22`, summary by `chaos/follower-pause-study.py`). Host: the i7-1255U laptop recorded in `bench/results`, Docker Desktop (WSL 2).

| | Runs | Anomalous | FAIL (acknowledged records unreadable) | Longest heartbeat silence of an unpaused broker |
|---|---|---|---|---|
| Before the fix (`before-fix.md`, commit `628d0b5`) | 22 | 22 (100%) | 4 (about 15,000 records each) | 6.8 to 7.5 s |
| After the fix (`after-fix.md`, commit `54a19c8`) | 22 | 0 | 0 | 249 to 262 ms |

An anomalous run is one where a broker that was not paused was declared dead, leadership moved off an unpaused leader, or the paused follower was not back in every ISR at the end.

## Root cause

1. Pausing a broker (`docker pause`, SIGSTOP) leaves the other brokers' replica fetchers blocked in a read from it.
2. The controller expires the paused broker and moves leadership. Each healthy broker applies that metadata on its heartbeat thread and shuts its fetcher for the paused broker down. `BlockingChannel.close()` was `synchronized` with the blocked `request()`, so the heartbeat thread waited for the read to time out (about 7 s). The healthy brokers missed their own heartbeats, were declared dead too, and partitions went briefly offline.
3. `ReplicaFetcher.shutdown()` also interrupted the fetcher thread. When the interrupt landed while that thread wrote or flushed fetched records, Java closed the log's `FileChannel`; every later flush failed ("flush failed in chaos-follower-pause-2"), the broker reported a storage failure and was fenced. When a partition's remaining ISR was fenced it went offline, and the verifier could not read about 15,000 acknowledged records: FAIL.

## Fix and regression tests

- `BlockingChannel.close()` closes the socket without taking the monitor, which wakes a blocked reader at once, and a closed channel never reconnects (`BlockingChannelTest`: close took 30 s before, 0.37 s after).
- `ReplicaFetcher.shutdown()` stops the thread with its flag and `notifyAll`, never `interrupt()` (`ReplicaFetcherShutdownTest`).

Why the frequency changed from about 1 in 5 (2026-10-26) to 22 in 22 is not established: the study adds a recorder polling the controller every 200 ms, and the build had since gained the F1 to F8 fixes. Both are timing differences around the same race; after the fix the race is gone in all 22 runs.
