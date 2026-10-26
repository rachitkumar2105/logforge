# follower-pause, repeated

`bash chaos/run-all.sh follower-pause`, run five times on 2026-10-26 on the host recorded in each file. Every run: PASS, 0 acknowledged records missing, 0 duplicates.

| Run | File | Acked | Recovery (longest ack gap) | Partition 0 leader and ISR: before / while paused / 15 s after resume |
|---|---|---|---|---|
| 1 | `run1-full-suite.json` (part of the full `run-all.sh`) | 76,797 | 3,025 ms | 3 [3,1,2] / 2 [2,3] / 2 [2,3] |
| 2 | not kept (overwritten by run 3; console output only) | 83,727 | 2,954 ms | 3 [3,1,2] / 3 [2,3] / 3 [1,2,3] |
| 3 | `run3.json` | 84,651 | 3,272 ms | 2 [2,3,1] / 2 [1,2] / 2 [1,2,3] |
| 4 | `run4.json` | 84,197 | 5,002 ms | 2 [2,3,1] / 2 [1,2] / 2 [1,2,3] |
| 5 | `run5.json` (also `../follower-pause.json`) | 83,910 | 3,063 ms | 2 [2,3,1] / 2 [1,2] / 2 [1,2,3] |

Run 1 is unexplained. Pausing follower 1 should leave broker 3 leading, but leadership moved to broker 2, and broker 1 was still outside the ISR 15 s after it resumed. Runs 2 to 5 did what was expected. Node logs were not kept for run 1 (`run-all.sh` saves them to `chaos/work/` since then), so the cause is not known. No acknowledged record was lost in that run either.
