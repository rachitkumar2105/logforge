#!/usr/bin/env bash
# Repeats the follower-pause scenario and keeps everything each run produced: the result, the
# 200 ms cluster timeline (liveness, heartbeat age, leaders, ISR, LEO and HW per replica) and
# every node's log. Then summarises with chaos/follower-pause-study.py.
#
#   bash chaos/follower-pause-study.sh 20
set -uo pipefail
cd "$(dirname "$0")/.."
N="${1:-20}"
OUT="chaos/work/fp-study"
rm -rf "$OUT"; mkdir -p "$OUT"
for i in $(seq 1 "$N"); do
  bash chaos/run-all.sh follower-pause > "$OUT/run$i.console" 2>&1
  mkdir -p "$OUT/run$i"
  cp chaos/results/follower-pause.json "$OUT/run$i/result.json"
  cp chaos/work/follower-pause.timeline "$OUT/run$i/timeline.jsonl"
  cp chaos/work/follower-pause-*.log "$OUT/run$i/"
  grep -E "before fault|SIGSTOP|ISR while|after recovery|verdict" "$OUT/run$i.console" | sed "s/^/run$i /"
done
python chaos/follower-pause-study.py "$OUT"
