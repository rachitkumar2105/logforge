"""Summarises repeated follower-pause runs (chaos/follower-pause-study.sh).

For every run: which follower was paused, partition 0's leader and ISR before, during and after,
whether leadership moved although the leader was never paused, which brokers the controller
declared dead (and after how long a silence), the longest heartbeat silence of every broker that
was *not* paused, and whether the paused follower was back in every ISR at the end.

    python chaos/follower-pause-study.py chaos/work/fp-study
"""

import json
import re
import sys
from pathlib import Path

TOPIC = "chaos-follower-pause"


def console_facts(text):
    before = re.search(r"before fault: partition 0 leader and ISR = (\d+) ([\d ]+)", text)
    paused = re.search(r"follower broker (\d+)", text)
    after = re.search(r"after recovery: partition 0 leader and ISR = (-?\d+) ?([\d ]*)", text)
    verdict = re.search(r"verdict (\w+) acked (\d+) missing (\d+) duplicates (\d+) recovery ms (\d+)", text)
    return {
        "leader_before": int(before.group(1)) if before else None,
        "paused": int(paused.group(1)) if paused else None,
        "leader_after": int(after.group(1)) if after else None,
        "isr_after": [int(x) for x in after.group(2).split()] if after else [],
        "verdict": verdict.group(1) if verdict else "?",
        "acked": int(verdict.group(2)) if verdict else 0,
        "missing": int(verdict.group(3)) if verdict else -1,
        "recovery_ms": int(verdict.group(5)) if verdict else -1,
    }


def timeline_facts(path, paused):
    leaders, max_silence, last = [], {}, None
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            frame = json.loads(line)
        except json.JSONDecodeError:
            continue
        c = frame["cluster"]
        for b in c.get("brokers", []):
            if b.get("controller") or b["id"] == paused:
                continue
            max_silence[b["id"]] = max(max_silence.get(b["id"], 0), b.get("lastHeartbeatMsAgo", 0))
        for t in c.get("topics", []):
            if t["name"] != TOPIC:
                continue
            for p in t["partitions"]:
                if p["partition"] == 0 and (not leaders or leaders[-1] != p["leader"]):
                    leaders.append(p["leader"])
        last = c
    all_isr_back = None
    if last is not None and paused is not None:
        isrs = [p["isr"] for t in last.get("topics", []) if t["name"] == TOPIC for p in t["partitions"]]
        all_isr_back = all(paused in isr for isr in isrs) if isrs else None
    return leaders, max_silence, all_isr_back


def log_facts(run_dir):
    expired, heartbeat_failures = [], {}
    for log in sorted(run_dir.glob("follower-pause-*.log")):
        node = log.stem.replace("follower-pause-", "")
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            if '"broker session expired"' in line:
                m = re.search(r'"broker":(\d+).*?"silentMs":(\d+)', line)
                if m:
                    expired.append((int(m.group(1)), int(m.group(2))))
            if '"heartbeat failed"' in line:
                heartbeat_failures[node] = heartbeat_failures.get(node, 0) + 1
    return expired, heartbeat_failures


def main(root):
    root = Path(root)
    runs = sorted((p for p in root.iterdir() if p.is_dir() and p.name.startswith("run")),
                  key=lambda p: int(p.name[3:]))
    anomalies = 0
    print("| run | paused | leader before -> after | leader path | declared dead (silent ms) | max silence of unpaused brokers (ms) | paused back in all ISRs | verdict | missing | recovery ms |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for r in runs:
        f = console_facts((root / f"{r.name}.console").read_text(encoding="utf-8", errors="replace"))
        leaders, silence, back = timeline_facts(r / "timeline.jsonl", f["paused"])
        expired, hb_fail = log_facts(r)
        unexpected = [b for b, _ in expired if b != f["paused"]]
        moved = f["leader_before"] != f["leader_after"] and f["leader_before"] != f["paused"]
        anomalous = moved or bool(unexpected) or back is False
        anomalies += anomalous
        print(f"| {r.name} | {f['paused']} | {f['leader_before']} -> {f['leader_after']} | {leaders} | "
              f"{expired or '-'} | {dict(sorted(silence.items()))} | {back} | {f['verdict']} | "
              f"{f['missing']} | {f['recovery_ms']} |" + (" **anomaly**" if anomalous else ""))
    print()
    print(f"runs: {len(runs)}, anomalous: {anomalies} ({100.0 * anomalies / max(1, len(runs)):.1f}%)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "chaos/work/fp-study")
