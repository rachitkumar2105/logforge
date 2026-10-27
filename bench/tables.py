"""Prints the benchmark and chaos tables used in FINAL-AUDIT.md and RUN-REPORT.md, straight from
bench/results and chaos/results, so no number in the reports is typed by hand.

    python bench/tables.py
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROWS = [
    ("size-100B", "100 B, acks=1"),
    ("baseline-1KB", "1 KB, acks=1 (baseline)"),
    ("size-10KB", "10 KB, acks=1"),
    ("acks-0", "1 KB, acks=0"),
    ("acks-all", "1 KB, acks=all"),
    ("partitions-1", "1 KB, 1 partition"),
    ("partitions-16", "1 KB, 16 partitions"),
    ("fsync-never", "1 KB, fsync never (same config as baseline)"),
    ("fsync-ms-1000", "1 KB, fsync every 1 s"),
    ("fsync-records-1", "1 KB, fsync every batch"),
]
SCENARIOS = ["leader-kill", "follower-pause", "disk-full", "network-partition", "restart-under-load"]


def load(system, label):
    return json.loads((ROOT / "bench" / "results" / f"{system}-{label}.json").read_text(encoding="utf-8"))


def bench():
    base = load("logforge", "baseline-1KB")
    c, hw = base["config"], base["hardware"]
    kafka = load("kafka", "baseline-1KB")
    print(f"Run: `{base['command']}`, finished {base['finishedAt'][:16].replace('T', ' ')} UTC (LogForge) and "
          f"{kafka['finishedAt'][:16].replace('T', ' ')} UTC (Kafka). LogForge {base['version']}; "
          f"Apache Kafka 3.9.0 brokers, {kafka['version']}.")
    print(f"Fixed config unless the row says otherwise: {c['recordSizeBytes']} B records, acks={c['acks']}, "
          f"{c['partitions']} partitions, replication factor {c['replicationFactor']}, batch {c['batchSizeBytes'] // 1024} KB, "
          f"linger {c['lingerMs']} ms, client buffer {c['producerBufferBytes'] // (1024 * 1024)} MiB, compression {c['compression']}, "
          f"{c['keys']}, warm-up {c['warmupSeconds']} s, measured {c['measureSeconds']} s.")
    print(f"Hardware: {hw['host']}. Containers: {hw['environment']}. JVM: {hw['java']}.")
    print()
    print("| Configuration | LogForge records/s | MB/s | p50 ms | p99 ms | Kafka records/s | MB/s | p50 ms | p99 ms | Kafka/LogForge MB/s |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for label, name in ROWS:
        lf, k = load("logforge", label), load("kafka", label)
        l, kr = lf["results"], k["results"]
        lp, kp = l["produceLatencyMs"], kr["produceLatencyMs"]
        print(f"| {name} | {l['recordsPerSec']:,} | {l['mbPerSec']} | {lp['p50']:.0f} | {lp['p99']:.0f} | "
              f"{kr['recordsPerSec']:,} | {kr['mbPerSec']} | {kp['p50']:.0f} | {kp['p99']:.0f} | "
              f"{kr['mbPerSec'] / l['mbPerSec']:.2f}x |")
    print()
    for system in ("logforge", "kafka"):
        r = load(system, "e2e-acks-all")["results"]
        print(f"- e2e {system}: sent {r['sent']:,}, received {r['received']:,}; send to receive p50 "
              f"{r['sendToReceiveMs']['p50']:.1f} ms, p99 {r['sendToReceiveMs']['p99']:.1f} ms; ack to receive p50 "
              f"{r['ackToReceiveMs']['p50']:.2f} ms, p99 {r['ackToReceiveMs']['p99']:.1f} ms")


def chaos():
    print()
    print("| Scenario | Fault | Acked | Missing | Duplicates | Recovery | Verdict | Finished (UTC) |")
    print("|---|---|---|---|---|---|---|---|")
    for s in SCENARIOS:
        d = json.loads((ROOT / "chaos" / "results" / f"{s}.json").read_text(encoding="utf-8"))
        print(f"| {s} | {d['description']} | {d['acked']:,} | {d['missingAcked']} | {d['duplicates']} | "
              f"{d['recoveryTimeMs'] / 1000:.2f} s | {d['verdict']} | {d['finishedAt'][:16].replace('T', ' ')} |")
    w = json.loads((ROOT / "chaos" / "results" / "leader-kill.json").read_text(encoding="utf-8"))["workload"]
    print()
    print(f"Workload: acks={w['acks']}, idempotent={w['idempotent']}, replication factor {w['replicationFactor']}, "
          f"min.insync.replicas {w['minInsyncReplicas']}, {w['partitions']} partitions, {w['ratePerSec']} records/s.")


if __name__ == "__main__":
    bench()
    chaos()
