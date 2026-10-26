"use client";

import { motion } from "motion/react";
import { useEffect, useMemo, useRef, useState } from "react";
import { BrokerIcon } from "./Icons";
import type { BrokerView, ClusterResponse, PartitionView, TopicView } from "@/lib/types";
import { fixed, int } from "@/lib/format";
import { usePageVisible, useReducedMotionPref } from "@/lib/useReducedMotion";

const POLL_MS = 3_000;

type Role = "leader" | "insync" | "lagging" | "down";

function roleOf(p: PartitionView, broker: BrokerView): Role {
  if (!broker.alive || p.offline) return "down";
  if (p.leader === broker.id) return "leader";
  return p.isr.includes(broker.id) ? "insync" : "lagging";
}

const roleText: Record<Role, string> = {
  leader: "leader",
  insync: "in-sync follower",
  lagging: "out-of-sync follower",
  down: "unavailable",
};

function Swatch({ role }: { role: Role }) {
  const styles: Record<Role, string> = {
    leader: "bg-accent border-accent",
    insync: "border-accent",
    lagging: "border-dashed border-[var(--warn-line)]",
    down: "border-fail bg-fail/30",
  };
  return <span aria-hidden className={`inline-block h-3 w-3 rounded-[2px] border ${styles[role]}`} />;
}

function Legend() {
  return (
    <ul className="flex flex-wrap gap-x-6 gap-y-2 text-sm text-muted" aria-label="Legend">
      {(Object.keys(roleText) as Role[]).map((r) => (
        <li key={r} className="flex items-center gap-2">
          <Swatch role={r} />
          {roleText[r]}
        </li>
      ))}
    </ul>
  );
}

function ReplicaChip({ p, broker }: { p: PartitionView; broker: BrokerView }) {
  const role = roleOf(p, broker);
  const state = p.replicaState.find((r) => r.brokerId === broker.id);
  const frame: Record<Role, string> = {
    leader: "border-accent",
    insync: "border-accent/70",
    lagging: "border-dashed border-[var(--warn-line)]",
    down: "border-fail",
  };
  return (
    <div
      className={`relative z-10 mx-auto flex w-full max-w-[13rem] items-center gap-2 rounded-md border bg-panel px-2 py-1.5 ${frame[role]}`}
    >
      <Swatch role={role} />
      <div className="mono min-w-0 text-xs leading-tight">
        <div className={role === "down" ? "text-fail" : role === "leader" ? "text-accent" : "text-text"}>
          p{p.partition} {roleText[role]}
        </div>
        <div className="text-muted">
          end {state ? int(state.logEndOffset) : "n/a"} / hw {state ? int(state.highWatermark) : "n/a"}
        </div>
      </div>
    </div>
  );
}

/** Lines from the leader's column to each follower's column, with pulses when data flowed. */
function ReplicationLines({
  p,
  brokers,
  pulseKey,
  flowing,
}: {
  p: PartitionView;
  brokers: BrokerView[];
  pulseKey: number;
  flowing: boolean;
}) {
  const reduced = useReducedMotionPref();
  const n = brokers.length;
  const col = (id: number) => brokers.findIndex((b) => b.id === id);
  const lc = col(p.leader);
  if (lc < 0 || p.offline) return null;
  return (
    <div aria-hidden className="pointer-events-none absolute inset-0">
      {p.replicas
        .filter((r) => r !== p.leader)
        .map((f) => {
          const fc = col(f);
          if (fc < 0) return null;
          const left = (Math.min(lc, fc) + 0.5) / n;
          const width = Math.abs(fc - lc) / n;
          const inSync = p.isr.includes(f);
          const forward = fc > lc;
          return (
            <div
              key={f}
              className="absolute top-1/2"
              style={{ left: `${left * 100}%`, width: `${width * 100}%` }}
            >
              <div
                className={`h-px w-full ${inSync ? "bg-accent/40" : "border-t border-dashed border-[var(--warn-line)]"}`}
              />
              {flowing && inSync && !reduced ? (
                <motion.div
                  key={`${pulseKey}-${f}`}
                  className="absolute top-0 h-0 w-full"
                  initial={{ x: forward ? "0%" : "100%", opacity: 0 }}
                  animate={{ x: forward ? "100%" : "0%", opacity: [0, 1, 1, 0] }}
                  transition={{ duration: 0.9, ease: [0.22, 1, 0.36, 1] }}
                >
                  <span className="absolute -top-[3px] -left-[3px] block h-[7px] w-[7px] rounded-full bg-accent" />
                </motion.div>
              ) : null}
            </div>
          );
        })}
    </div>
  );
}

function BrokerHeader({ b, leaders }: { b: BrokerView; leaders: number }) {
  return (
    <div className={`panel relative overflow-hidden p-3 ${b.alive ? "" : "border-fail"}`}>
      {/* The "drain" to the failure colour is an opacity change on an overlay. */}
      <div
        aria-hidden
        className="absolute inset-0 bg-fail transition-opacity duration-700"
        style={{ opacity: b.alive ? 0 : 0.14, transitionTimingFunction: "var(--ease-state)" }}
      />
      <div className="relative flex items-center gap-2">
        <BrokerIcon className={b.alive ? "text-accent" : "text-fail"} />
        <span className="mono text-sm">broker {b.id}</span>
        <span className={`mono ml-auto text-xs ${b.alive ? "text-muted" : "text-fail"}`}>
          {b.alive ? `${leaders} leading` : "DOWN"}
        </span>
      </div>
    </div>
  );
}

export function Topology({ initial }: { initial: ClusterResponse }) {
  const [data, setData] = useState<ClusterResponse>(initial);
  const [pollCount, setPollCount] = useState(0);
  const [showInternal, setShowInternal] = useState(false);
  const [topicName, setTopicName] = useState<string | null>(null);
  const prevEnd = useRef<Map<string, number>>(new Map());
  const [flowing, setFlowing] = useState<Set<string>>(new Set());
  const visible = usePageVisible();

  useEffect(() => {
    if (!visible) return;
    let stopped = false;
    const tick = async () => {
      try {
        const res = await fetch("/api/cluster", { cache: "no-store" });
        if (!res.ok) return;
        const next = (await res.json()) as ClusterResponse;
        if (stopped) return;
        const grew = new Set<string>();
        for (const t of next.cluster.topics) {
          for (const p of t.partitions) {
            const key = `${t.name}-${p.partition}`;
            const leaderEnd = p.replicaState.find((r) => r.brokerId === p.leader)?.logEndOffset ?? -1;
            const before = prevEnd.current.get(key);
            if (before !== undefined && leaderEnd > before) grew.add(key);
            prevEnd.current.set(key, leaderEnd);
          }
        }
        setFlowing(grew);
        setData(next);
        setPollCount((c) => c + 1);
      } catch {
        // keep showing the last state; the status line says when it was fetched
      }
    };
    void tick();
    const id = window.setInterval(tick, POLL_MS);
    return () => {
      stopped = true;
      window.clearInterval(id);
    };
  }, [visible]);

  const cluster = data.cluster;
  const brokers = useMemo(
    () =>
      cluster.brokers
        .filter((b) => !(b.controller && !cluster.topics.some((t) => t.partitions.some((p) => p.replicas.includes(b.id)))))
        .sort((a, b) => a.id - b.id),
    [cluster],
  );
  const topics: TopicView[] = cluster.topics.filter((t) => showInternal || !t.internal);
  const topic = topics.find((t) => t.name === topicName) ?? topics[0];
  const partitions = topic?.partitions.slice(0, 16) ?? [];
  const allPartitions = cluster.topics.flatMap((t) => t.partitions);
  const underReplicated = allPartitions.filter((p) => p.underReplicated || p.offline).length;
  const alive = brokers.filter((b) => b.alive).length;
  const leadersOf = (id: number) => allPartitions.filter((p) => p.leader === id).length;
  const controller = cluster.brokers.find((b) => b.controller);

  const status =
    data.source === "live"
      ? `Live from the cluster. ${alive} of ${brokers.length} brokers up, ${allPartitions.length} partitions, ${underReplicated} under-replicated or offline.`
      : `Showing a recorded snapshot from ${new Date(cluster.capturedAtMs).toISOString().slice(0, 16).replace("T", " ")} UTC (${data.reason ?? "cluster unreachable"}).`;

  return (
    <section id="topology" aria-labelledby="topology-title" className="border-b border-line py-20">
      <div className="wrap">
        <p className="label">02 / Live cluster topology</p>
        <h2 id="topology-title" className="h2 mt-3 max-w-3xl">
          Who leads, who follows, who is in sync
        </h2>
        <p className="mt-4 max-w-2xl text-muted">
          Polled every three seconds from the controller&apos;s admin API through a server-side route; the browser
          never holds a token. Leaders are solid, in-sync followers are outlined, followers outside the ISR are dashed.
          When a broker dies its column turns red, its partitions lose it from the ISR and leadership moves.
        </p>

        <div className="mt-8 flex flex-wrap items-center gap-x-8 gap-y-3">
          <p
            aria-live="polite"
            className={`mono text-sm ${data.source === "live" ? "text-accent" : "text-muted"}`}
          >
            <span
              aria-hidden
              className={`mr-2 inline-block h-2 w-2 rounded-full ${data.source === "live" ? "bg-accent" : "bg-[var(--warn-line)]"}`}
            />
            {status}
          </p>
          <p className="mono text-sm text-muted">
            {fixed(cluster.rates.messagesInPerSec, 0)} msg/s in, {fixed(cluster.rates.bytesInPerSec / 1e6, 2)} MB/s
          </p>
          {underReplicated > 0 ? (
            <p className="mono text-sm text-fail">{underReplicated} under-replicated</p>
          ) : null}
        </div>

        <div className="mt-6 flex flex-wrap items-center gap-3">
          {topics.map((t) => (
            <button
              key={t.name}
              type="button"
              className="btn"
              aria-pressed={t.name === topic?.name}
              onClick={() => setTopicName(t.name)}
            >
              {t.name}
            </button>
          ))}
          <label className="btn">
            <input
              type="checkbox"
              className="accent-[var(--accent)]"
              checked={showInternal}
              onChange={(e) => setShowInternal(e.target.checked)}
            />
            internal topics
          </label>
        </div>

        <div className="mt-6">
          <Legend />
        </div>
        {controller ? (
          <p className="mono mt-3 text-xs text-muted">
            controller: node {controller.id} ({controller.alive ? "up" : "down"}), single fixed controller, see
            limitations
          </p>
        ) : null}

        {!topic ? (
          <p className="panel mt-8 p-6 text-muted">No topics yet. Brokers are shown above once they register.</p>
        ) : (
          <>
            {/* Desktop and tablet: brokers as columns, partitions as rows. */}
            <div className="mt-8 hidden md:block">
              <div className="grid gap-3" style={{ gridTemplateColumns: `repeat(${brokers.length}, minmax(0, 1fr))` }}>
                {brokers.map((b) => (
                  <BrokerHeader key={b.id} b={b} leaders={leadersOf(b.id)} />
                ))}
              </div>
              <div className="mt-3 grid gap-2">
                {partitions.map((p) => (
                  <div
                    key={p.partition}
                    className="relative grid items-center gap-3 py-1"
                    style={{ gridTemplateColumns: `repeat(${brokers.length}, minmax(0, 1fr))` }}
                  >
                    <ReplicationLines
                      p={p}
                      brokers={brokers}
                      pulseKey={pollCount}
                      flowing={flowing.has(`${topic.name}-${p.partition}`)}
                    />
                    {brokers.map((b) => (
                      <div key={b.id}>{p.replicas.includes(b.id) ? <ReplicaChip p={p} broker={b} /> : null}</div>
                    ))}
                  </div>
                ))}
              </div>
            </div>

            {/* Phones: brokers stacked, partition detail behind a tap. */}
            <div className="mt-8 grid gap-3 md:hidden">
              {brokers.map((b) => {
                const held = partitions.filter((p) => p.replicas.includes(b.id));
                return (
                  <div key={b.id}>
                    <BrokerHeader b={b} leaders={leadersOf(b.id)} />
                    <details className="panel mt-1 px-3">
                      <summary className="mono text-sm text-muted">
                        {held.length} replicas of {topic.name}
                      </summary>
                      <div className="grid gap-2 pb-3">
                        {held.map((p) => (
                          <ReplicaChip key={p.partition} p={p} broker={b} />
                        ))}
                      </div>
                    </details>
                  </div>
                );
              })}
            </div>

            <details className="panel mt-6 px-4">
              <summary className="text-sm">Topology as a table</summary>
              <div className="overflow-x-auto pb-4">
                <table className="data">
                  <caption className="sr-only">Partitions of {topic.name}</caption>
                  <thead>
                    <tr>
                      <th scope="col">partition</th>
                      <th scope="col">leader</th>
                      <th scope="col">replicas</th>
                      <th scope="col">in-sync</th>
                      <th scope="col">log end by replica</th>
                      <th scope="col">state</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topic.partitions.map((p) => (
                      <tr key={p.partition}>
                        <td>{p.partition}</td>
                        <td>{p.leader < 0 ? "none" : p.leader}</td>
                        <td>{p.replicas.join(", ")}</td>
                        <td>{p.isr.join(", ")}</td>
                        <td>{p.replicaState.map((r) => `${r.brokerId}:${r.logEndOffset}`).join("  ")}</td>
                        <td className={p.offline || p.underReplicated ? "text-fail" : ""}>
                          {p.offline ? "offline" : p.underReplicated ? "under-replicated" : "healthy"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>
          </>
        )}
      </div>
    </section>
  );
}
