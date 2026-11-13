"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { PauseIcon, PlayIcon, PowerIcon, RotateIcon, SendIcon, SlowIcon } from "./Icons";
import { int } from "@/lib/format";
import {
  createSim,
  DEFAULTS,
  kill,
  pause,
  produce,
  restart,
  resume,
  run,
  setRate,
  setSlow,
  setUnclean,
  view,
  type Sim,
  type SimEvent,
  type SimView,
} from "@/lib/replicationSim";
import { usePageVisible, useReducedMotionPref } from "@/lib/useReducedMotion";

/** Offsets shown on each tape. */
const WINDOW = 28;
/** React re-renders at most this often; the model itself steps every 10 ms of model time. */
const FRAME_MS = 66;
const RATES = [0, 10, 40, 150] as const;
const HISTORY = 60; // samples of acknowledged/s, one every 500 ms: the last 30 s

type CellState = "committed" | "pending" | "divergent";
type Cell = { offset: number; id: number; state: CellState };
type Frame = {
  v: SimView;
  start: number;
  rows: Cell[][];
  events: SimEvent[];
  history: number[];
};

type Step = { at: number; label: string; act: (s: Sim) => void };
type Scenario = { key: string; label: string; summary: string; steps: (s: Sim) => Step[] };

const firstFollower = (s: Sim) => s.brokers.find((b) => b.id !== s.part.leader && b.status === "up")?.id ?? 1;

const SCENARIOS: Scenario[] = [
  {
    key: "failover",
    label: "Kill the leader",
    summary: "SIGKILL the leader, watch the controller fence it after 3 s and elect from the ISR, then bring it back.",
    steps: (s) => {
      const leader = s.part.leader ?? 0;
      return [
        { at: 0, label: `kill broker ${leader + 1}`, act: (x) => kill(x, leader) },
        { at: 8_000, label: `restart broker ${leader + 1}`, act: (x) => restart(x, leader) },
      ];
    },
  },
  {
    key: "lag",
    label: "Stall a follower",
    summary: "A follower keeps heartbeating but stops fetching. After 10 s of lag the leader drops it from the ISR so acks keep flowing.",
    steps: (s) => {
      const f = firstFollower(s);
      return [
        { at: 0, label: `stall broker ${f + 1}'s fetcher`, act: (x) => setSlow(x, f, true) },
        { at: 14_000, label: `unstall broker ${f + 1}`, act: (x) => setSlow(x, f, false) },
      ];
    },
  },
  {
    key: "unclean",
    label: "Lose data (unclean election)",
    summary:
      "Turn on unclean election, shrink the ISR to the leader alone, then kill it while a lagging replica is the only one alive.",
    steps: (s) => {
      const leader = s.part.leader ?? 0;
      const [a, b] = s.brokers.filter((x) => x.id !== leader).map((x) => x.id) as [number, number];
      return [
        { at: 0, label: "enable unclean election", act: (x) => setUnclean(x, true) },
        { at: 0, label: `kill broker ${a + 1}`, act: (x) => kill(x, a) },
        { at: 5_000, label: `kill broker ${b + 1}`, act: (x) => kill(x, b) },
        {
          at: 9_000,
          label: `restart broker ${a + 1}, kill broker ${leader + 1}`,
          act: (x) => {
            restart(x, a);
            kill(x, leader);
          },
        },
      ];
    },
  },
];

function snapshot(s: Sim, history: number[]): Frame {
  const v = view(s);
  // Follow the leader's log; with no leader, the longest log.
  const end = v.leader === null ? Math.max(...s.brokers.map((b) => b.log.length)) : s.brokers[v.leader]!.log.length;
  const start = Math.max(0, end - WINDOW);
  const leaderLog = v.leader === null ? null : s.brokers[v.leader]!.log;
  const rows = s.brokers.map((b) => {
    const cells: Cell[] = [];
    for (let o = start; o < Math.min(start + WINDOW, b.log.length); o++) {
      const r = b.log[o]!;
      const agrees = leaderLog ? leaderLog[o]?.id === r.id : true;
      cells.push({
        offset: o,
        id: r.id,
        state: !agrees ? "divergent" : o < v.hw ? "committed" : "pending",
      });
    }
    return cells;
  });
  return { v, start, rows, events: s.events.slice(-7).reverse(), history: [...history] };
}

const fmtT = (ms: number) => `${(ms / 1000).toFixed(2)} s`;

export function ReplicationLab() {
  const reduced = useReducedMotionPref();
  const pageVisible = usePageVisible();
  const sectionRef = useRef<HTMLElement>(null);
  const simRef = useRef<Sim>(createSim({ seed: 2105 }));
  const historyRef = useRef<number[]>([]);
  const lastSampleRef = useRef({ at: 0, acked: 0 });
  const scriptRef = useRef<{ key: string; steps: Step[]; startedAt: number } | null>(null);
  const [frame, setFrame] = useState<Frame>(() => snapshot(simRef.current, []));
  const [onScreen, setOnScreen] = useState(false);
  const [running, setRunning] = useState(false);
  const [scenario, setScenario] = useState<string | null>(null);
  const [startedOnce, setStartedOnce] = useState(false);

  const redraw = useCallback(() => setFrame(snapshot(simRef.current, historyRef.current)), []);

  // Start producing the first time the lab scrolls into view, unless motion is reduced.
  useEffect(() => {
    const el = sectionRef.current;
    if (!el) return;
    const io = new IntersectionObserver(([e]) => setOnScreen(Boolean(e?.isIntersecting)), { threshold: 0.15 });
    io.observe(el);
    return () => io.disconnect();
  }, []);

  useEffect(() => {
    if (onScreen && !startedOnce && !reduced) {
      setStartedOnce(true);
      setRate(simRef.current, 40);
      setRunning(true);
    }
  }, [onScreen, startedOnce, reduced]);

  // The clock: real time drives model time while the lab is visible and running.
  useEffect(() => {
    if (!running || !onScreen || !pageVisible) return;
    let raf = 0;
    let last = performance.now();
    let carry = 0;
    let lastPaint = 0;
    const tick = (t: number) => {
      const s = simRef.current;
      carry += Math.min(250, t - last); // a long frame (tab switch) is not replayed in one burst
      last = t;
      const ms = Math.floor(carry / 10) * 10;
      carry -= ms;
      const script = scriptRef.current;
      if (script) {
        // Fire scripted steps at their model time, even inside a long frame.
        let remaining = ms;
        while (remaining > 0) {
          const due = script.steps[0];
          const until = due ? Math.max(0, script.startedAt + due.at - s.now) : remaining;
          const chunk = Math.min(remaining, Math.ceil(until / 10) * 10);
          if (chunk > 0) run(s, chunk);
          remaining -= chunk;
          if (due && s.now >= script.startedAt + due.at) {
            due.act(s);
            script.steps.shift();
          } else if (!due) {
            break;
          }
        }
        if (remaining > 0) run(s, remaining);
        if (script.steps.length === 0) {
          scriptRef.current = null;
          setScenario(null);
        }
      } else if (ms > 0) {
        run(s, ms);
      }
      const sample = lastSampleRef.current;
      if (s.now - sample.at >= 500) {
        const perSec = ((s.stats.acked - sample.acked) * 1000) / (s.now - sample.at);
        historyRef.current = [...historyRef.current, perSec].slice(-HISTORY);
        lastSampleRef.current = { at: s.now, acked: s.stats.acked };
      }
      if (t - lastPaint >= FRAME_MS) {
        lastPaint = t;
        redraw();
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [running, onScreen, pageVisible, redraw]);

  const act = (fn: (s: Sim) => void) => {
    fn(simRef.current);
    redraw();
  };

  const startScenario = (sc: Scenario) => {
    const s = simRef.current;
    if (s.config.ratePerSec < 10) setRate(s, 40);
    scriptRef.current = { key: sc.key, steps: sc.steps(s), startedAt: s.now };
    setScenario(sc.key);
    setRunning(true);
    redraw();
  };

  const reset = () => {
    const rate = simRef.current.config.ratePerSec;
    simRef.current = createSim({ seed: Math.floor(Math.random() * 1e9) });
    setRate(simRef.current, rate);
    historyRef.current = [];
    lastSampleRef.current = { at: 0, acked: 0 };
    scriptRef.current = null;
    setScenario(null);
    redraw();
  };

  const { v, start, rows, events, history } = frame;
  const lost = v.stats.lostAcked;
  const peak = Math.max(10, ...history);

  return (
    <section ref={sectionRef} id="lab" aria-labelledby="lab-title" className="border-b border-line py-20">
      <div className="wrap">
        <p className="label">02b / Replication lab</p>
        <div className="mt-3 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <h2 id="lab-title" className="h2 max-w-3xl">
            Break the cluster yourself
          </h2>
          <p className="mono text-xs text-muted lg:text-right" data-testid="lab-model-note">
            model in your browser · broker rules and default timings
          </p>
        </div>
        <p className="mt-4 max-w-3xl text-muted">
          Three brokers, one partition, replication factor 3, <span className="mono text-text">acks=all</span>,{" "}
          <span className="mono text-text">min.insync.replicas={DEFAULTS.minInsyncReplicas}</span>. It runs the
          broker&apos;s own decision rules in real time: heartbeats every {DEFAULTS.heartbeatIntervalMs} ms, fencing after{" "}
          {DEFAULTS.sessionTimeoutMs / 1000} s of silence, ISR shrink after {DEFAULTS.replicaLagTimeMaxMs / 1000} s of lag,
          election from the in-sync set and epoch-based truncation. It is a model, not the hosted broker, so its counts
          are the model&apos;s. Kill, freeze or stall a broker and see which records survive.
        </p>

        {/* Controls */}
        <div className="panel mt-8 flex flex-col gap-4 p-4 md:flex-row md:flex-wrap md:items-center md:justify-between">
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              className="btn"
              onClick={() => {
                setStartedOnce(true);
                setRunning((r) => !r);
              }}
            >
              {running ? <PauseIcon /> : <PlayIcon />}
              {running ? "Pause time" : "Run"}
            </button>
            <fieldset className="flex items-center gap-1" aria-label="Producer rate">
              <legend className="sr-only">Producer rate, records per second</legend>
              <span className="label mr-1 hidden sm:inline">producer</span>
              {RATES.map((r) => (
                <button
                  key={r}
                  type="button"
                  className="btn px-3 tabular-nums"
                  aria-pressed={v.ratePerSec === r}
                  onClick={() => act((s) => setRate(s, r))}
                >
                  {r === 0 ? "off" : `${r}/s`}
                </button>
              ))}
            </fieldset>
            <button type="button" className="btn" onClick={() => act((s) => produce(s, 1))}>
              <SendIcon /> send 1
            </button>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              className={`btn ${v.unclean ? "!border-fail !text-fail" : ""}`}
              aria-pressed={v.unclean}
              onClick={() => act((s) => setUnclean(s, !s.config.unclean))}
            >
              unclean election {v.unclean ? "on" : "off"}
            </button>
            <button type="button" className="btn" onClick={reset}>
              <RotateIcon /> reset
            </button>
          </div>
        </div>

        <div className="mt-3 flex flex-wrap items-center gap-2" role="group" aria-label="Scenarios">
          <span className="label mr-1">scenarios</span>
          {SCENARIOS.map((sc) => (
            <button
              key={sc.key}
              type="button"
              className="btn"
              aria-pressed={scenario === sc.key}
              disabled={scenario !== null && scenario !== sc.key}
              title={sc.summary}
              onClick={() => startScenario(sc)}
            >
              {sc.label}
            </button>
          ))}
        </div>
        <p className="mt-2 min-h-5 text-sm text-muted" aria-live="polite">
          {scenario ? SCENARIOS.find((x) => x.key === scenario)?.summary : ""}
        </p>

        {/* Readouts */}
        <dl className="lab-readouts panel mt-4 grid grid-cols-2 gap-px overflow-hidden sm:grid-cols-4 lg:grid-cols-8">
          <Readout label="model time" value={fmtT(v.now)} />
          <Readout
            label="leader"
            value={v.leader === null ? "none" : `broker ${v.leader + 1}`}
            sub={`epoch ${v.epoch}`}
            tone={v.leader === null ? "fail" : "accent"}
          />
          <Readout label="in-sync set" value={`{${v.isr.map((x) => x + 1).join(", ")}}`} sub={v.offline ? "offline" : `${v.isr.length} of 3`} tone={v.offline ? "fail" : undefined} />
          <Readout label="high watermark" value={int(v.hw)} />
          <Readout label="acknowledged" value={int(v.stats.acked)} sub={`${int(v.stats.produced)} sent`} />
          <Readout label="in flight" value={int(v.inFlight)} sub={`${int(v.stats.refused + v.stats.failed)} refused or retried`} />
          <Readout
            label="ack latency"
            value={v.p50 === null ? "n/a" : `${int(v.p50)} ms`}
            sub={v.p99 === null ? "p50" : `p50 · p99 ${int(v.p99)} ms`}
          />
          <Readout
            label="acked records lost"
            value={int(lost)}
            sub={`${int(v.stats.truncatedUnacked)} unacked truncated`}
            tone={lost > 0 ? "fail" : "accent"}
            testId="lab-lost"
          />
        </dl>

        {/* Tapes */}
        <div className="panel mt-4 p-4">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2 text-xs text-muted">
            <span className="mono">
              offsets {int(start)} to {int(start + WINDOW - 1)}
            </span>
            <ul className="flex flex-wrap gap-x-4 gap-y-1" aria-label="Tape legend">
              <li className="flex items-center gap-1.5">
                <span aria-hidden className="lab-swatch" data-state="committed" /> committed (below HW)
              </li>
              <li className="flex items-center gap-1.5">
                <span aria-hidden className="lab-swatch" data-state="pending" /> written, not yet committed
              </li>
              <li className="flex items-center gap-1.5">
                <span aria-hidden className="lab-swatch" data-state="divergent" /> diverges from the leader
              </li>
            </ul>
          </div>
          <div className="grid gap-3">
            {v.brokers.map((b, i) => (
              <BrokerRow
                key={b.id}
                b={b}
                cells={rows[i]!}
                start={start}
                hw={v.hw}
                reduced={reduced}
                onKill={() => act((s) => kill(s, b.id))}
                onRestart={() => act((s) => restart(s, b.id))}
                onPause={() => act((s) => (b.status === "paused" ? resume(s, b.id) : pause(s, b.id)))}
                onSlow={() => act((s) => setSlow(s, b.id, !b.slow))}
              />
            ))}
          </div>
        </div>

        {/* Throughput and events */}
        <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,3fr)]">
          <figure className="panel p-4">
            <figcaption className="label">acknowledged per second, last 30 s</figcaption>
            <svg viewBox={`0 0 ${HISTORY * 6} 80`} className="mt-3 h-24 w-full" preserveAspectRatio="none" role="img" aria-label={`Latest: ${int(history.at(-1) ?? 0)} acknowledged per second`}>
              <line x1="0" x2={HISTORY * 6} y1="79.5" y2="79.5" stroke="var(--line)" />
              {history.map((h, i) => {
                const height = Math.max(h > 0 ? 1.5 : 0, (h / peak) * 74);
                const x = (HISTORY - history.length + i) * 6;
                return (
                  <rect
                    key={i}
                    x={x + 1}
                    y={80 - height}
                    width={4}
                    height={height}
                    rx={1}
                    fill={h === 0 && v.stats.produced > 0 ? "var(--line)" : "var(--accent)"}
                    opacity={0.35 + (0.65 * (i + 1)) / Math.max(1, history.length)}
                  />
                );
              })}
            </svg>
            <p className="mono mt-2 text-xs text-muted">
              now {int(history.at(-1) ?? 0)}/s · peak {int(Math.max(0, ...history))}/s
            </p>
          </figure>
          <div className="panel p-4">
            <p className="label">controller and broker log</p>
            <ol className="mt-3 grid gap-1.5 text-sm" aria-live="polite" aria-relevant="additions">
              {events.length === 0 ? (
                <li className="text-muted">Quiet. Start the producer or a scenario.</li>
              ) : (
                events.map((e) => (
                  <li key={`${e.t}-${e.text}`} className="lab-event flex gap-3">
                    <span className="mono shrink-0 tabular-nums text-muted">{fmtT(e.t)}</span>
                    <span className={e.level === "fail" ? "text-fail" : e.level === "ok" ? "text-accent" : "text-text"}>
                      {e.text}
                    </span>
                  </li>
                ))
              )}
            </ol>
          </div>
        </div>
      </div>
    </section>
  );
}

function Readout({
  label,
  value,
  sub,
  tone,
  testId,
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: "accent" | "fail";
  testId?: string;
}) {
  return (
    <div className="bg-panel p-3">
      <dt className="label">{label}</dt>
      <dd
        className={`mono mt-1 text-lg tabular-nums ${tone === "fail" ? "text-fail" : tone === "accent" ? "text-accent" : "text-text"}`}
        data-testid={testId}
      >
        {value}
      </dd>
      {sub ? <dd className="mono text-xs text-muted">{sub}</dd> : null}
    </div>
  );
}

function BrokerRow({
  b,
  cells,
  start,
  hw,
  reduced,
  onKill,
  onRestart,
  onPause,
  onSlow,
}: {
  b: SimView["brokers"][number];
  cells: Cell[];
  start: number;
  hw: number;
  reduced: boolean;
  onKill: () => void;
  onRestart: () => void;
  onPause: () => void;
  onSlow: () => void;
}) {
  const statusText =
    b.status === "down"
      ? "killed"
      : b.status === "paused"
        ? "frozen"
        : !b.alive
          ? "fenced"
          : b.slow
            ? "fetcher stalled"
            : "running";
  const failing = b.status === "down" || !b.alive;
  // Slot i shows offset start + i. Slots are fixed, so a running tape reads like a ticker.
  const slots: (Cell | null)[] = Array.from({ length: WINDOW }, () => null);
  for (const c of cells) slots[c.offset - start] = c;
  return (
    <div
      className="lab-row grid gap-3 rounded-md border border-line p-3 md:grid-cols-[11rem_minmax(0,1fr)_auto] md:items-center"
      data-role={b.role}
      data-failing={failing}
    >
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 md:block">
        <p className="flex items-center gap-2 whitespace-nowrap text-sm font-semibold">
          <span
            aria-hidden
            className={`inline-block h-3 w-3 rounded-[2px] border ${
              failing ? "border-fail bg-fail/30" : b.role === "leader" ? "border-accent bg-accent" : b.inIsr ? "border-accent" : "border-dashed border-[var(--warn-line)]"
            }`}
          />
          broker {b.id + 1}
          <span className={`mono text-xs font-normal ${b.role === "leader" && !failing ? "text-accent" : "text-muted"}`}>
            {b.role}
          </span>
        </p>
        <p className={`mono text-xs ${failing ? "text-fail" : "text-muted"}`}>
          {statusText} · {b.inIsr ? "in ISR" : "out of ISR"}
        </p>
        <p className="mono text-xs text-muted tabular-nums">
          LEO {int(b.leo)} · hb {b.status === "up" ? `${int(Math.min(b.heartbeatAgoMs, 9999))} ms` : "none"}
        </p>
      </div>

      <div className="lab-tape" aria-hidden data-reduced={reduced}>
        {slots.map((c, i) => (
          <span
            key={i}
            className="lab-slot"
            data-state={c?.state ?? "empty"}
            data-hw={start + i === hw ? "edge" : start + i === hw - 1 && hw === start + WINDOW ? "end" : undefined}
          />
        ))}
      </div>
      <p className="sr-only">
        Broker {b.id + 1}: {b.role}, {statusText}, {b.inIsr ? "in" : "out of"} the in-sync set, log end offset {b.leo}.
      </p>

      <div className="flex flex-wrap gap-2 md:justify-end">
        {b.status === "down" ? (
          <button type="button" className="btn text-xs" onClick={onRestart}>
            <RotateIcon width={16} height={16} /> restart
          </button>
        ) : (
          <button type="button" className="btn text-xs" onClick={onKill}>
            <PowerIcon width={16} height={16} /> kill
          </button>
        )}
        <button
          type="button"
          className="btn text-xs"
          aria-pressed={b.status === "paused"}
          disabled={b.status === "down"}
          onClick={onPause}
        >
          {b.status === "paused" ? <PlayIcon width={16} height={16} /> : <PauseIcon width={16} height={16} />}
          {b.status === "paused" ? "resume" : "freeze"}
        </button>
        <button
          type="button"
          className="btn text-xs"
          aria-pressed={b.slow}
          disabled={b.status === "down"}
          onClick={onSlow}
          aria-label={`${b.slow ? "Unstall" : "Stall"} broker ${b.id + 1}'s fetcher`}
        >
          <SlowIcon width={16} height={16} /> {b.slow ? "unstall" : "stall"}
        </button>
      </div>
    </div>
  );
}
