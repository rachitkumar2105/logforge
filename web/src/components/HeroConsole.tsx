"use client";

import { useEffect, useRef, useState } from "react";
import { PauseIcon, PlayIcon } from "./Icons";
import { int } from "@/lib/format";
import { createSim, kill, restart, run, setRate, view, type Sim } from "@/lib/replicationSim";
import { usePageVisible, useReducedMotionPref } from "@/lib/useReducedMotion";

const CELLS = 18;
/** Every cycle the console kills its leader, waits for the controller to fence it, then restarts it. */
const CYCLE_MS = 16_000;
const KILL_AT_MS = 5_000;
const RESTART_AT_MS = 11_000;

type Row = { id: number; role: string; state: string; failing: boolean; cells: ("c" | "p" | "e")[] };
type Snap = { t: number; leader: number | null; epoch: number; isr: number[]; hw: number; acked: number; lost: number; rows: Row[]; last: string };

function snap(s: Sim): Snap {
  const v = view(s);
  const end = v.leader === null ? Math.max(...s.brokers.map((b) => b.log.length)) : s.brokers[v.leader]!.log.length;
  const start = Math.max(0, end - CELLS);
  const rows = v.brokers.map((b) => {
    const len = s.brokers[b.id]!.log.length;
    const cells = Array.from({ length: CELLS }, (_, i) => {
      const o = start + i;
      return o >= len ? "e" : o < v.hw ? "c" : "p";
    }) as Row["cells"];
    const failing = b.status === "down" || !b.alive;
    return {
      id: b.id,
      role: b.role,
      failing,
      state: b.status === "down" ? "killed" : !b.alive ? "fenced" : b.inIsr ? "in sync" : "catching up",
      cells,
    };
  });
  return {
    t: v.now,
    leader: v.leader,
    epoch: v.epoch,
    isr: v.isr,
    hw: v.hw,
    acked: v.stats.acked,
    lost: v.stats.lostAcked,
    rows,
    last: s.events.at(-1)?.text ?? "producing",
  };
}

/**
 * The hero's live console: the replication model from the lab, running unattended. It is the same
 * code and rules, so the failover it shows every cycle is the model's, not an animation.
 */
export function HeroConsole() {
  const reduced = useReducedMotionPref();
  const visible = usePageVisible();
  const simRef = useRef<Sim | null>(null);
  const cycleRef = useRef({ killed: -1, restarted: -1 });
  const [s0] = useState(() => {
    const s = createSim({ seed: 27 });
    setRate(s, 25);
    run(s, 2_500); // start with a filled tape, not an empty one
    simRef.current = s;
    return snap(s);
  });
  const [frame, setFrame] = useState<Snap>(s0);
  const [paused, setPaused] = useState(false);

  useEffect(() => {
    if (reduced || paused || !visible) return;
    let raf = 0;
    let last = performance.now();
    let carry = 0;
    let painted = 0;
    const tick = (t: number) => {
      const s = simRef.current!;
      carry += Math.min(250, t - last);
      last = t;
      const ms = Math.floor(carry / 10) * 10;
      carry -= ms;
      if (ms > 0) run(s, ms);
      const cycle = Math.floor(s.now / CYCLE_MS);
      const phase = s.now % CYCLE_MS;
      const c = cycleRef.current;
      if (phase >= KILL_AT_MS && c.killed !== cycle && s.part.leader !== null) {
        c.killed = cycle;
        kill(s, s.part.leader);
      }
      if (phase >= RESTART_AT_MS && c.restarted !== cycle) {
        c.restarted = cycle;
        for (const b of s.brokers) if (b.status === "down") restart(s, b.id);
      }
      if (t - painted > 90) {
        painted = t;
        setFrame(snap(s));
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [reduced, paused, visible]);

  return (
    <div className="panel hero-console relative p-4 sm:p-5" aria-label="Replication model, running live">
      <div className="flex items-center justify-between gap-3">
        <p className="label flex items-center gap-2">
          <span aria-hidden className="broker-dot" data-phase={reduced || paused ? "unavailable" : "live"} />
          replication model, live
        </p>
        {reduced ? null : (
          <button
            type="button"
            className="btn h-9 min-h-9 px-2.5 text-xs"
            aria-pressed={paused}
            aria-label={paused ? "Resume the live console" : "Pause the live console"}
            onClick={() => setPaused((p) => !p)}
          >
            {paused ? <PlayIcon width={16} height={16} /> : <PauseIcon width={16} height={16} />}
          </button>
        )}
      </div>

      <div className="mt-4 grid gap-2.5" aria-hidden>
        {frame.rows.map((r) => (
          <div key={r.id} className="grid grid-cols-[4.75rem_minmax(0,1fr)] items-center gap-3">
            <div className="mono text-xs leading-tight">
              <span className={r.failing ? "text-fail" : r.role === "leader" ? "text-accent" : "text-text"}>
                broker {r.id + 1}
              </span>
              <br />
              <span className={r.failing ? "text-fail" : "text-muted"}>{r.role === "leader" && !r.failing ? "leader" : r.state}</span>
            </div>
            <div className="hero-tape" data-failing={r.failing}>
              {r.cells.map((c, i) => (
                <span key={i} className="hero-cell" data-c={c} />
              ))}
            </div>
          </div>
        ))}
      </div>

      <dl className="mono mt-5 grid grid-cols-3 gap-3 border-t border-line pt-4 text-xs">
        <div>
          <dt className="text-muted">leader · epoch</dt>
          <dd className={`mt-1 text-sm ${frame.leader === null ? "text-fail" : "text-accent"}`}>
            {frame.leader === null ? "electing" : `${frame.leader + 1} · ${frame.epoch}`}
          </dd>
        </div>
        <div>
          <dt className="text-muted">acknowledged</dt>
          <dd className="mt-1 text-sm tabular-nums">{int(frame.acked)}</dd>
        </div>
        <div>
          <dt className="text-muted">acked lost</dt>
          <dd className={`mt-1 text-sm tabular-nums ${frame.lost > 0 ? "text-fail" : "text-accent"}`}>{int(frame.lost)}</dd>
        </div>
      </dl>
      <p className="mono mt-3 truncate text-xs text-muted" aria-live="off">
        <span className="tabular-nums">{(frame.t / 1000).toFixed(1)} s</span> · {frame.last}
      </p>
      <p className="mt-3 text-xs text-muted">
        Kills its own leader every {CYCLE_MS / 1000} s with the broker&apos;s timings.{" "}
        <a href="#lab">Run it yourself</a>
      </p>
    </div>
  );
}
