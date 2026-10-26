"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { PauseIcon, PlayIcon, StepBackIcon, StepForwardIcon } from "./Icons";
import type { ChaosResult, Replay as ReplayData, ReplayFrame } from "@/lib/types";
import { int, ms } from "@/lib/format";
import { useReducedMotionPref } from "@/lib/useReducedMotion";

type Event = { at: number; label: string; tone: "fail" | "accent" | "muted" };

/** Derives the story's events from the recorded frames; nothing here is scripted. */
function events(data: ReplayData): Event[] {
  const out: Event[] = [];
  const frames = data.frames;
  const first = frames[0];
  const p0 = first?.partitions[0];
  if (!first || !p0) return out;
  const originalLeader = p0.leader;
  out.push({ at: data.leaderStoppedAtMs, label: `leader broker ${originalLeader} killed (SIGKILL); acks stop`, tone: "fail" });
  const down = frames.find((f) => f.brokers.some((b) => b.id === originalLeader && !b.alive));
  if (down) out.push({ at: down.t, label: `controller declares broker ${originalLeader} dead`, tone: "fail" });
  const elected = frames.find((f) => {
    const p = f.partitions[0];
    return p !== undefined && p.leader > 0 && p.leader !== originalLeader;
  });
  if (elected) {
    out.push({ at: elected.t, label: `broker ${elected.partitions[0]?.leader} elected leader from the ISR`, tone: "accent" });
  }
  out.push({ at: data.writesResumedAtMs, label: "acknowledgements resume", tone: "accent" });
  const back = frames.find(
    (f) => down && f.t > down.t && f.brokers.some((b) => b.id === originalLeader && b.alive),
  );
  if (back) out.push({ at: back.t, label: `broker ${originalLeader} restarts as a follower`, tone: "muted" });
  const rejoined = frames.find((f) => back && f.t > back.t && f.partitions[0]?.isr.includes(originalLeader));
  if (rejoined) out.push({ at: rejoined.t, label: `broker ${originalLeader} caught up, back in the ISR`, tone: "accent" });
  return out.sort((a, b) => a.at - b.at);
}

function Tape({ frame, max }: { frame: ReplayFrame; max: number }) {
  const p = frame.partitions[0];
  if (!p) return null;
  const hw = p.replicaState.find((r) => r.brokerId === p.leader)?.highWatermark ?? 0;
  return (
    <div className="grid gap-3">
      {frame.brokers.map((b) => {
        const st = p.replicaState.find((r) => r.brokerId === b.id);
        const end = st && st.logEndOffset >= 0 ? st.logEndOffset : 0;
        const role = !b.alive ? "down" : p.leader === b.id ? "leader" : p.isr.includes(b.id) ? "in sync" : "catching up";
        const color = !b.alive ? "bg-fail" : p.leader === b.id ? "bg-accent" : p.isr.includes(b.id) ? "bg-accent-dim" : "bg-[var(--warn-line)]";
        return (
          <div key={b.id} className="grid grid-cols-[7.5rem_1fr] items-center gap-3 sm:grid-cols-[9rem_1fr]">
            <div className="mono text-xs">
              <div className={!b.alive ? "text-fail" : "text-text"}>broker {b.id}</div>
              <div className="text-muted">{role}</div>
            </div>
            <div className="relative h-6 overflow-hidden rounded-sm border border-line bg-panel-2">
              {/* Width is fixed; the bar is scaled with transform so only transform animates. */}
              <div
                className={`absolute inset-y-0 left-0 w-full origin-left ${color} transition-transform duration-200`}
                style={{ transform: `scaleX(${max > 0 ? Math.min(1, end / max) : 0})`, opacity: 0.75, transitionTimingFunction: "var(--ease-state)" }}
              />
              {/* Full-width wrapper moved by a percentage of its own width: the line sits at hw/max. */}
              <div
                className="absolute inset-y-0 left-0 w-full transition-transform duration-200"
                style={{
                  transform: `translateX(${max > 0 ? Math.min(100, (hw / max) * 100) : 0}%)`,
                  transitionTimingFunction: "var(--ease-state)",
                }}
              >
                <span className="block h-full w-px bg-text" />
              </div>
              <span className="mono absolute right-2 top-1/2 -translate-y-1/2 text-[11px] text-text">
                {int(end)}
              </span>
            </div>
          </div>
        );
      })}
      <p className="mono text-xs text-muted">bar: log end offset per replica of partition 0, line: high watermark</p>
    </div>
  );
}

export function Replay({ data, chaos }: { data: ReplayData | null; chaos: ChaosResult | undefined }) {
  const reduced = useReducedMotionPref();
  const frames = useMemo(() => data?.frames ?? [], [data]);
  const [idx, setIdx] = useState(0);
  const [playing, setPlaying] = useState(false);
  const started = useRef(false);
  const rootRef = useRef<HTMLElement>(null);
  const evs = useMemo(() => (data ? events(data) : []), [data]);
  const max = useMemo(
    () =>
      frames.reduce(
        (m, f) => Math.max(m, ...(f.partitions[0]?.replicaState.map((r) => r.logEndOffset) ?? [0])),
        0,
      ),
    [frames],
  );

  // Autoplay once when scrolled into view, never under reduced motion.
  useEffect(() => {
    if (reduced || !rootRef.current) return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting) && !started.current) {
          started.current = true;
          setPlaying(true);
        }
      },
      { threshold: 0.4 },
    );
    io.observe(rootRef.current);
    return () => io.disconnect();
  }, [reduced]);

  useEffect(() => {
    if (!playing || reduced) return;
    const id = window.setInterval(() => {
      setIdx((i) => {
        if (i >= frames.length - 1) {
          setPlaying(false);
          return i;
        }
        return i + 1;
      });
    }, 120);
    return () => window.clearInterval(id);
  }, [playing, reduced, frames.length]);

  if (!data || frames.length === 0) {
    return (
      <section id="replay" className="border-b border-line py-20">
        <div className="wrap">
          <p className="label">03 / Crash-recovery replay</p>
          <p className="mt-4 text-muted">No recorded chaos run is bundled with this build.</p>
        </div>
      </section>
    );
  }

  const frame = frames[idx]!;
  const total = frames[frames.length - 1]!.t;
  const p0 = frame.partitions[0];
  const passed = evs.filter((e) => e.at <= frame.t);

  return (
    <section ref={rootRef} id="replay" aria-labelledby="replay-title" className="border-b border-line py-20">
      <div className="wrap">
        <p className="label">03 / Crash-recovery replay</p>
        <h2 id="replay-title" className="h2 mt-3 max-w-3xl">
          Kill the leader mid-write and watch the cluster recover
        </h2>
        <p className="mt-4 max-w-2xl text-muted">
          A recorded run of <span className="mono">chaos/run-all.sh leader-kill</span>: an idempotent acks=all producer
          writes 2,000 records a second to a topic with replication factor 3 while the leader of partition 0 is killed
          with SIGKILL and restarted 15 seconds later. Every frame is a real 200 ms sample of the controller&apos;s view.
        </p>

        <div className="mt-10 grid gap-8 lg:grid-cols-[1fr_20rem]">
          <div className="panel p-5">
            <div className="mb-6 flex flex-wrap items-end gap-x-10 gap-y-4">
              <div>
                <p className="label">time</p>
                <p className="mono text-2xl">{ms(frame.t)}</p>
              </div>
              <div>
                <p className="label">acknowledged records</p>
                <p className="mono text-2xl text-accent">{int(frame.acked)}</p>
              </div>
              <div>
                <p className="label">partition 0</p>
                <p className="mono text-sm">
                  leader {p0 && p0.leader > 0 ? p0.leader : "none"}, epoch {p0?.leaderEpoch ?? "n/a"}, ISR [
                  {p0?.isr.join(", ")}]
                </p>
              </div>
            </div>
            <Tape frame={frame} max={max} />
            <div className="mt-8 flex flex-wrap items-center gap-2">
              <button type="button" className="btn" aria-label="Step back" onClick={() => setIdx((i) => Math.max(0, i - 1))}>
                <StepBackIcon />
              </button>
              <button
                type="button"
                className="btn"
                aria-label={playing ? "Pause" : "Play"}
                aria-pressed={playing}
                onClick={() => {
                  if (idx >= frames.length - 1) setIdx(0);
                  setPlaying((p) => !p);
                }}
              >
                {playing ? <PauseIcon /> : <PlayIcon />}
              </button>
              <button
                type="button"
                className="btn"
                aria-label="Step forward"
                onClick={() => setIdx((i) => Math.min(frames.length - 1, i + 1))}
              >
                <StepForwardIcon />
              </button>
              <label className="flex min-h-[44px] flex-1 items-center gap-3">
                <span className="sr-only">Replay position</span>
                <input
                  type="range"
                  min={0}
                  max={frames.length - 1}
                  value={idx}
                  onChange={(e) => {
                    setPlaying(false);
                    setIdx(Number(e.target.value));
                  }}
                  aria-valuetext={`${ms(frame.t)} of ${ms(total)}`}
                  className="w-full accent-[var(--accent)]"
                />
              </label>
            </div>
          </div>

          <aside className="panel p-5" aria-live="polite">
            <p className="label">measured recovery time</p>
            <p className="mono mt-1 text-4xl text-accent">{ms(data.recoveryTimeMs)}</p>
            <p className="mt-2 text-sm text-muted">
              Longest gap between two acknowledgements, from the producer&apos;s own log.
              {chaos ? ` Verdict ${chaos.verdict}: ${int(chaos.acked)} acked, ${int(chaos.missingAcked)} missing, ${int(chaos.duplicates)} duplicated.` : ""}
            </p>
            <ol className="mt-6 grid gap-3">
              {evs.map((e) => {
                const done = passed.includes(e);
                return (
                  <li key={`${e.at}-${e.label}`} className={`grid grid-cols-[4.5rem_1fr] gap-2 text-sm ${done ? "" : "opacity-40"}`}>
                    <span className="mono text-muted">{ms(e.at)}</span>
                    <span className={done && e.tone === "fail" ? "text-fail" : done && e.tone === "accent" ? "text-accent" : ""}>
                      {e.label}
                    </span>
                  </li>
                );
              })}
            </ol>
          </aside>
        </div>
      </div>
    </section>
  );
}
