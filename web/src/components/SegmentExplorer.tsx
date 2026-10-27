"use client";

import { useEffect, useMemo, useState } from "react";
import { lookup, type LookupResult } from "@/lib/lookup";
import type { SegmentSample } from "@/lib/types";
import { bytes, int } from "@/lib/format";
import { useReducedMotionPref } from "@/lib/useReducedMotion";

/**
 * Enter an offset and watch the broker's read path: pick the segment file by base offset, binary
 * search its sparse index, then scan batch headers forward. The data is a real partition written
 * by the real Log code (bench SegmentSample); the lookup runs in the browser.
 */
export function SegmentExplorer({ sample }: { sample: SegmentSample }) {
  const reduced = useReducedMotionPref();
  const last = sample.segments[sample.segments.length - 1];
  const end = last ? last.nextOffset : 0;
  const [input, setInput] = useState(String(Math.floor(end * 0.62)));
  const [result, setResult] = useState<LookupResult | null>(null);
  const [stage, setStage] = useState(3);

  const run = (value: string) => {
    const n = Number(value);
    const r = lookup(sample, Number.isFinite(n) ? n : -1);
    setResult(r);
    setStage(reduced ? 3 : 0);
  };

  useEffect(() => {
    run(input);
    // run once with the default offset
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Walk the three steps one after another.
  useEffect(() => {
    if (stage >= 3) return;
    const id = window.setTimeout(() => setStage((s) => s + 1), 420);
    return () => window.clearTimeout(id);
  }, [stage]);

  const totalBytes = useMemo(() => sample.segments.reduce((s, x) => s + x.sizeBytes, 0), [sample]);
  const ok = result && result.ok ? result : null;
  const seg = ok ? sample.segments[ok.segment] : undefined;
  const idxStep = ok?.steps[1];
  const scanStep = ok?.steps[2];

  return (
    <section id="explorer" aria-labelledby="explorer-title" className="border-b border-line py-20">
      <div className="wrap">
        <p className="label">04 / Log segment explorer</p>
        <h2 id="explorer-title" className="h2 mt-3 max-w-3xl">
          Where does offset N live on disk?
        </h2>
        <p className="mt-4 max-w-2xl text-muted">
          Partition <span className="mono">orders-0</span>, written by the broker&apos;s own log code: segments roll
          at {bytes(sample.segmentBytes)} and the sparse index holds one entry per {bytes(sample.indexIntervalBytes)}{" "}
          of data. A lookup picks the segment, binary-searches its index, then scans batch headers forward.
        </p>

        <form
          className="mt-8 flex flex-wrap items-end gap-3"
          onSubmit={(e) => {
            e.preventDefault();
            run(input);
          }}
        >
          <label className="grid gap-1">
            <span className="label">offset (0 to {int(end - 1)})</span>
            <input
              name="offset"
              inputMode="numeric"
              autoComplete="off"
              spellCheck={false}
              value={input}
              onChange={(e) => setInput(e.target.value.replace(/[^0-9]/g, ""))}
              className="mono min-h-[44px] w-40 rounded-md border border-line bg-panel-2 px-3 text-text"
            />
          </label>
          <button type="submit" className="btn">
            Look up
          </button>
          {[0, Math.floor(end / 2), end - 1].map((o) => (
            <button
              key={o}
              type="button"
              className="btn"
              onClick={() => {
                setInput(String(o));
                run(String(o));
              }}
            >
              {int(o)}
            </button>
          ))}
        </form>

        {result && !result.ok ? <p className="mt-6 text-sm text-muted">{result.reason}</p> : null}

        {/* Step 1: the segment files, widths proportional to their size. */}
        <div className="mt-8">
          <p className="label mb-2">1. segment files</p>
          <div className="flex h-14 w-full gap-1" role="list" aria-label="Segment files">
            {sample.segments.map((s, i) => {
              const chosen = ok && stage >= 1 && ok.segment === i;
              const probed = ok && stage >= 0 && ok.steps[0]?.kind === "segment" && ok.steps[0].probes.includes(i);
              return (
                <div
                  key={s.baseOffset}
                  role="listitem"
                  className={`relative min-w-0 rounded-sm border px-1 py-1 ${chosen ? "border-accent" : "border-line"} bg-panel-2`}
                  style={{ flexGrow: s.sizeBytes / totalBytes }}
                  aria-current={chosen ? "true" : undefined}
                >
                  <div
                    aria-hidden
                    className="absolute inset-0 bg-accent transition-opacity duration-300"
                    style={{ opacity: chosen ? 0.22 : probed ? 0.08 : 0 }}
                  />
                  <span className="mono relative block truncate text-[11px] text-muted">{int(s.baseOffset)}</span>
                </div>
              );
            })}
          </div>
        </div>

        {seg && ok ? (
          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            <div className="panel p-4">
              <p className="label mb-3">2. sparse index of {seg.fileName.replace(".log", ".index")}</p>
              <ol className="mono grid max-h-64 grid-cols-2 gap-1 overflow-y-auto text-xs sm:grid-cols-3" aria-label="Index entries">
                {seg.index.length === 0 ? <li className="text-muted">empty: start at byte 0</li> : null}
                {seg.index.map((e, i) => {
                  const hit = idxStep?.kind === "index" && stage >= 2 && idxStep.entry === i;
                  const probed = idxStep?.kind === "index" && stage >= 1 && idxStep.probes.includes(i);
                  return (
                    <li
                      key={e.offset}
                      className={`rounded-sm border px-2 py-1 ${hit ? "border-accent text-accent" : probed ? "border-line text-text" : "border-transparent text-muted"}`}
                    >
                      {int(e.offset)} @ {int(e.position)}
                    </li>
                  );
                })}
              </ol>
            </div>
            <div className="panel p-4">
              <p className="label mb-3">3. scan batch headers from byte {int(idxStep?.kind === "index" ? idxStep.position : 0)}</p>
              <ol className="mono grid gap-1 text-xs" aria-label="Batches scanned">
                {scanStep?.kind === "scan"
                  ? scanStep.visited.map((bi) => {
                      const b = seg.batches[bi]!;
                      const hit = bi === ok.batch && stage >= 3;
                      return (
                        <li
                          key={bi}
                          className={`flex justify-between rounded-sm border px-2 py-1 ${hit ? "border-accent text-accent" : "border-line text-muted"}`}
                          style={{ opacity: stage >= 2 ? 1 : 0.3 }}
                        >
                          <span>
                            offsets {int(b.baseOffset)} to {int(b.lastOffset)}
                          </span>
                          <span>byte {int(b.position)}</span>
                        </li>
                      );
                    })
                  : null}
              </ol>
              <p className="mt-4 text-sm" aria-live="polite">
                {stage >= 3 ? (
                  <>
                    Offset <span className="mono text-accent">{input}</span> is in{" "}
                    <span className="mono">{seg.fileName}</span> at byte{" "}
                    <span className="mono">{int(ok.position)}</span>, after{" "}
                    {ok.steps[0]?.kind === "segment" ? ok.steps[0].probes.length : 0} segment probes,{" "}
                    {idxStep?.kind === "index" ? idxStep.probes.length : 0} index probes and{" "}
                    {scanStep?.kind === "scan" ? scanStep.visited.length : 0} header reads.
                  </>
                ) : (
                  <span className="text-muted">looking up…</span>
                )}
              </p>
            </div>
          </div>
        ) : null}
      </div>
    </section>
  );
}
