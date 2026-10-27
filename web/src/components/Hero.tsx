import type { BenchResult } from "@/lib/types";
import { fixed, int, recordSize } from "@/lib/format";
import { TapeToggle } from "./TapeToggle";

/** The partition tape: record batches of varying width drifting along an offset rail. */
function TapeBackground() {
  // Deterministic widths so server and client render the same SVG.
  const widths = [38, 22, 54, 30, 46, 26, 62, 34, 28, 50, 40, 24, 58, 32, 44, 36];
  let x = 0;
  const cells = widths.map((w, i) => {
    const cell = { x, w, i };
    x += w + 6;
    return cell;
  });
  const span = x;
  const row = (y: number, offset: number, opacity: number) => (
    <g opacity={opacity}>
      {[0, 1].map((copy) =>
        cells.map((c) => (
          <rect
            key={`${copy}-${c.i}`}
            x={c.x + copy * span + offset}
            y={y}
            width={c.w}
            height={14}
            rx={2}
            fill={c.i % 5 === 0 ? "var(--accent)" : "var(--panel-2)"}
            stroke="var(--line)"
            fillOpacity={c.i % 5 === 0 ? 0.5 : 1}
          />
        )),
      )}
    </g>
  );
  return (
    <svg
      className="pointer-events-none absolute inset-x-0 bottom-6 h-28 w-full"
      viewBox={`0 0 ${span} 110`}
      preserveAspectRatio="xMidYMid slice"
      aria-hidden
    >
      <line x1="0" y1="55" x2={span * 2} y2="55" stroke="var(--line)" strokeDasharray="2 6" />
      <g className="drift" style={{ transformBox: "view-box" }}>
        {row(20, 0, 0.55)}
        {row(48, -90, 0.9)}
        {row(76, -40, 0.45)}
      </g>
    </svg>
  );
}

export function Hero({ headline }: { headline: BenchResult | undefined }) {
  const r = headline?.results;
  const c = headline?.config;
  return (
    <section aria-labelledby="hero-title" className="relative overflow-hidden border-b border-line">
      <TapeBackground />
      <TapeToggle />
      <div className="wrap relative pt-16 pb-44 md:pt-24 md:pb-52">
        <p className="label mb-6">Distributed commit log / Java 21 / no Kafka libraries in the broker</p>
        <h1 id="hero-title" className="h1 max-w-4xl" translate="no">
          LogForge
        </h1>
        <p className="mt-6 max-w-2xl text-muted">
          A Kafka-style distributed log written from scratch: segmented files, a sparse offset index, leader and
          follower replication with an in-sync set, consumer groups, idempotent producers and compaction. Every
          durability claim on this page comes from a test that killed a process.
        </p>
        <p className="mt-4 max-w-2xl text-sm text-muted">
          <strong className="font-semibold text-text">Live and recorded, plainly.</strong> The hosted demo is a single
          broker on a free tier: it proves the broker runs, and the topology section shows it when it is awake. The
          durability and throughput claims rest on the crash-recovery replay and on the benchmark and chaos results,
          all measured on a local three-node cluster and committed to the repository.
        </p>
        {r && c ? (
          <div className="mt-12 grid gap-2">
            <p className="label">Measured throughput, local three-broker cluster</p>
            <p className="mono text-[length:clamp(2.2rem,1.4rem+3vw,4rem)] leading-none text-accent">
              {int(r.recordsPerSec)}
              <span className="ml-3 text-[0.4em] text-text">records/s</span>
            </p>
            <p className="mono text-muted text-sm">
              {fixed(r.mbPerSec, 1)} MB/s with {recordSize(c.recordSizeBytes)} records, acks={c.acks},{" "}
              {c.partitions} partitions, replication factor {c.replicationFactor}, fsync {c.fsync}, batch{" "}
              {c.batchSizeBytes / 1024} KB, linger {c.lingerMs} ms.{" "}
              <a href="#benchmarks">Hardware, method and the Kafka comparison</a>
            </p>
          </div>
        ) : (
          <p className="mt-12 text-muted">No benchmark results recorded yet.</p>
        )}
      </div>
    </section>
  );
}
