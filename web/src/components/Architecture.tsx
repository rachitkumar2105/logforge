"use client";

import { motion } from "motion/react";

type Node = { x: number; y: number; w: number; label: string; sub?: string; accent?: boolean };
type Step = { id: string; title: string; text: string; nodes: Node[]; paths: string[] };

const STEPS: Step[] = [
  {
    id: "write",
    title: "1. Write path",
    text:
      "The producer batches records per partition and sends one batch per partition to the leader. The leader checks the CRC and the producer's sequence number, stamps the offset and leader epoch, and appends with one sequential write into the page cache. The sparse index gains an entry every 4 KB.",
    nodes: [
      { x: 20, y: 60, w: 120, label: "producer", sub: "batch + linger" },
      { x: 200, y: 60, w: 130, label: "leader", sub: "crc, sequence", accent: true },
      { x: 390, y: 20, w: 170, label: "active segment", sub: "append, page cache" },
      { x: 390, y: 100, w: 170, label: "sparse index", sub: "1 entry / 4 KB" },
    ],
    paths: ["M140 82 H200", "M330 82 C360 82 360 42 390 42", "M330 82 C360 82 360 122 390 122"],
  },
  {
    id: "replicate",
    title: "2. Replication path",
    text:
      "Followers pull. Each asks the leader for everything after its own log end offset; that offset tells the leader exactly how far the follower has got. The high watermark is the smallest log end across the in-sync replicas, and acks=all answers once it passes the batch.",
    nodes: [
      { x: 20, y: 20, w: 140, label: "follower A", sub: "fetch from LEO" },
      { x: 20, y: 100, w: 140, label: "follower B", sub: "fetch from LEO" },
      { x: 230, y: 60, w: 130, label: "leader", sub: "tracks LEOs", accent: true },
      { x: 420, y: 60, w: 140, label: "high watermark", sub: "min LEO over ISR" },
    ],
    paths: ["M160 42 C195 42 195 82 230 82", "M160 122 C195 122 195 82 230 82", "M360 82 H420"],
  },
  {
    id: "read",
    title: "3. Read path",
    text:
      "A consumer fetch for offset N finds the segment whose base offset is the greatest one at or below N, binary-searches that segment's index for the floor entry, then reads batch headers forward to the batch holding N. It never receives anything at or above the high watermark.",
    nodes: [
      { x: 20, y: 60, w: 120, label: "consumer", sub: "fetch offset N" },
      { x: 190, y: 60, w: 110, label: "segment", sub: "floor base" },
      { x: 340, y: 60, w: 100, label: "index", sub: "binary search" },
      { x: 470, y: 60, w: 90, label: "scan", sub: "to N" },
    ],
    paths: ["M140 82 H190", "M300 82 H340", "M440 82 H470"],
  },
];

function Diagram({ step }: { step: Step }) {
  return (
    <svg viewBox="0 0 580 165" className="h-auto w-full" role="img" aria-label={`${step.title} diagram`}>
      {step.paths.map((d, i) => (
        <motion.path
          key={d}
          d={d}
          fill="none"
          stroke="var(--accent)"
          strokeWidth={1.5}
          initial={{ pathLength: 0 }}
          whileInView={{ pathLength: 1 }}
          viewport={{ once: true, amount: 0.6 }}
          transition={{ duration: 0.7, delay: 0.15 * i, ease: [0.22, 1, 0.36, 1] }}
        />
      ))}
      {step.nodes.map((n) => (
        <g key={n.label}>
          <rect
            x={n.x}
            y={n.y}
            width={n.w}
            height={44}
            rx={5}
            fill="var(--panel-2)"
            stroke={n.accent ? "var(--accent)" : "var(--line)"}
          />
          <text x={n.x + 10} y={n.y + 19} fill="var(--text)" fontSize="13" fontFamily="var(--font-geist-mono)">
            {n.label}
          </text>
          {n.sub ? (
            <text x={n.x + 10} y={n.y + 35} fill="var(--muted)" fontSize="11" fontFamily="var(--font-geist-mono)">
              {n.sub}
            </text>
          ) : null}
        </g>
      ))}
    </svg>
  );
}

export function Architecture() {
  return (
    <section id="architecture" aria-labelledby="arch-title" className="border-b border-line py-20">
      <div className="wrap">
        <p className="label">06 / Architecture walkthrough</p>
        <h2 id="arch-title" className="h2 mt-3 max-w-3xl">
          Three paths through the broker
        </h2>
        <div className="mt-10 grid gap-6">
          {STEPS.map((s) => (
            <motion.article
              key={s.id}
              className="panel grid items-center gap-6 p-5 lg:grid-cols-[1fr_1.2fr]"
              initial={{ opacity: 0, y: 24 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, amount: 0.35 }}
              transition={{ duration: 0.6, ease: [0.22, 1, 0.36, 1] }}
            >
              <div>
                <h3 className="h3">{s.title}</h3>
                <p className="mt-3 text-muted">{s.text}</p>
              </div>
              <Diagram step={s} />
            </motion.article>
          ))}
        </div>
      </div>
    </section>
  );
}
