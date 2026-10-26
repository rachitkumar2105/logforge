"use client";

import { useEffect, useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { BenchResult } from "@/lib/types";
import { fixed, int, ms, recordSize } from "@/lib/format";

type Dimension = "recordSize" | "acks" | "partitions" | "fsync";
type Metric = "mbPerSec" | "recordsPerSec" | "p50" | "p99";

const DIMENSIONS: { id: Dimension; title: string; labels: [string, string][] }[] = [
  { id: "recordSize", title: "Record size", labels: [["size-100B", "100 B"], ["baseline-1KB", "1 KB"], ["size-10KB", "10 KB"]] },
  { id: "acks", title: "acks", labels: [["acks-0", "acks=0"], ["baseline-1KB", "acks=1"], ["acks-all", "acks=all"]] },
  { id: "partitions", title: "Partitions", labels: [["partitions-1", "1"], ["baseline-1KB", "4"], ["partitions-16", "16"]] },
  { id: "fsync", title: "fsync policy", labels: [["fsync-never", "never"], ["fsync-ms-1000", "every 1 s"], ["fsync-records-1", "every batch"]] },
];

const METRICS: { id: Metric; title: string; unit: string }[] = [
  { id: "mbPerSec", title: "Throughput", unit: "MB/s" },
  { id: "recordsPerSec", title: "Records/s", unit: "records/s" },
  { id: "p50", title: "Latency p50", unit: "ms" },
  { id: "p99", title: "Latency p99", unit: "ms" },
];

function value(r: BenchResult | undefined, m: Metric): number | undefined {
  if (!r) return undefined;
  if (m === "mbPerSec") return r.results.mbPerSec;
  if (m === "recordsPerSec") return r.results.recordsPerSec;
  return r.results.produceLatencyMs?.[m];
}

function useNarrow(): boolean {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia("(max-width: 640px)");
    const update = () => setNarrow(mq.matches);
    update();
    mq.addEventListener("change", update);
    return () => mq.removeEventListener("change", update);
  }, []);
  return narrow;
}

export function Benchmarks({ results, repoUrl }: { results: BenchResult[]; repoUrl: string }) {
  const [dim, setDim] = useState<Dimension>("recordSize");
  const [metric, setMetric] = useState<Metric>("mbPerSec");
  const [single, setSingle] = useState<"logforge" | "kafka">("logforge");
  const narrow = useNarrow();

  const find = (system: string, label: string) =>
    results.find((r) => r.system === system && r.config.label === label);
  const dimension = DIMENSIONS.find((d) => d.id === dim)!;
  const metricDef = METRICS.find((m) => m.id === metric)!;
  const rows = useMemo(
    () =>
      dimension.labels.map(([label, name]) => ({
        name,
        label,
        logforge: value(find("logforge", label), metric),
        kafka: value(find("kafka", label), metric),
      })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [dim, metric, results],
  );
  const e2e = { logforge: find("logforge", "e2e-acks-all"), kafka: find("kafka", "e2e-acks-all") };
  const baseline = find("logforge", "baseline-1KB");
  const hw = (baseline ?? results[0])?.hardware;
  const kafkaVersion = find("kafka", "baseline-1KB")?.version;

  if (results.length === 0) {
    return (
      <section id="benchmarks" className="border-b border-line py-20">
        <div className="wrap">
          <p className="label">05 / Benchmarks</p>
          <p className="mt-4 text-muted">No results recorded. Run bash bench/run.sh.</p>
        </div>
      </section>
    );
  }

  const showLf = !narrow || single === "logforge";
  const showKafka = !narrow || single === "kafka";

  return (
    <section id="benchmarks" aria-labelledby="bench-title" className="border-b border-line py-20">
      <div className="wrap">
        <p className="label">05 / Benchmark explorer</p>
        <h2 id="bench-title" className="h2 mt-3 max-w-3xl">
          LogForge and Apache Kafka, same workload, same machine
        </h2>
        <p className="mt-4 max-w-3xl text-muted">
          Both clusters have three brokers and run in Docker on the same laptop, with data on a volume and a 1 GiB
          heap per broker. One producer drives each through its own Java client: 64 KB batches, 5 ms linger, an
          8 MiB client buffer, no compression, replication factor 3. One dimension changes at a time from the 1 KB
          baseline. Produce latency is measured at full speed, so it includes time queued in the client buffer.
        </p>

        <div className="mt-8 flex flex-wrap gap-2" role="group" aria-label="Dimension">
          {DIMENSIONS.map((d) => (
            <button key={d.id} type="button" className="btn" aria-pressed={dim === d.id} onClick={() => setDim(d.id)}>
              {d.title}
            </button>
          ))}
        </div>
        <div className="mt-3 flex flex-wrap gap-2" role="group" aria-label="Metric">
          {METRICS.map((m) => (
            <button key={m.id} type="button" className="btn" aria-pressed={metric === m.id} onClick={() => setMetric(m.id)}>
              {m.title}
            </button>
          ))}
        </div>
        {narrow ? (
          <div className="mt-3 flex gap-2" role="group" aria-label="System">
            {(["logforge", "kafka"] as const).map((s) => (
              <button key={s} type="button" className="btn" aria-pressed={single === s} onClick={() => setSingle(s)}>
                {s === "logforge" ? "LogForge" : "Kafka"}
              </button>
            ))}
          </div>
        ) : null}

        <figure className="panel mt-6 p-4">
          <figcaption className="mb-3 flex flex-wrap items-center gap-x-6 gap-y-1 text-sm">
            <span>
              {metricDef.title} by {dimension.title.toLowerCase()} ({metricDef.unit})
            </span>
            {showLf ? (
              <span className="flex items-center gap-2 text-muted">
                <span aria-hidden className="h-3 w-3 rounded-[2px] bg-accent" /> LogForge
              </span>
            ) : null}
            {showKafka ? (
              <span className="flex items-center gap-2 text-muted">
                <span aria-hidden className="h-3 w-3 rounded-[2px] bg-[var(--warn-line)]" /> Apache Kafka
              </span>
            ) : null}
          </figcaption>
          <div className="h-72" aria-hidden>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={rows} margin={{ top: 8, right: 8, bottom: 0, left: 0 }} barGap={6}>
                <CartesianGrid stroke="var(--line)" vertical={false} />
                <XAxis dataKey="name" stroke="var(--muted)" tickLine={false} axisLine={{ stroke: "var(--line)" }} fontSize={12} />
                <YAxis stroke="var(--muted)" tickLine={false} axisLine={false} fontSize={12} width={64} />
                <Tooltip
                  cursor={{ fill: "var(--panel-2)" }}
                  contentStyle={{ background: "var(--panel)", border: "1px solid var(--line)", borderRadius: 6 }}
                  labelStyle={{ color: "var(--text)" }}
                />
                {showLf ? <Bar dataKey="logforge" name="LogForge" fill="var(--accent)" radius={[2, 2, 0, 0]} animationDuration={600} /> : null}
                {showKafka ? <Bar dataKey="kafka" name="Apache Kafka" fill="var(--warn-line)" radius={[2, 2, 0, 0]} animationDuration={600} /> : null}
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-4 overflow-x-auto">
            <table className="data">
              <caption className="sr-only">
                {metricDef.title} by {dimension.title}
              </caption>
              <thead>
                <tr>
                  <th scope="col">{dimension.title}</th>
                  <th scope="col" className="num">LogForge</th>
                  <th scope="col" className="num">Kafka</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.label}>
                    <th scope="row" className="font-normal">{r.name}</th>
                    <td className="num">{r.logforge === undefined ? "n/a" : metric === "p50" || metric === "p99" ? ms(r.logforge) : metric === "mbPerSec" ? fixed(r.logforge, 1) : int(r.logforge)}</td>
                    <td className="num">{r.kafka === undefined ? "n/a" : metric === "p50" || metric === "p99" ? ms(r.kafka) : metric === "mbPerSec" ? fixed(r.kafka, 1) : int(r.kafka)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </figure>

        <div className="mt-6 grid gap-6 lg:grid-cols-2">
          <div className="panel min-w-0 p-4">
            <p className="label mb-3">End-to-end latency, acks=all, 1,000 records/s, 1 KB</p>
            <div className="overflow-x-auto">
            <table className="data">
              <thead>
                <tr>
                  <th scope="col">system</th>
                  <th scope="col" className="num">send to receive p50</th>
                  <th scope="col" className="num">p99</th>
                  <th scope="col" className="num">ack to receive p50</th>
                </tr>
              </thead>
              <tbody>
                {(["logforge", "kafka"] as const).map((s) => {
                  const r = e2e[s]?.results;
                  return (
                    <tr key={s}>
                      <th scope="row" className="font-normal">{s === "logforge" ? "LogForge" : "Kafka"}</th>
                      <td className="num">{ms(r?.sendToReceiveMs?.p50)}</td>
                      <td className="num">{ms(r?.sendToReceiveMs?.p99)}</td>
                      <td className="num">{ms(r?.ackToReceiveMs?.p50)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            </div>
          </div>
          <div className="panel min-w-0 p-4 text-sm">
            <p className="label mb-3">Measured on</p>
            {hw ? (
              <dl className="mono grid grid-cols-[7rem_minmax(0,1fr)] gap-x-3 gap-y-1 text-xs [overflow-wrap:anywhere]">
                <dt className="text-muted">host</dt>
                <dd>{hw.host}</dd>
                <dt className="text-muted">containers</dt>
                <dd>{hw.environment}</dd>
                <dt className="text-muted">cpu</dt>
                <dd>{hw.cpuModel}, {hw.cpusVisible} visible</dd>
                <dt className="text-muted">memory</dt>
                <dd>{hw.memTotal}</dd>
                <dt className="text-muted">java</dt>
                <dd>{hw.java}</dd>
                <dt className="text-muted">versions</dt>
                <dd>LogForge {baseline?.version}; Apache Kafka 3.9.0 broker, {kafkaVersion}</dd>
                <dt className="text-muted">window</dt>
                <dd>{baseline ? `${baseline.config.warmupSeconds} s warm-up, ${baseline.config.measureSeconds} s measured` : "n/a"}</dd>
              </dl>
            ) : null}
            <p className="mt-4">
              Regenerate every number: <code className="text-accent">bash bench/run.sh</code> (
              <a href={`${repoUrl}/blob/main/bench/run.sh`}>script</a>,{" "}
              <a href={`${repoUrl}/tree/main/bench/results`}>raw results</a>). Record sizes {recordSize(100)},{" "}
              {recordSize(1024)} and {recordSize(10240)}.
            </p>
          </div>
        </div>
        <Reading find={find} />
      </div>
    </section>
  );
}

const COMPARED: [string, string][] = [
  ["size-100B", "100 B records"],
  ["baseline-1KB", "1 KB, acks=1"],
  ["size-10KB", "10 KB records"],
  ["acks-0", "acks=0"],
  ["acks-all", "acks=all"],
  ["partitions-1", "1 partition"],
  ["partitions-16", "16 partitions"],
  ["fsync-records-1", "fsync every batch"],
];

/** Kafka's throughput divided by LogForge's for one configuration. */
function ratio(find: (s: string, l: string) => BenchResult | undefined, label: string): number | undefined {
  const lf = find("logforge", label)?.results.mbPerSec;
  const k = find("kafka", label)?.results.mbPerSec;
  return lf && k ? k / lf : undefined;
}

function Reading({ find }: { find: (s: string, l: string) => BenchResult | undefined }) {
  const mb = (s: string, l: string) => fixed(find(s, l)?.results.mbPerSec, 1);
  const rows = COMPARED.map(([label, name]) => ({ label, name, r: ratio(find, label) })).filter(
    (x): x is { label: string; name: string; r: number } => x.r !== undefined,
  );
  const e2eLf = find("logforge", "e2e-acks-all")?.results.ackToReceiveMs?.p50;
  const e2eK = find("kafka", "e2e-acks-all")?.results.ackToReceiveMs?.p50;
  return (
    <div className="mt-6 grid gap-6 lg:grid-cols-[1fr_20rem]">
      <div className="max-w-3xl space-y-4 text-sm leading-relaxed">
        <p className="label">Reading the comparison</p>
        <p>
          <strong>Noise first.</strong> The 1 KB baseline and the fsync never run are the same configuration
          (acks=1, 4 partitions, no forced flush) measured minutes apart. LogForge gave{" "}
          <span className="mono">{mb("logforge", "baseline-1KB")}</span> and{" "}
          <span className="mono">{mb("logforge", "fsync-never")}</span> MB/s, Kafka{" "}
          <span className="mono">{mb("kafka", "baseline-1KB")}</span> and{" "}
          <span className="mono">{mb("kafka", "fsync-never")}</span> MB/s. On a shared laptop a gap smaller than
          that spread is not evidence of anything.
        </p>
        <p>
          <strong>Where Kafka pulls ahead</strong> (replicated writes at acks=all, a single partition, large
          records, and handing acknowledged records to a consumer), the likely causes are known from the code,
          not from profiling. The LogForge producer keeps one request in flight per partition, where Kafka
          pipelines up to five per connection. Followers and consumers get bytes copied through the JVM heap,
          not sent from the page cache with sendfile. And the LogForge consumer waits for every leader&apos;s
          fetch to return before handing any records over, so one long poll delays the rest: acknowledged
          records reached the consumer after a median <span className="mono">{ms(e2eLf)}</span> against
          Kafka&apos;s <span className="mono">{ms(e2eK)}</span>.
        </p>
        <p>
          <strong>Where LogForge is level or ahead</strong>, read it as both clusters waiting on the same single
          producer and the same machine rather than as a faster broker. Kafka has had a decade of tuning; the
          point of this table is that the gap is measured and explained, not hidden.
        </p>
      </div>
      <div className="panel self-start p-4">
        <p className="label mb-3">Kafka MB/s divided by LogForge MB/s</p>
        <table className="data">
          <tbody>
            {rows.map((x) => (
              <tr key={x.label}>
                <th scope="row" className="font-normal">{x.name}</th>
                <td className="num">{fixed(x.r, 2)}&times;</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
