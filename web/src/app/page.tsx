import benchData from "@/data/bench.json";
import chaosData from "@/data/chaos.json";
import replayData from "@/data/replay.json";
import segmentSample from "@/data/segment-sample.json";
import { Architecture } from "@/components/Architecture";
import { Benchmarks } from "@/components/Benchmarks";
import { Hero } from "@/components/Hero";
import { Limitations } from "@/components/Limitations";
import { Replay } from "@/components/Replay";
import { SegmentExplorer } from "@/components/SegmentExplorer";
import { Footer, Header, REPO_URL } from "@/components/SiteChrome";
import { Topology } from "@/components/Topology";
import { fixed } from "@/lib/format";
import type { BenchResult, ChaosResult, Replay as ReplayData, SegmentSample } from "@/lib/types";

// Rendered per request so the CSP nonce applies. Nothing here waits for the broker: the live
// topology paints with the recorded snapshot and asks the hosted broker from the browser.
export const dynamic = "force-dynamic";

export default function Home() {
  const bench = (benchData as { results: BenchResult[] }).results;
  const chaos = (chaosData as { results: ChaosResult[] }).results;
  const headline = bench.find((r) => r.system === "logforge" && r.config.label === "baseline-1KB");
  return (
    <>
      <Header />
      <main id="main">
        <Hero headline={headline} />
        <Topology />
        <Replay data={replayData as unknown as ReplayData} chaos={chaos.find((c) => c.scenario === "leader-kill")} />
        <SegmentExplorer sample={segmentSample as unknown as SegmentSample} />
        <Benchmarks results={bench} repoUrl={REPO_URL} />
        <ChaosTable results={chaos} />
        <Architecture />
        <Limitations />
      </main>
      <Footer />
    </>
  );
}

function ChaosTable({ results }: { results: ChaosResult[] }) {
  return (
    <section aria-labelledby="chaos-title" className="border-b border-line py-20">
      <div className="wrap">
        <p className="label">05b / Chaos results</p>
        <h2 id="chaos-title" className="h2 mt-3 max-w-3xl">
          Five ways to break the cluster, measured
        </h2>
        <p className="mt-4 max-w-2xl text-muted">
          Each run writes 2,000 records a second with an idempotent acks=all producer (replication factor 3,
          min.insync.replicas 2) while <span className="mono">chaos/run-all.sh</span> injects one fault, then reads the
          topic back. PASS means every acknowledged record was found.
        </p>
        {results.length === 0 ? (
          <p className="panel mt-8 p-6 text-muted">No chaos runs recorded.</p>
        ) : (
          <div className="panel mt-8 overflow-x-auto p-2">
            <table className="data">
              <caption className="sr-only">Chaos scenarios and results</caption>
              <thead>
                <tr>
                  <th scope="col">scenario</th>
                  <th scope="col">fault</th>
                  <th scope="col" className="num">acked</th>
                  <th scope="col" className="num">missing</th>
                  <th scope="col" className="num">duplicates</th>
                  <th scope="col" className="num">recovery</th>
                  <th scope="col">verdict</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r) => (
                  <tr key={r.scenario}>
                    <th scope="row" className="font-normal">{r.scenario}</th>
                    <td className="whitespace-normal text-muted">{r.description}</td>
                    <td className="num">{new Intl.NumberFormat("en-US").format(r.acked)}</td>
                    <td className={`num ${r.missingAcked > 0 ? "text-fail" : ""}`}>{r.missingAcked}</td>
                    <td className="num">{r.duplicates}</td>
                    <td className="num">{fixed(r.recoveryTimeMs / 1000, 2)} s</td>
                    <td className={r.verdict === "PASS" ? "text-accent" : "text-fail"}>{r.verdict}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
}
