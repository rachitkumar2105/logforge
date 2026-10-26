import type { Metadata } from "next";
import { Footer, Header } from "@/components/SiteChrome";

export const metadata: Metadata = { title: "Terms (draft) | LogForge" };

export default function Terms() {
  return (
    <>
      <Header />
      <main id="main" className="wrap max-w-3xl py-16">
        <p className="label">Draft, not legal advice</p>
        <h1 className="h2 mt-3">Terms</h1>
        <p className="mt-6 text-muted">
          This is a draft written for a personal portfolio project. It has not been reviewed by a lawyer and is not
          legal advice.
        </p>
        <h2 className="h3 mt-10">What this is</h2>
        <p className="mt-3 text-muted">
          LogForge is an educational project that demonstrates how a distributed commit log works. The site and the
          demonstration cluster are provided as they are, without any warranty, and may be offline or reset at any
          time.
        </p>
        <h2 className="h3 mt-10">Not for production data</h2>
        <p className="mt-3 text-muted">
          LogForge is not intended for storing data you care about. Its limitations are listed on the main page.
        </p>
        <h2 className="h3 mt-10">Benchmarks</h2>
        <p className="mt-3 text-muted">
          Benchmark figures describe one run on one machine with the configuration shown next to them. They are not a
          claim about any other environment.
        </p>
        <h2 className="h3 mt-10">Code</h2>
        <p className="mt-3 text-muted">The source code is published in the GitHub repository linked in the footer.</p>
      </main>
      <Footer />
    </>
  );
}
