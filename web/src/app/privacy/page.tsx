import type { Metadata } from "next";
import { Footer, Header } from "@/components/SiteChrome";

export const metadata: Metadata = { title: "Privacy (draft) | LogForge" };

export default function Privacy() {
  return (
    <>
      <Header />
      <main id="main" className="wrap max-w-3xl py-16">
        <p className="label">Draft, not legal advice</p>
        <h1 className="h2 mt-3">Privacy</h1>
        <p className="mt-6 text-muted">
          This is a draft written for a personal portfolio project. It has not been reviewed by a lawyer and is not
          legal advice.
        </p>
        <h2 className="h3 mt-10">What this site collects</h2>
        <p className="mt-3 text-muted">
          Nothing about you. There are no accounts, no forms, no cookies set by the site, no analytics and no
          third-party scripts. The page loads its fonts from this site, not from a font service.
        </p>
        <h2 className="h3 mt-10">What the hosting provider sees</h2>
        <p className="mt-3 text-muted">
          Like any website, the hosting platform receives standard request information (IP address, user agent,
          requested path) in its own access logs, under its own privacy policy. This project does not read or keep
          those logs.
        </p>
        <h2 className="h3 mt-10">Live cluster data</h2>
        <p className="mt-3 text-muted">
          The topology section shows the state of a demonstration cluster: topic names, offsets and broker health. It
          contains no personal data.
        </p>
        <h2 className="h3 mt-10">Contact</h2>
        <p className="mt-3 text-muted">
          Questions go to the author through the GitHub repository linked in the footer.
        </p>
      </main>
      <Footer />
    </>
  );
}
