import Link from "next/link";
import { Footer, Header } from "@/components/SiteChrome";

// Replaces Next.js's default 404, whose inline <style> carries no CSP nonce.
export default function NotFound() {
  return (
    <>
      <Header />
      <main id="main" className="wrap max-w-3xl py-16">
        <p className="label">404</p>
        <h1 className="h2 mt-3">No such offset</h1>
        <p className="mt-6 text-muted">
          This page is past the end of the log. <Link href="/">Back to the cluster</Link>.
        </p>
      </main>
      <Footer />
    </>
  );
}
