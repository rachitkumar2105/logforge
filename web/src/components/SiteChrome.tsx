import Link from "next/link";
import { GithubIcon } from "./Icons";

export const REPO_URL = "https://github.com/rachitkumar2105/logforge";

export function Header() {
  return (
    <header className="border-b border-line">
      <nav aria-label="Primary" className="wrap flex min-h-14 flex-wrap items-center gap-x-6 gap-y-2 py-2">
        <Link href="/" className="mono text-sm font-semibold text-text no-underline">
          logforge
        </Link>
        <ul className="ml-auto flex flex-wrap gap-x-5 gap-y-1 text-sm">
          <li><a href="/#topology">Topology</a></li>
          <li><a href="/#replay">Replay</a></li>
          <li><a href="/#explorer">Explorer</a></li>
          <li><a href="/#benchmarks">Benchmarks</a></li>
          <li><a href="/#limitations">Limitations</a></li>
        </ul>
      </nav>
    </header>
  );
}

export function Footer() {
  return (
    <footer className="py-12">
      <div className="wrap flex flex-wrap items-center gap-x-8 gap-y-4 text-sm text-muted">
        <a href={REPO_URL} className="inline-flex min-h-[44px] items-center gap-2">
          <GithubIcon /> Source on GitHub
        </a>
        <Link href="/privacy" className="inline-flex min-h-[44px] items-center">
          Privacy
        </Link>
        <Link href="/terms" className="inline-flex min-h-[44px] items-center">
          Terms
        </Link>
        <p className="mono text-xs">
          Every number here comes from bench/results and chaos/results in the repository.
        </p>
      </div>
    </footer>
  );
}
