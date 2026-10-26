"use client";

import { pillText, type Phase } from "@/lib/brokerStatus";

/**
 * Small status pill in the live topology's header. Its box keeps a fixed width and height at
 * every breakpoint, so changing text or fading out never moves anything. The text always names
 * the state; the dot only repeats it.
 */
export function BrokerPill({ phase, elapsedS, shown }: { phase: Phase; elapsedS: number; shown: boolean }) {
  return (
    <div className="flex h-8 w-[19.5rem] max-w-full shrink-0 items-center sm:justify-end">
      <div
        className="broker-pill panel inline-flex h-8 items-center gap-2 rounded-full px-3 text-xs"
        data-hidden={!shown}
        aria-hidden={!shown}
        data-testid="broker-pill"
        data-phase={phase}
      >
        <span aria-hidden className="broker-dot" data-phase={phase} />
        <span aria-live="polite" className={phase === "live" ? "text-accent" : "text-muted"}>
          {pillText(phase)}
        </span>
        {phase === "waking" ? (
          <span className="mono tabular-nums text-muted" data-testid="broker-elapsed">
            {elapsedS}s
          </span>
        ) : null}
      </div>
    </div>
  );
}
