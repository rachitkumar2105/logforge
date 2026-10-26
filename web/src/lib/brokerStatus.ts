// The live topology's cold-start indicator: schedule and wording. No imports, so the tests in
// scripts/brokerStatus.test.mjs can load this file directly with Node.
//
// The hosted broker runs on a free tier that sleeps after 15 idle minutes and takes about a
// minute to wake. The page never waits for it: everything except the live topology is static or
// client-side, and the topology shows the recorded snapshot until the broker answers.

export type Phase = "checking" | "waking" | "live" | "unavailable";

export type PingResult = { ok: true } | { ok: false; reason: "not-configured" | "unreachable" };

/** Show "waking" only if the first ping has not answered by then, so a warm broker never flashes it. */
export const WAKING_AFTER_MS = 2_000;
/** Stop retrying this long after the first ping; the asleep state then persists. */
export const GIVE_UP_AFTER_MS = 90_000;
/** How long "broker live" stays before the pill fades out. */
export const LIVE_HOLD_MS = 2_000;
/** Live cluster polling once the broker has answered. */
export const POLL_MS = 5_000;

const BACKOFF_MS = [1_000, 2_000, 4_000, 8_000];

/** Wait before retry number {@code attempt + 1}: 1 s, 2 s, 4 s, 8 s, then every 8 s. */
export function backoffMs(attempt: number): number {
  return BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)] ?? 8_000;
}

/**
 * When the next ping should start, in ms since the first one, given that ping number
 * {@code attempt} finished at {@code finishedAtMs}. Null once that would pass the 90 s budget.
 */
export function nextAttemptAt(attempt: number, finishedAtMs: number): number | null {
  const at = finishedAtMs + backoffMs(attempt);
  return at < GIVE_UP_AFTER_MS ? at : null;
}

export function phaseAfterPing(phase: Phase, result: PingResult): Phase {
  if (phase === "unavailable" || phase === "live") return phase;
  if (result.ok) return "live";
  if (result.reason === "not-configured") return "unavailable";
  return phase;
}

export function pillText(phase: Phase): string {
  switch (phase) {
    case "checking":
      return "checking broker";
    case "waking":
      return "waking broker, about 45s";
    case "live":
      return "broker live";
    case "unavailable":
      return "broker asleep, showing recorded state";
  }
}
