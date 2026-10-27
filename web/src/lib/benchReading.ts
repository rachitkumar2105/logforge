// How the site reads the benchmark. One run per configuration on a shared laptop is noisy, so a gap
// between the two systems counts only when it is larger than the spread between two runs of the
// same configuration. No imports, so scripts/benchReading.test.mjs can load this file directly.

export type Verdict = "kafka" | "logforge" | "within";

/** Largest max/min spread over pairs of throughputs measured for the same configuration. */
export function noiseFactor(pairs: [number, number][]): number {
  let worst = 1;
  for (const [a, b] of pairs) {
    if (a > 0 && b > 0) worst = Math.max(worst, Math.max(a, b) / Math.min(a, b));
  }
  return worst;
}

/** Kafka MB/s divided by LogForge MB/s, judged against the noise band. */
export function classify(ratio: number, noise: number): Verdict {
  if (ratio > noise) return "kafka";
  if (ratio < 1 / noise) return "logforge";
  return "within";
}
