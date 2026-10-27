// How the site reads the benchmark: a gap only counts if it is larger than the run-to-run noise
// measured between two identical configurations.
import assert from "node:assert/strict";
import { test } from "node:test";
import { classify, noiseFactor } from "../src/lib/benchReading.ts";

test("noise is the largest spread between two runs of the same configuration", () => {
  assert.equal(noiseFactor([[56.6, 87.3], [81.0, 54.5]]).toFixed(3), (87.3 / 56.6).toFixed(3));
  assert.equal(noiseFactor([[50, 50]]), 1);
  assert.equal(noiseFactor([]), 1);
});

test("a ratio counts only beyond the noise band, in either direction", () => {
  const noise = 1.5;
  assert.equal(classify(1.82, noise), "kafka");
  assert.equal(classify(1.43, noise), "within");
  assert.equal(classify(1.5, noise), "within");
  assert.equal(classify(0.52, noise), "logforge");
  assert.equal(classify(0.7, noise), "within");
});
