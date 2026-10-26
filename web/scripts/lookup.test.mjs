// node --test: the segment explorer's lookup against a brute-force scan, for every offset.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { lookup } from "../src/lib/lookup.ts";

const sample = JSON.parse(readFileSync(new URL("../src/data/segment-sample.json", import.meta.url), "utf8"));

test("every written offset resolves to the batch that holds it", () => {
  const segs = sample.segments;
  const end = segs[segs.length - 1].nextOffset;
  for (let target = segs[0].baseOffset; target < end; target++) {
    const r = lookup(sample, target);
    assert.equal(r.ok, true, `offset ${target}`);
    const seg = segs[r.segment];
    const batch = seg.batches[r.batch];
    assert.ok(batch.baseOffset <= target && target <= batch.lastOffset, `offset ${target} in batch`);
    // Brute force agrees on the segment and the byte position.
    const bfSeg = segs.findLastIndex((s) => s.baseOffset <= target);
    assert.equal(r.segment, bfSeg);
    assert.equal(r.position, batch.position);
    // The index step never lands after the target.
    const idx = r.steps[1];
    assert.ok(idx.position <= batch.position);
  }
});

test("out-of-range offsets are explained, not resolved", () => {
  assert.equal(lookup(sample, -1).ok, false);
  const end = sample.segments[sample.segments.length - 1].nextOffset;
  assert.equal(lookup(sample, end).ok, false);
  assert.equal(lookup(sample, 1.5).ok, false);
});
