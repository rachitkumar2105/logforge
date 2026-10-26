import type { SegmentSample } from "./types";

/**
 * The broker's offset lookup, step by step, for the segment explorer:
 * 1. segment: the greatest base offset <= target (a binary search over segment files);
 * 2. index: the greatest index entry <= target inside that segment (binary search);
 * 3. scan: walk batch headers forward from that byte position to the batch holding the target.
 * Mirrors Log.read, OffsetIndex.lookup and LogSegment.translateOffset in the broker.
 */
export type LookupStep =
  | { kind: "segment"; probes: number[]; segment: number }
  | { kind: "index"; probes: number[]; entry: number; position: number }
  | { kind: "scan"; visited: number[]; batch: number; position: number };

export type LookupResult =
  | { ok: true; steps: LookupStep[]; segment: number; batch: number; position: number }
  | { ok: false; reason: string };

export function lookup(sample: SegmentSample, target: number): LookupResult {
  const segs = sample.segments;
  const first = segs[0];
  const last = segs[segs.length - 1];
  if (!first || !last) return { ok: false, reason: "no segments" };
  if (!Number.isInteger(target) || target < first.baseOffset) {
    return { ok: false, reason: `Offsets start at ${first.baseOffset}.` };
  }
  if (target >= last.nextOffset) {
    return { ok: false, reason: `The log ends at ${last.nextOffset - 1}; ${target} has not been written yet.` };
  }

  // 1. segment: binary search for the floor base offset.
  const segProbes: number[] = [];
  let lo = 0;
  let hi = segs.length - 1;
  let seg = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    segProbes.push(mid);
    if (segs[mid]!.baseOffset <= target) {
      seg = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  const segment = segs[seg]!;

  // 2. index: binary search for the floor entry; none means "start of the file".
  const idxProbes: number[] = [];
  lo = 0;
  hi = segment.index.length - 1;
  let entry = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    idxProbes.push(mid);
    if (segment.index[mid]!.offset <= target) {
      entry = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  const startPosition = entry >= 0 ? segment.index[entry]!.position : 0;

  // 3. scan: forward over batch headers until one reaches the target.
  const visited: number[] = [];
  let batch = segment.batches.findIndex((b) => b.position === startPosition);
  if (batch < 0) batch = 0;
  for (; batch < segment.batches.length; batch++) {
    visited.push(batch);
    if (segment.batches[batch]!.lastOffset >= target) break;
  }
  const found = segment.batches[batch];
  if (!found) return { ok: false, reason: "offset not found in segment" };
  return {
    ok: true,
    segment: seg,
    batch,
    position: found.position,
    steps: [
      { kind: "segment", probes: segProbes, segment: seg },
      { kind: "index", probes: idxProbes, entry, position: startPosition },
      { kind: "scan", visited, batch, position: found.position },
    ],
  };
}
