// The cold-start indicator's schedule and wording, without a browser. Run: pnpm test
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  GIVE_UP_AFTER_MS,
  LIVE_HOLD_MS,
  WAKING_AFTER_MS,
  backoffMs,
  nextAttemptAt,
  pillText,
  phaseAfterPing,
} from "../src/lib/brokerStatus.ts";

test("backoff is 1 s, 2 s, 4 s, 8 s, then every 8 s", () => {
  assert.deepEqual(
    [0, 1, 2, 3, 4, 5, 9].map(backoffMs),
    [1000, 2000, 4000, 8000, 8000, 8000, 8000],
  );
});

test("pings that fail at once are retried until 90 s have passed, then the indicator gives up", () => {
  const starts = [];
  let at = 0;
  for (let attempt = 0; at !== null; attempt++) {
    starts.push(at);
    at = nextAttemptAt(attempt, at); // each ping fails instantly
  }
  assert.deepEqual(starts, [0, 1000, 3000, 7000, 15000, 23000, 31000, 39000, 47000, 55000, 63000, 71000, 79000, 87000]);
  assert.equal(GIVE_UP_AFTER_MS, 90_000);
});

test("a slow ping counts against the same 90 s budget", () => {
  // A ping that hangs for 85 s (a platform proxy holding the request) leaves no time to retry.
  assert.equal(nextAttemptAt(0, 85_000), 86_000);
  assert.equal(nextAttemptAt(1, 89_500), null);
});

test("the waking state only appears after 2 s, and live holds for 2 s", () => {
  assert.equal(WAKING_AFTER_MS, 2000);
  assert.equal(LIVE_HOLD_MS, 2000);
});

test("ping results move the phase", () => {
  assert.equal(phaseAfterPing("checking", { ok: true }), "live");
  assert.equal(phaseAfterPing("waking", { ok: true }), "live");
  assert.equal(phaseAfterPing("checking", { ok: false, reason: "not-configured" }), "unavailable");
  assert.equal(phaseAfterPing("checking", { ok: false, reason: "unreachable" }), "checking");
  assert.equal(phaseAfterPing("waking", { ok: false, reason: "unreachable" }), "waking");
  assert.equal(phaseAfterPing("unavailable", { ok: true }), "unavailable");
});

test("every state is stated in words, never by the dot alone", () => {
  assert.equal(pillText("checking"), "checking broker");
  assert.equal(pillText("waking"), "waking broker, about 45s");
  assert.equal(pillText("live"), "broker live");
  assert.equal(pillText("unavailable"), "broker asleep, showing recorded state");
});
