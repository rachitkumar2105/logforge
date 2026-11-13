// node --test: the replication lab's simulation follows the broker's rules (IsrManager, Partition,
// Controller): HW = min LEO over the ISR, acks=all completes only below the HW, the controller fences
// a broker after the session timeout and elects the first live ISR replica, a returning replica
// truncates its divergent tail, and an unclean election is the only way to lose acknowledged records.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createSim,
  run,
  kill,
  pause,
  resume,
  restart,
  setRate,
  setUnclean,
  view,
  DEFAULTS,
} from "../src/lib/replicationSim.ts";

const leaderOf = (s) => view(s).leader;
const logIds = (s, id) => s.brokers[id].log.map((r) => r.id);

test("steady state: every record is acknowledged, HW reaches the leader's end, ISR stays full", () => {
  const s = createSim({ seed: 1 });
  setRate(s, 20);
  run(s, 10_000);
  setRate(s, 0);
  run(s, 2_000);
  const v = view(s);
  assert.equal(v.isr.length, 3);
  assert.ok(v.stats.produced >= 190, `produced ${v.stats.produced}`);
  assert.equal(v.stats.acked, v.stats.produced);
  assert.equal(v.hw, s.brokers[v.leader].log.length);
  assert.equal(v.stats.lostAcked, 0);
  for (const b of v.brokers) assert.equal(b.leo, v.hw);
});

test("acks=all never completes above the HW", () => {
  const s = createSim({ seed: 2 });
  setRate(s, 40);
  for (let i = 0; i < 400; i++) {
    run(s, 25);
    for (const [, offset] of s.ackedOffsets) assert.ok(offset < s.part.hw, `acked offset ${offset} >= hw ${s.part.hw}`);
  }
});

test("leader kill: controller fences it after the session timeout and elects a live ISR replica", () => {
  const s = createSim({ seed: 3 });
  setRate(s, 20);
  run(s, 5_000);
  const old = leaderOf(s);
  const epoch = s.part.epoch;
  kill(s, old);
  run(s, DEFAULTS.sessionTimeoutMs - 500);
  assert.equal(leaderOf(s), old, "not fenced before the session timeout");
  run(s, 1_500);
  const next = leaderOf(s);
  assert.notEqual(next, null);
  assert.notEqual(next, old);
  assert.equal(s.part.epoch, epoch + 1);
  assert.ok(!s.part.isr.includes(old));
  run(s, 5_000);
  assert.equal(view(s).stats.lostAcked, 0);
  assert.ok(view(s).stats.acked > 100);
});

test("a returning old leader truncates its unacknowledged tail and rejoins the ISR", () => {
  const s = createSim({ seed: 4 });
  setRate(s, 20);
  run(s, 3_000);
  const old = leaderOf(s);
  // Stop both followers so the leader appends records nobody else has, then kill it.
  for (const b of s.brokers) if (b.id !== old) pause(s, b.id);
  run(s, 1_000);
  for (const b of s.brokers) if (b.id !== old) resume(s, b.id);
  kill(s, old);
  run(s, 5_000);
  const next = leaderOf(s);
  assert.notEqual(next, old);
  const before = s.stats.truncatedUnacked;
  restart(s, old);
  run(s, 3_000);
  assert.ok(s.stats.truncatedUnacked > before, "old leader truncated its divergent tail");
  assert.ok(s.part.isr.includes(old), "old leader rejoined as an in-sync follower");
  const leaderLog = logIds(s, next);
  const oldLog = logIds(s, old);
  assert.deepEqual(oldLog, leaderLog.slice(0, oldLog.length));
  assert.equal(view(s).stats.lostAcked, 0);
});

test("a paused follower leaves the ISR after replica.lag.time.max.ms, so it cannot stall acks", () => {
  const s = createSim({ seed: 5 });
  setRate(s, 20);
  run(s, 2_000);
  const follower = s.brokers.find((b) => b.id !== leaderOf(s)).id;
  pause(s, follower);
  // Paused processes stop heartbeating: the controller fences it after the session timeout.
  run(s, DEFAULTS.sessionTimeoutMs + 500);
  assert.ok(!s.part.isr.includes(follower));
  const acked = s.stats.acked;
  run(s, 3_000);
  assert.ok(s.stats.acked > acked + 40, "the remaining ISR keeps acknowledging");
  resume(s, follower);
  run(s, 3_000);
  assert.ok(s.part.isr.includes(follower), "rejoins once its log end reaches the HW");
});

test("a slow follower that still heartbeats is shrunk out by replica lag, not by the controller", () => {
  const s = createSim({ seed: 6 });
  setRate(s, 20);
  run(s, 2_000);
  const follower = s.brokers.find((b) => b.id !== leaderOf(s)).id;
  s.brokers[follower].slow = true; // heartbeats continue, fetches stop
  run(s, DEFAULTS.replicaLagTimeMaxMs - 1_000);
  assert.ok(s.part.isr.includes(follower), "still in sync before the lag limit");
  run(s, 2_000);
  assert.ok(!s.part.isr.includes(follower), "shrunk after the lag limit");
  assert.ok(s.controller.alive[follower], "the controller still sees it alive");
});

test("min.insync.replicas: with only the leader in sync, acks=all produces are refused", () => {
  const s = createSim({ seed: 7 });
  setRate(s, 20);
  run(s, 2_000);
  for (const b of s.brokers) if (b.id !== leaderOf(s)) kill(s, b.id);
  run(s, DEFAULTS.sessionTimeoutMs + 1_000);
  assert.deepEqual(s.part.isr, [leaderOf(s)]);
  const acked = s.stats.acked;
  const refused = s.stats.refused;
  run(s, 2_000);
  assert.equal(s.stats.acked, acked);
  assert.ok(s.stats.refused > refused + 20);
});

function uncleanScenario(unclean) {
  const s = createSim({ seed: 8 });
  setUnclean(s, unclean);
  setRate(s, 20);
  run(s, 2_000);
  const leader = leaderOf(s);
  const [a, b] = s.brokers.filter((x) => x.id !== leader).map((x) => x.id);
  kill(s, a); // a falls behind and leaves the ISR
  run(s, DEFAULTS.sessionTimeoutMs + 3_000);
  kill(s, b); // ISR shrinks to the leader alone
  run(s, DEFAULTS.sessionTimeoutMs + 500);
  restart(s, a); // a comes back far behind...
  kill(s, leader); // ...and the leader dies before a can fetch anything from it
  run(s, DEFAULTS.sessionTimeoutMs + 2_000);
  return { s, leader, a };
}

test("clean election: a dead last ISR member leaves the partition offline, nothing acknowledged is lost", () => {
  const { s } = uncleanScenario(false);
  assert.equal(leaderOf(s), null);
  assert.equal(view(s).offline, true);
  assert.equal(view(s).stats.lostAcked, 0);
});

test("unclean election elects the lagging replica and loses acknowledged records", () => {
  const { s, a } = uncleanScenario(true);
  assert.equal(leaderOf(s), a);
  assert.ok(view(s).stats.lostAcked > 0, "acknowledged records are gone");
});

test("the same seed and actions give the same history", () => {
  const go = () => {
    const s = createSim({ seed: 9 });
    setRate(s, 25);
    run(s, 3_000);
    kill(s, leaderOf(s));
    run(s, 6_000);
    return JSON.stringify(view(s));
  };
  assert.equal(go(), go());
});
