// The replication lab: one partition, replication factor 3, run in the browser by the same rules
// the broker follows. It is a model, not the broker: no bytes, no sockets, no disk. What it keeps
// faithful is the decision logic, with the broker's default timings:
//
//   - acks=all completes only when the high watermark passes the record     (Partition)
//   - HW = smallest log end offset (LEO) across the in-sync set (ISR)        (IsrManager)
//   - a follower that has not caught up for replica.lag.time.max.ms leaves   (IsrManager shrink)
//   - a follower outside the ISR rejoins once its LEO reaches the HW        (IsrManager expand)
//   - produce is refused when the ISR is smaller than min.insync.replicas   (Partition)
//   - the controller fences a broker after broker.session.timeout.ms        (Controller)
//   - the next leader is the first live ISR replica, epoch + 1; with no live ISR replica the
//     partition goes offline unless unclean election is enabled             (Controller)
//   - a returning replica truncates to the leader's end offset for its last epoch, then fetches
//                                                                           (OffsetForLeaderEpoch)
//
// Everything is deterministic for a given seed and sequence of actions, so it is unit tested
// (scripts/replicationSim.test.mjs). Numbers it shows are the model's, and the page says so.

export const DEFAULTS = {
  heartbeatIntervalMs: 250, // NodeConfig.heartbeatIntervalMs
  sessionTimeoutMs: 3_000, // NodeConfig.brokerSessionTimeoutMs
  replicaLagTimeMaxMs: 10_000, // NodeConfig.replicaLagTimeMaxMs
  replicaFetchWaitMs: 200, // NodeConfig.replicaFetchWaitMs (follower long-poll)
  minInsyncReplicas: 2, // the chaos runs' topic config
  // Model-only parameters (not broker settings): one network round trip, a produce timeout.
  rttMs: 30,
  produceTimeoutMs: 5_000,
  maxFetchRecords: 200,
} as const;

const STEP_MS = 10;
const MAX_EVENTS = 60;
const MAX_LATENCIES = 400;

export type BrokerStatus = "up" | "paused" | "down";
export type Rec = { offset: number; epoch: number; id: number };
export type SimEvent = { t: number; level: "info" | "warn" | "fail" | "ok"; text: string };

export type Broker = {
  id: number;
  status: BrokerStatus;
  /** Heartbeats continue but fetching stops: a slow disk or a GC pause on the fetcher thread. */
  slow: boolean;
  log: Rec[];
  lastHeartbeatAt: number;
  /** The leader epoch this broker last learned from the controller. */
  knownEpoch: number;
  needsTruncation: boolean;
  /** Fetch in flight completes at this time; null when idle. */
  fetchDueAt: number | null;
  /** Long-poll started at this time; null when not waiting. */
  waitingSince: number | null;
};

type Pending = { id: number; offset: number; sentAt: number; epoch: number; leader: number };

export type Sim = {
  now: number;
  seed: number;
  brokers: Broker[];
  controller: { alive: boolean[]; lastHeartbeat: number[] };
  part: {
    leader: number | null;
    epoch: number;
    isr: number[];
    hw: number;
    followerLeo: number[];
    lastCaughtUp: number[];
  };
  config: { unclean: boolean; ratePerSec: number };
  produceCarry: number;
  nextId: number;
  pending: Pending[];
  ackedOffsets: Map<number, number>;
  latencies: number[];
  stats: {
    produced: number;
    acked: number;
    refused: number;
    failed: number;
    lostAcked: number;
    truncatedUnacked: number;
    elections: number;
  };
  events: SimEvent[];
  lossDirty: boolean;
  rng: () => number;
};

function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function createSim({ seed = 7 }: { seed?: number } = {}): Sim {
  const brokers: Broker[] = [0, 1, 2].map((id) => ({
    id,
    status: "up",
    slow: false,
    log: [],
    lastHeartbeatAt: -id * 80, // staggered, as real brokers are
    knownEpoch: 0,
    needsTruncation: false,
    fetchDueAt: null,
    waitingSince: null,
  }));
  return {
    now: 0,
    seed,
    brokers,
    controller: { alive: [true, true, true], lastHeartbeat: [0, 0, 0] },
    part: { leader: 0, epoch: 0, isr: [0, 1, 2], hw: 0, followerLeo: [0, 0, 0], lastCaughtUp: [0, 0, 0] },
    config: { unclean: false, ratePerSec: 0 },
    produceCarry: 0,
    nextId: 0,
    pending: [],
    ackedOffsets: new Map(),
    latencies: [],
    stats: { produced: 0, acked: 0, refused: 0, failed: 0, lostAcked: 0, truncatedUnacked: 0, elections: 0 },
    events: [],
    lossDirty: false,
    rng: mulberry32(seed),
  };
}

const name = (id: number) => `broker ${id + 1}`;

function emit(s: Sim, level: SimEvent["level"], text: string) {
  s.events.push({ t: s.now, level, text });
  if (s.events.length > MAX_EVENTS) s.events.splice(0, s.events.length - MAX_EVENTS);
}

const leo = (b: Broker) => b.log.length;

// ---------------------------------------------------------------- actions (from the UI)

export function setRate(s: Sim, perSec: number) {
  const next = Math.max(0, Math.min(200, perSec));
  if (next === s.config.ratePerSec) return;
  s.config.ratePerSec = next;
  emit(s, "info", next === 0 ? "producer stopped" : `producer: ${next} records/s, acks=all`);
}

export function setUnclean(s: Sim, on: boolean) {
  s.config.unclean = on;
  emit(s, on ? "warn" : "info", `unclean.leader.election.enable = ${on}`);
}

/** SIGKILL: the process is gone. Its log stays on disk; the page cache survives a process kill. */
export function kill(s: Sim, id: number) {
  const b = s.brokers[id]!;
  if (b.status === "down") return;
  b.status = "down";
  b.fetchDueAt = b.waitingSince = null;
  emit(s, "fail", `${name(id)} killed (SIGKILL)`);
}

/** SIGSTOP: the process is frozen. No heartbeats, no fetches, no appends. */
export function pause(s: Sim, id: number) {
  const b = s.brokers[id]!;
  if (b.status !== "up") return;
  b.status = "paused";
  b.fetchDueAt = b.waitingSince = null;
  emit(s, "warn", `${name(id)} paused (SIGSTOP)`);
}

/** SIGCONT. */
export function resume(s: Sim, id: number) {
  const b = s.brokers[id]!;
  if (b.status !== "paused") return;
  b.status = "up";
  b.lastHeartbeatAt = s.now - DEFAULTS.heartbeatIntervalMs; // heartbeats on the next step
  emit(s, "info", `${name(id)} resumed (SIGCONT)`);
}

/** Process restart after a kill: recovers its log from disk, then must check it against the leader. */
export function restart(s: Sim, id: number) {
  const b = s.brokers[id]!;
  if (b.status !== "down") return;
  b.status = "up";
  b.slow = false;
  b.needsTruncation = true;
  b.lastHeartbeatAt = s.now - DEFAULTS.heartbeatIntervalMs;
  emit(s, "info", `${name(id)} restarted, recovered ${leo(b)} records from its log`);
}

export function setSlow(s: Sim, id: number, slow: boolean) {
  const b = s.brokers[id]!;
  if (b.slow === slow) return;
  b.slow = slow;
  emit(s, slow ? "warn" : "info", slow ? `${name(id)} fetcher stalled (heartbeats continue)` : `${name(id)} fetcher recovered`);
}

/** One acks=all produce of `n` records, as a client would send it to the current leader. */
export function produce(s: Sim, n = 1) {
  for (let i = 0; i < n; i++) produceOne(s);
}

function produceOne(s: Sim) {
  const leaderId = s.part.leader;
  if (leaderId === null) {
    s.stats.refused++;
    return;
  }
  const leader = s.brokers[leaderId]!;
  if (leader.status === "up" && s.part.isr.length < DEFAULTS.minInsyncReplicas) {
    s.stats.refused++; // NOT_ENOUGH_REPLICAS: rejected before append, nothing written
    return;
  }
  const id = s.nextId++;
  s.stats.produced++;
  if (leader.status !== "up") {
    // The request reaches a frozen or dead process: it will time out on the client.
    s.pending.push({ id, offset: -1, sentAt: s.now, epoch: s.part.epoch, leader: leaderId });
    return;
  }
  const offset = leo(leader);
  leader.log.push({ offset, epoch: s.part.epoch, id });
  s.pending.push({ id, offset, sentAt: s.now, epoch: s.part.epoch, leader: leaderId });
}

// ---------------------------------------------------------------- time

/** Advances the model by `ms`, in fixed steps so results do not depend on frame rate. */
export function run(s: Sim, ms: number) {
  const end = s.now + ms;
  while (s.now + STEP_MS <= end + 1e-9) step(s, STEP_MS);
}

function step(s: Sim, dt: number) {
  s.now += dt;
  producerStep(s, dt);
  heartbeats(s);
  controllerStep(s);
  fetchers(s);
  leaderStep(s);
  completeRequests(s);
  if (s.lossDirty) recountLoss(s);
}

function producerStep(s: Sim, dt: number) {
  if (s.config.ratePerSec <= 0) {
    s.produceCarry = 0;
    return;
  }
  s.produceCarry += (s.config.ratePerSec * dt) / 1000;
  const n = Math.floor(s.produceCarry);
  s.produceCarry -= n;
  if (n > 0) produce(s, n);
}

function heartbeats(s: Sim) {
  for (const b of s.brokers) {
    if (b.status !== "up") continue;
    if (s.now - b.lastHeartbeatAt < DEFAULTS.heartbeatIntervalMs) continue;
    b.lastHeartbeatAt = s.now;
    s.controller.lastHeartbeat[b.id] = s.now;
    if (!s.controller.alive[b.id]) {
      s.controller.alive[b.id] = true;
      emit(s, "info", `controller: ${name(b.id)} is back (heartbeat received)`);
    }
    // The heartbeat response carries metadata. A broker that missed a leader change learns it
    // here, and must reconcile its log with the new leader before fetching (fencing).
    if (b.knownEpoch !== s.part.epoch) {
      if (s.part.leader !== b.id) b.needsTruncation = true;
      b.knownEpoch = s.part.epoch;
    }
  }
}

function controllerStep(s: Sim) {
  const p = s.part;
  for (const b of s.brokers) {
    if (!s.controller.alive[b.id]) continue;
    if (s.now - s.controller.lastHeartbeat[b.id]! <= DEFAULTS.sessionTimeoutMs) continue;
    s.controller.alive[b.id] = false;
    emit(s, "fail", `controller: no heartbeat from ${name(b.id)} for ${DEFAULTS.sessionTimeoutMs / 1000} s, fenced`);
    if (p.leader === b.id) {
      elect(s, b.id);
    } else if (p.isr.includes(b.id)) {
      p.isr = p.isr.filter((x) => x !== b.id);
      emit(s, "warn", `ISR shrinks to {${p.isr.map((x) => x + 1).join(", ")}}`);
    }
  }
  // An offline partition comes back when a member of its last ISR returns.
  if (p.leader === null) {
    const back = p.isr.find((x) => s.controller.alive[x]);
    if (back !== undefined) becomeLeader(s, back, "clean");
  }
}

function elect(s: Sim, dead: number) {
  const p = s.part;
  const candidates = p.isr.filter((x) => x !== dead && s.controller.alive[x]);
  if (candidates.length > 0) {
    p.isr = p.isr.filter((x) => x !== dead);
    becomeLeader(s, candidates[0]!, "clean");
    return;
  }
  const anyLive = s.brokers.find((b) => b.id !== dead && s.controller.alive[b.id]);
  if (s.config.unclean && anyLive) {
    p.isr = [anyLive.id];
    becomeLeader(s, anyLive.id, "unclean");
    return;
  }
  // The ISR keeps the dead leader: it is the only replica known to hold every committed record.
  p.leader = null;
  emit(s, "fail", `no live in-sync replica: partition offline until ${name(dead)} returns`);
}

function becomeLeader(s: Sim, id: number, kind: "clean" | "unclean") {
  const p = s.part;
  const b = s.brokers[id]!;
  p.leader = id;
  p.epoch += 1;
  b.knownEpoch = p.epoch;
  b.needsTruncation = false;
  b.fetchDueAt = b.waitingSince = null;
  if (!p.isr.includes(id)) p.isr = [id, ...p.isr];
  // The new leader never advertises an HW beyond its own log.
  p.hw = Math.min(p.hw, leo(b));
  for (const other of s.brokers) {
    p.followerLeo[other.id] = other.id === id ? leo(b) : 0;
    p.lastCaughtUp[other.id] = s.now;
  }
  s.stats.elections++;
  s.lossDirty = true;
  emit(
    s,
    kind === "clean" ? "ok" : "fail",
    kind === "clean"
      ? `${name(id)} elected leader from the ISR, epoch ${p.epoch}`
      : `UNCLEAN: ${name(id)} elected outside the ISR, epoch ${p.epoch}`,
  );
}

function fetchers(s: Sim) {
  const p = s.part;
  if (p.leader === null) return;
  const leader = s.brokers[p.leader]!;
  for (const f of s.brokers) {
    if (f.id === p.leader || f.status !== "up" || f.slow) continue;
    if (!s.controller.alive[f.id] || f.knownEpoch !== p.epoch) continue; // waits for metadata
    if (leader.status !== "up") {
      f.fetchDueAt = f.waitingSince = null; // connection refused or hanging: nothing arrives
      continue;
    }
    if (f.needsTruncation) truncate(s, f, leader);
    if (f.fetchDueAt === null && f.waitingSince === null) {
      // Issue a fetch; it answers after a round trip if there is data, else long-polls.
      if (leo(leader) > leo(f)) f.fetchDueAt = s.now + DEFAULTS.rttMs * (0.75 + s.rng() * 0.5);
      else f.waitingSince = s.now;
    }
    if (f.waitingSince !== null) {
      const timedOut = s.now - f.waitingSince >= DEFAULTS.replicaFetchWaitMs;
      if (leo(leader) > leo(f) || timedOut) {
        f.waitingSince = null;
        f.fetchDueAt = s.now + DEFAULTS.rttMs / 2;
      }
    }
    if (f.fetchDueAt !== null && s.now >= f.fetchDueAt) {
      f.fetchDueAt = null;
      const from = leo(f);
      const to = Math.min(leo(leader), from + DEFAULTS.maxFetchRecords);
      for (let o = from; o < to; o++) f.log.push({ ...leader.log[o]! });
      p.followerLeo[f.id] = leo(f);
      if (leo(f) >= leo(leader)) p.lastCaughtUp[f.id] = s.now;
      if (!p.isr.includes(f.id) && leo(f) >= p.hw) {
        p.isr = [...p.isr, f.id].sort((a, b) => a - b);
        emit(s, "ok", `${name(f.id)} caught up to the HW and rejoins the ISR`);
      }
    }
  }
}

/** OffsetForLeaderEpoch: cut the follower back to where its last epoch ends on the leader. */
function truncate(s: Sim, f: Broker, leader: Broker) {
  f.needsTruncation = false;
  const last = f.log[f.log.length - 1];
  if (!last) return;
  const firstLater = leader.log.find((r) => r.epoch > last.epoch);
  let end = Math.min(leo(f), firstLater ? firstLater.offset : leo(leader));
  // Safety net for multi-epoch divergence: keep cutting while the tail disagrees with the leader.
  while (end > 0 && f.log[end - 1]!.id !== leader.log[end - 1]?.id) end--;
  const cut = f.log.length - end;
  if (cut <= 0) return;
  const removed = f.log.splice(end);
  s.stats.truncatedUnacked += removed.filter((r) => !s.ackedOffsets.has(r.id)).length;
  s.lossDirty = true;
  emit(
    s,
    "warn",
    `${name(f.id)} truncates ${cut} record${cut === 1 ? "" : "s"} (offsets ${end} to ${end + cut - 1}) that the new leader never had`,
  );
}

function leaderStep(s: Sim) {
  const p = s.part;
  if (p.leader === null) return;
  const leader = s.brokers[p.leader]!;
  if (leader.status !== "up") return;
  p.followerLeo[leader.id] = leo(leader);
  // Shrink: a follower not caught up for replica.lag.time.max.ms leaves the ISR.
  for (const id of p.isr) {
    if (id === leader.id) continue;
    if (s.now - p.lastCaughtUp[id]! > DEFAULTS.replicaLagTimeMaxMs) {
      p.isr = p.isr.filter((x) => x !== id);
      emit(s, "warn", `${name(id)} not caught up for ${DEFAULTS.replicaLagTimeMaxMs / 1000} s, leaves the ISR`);
    }
  }
  const hw = Math.min(...p.isr.map((id) => p.followerLeo[id]!));
  if (hw > p.hw) p.hw = hw;
}

function completeRequests(s: Sim) {
  const p = s.part;
  const leader = p.leader === null ? null : s.brokers[p.leader]!;
  const keep: Pending[] = [];
  for (const r of s.pending) {
    if (r.offset >= 0 && r.epoch === p.epoch && leader && leader.status === "up" && r.offset < p.hw) {
      s.stats.acked++;
      s.ackedOffsets.set(r.id, r.offset);
      s.latencies.push(s.now - r.sentAt);
      if (s.latencies.length > MAX_LATENCIES) s.latencies.splice(0, s.latencies.length - MAX_LATENCIES);
      continue;
    }
    const leaderMoved = r.epoch !== p.epoch;
    const expired = s.now - r.sentAt >= DEFAULTS.produceTimeoutMs;
    if (leaderMoved || expired) {
      s.stats.failed++; // NOT_LEADER or timeout: the client retries; nothing was promised
      continue;
    }
    keep.push(r);
  }
  s.pending = keep;
}

function recountLoss(s: Sim) {
  s.lossDirty = false;
  const p = s.part;
  if (p.leader === null) return; // offline: nothing is served, nothing is known lost yet
  const log = s.brokers[p.leader]!.log;
  let lost = 0;
  for (const [id, offset] of s.ackedOffsets) if (log[offset]?.id !== id) lost++;
  if (lost > s.stats.lostAcked) emit(s, "fail", `${lost - s.stats.lostAcked} acknowledged records are gone`);
  s.stats.lostAcked = lost;
}

// ---------------------------------------------------------------- read model for the UI

export type BrokerView = {
  id: number;
  status: BrokerStatus;
  slow: boolean;
  role: "leader" | "follower";
  alive: boolean;
  inIsr: boolean;
  leo: number;
  heartbeatAgoMs: number;
  caughtUpAgoMs: number | null;
  truncating: boolean;
};

function percentile(sorted: number[], q: number) {
  if (sorted.length === 0) return null;
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
}

export function view(s: Sim) {
  const p = s.part;
  const sorted = [...s.latencies].sort((a, b) => a - b);
  return {
    now: s.now,
    leader: p.leader,
    epoch: p.epoch,
    isr: [...p.isr],
    hw: p.hw,
    offline: p.leader === null,
    unclean: s.config.unclean,
    ratePerSec: s.config.ratePerSec,
    inFlight: s.pending.length,
    p50: percentile(sorted, 0.5),
    p99: percentile(sorted, 0.99),
    stats: { ...s.stats },
    brokers: s.brokers.map<BrokerView>((b) => ({
      id: b.id,
      status: b.status,
      slow: b.slow,
      role: p.leader === b.id ? "leader" : "follower",
      alive: s.controller.alive[b.id]!,
      inIsr: p.isr.includes(b.id),
      leo: leo(b),
      heartbeatAgoMs: s.now - s.controller.lastHeartbeat[b.id]!,
      caughtUpAgoMs: p.leader === b.id ? null : s.now - p.lastCaughtUp[b.id]!,
      truncating: b.needsTruncation,
    })),
  };
}

export type SimView = ReturnType<typeof view>;

/** The last `n` records of a broker's log, oldest first, for drawing the tape. */
export function tail(s: Sim, id: number, n: number): Rec[] {
  const log = s.brokers[id]!.log;
  return log.slice(Math.max(0, log.length - n));
}
