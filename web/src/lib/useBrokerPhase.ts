"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  GIVE_UP_AFTER_MS,
  LIVE_HOLD_MS,
  WAKING_AFTER_MS,
  nextAttemptAt,
  phaseAfterPing,
  type Phase,
  type PingResult,
} from "./brokerStatus";
import { usePageVisible } from "./useReducedMotion";

/** Client-side cap on one ping; the route handler gives up at 8 s. */
const PING_TIMEOUT_MS = 9_500;

async function ping(signal: AbortSignal): Promise<PingResult> {
  try {
    const res = await fetch("/api/broker/health", { cache: "no-store", signal });
    if (!res.ok) return { ok: false, reason: "unreachable" };
    const body = (await res.json()) as PingResult;
    return body.ok ? { ok: true } : { ok: false, reason: body.reason === "not-configured" ? "not-configured" : "unreachable" };
  } catch {
    return { ok: false, reason: "unreachable" };
  }
}

export type BrokerPhase = {
  phase: Phase;
  /** Seconds since the first ping, while waking. */
  elapsedS: number;
  /** False once "broker live" has been shown for its hold time. */
  pillShown: boolean;
  /** Start over from "checking", e.g. when live polling keeps failing. */
  restart: () => void;
};

/**
 * Pings the hosted broker through the site's own route handler: retries with backoff for up to
 * 90 s, pauses while the tab is hidden and starts over when it is shown again. Never blocks
 * rendering; callers show the recorded snapshot until the phase is "live".
 */
export function useBrokerPhase(): BrokerPhase {
  const visible = usePageVisible();
  const [phase, setPhase] = useState<Phase>("checking");
  const [elapsedS, setElapsedS] = useState(0);
  const [pillShown, setPillShown] = useState(true);
  const [run, setRun] = useState(0);
  const phaseRef = useRef<Phase>("checking");

  const set = useCallback((p: Phase) => {
    phaseRef.current = p;
    setPhase(p);
  }, []);

  const restart = useCallback(() => {
    set("checking");
    setPillShown(true);
    setElapsedS(0);
    setRun((r) => r + 1);
  }, [set]);

  useEffect(() => {
    // "unavailable" persists; "live" is kept alive by the caller's polling.
    if (!visible || phaseRef.current === "unavailable" || phaseRef.current === "live") return;
    set("checking");
    setElapsedS(0);
    const start = performance.now();
    const since = () => performance.now() - start;
    const timers: number[] = [];
    let controller: AbortController | null = null;
    let cancelled = false;

    const finish = (p: Phase) => {
      cancelled = true;
      controller?.abort();
      timers.forEach((t) => window.clearTimeout(t));
      window.clearInterval(counter);
      set(p);
    };

    timers.push(
      window.setTimeout(() => {
        if (phaseRef.current === "checking") set("waking");
      }, WAKING_AFTER_MS),
    );
    timers.push(window.setTimeout(() => finish("unavailable"), GIVE_UP_AFTER_MS));
    const counter = window.setInterval(() => setElapsedS(Math.floor(since() / 1000)), 1000);

    const attempt = async (n: number) => {
      controller = new AbortController();
      const timeout = window.setTimeout(() => controller?.abort(), PING_TIMEOUT_MS);
      const result = await ping(controller.signal);
      window.clearTimeout(timeout);
      if (cancelled) return;
      const next = phaseAfterPing(phaseRef.current, result);
      if (next === "live" || next === "unavailable") {
        finish(next);
        return;
      }
      const at = nextAttemptAt(n, since());
      if (at === null) return; // the give-up timer ends it at 90 s
      timers.push(window.setTimeout(() => void attempt(n + 1), Math.max(0, at - since())));
    };
    void attempt(0);

    return () => {
      cancelled = true;
      controller?.abort();
      timers.forEach((t) => window.clearTimeout(t));
      window.clearInterval(counter);
    };
  }, [visible, run, set]);

  useEffect(() => {
    if (phase !== "live") return;
    const t = window.setTimeout(() => setPillShown(false), LIVE_HOLD_MS);
    return () => window.clearTimeout(t);
  }, [phase]);

  return { phase, elapsedS, pillShown, restart };
}
