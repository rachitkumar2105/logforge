import "server-only";
import snapshot from "@/data/cluster-snapshot.json";
import type { ClusterResponse, ClusterView } from "./types";

// Read-only access to the cluster's admin API, server side only. The read token never leaves the
// server (S8, S12). Responses are cached for 2 s so all visitors together cost at most one admin
// request per interval (the admin API rate-limits per IP). When the cluster is unreachable the
// recorded snapshot is returned, labelled as such.

const CACHE_MS = 2_000;
let cached: { at: number; body: ClusterResponse } | null = null;

function fromSnapshot(reason: string): ClusterResponse {
  return { source: "snapshot", fetchedAtMs: Date.now(), reason, cluster: snapshot as unknown as ClusterView };
}

async function fetchCluster(): Promise<ClusterResponse> {
  const base = process.env.LOGFORGE_ADMIN_URL;
  const token = process.env.LOGFORGE_READ_TOKEN;
  if (!base || !token) {
    return fromSnapshot("live cluster not configured");
  }
  try {
    // The URL comes from server configuration only, never from a request (S16).
    const res = await fetch(new URL("/api/v1/cluster", base), {
      headers: { Authorization: `Bearer ${token}` },
      cache: "no-store",
      signal: AbortSignal.timeout(2_500),
    });
    if (!res.ok) {
      return fromSnapshot(`admin API answered ${res.status}`);
    }
    return { source: "live", fetchedAtMs: Date.now(), cluster: (await res.json()) as ClusterView };
  } catch {
    return fromSnapshot("cluster unreachable");
  }
}

export async function getCluster(): Promise<ClusterResponse> {
  const now = Date.now();
  if (!cached || now - cached.at > CACHE_MS) {
    cached = { at: now, body: await fetchCluster() };
  }
  return cached.body;
}
