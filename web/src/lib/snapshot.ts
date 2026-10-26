import snapshot from "@/data/cluster-snapshot.json";
import type { ClusterResponse, ClusterView } from "./types";

// The recorded view of the local three-node cluster (web/scripts/capture-snapshot.sh). It is
// what the live topology shows until the hosted broker answers, and whenever it does not.
export const recordedCluster = snapshot as unknown as ClusterView;

export const recordedResponse: ClusterResponse = {
  source: "snapshot",
  fetchedAtMs: recordedCluster.capturedAtMs,
  reason: "recorded",
  cluster: recordedCluster,
};
