// Shapes of the JSON the broker's admin API and the bench/chaos tools write (docs/API.md).

export type ReplicaState = { brokerId: number; logEndOffset: number; highWatermark: number };

export type PartitionView = {
  partition: number;
  leader: number;
  leaderEpoch: number;
  replicas: number[];
  isr: number[];
  underReplicated: boolean;
  offline: boolean;
  replicaState: ReplicaState[];
};

export type TopicView = { name: string; internal: boolean; cleanupPolicy: string; partitions: PartitionView[] };

export type BrokerView = {
  id: number;
  host: string;
  port: number;
  alive: boolean;
  controller: boolean;
  lastHeartbeatMsAgo: number;
};

export type GroupView = {
  groupId: string;
  coordinator: number;
  state: string;
  generation: number;
  members: number;
  lag: { topic: string; partition: number; committed: number; highWatermark: number; lag: number }[];
};

export type ClusterView = {
  clusterId: string;
  controllerId: number;
  metadataVersion: number;
  capturedAtMs: number;
  brokers: BrokerView[];
  topics: TopicView[];
  groups: GroupView[];
  rates: { messagesInPerSec: number; bytesInPerSec: number; bytesOutPerSec: number };
};

export type ClusterResponse = {
  source: "live" | "snapshot";
  fetchedAtMs: number;
  reason?: string;
  cluster: ClusterView;
};

export type Percentiles = { samples: number; p50?: number; p95?: number; p99?: number; max?: number };

export type BenchConfig = {
  label: string;
  mode: "throughput" | "e2e";
  recordSizeBytes: number;
  acks: string;
  partitions: number;
  replicationFactor: number;
  batchSizeBytes: number;
  lingerMs: number;
  fsync: string;
  compression: string;
  producerBufferBytes?: number;
  idempotentProducer: boolean;
  warmupSeconds: number;
  measureSeconds: number;
  targetRatePerSec: number;
};

export type Hardware = {
  cpuModel: string;
  cpusVisible: number;
  memTotal: string;
  os: string;
  java: string;
  host: string;
  environment: string;
};

export type BenchResult = {
  system: "logforge" | "kafka";
  version: string;
  finishedAt: string;
  config: BenchConfig;
  results: {
    recordsPerSec?: number;
    mbPerSec?: number;
    ackedInWindow?: number;
    sendErrors?: number;
    produceLatencyMs?: Percentiles;
    sent?: number;
    received?: number;
    sendToReceiveMs?: Percentiles;
    ackToReceiveMs?: Percentiles;
  };
  hardware: Hardware;
  command: string;
};

export type ChaosResult = {
  scenario: string;
  description: string;
  acked: number;
  consumed: number;
  missingAcked: number;
  duplicates: number;
  recoveryTimeMs: number;
  verdict: "PASS" | "FAIL";
  command: string;
};

export type ReplayFrame = {
  t: number;
  acked: number;
  brokers: { id: number; alive: boolean }[];
  partitions: {
    partition: number;
    leader: number;
    leaderEpoch: number;
    isr: number[];
    replicaState: ReplicaState[];
  }[];
};

export type Replay = {
  topic: string;
  sampleIntervalMs: number;
  leaderStoppedAtMs: number;
  writesResumedAtMs: number;
  recoveryTimeMs: number;
  frames: ReplayFrame[];
};

export type SegmentSample = {
  topic: string;
  partition: number;
  generatedBy: string;
  segmentBytes: number;
  indexIntervalBytes: number;
  segments: {
    baseOffset: number;
    nextOffset: number;
    sizeBytes: number;
    active: boolean;
    fileName: string;
    index: { offset: number; position: number }[];
    batches: { baseOffset: number; lastOffset: number; records: number; sizeBytes: number; position: number }[];
  }[];
};
