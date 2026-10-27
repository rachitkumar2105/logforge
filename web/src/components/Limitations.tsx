// Section 07: what LogForge does not do. Every item is backed by DECISIONS.md or SECURITY-AUDIT.md.

const ITEMS: { title: string; body: string }[] = [
  {
    title: "One fixed controller, no consensus",
    body:
      "Cluster metadata lives on a single controller node. If it is down, brokers keep serving with the metadata they have, but nothing can elect a new leader, change an ISR or create a topic until it returns. Kafka replicates its metadata with Raft (KRaft); LogForge deliberately does not.",
  },
  {
    title: "No encryption, and only a shared token, on the broker protocol",
    body:
      "Creating or deleting topics, registering a broker, changing an ISR and replicating need a shared cluster token, but producing and consuming need nothing, and the token itself crosses the network in clear text. The cluster is only safe on a private network, which is how Compose and Render run it. The token-protected admin HTTP API is the only thing exposed.",
  },
  {
    title: "Slower data path than Kafka by design",
    body:
      "Fetches copy bytes through the JVM heap (no sendfile zero-copy), the Java producer keeps one request in flight per broker, and there is no compression. The benchmark section shows what that costs on this machine.",
  },
  {
    title: "Failover takes about one session timeout",
    body:
      "A dead broker is noticed when its heartbeats stop for the session timeout (3 s in Compose, 2 s in the gate test), so writes to its partitions pause roughly that long. The replay above measures it.",
  },
  {
    title: "acks=1 can lose acknowledged writes",
    body:
      "If the leader dies before a follower copies a batch, that batch is gone; a test measures exactly this. Use acks=all with min.insync.replicas=2 for anything that matters. Unclean leader election is off by default for the same reason.",
  },
  {
    title: "Consumer groups are simple",
    body:
      "The coordinator computes assignments itself (range or round-robin; no custom assignors), rebalances stop the whole group (no incremental cooperative protocol), membership is in memory, and a group does not rebalance when a subscribed topic gains partitions.",
  },
  {
    title: "Compaction is not built for scale",
    body:
      "Each pass rescans the whole partition and holds every key in memory, and tombstone expiry trusts the record timestamp. Fine for the offsets topic and demos, not for terabytes.",
  },
  {
    title: "No transactions, no Connect, no Streams",
    body:
      "Idempotence is per partition only. There is no cross-partition atomicity and none of Kafka's ecosystem; these were out of scope from the start.",
  },
];

export function Limitations() {
  return (
    <section id="limitations" aria-labelledby="limits-title" className="border-b border-line py-20">
      <div className="wrap">
        <p className="label">07 / Honest limitations</p>
        <h2 id="limits-title" className="h2 mt-3 max-w-3xl">
          What LogForge does not do
        </h2>
        <p className="mt-4 max-w-2xl text-muted">
          Knowing where a system stops is part of understanding it. These are known, documented and, where they
          matter for safety, tested.
        </p>
        <ul className="mt-10 grid gap-4 md:grid-cols-2">
          {ITEMS.map((i) => (
            <li key={i.title} className="panel p-5">
              <h3 className="h3">{i.title}</h3>
              <p className="mt-2 text-sm text-muted">{i.body}</p>
            </li>
          ))}
        </ul>
      </div>
    </section>
  );
}
