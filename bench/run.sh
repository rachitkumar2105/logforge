#!/usr/bin/env bash
# Regenerates bench/results/: LogForge and Apache Kafka, same client workload, same Docker host.
#
#   bash bench/run.sh            # full matrix (about 15 minutes)
#   MEASURE=10 bash bench/run.sh # shorter measurement windows
#
# Needs Docker with Compose and a .env holding the admin tokens (see .env.example).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

WARMUP="${WARMUP:-5}"
MEASURE="${MEASURE:-20}"
RESULTS="$(pwd)/bench/results"
set -a; . ./.env; set +a
SYSTEMS="${SYSTEMS:-logforge kafka}"
mkdir -p "$RESULTS"
for sys in $SYSTEMS; do rm -f "$RESULTS/$sys"-*.json; done
rm -f "$RESULTS/summary.json"
RESULTS_MOUNT="$RESULTS"
if command -v cygpath >/dev/null 2>&1; then RESULTS_MOUNT="$(cygpath -w "$RESULTS")"; fi

BENCH_HOST="${BENCH_HOST:-$(uname -srm)}"
BENCH_ENV="Docker $(docker version --format '{{.Server.Version}}') on $(docker info --format '{{.OperatingSystem}}'), $(docker info --format '{{.NCPU}}') CPUs and $(( $(docker info --format '{{.MemTotal}}') / 1024 / 1024 )) MiB visible to containers; brokers and the client share this host"
CMD="bash bench/run.sh (WARMUP=$WARMUP MEASURE=$MEASURE)"

echo "== building images"
docker compose build >/dev/null
docker build -q -f bench/Dockerfile -t logforge-bench . >/dev/null

bench() { # network bootstrap args...
  local net="$1" boot="$2"; shift 2
  docker run --rm --network "$net" -v "$RESULTS_MOUNT:/results" \
    -e BENCH_HOST="$BENCH_HOST" -e BENCH_ENV="$BENCH_ENV" -e LOGFORGE_CLUSTER_TOKEN \
    logforge-bench suite --bootstrap "$boot" --out /results --warmup "$WARMUP" --measure "$MEASURE" --command "$CMD" "$@"
}

wait_healthy() {
  for _ in $(seq 1 90); do
    local n; n=$(docker compose ps --format '{{.Health}}' | grep -c healthy || true)
    [ "$n" -ge 4 ] && return 0
    sleep 2
  done
  echo "cluster did not become healthy" >&2; exit 1
}

LF_BOOT="broker1:9092,broker2:9092,broker3:9092"
if [[ " $SYSTEMS " == *" logforge "* ]]; then
for policy in never ms:1000 records:1; do
  echo "== LogForge, fsync=$policy"
  docker compose down -v >/dev/null 2>&1 || true
  LOGFORGE_FSYNC="$policy" LOGFORGE_JAVA_OPTS="-Xms512m -Xmx1g" docker compose up -d >/dev/null
  wait_healthy
  if [ "$policy" = "never" ]; then
    bench logforge_cluster "$LF_BOOT" --system logforge --suite core --fsync never
    # Prove replication kept up: every follower's log end must equal its leader's after the runs.
    sleep 5
    curl -s -H "Authorization: Bearer $LOGFORGE_READ_TOKEN" http://127.0.0.1:8080/api/v1/cluster |
      python -c "
import json, sys
d = json.load(sys.stdin)
rows = []
for t in d['topics']:
    if not t['name'].startswith('bench-'):
        continue
    for p in t['partitions']:
        ends = {r['brokerId']: r['logEndOffset'] for r in p['replicaState']}
        leader_end = ends.get(p['leader'], -1)
        rows.append((t['name'], p['partition'], leader_end, min(ends.values()), sorted(p['isr'])))
lagging = [r for r in rows if r[3] != r[2]]
print(f'replication check: {len(rows)} partitions, {len(lagging)} with a follower behind its leader')
for r in rows:
    print(f'  {r[0]}-{r[1]}: leader end {r[2]}, slowest replica end {r[3]}, isr {r[4]}')
" > "$RESULTS/replication-check.txt"
    cat "$RESULTS/replication-check.txt" | head -3
  fi
  bench logforge_cluster "$LF_BOOT" --system logforge --suite fsync --fsync "$policy"
done
docker compose down -v >/dev/null
fi

if [[ " $SYSTEMS " == *" kafka "* ]]; then
echo "== Apache Kafka 3.9.0 (KRaft, 3 nodes)"
docker compose -f bench/kafka-compose.yml -p kafkabench down -v >/dev/null 2>&1 || true
docker compose -f bench/kafka-compose.yml -p kafkabench up -d >/dev/null
sleep 30
K_BOOT="kafka1:9092,kafka2:9092,kafka3:9092"
bench kafkabench_kafka "$K_BOOT" --system kafka --suite core --fsync "Kafka default (no forced flush)"
for policy in never ms:1000 records:1; do
  bench kafkabench_kafka "$K_BOOT" --system kafka --suite fsync --fsync "$policy"
done
docker compose -f bench/kafka-compose.yml -p kafkabench down -v >/dev/null
fi

docker run --rm -v "$RESULTS_MOUNT:/results" logforge-bench summarize --in /results
echo "== done: $RESULTS"
