#!/usr/bin/env bash
# Hosted-mode check: one node under Render free-tier limits (512 MB, no swap, 0.1 CPU, PORT=10000),
# seeded topic, then 8 minutes at 500 records/s plus the admin cluster view once a second.
# Usage: bash bench/hosted-memory-test.sh [output file]
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
OUT="${1:-bench/results/hosted-512mb.txt}"; WORK="$(pwd)/chaos/work"; mkdir -p "$WORK"; WORK_W="$WORK"; command -v cygpath >/dev/null 2>&1 && WORK_W="$(cygpath -w "$WORK")"
JOPTS="-Xmx300m -Xms64m -XX:+UseSerialGC -Xss512k -XX:MaxMetaspaceSize=96m -XX:ReservedCodeCacheSize=48m -XX:MaxDirectMemorySize=64m -XX:TieredStopAtLevel=1 -XX:+ExitOnOutOfMemoryError"
docker rm -f lf-hosted lf-load >/dev/null 2>&1; docker network rm lf-mem >/dev/null 2>&1
docker network create lf-mem >/dev/null
START=$(date +%s)
docker run -d --name lf-hosted --network lf-mem --memory=512m --memory-swap=512m --cpus=0.1 \
  -p 127.0.0.1:10000:10000 \
  -e PORT=10000 -e LOGFORGE_NODE_ID=1 -e LOGFORGE_ROLES=broker,controller \
  -e LOGFORGE_ADVERTISED=lf-hosted:9092 -e LOGFORGE_IO_THREADS=1 -e LOGFORGE_HANDLER_THREADS=2 \
  -e LOGFORGE_SEED_TOPIC=demo-orders:3:600 -e LOGFORGE_CORS_ORIGINS=http://localhost:3000 \
  -e LOGFORGE_ADMIN_TOKEN -e LOGFORGE_READ_TOKEN -e LOGFORGE_CLUSTER_TOKEN -e JAVA_TOOL_OPTIONS="$JOPTS" \
  logforge:local >/dev/null
until curl -sf http://127.0.0.1:10000/health >/dev/null; do sleep 0.5; [ $(( $(date +%s) - START )) -gt 180 ] && { echo "no health after 180 s"; break; }; done
echo "health after $(( $(date +%s) - START )) s (0.1 CPU)" > "$OUT"
sleep 10
echo "seed: $(docker logs lf-hosted 2>&1 | grep -o '"msg":"seed[^"]*"[^}]*' | head -2)" >> "$OUT"
# Load: rf=1 producer at 500 records/s for 8 minutes, plus the admin cluster view once a second.
docker run -d --name lf-load --network lf-mem -v "$WORK_W:/work" -e LOGFORGE_CLUSTER_TOKEN \
  logforge-bench chaos-produce \
  --bootstrap lf-hosted:9092 --topic load --acks /work/load.acks --stop /work/load.stop \
  --rate 500 --partitions 3 --size 512 --rf 1 >/dev/null
rm -f "$WORK/load.stop"
for i in $(seq 1 40); do
  for j in $(seq 1 15); do curl -s -o /dev/null -H "Authorization: Bearer $LOGFORGE_READ_TOKEN" http://127.0.0.1:10000/api/v1/cluster; sleep 1; done
  m=$(docker stats --no-stream --format '{{.MemUsage}} {{.CPUPerc}}' lf-hosted)
  rss=$(docker exec lf-hosted sh -c 'grep VmRSS /proc/1/status' 2>/dev/null | tr -s ' ')
  echo "t=$(( i*15 ))s cgroup=$m $rss" >> "$OUT"
  [ "$i" = 32 ] && touch "$WORK/load.stop"
done
docker wait lf-load >/dev/null; echo "producer: $(docker logs lf-load 2>&1 | grep chaos-produce | tail -1)" >> "$OUT"
echo "state: $(docker inspect -f 'running={{.State.Running}} oomkilled={{.State.OOMKilled}} restarts={{.RestartCount}} exit={{.State.ExitCode}}' lf-hosted)" >> "$OUT"
echo "errors in log: $(docker logs lf-hosted 2>&1 | grep -c '"level":"ERROR"')" >> "$OUT"
docker logs lf-hosted > "$WORK/broker.log" 2>&1
docker rm -f lf-hosted lf-load >/dev/null; docker network rm lf-mem >/dev/null
echo done >> "$OUT"
