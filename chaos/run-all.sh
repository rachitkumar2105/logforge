#!/usr/bin/env bash
# Kill-and-verify runs against the Docker Compose cluster (1 controller, 3 brokers).
#
#   bash chaos/run-all.sh                  # every scenario
#   bash chaos/run-all.sh leader-kill      # one scenario
#
# Each scenario: fresh cluster, topic with rf=3 and min.insync.replicas=2, an idempotent
# acks=all producer writing numbered records at a steady rate and logging every ack, a fault,
# then a verifier that reads the topic back. The verdict is PASS only if every acknowledged
# record is present. The recovery time is the longest gap between consecutive acks.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

SCENARIOS="${*:-leader-kill follower-pause disk-full network-partition restart-under-load}"
RESULTS="$(pwd)/chaos/results"
WORK="$(pwd)/chaos/work"
mkdir -p "$RESULTS" "$WORK"
mount_path() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else echo "$1"; fi; }
RESULTS_MOUNT="$(mount_path "$RESULTS")"
WORK_MOUNT="$(mount_path "$WORK")"
set -a; . ./.env; set +a

BOOT="broker1:9092,broker2:9092,broker3:9092"
BENCH_HOST="${BENCH_HOST:-$(uname -srm)}"
BENCH_ENV="Docker $(docker version --format '{{.Server.Version}}'), $(docker info --format '{{.NCPU}}') CPUs; cluster and client share this host"

docker compose build >/dev/null
docker build -q -f bench/Dockerfile -t logforge-bench . >/dev/null

compose() {
  if [ "${DISK_FULL:-0}" = "1" ]; then
    docker compose -f docker-compose.yml -f chaos/disk-full.yml "$@"
  else
    docker compose "$@"
  fi
}

wait_healthy() {
  for _ in $(seq 1 90); do
    local n; n=$(compose ps --format '{{.Health}}' | grep -c healthy || true)
    [ "$n" -ge 4 ] && return 0
    sleep 2
  done
  echo "cluster did not become healthy" >&2; exit 1
}

# Prints "leader isr..." for partition 0 of $1, read from the controller's admin API.
partition_state() {
  curl -s -H "Authorization: Bearer $LOGFORGE_READ_TOKEN" http://127.0.0.1:8080/api/v1/cluster |
    python -c "import json,sys; d=json.load(sys.stdin); t=[t for t in d['topics'] if t['name']=='$1']; p=t[0]['partitions'][0] if t else {'leader':-1,'isr':[]}; print(p['leader'], ' '.join(map(str,p['isr'])))"
}

container() { echo "logforge-broker$1-1"; }

run_scenario() {
  local name="$1" description="$2" size="${3:-8}"
  local topic="chaos-${name}"
  echo "== $name: $description"
  compose down -v >/dev/null 2>&1 || true
  compose up -d >/dev/null
  wait_healthy
  rm -f "$WORK/$name.acks" "$WORK/$name.stop"
  docker rm -f chaos-producer >/dev/null 2>&1 || true
  docker run -d --name chaos-producer --network logforge_cluster -v "$WORK_MOUNT:/work" \
    -e LOGFORGE_CLUSTER_TOKEN logforge-bench \
    chaos-produce --bootstrap "$BOOT" --topic "$topic" --acks "/work/$name.acks" --stop "/work/$name.stop" \
    --rate 2000 --partitions 3 --size "$size" >/dev/null
  local replay_args=()
  if [ "$name" = "leader-kill" ]; then
    # Record the cluster view every 200 ms for the website's crash-recovery replay.
    docker rm -f chaos-recorder >/dev/null 2>&1 || true
    rm -f "$WORK/$name.timeline"
    docker run -d --name chaos-recorder --network logforge_cluster -v "$WORK_MOUNT:/work"       -e LOGFORGE_READ_TOKEN logforge-bench       chaos-record --admin http://controller:8080 --out "/work/$name.timeline" --stop "/work/$name.stop" >/dev/null
    replay_args=(--timeline "/work/$name.timeline" --replay "/results/$name-replay.json")
  fi
  sleep 10
  local state; state=$(partition_state "$topic")
  echo "   before fault: partition 0 leader and ISR = $state"
  "fault_${name//-/_}" $state
  sleep 15
  echo "   after recovery: partition 0 leader and ISR = $(partition_state "$topic")"
  touch "$WORK/$name.stop"
  docker wait chaos-producer >/dev/null
  docker logs chaos-producer 2>&1 | grep -E "chaos-produce" || true
  docker rm chaos-producer >/dev/null
  if [ "$name" = "leader-kill" ]; then docker wait chaos-recorder >/dev/null; docker rm chaos-recorder >/dev/null; fi
  docker run --rm --network logforge_cluster -v "$WORK_MOUNT:/work" -v "$RESULTS_MOUNT:/results" \
    -e BENCH_HOST="$BENCH_HOST" -e BENCH_ENV="$BENCH_ENV" -e LOGFORGE_CLUSTER_TOKEN logforge-bench \
    chaos-verify --bootstrap "$BOOT" --topic "$topic" --acks "/work/$name.acks" --partitions 3 \
    --out "/results/$name.json" --scenario "$name" --description "$description" --rate 2000 \
    --command "bash chaos/run-all.sh $name" "${replay_args[@]}" | tail -1 |
    python -c "import json,sys; d=json.load(sys.stdin); print('   verdict', d['verdict'], 'acked', d['acked'], 'missing', d['missingAcked'], 'duplicates', d['duplicates'], 'recovery ms', d['recoveryTimeMs'])"
  # Keep every node's log for diagnosis (chaos/work is not committed).
  for c in controller broker1 broker2 broker3; do
    docker logs "logforge-$c-1" > "$WORK/$name-$c.log" 2>&1 || true
  done
  compose down -v >/dev/null
}

fault_leader_kill() { # leader isr...
  echo "   SIGKILL broker $1 (partition 0 leader)"
  docker kill -s KILL "$(container "$1")" >/dev/null
  sleep 15
  docker start "$(container "$1")" >/dev/null
}

fault_follower_pause() {
  local leader="$1"; shift
  local follower=""
  for r in "$@"; do [ "$r" != "$leader" ] && follower="$r" && break; done
  echo "   SIGSTOP (docker pause) follower broker $follower"
  docker pause "$(container "$follower")" >/dev/null
  sleep 15
  echo "   ISR while paused: $(partition_state chaos-follower-pause)"
  docker unpause "$(container "$follower")" >/dev/null
}

fault_disk_full() {
  echo "   broker3 writes to a 48 MiB tmpfs; filling it with 4 KiB records"
  sleep 25
}

fault_network_partition() {
  echo "   disconnecting broker $1 (partition 0 leader) from the cluster network"
  docker network disconnect logforge_cluster "$(container "$1")"
  sleep 15
  docker network connect --alias "broker$1" logforge_cluster "$(container "$1")"
}

fault_restart_under_load() {
  echo "   docker restart (SIGTERM, then start) of broker $1 under load"
  docker restart "$(container "$1")" >/dev/null
}

for s in $SCENARIOS; do
  case "$s" in
    leader-kill) run_scenario leader-kill "SIGKILL the partition leader mid-write, restart it 15 s later" ;;
    follower-pause) run_scenario follower-pause "SIGSTOP a follower for 15 s (docker pause), then resume" ;;
    disk-full) DISK_FULL=1 run_scenario disk-full "broker3's data volume is a 48 MiB tmpfs that fills under load" 4096 ;;
    network-partition) run_scenario network-partition "Cut the partition leader off the cluster network for 15 s" ;;
    restart-under-load) run_scenario restart-under-load "Graceful restart of the partition leader under load" ;;
    *) echo "unknown scenario $s" >&2; exit 2 ;;
  esac
done
docker run --rm -v "$RESULTS_MOUNT:/results" logforge-bench summarize --in /results
