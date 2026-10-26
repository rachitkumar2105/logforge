#!/usr/bin/env bash
# Starts the Compose cluster, writes a real load to demo-orders (3 partitions, replication factor
# 3, the same topic the hosted single broker seeds), and saves the controller's /api/v1/cluster
# view as the site's recorded snapshot. Leaves the cluster
# running so `pnpm dev` can show it live (LOGFORGE_ADMIN_URL=http://127.0.0.1:8080).
set -euo pipefail
cd "$(dirname "$0")/../.."
export MSYS_NO_PATHCONV=1
set -a; . ./.env; set +a

docker compose up -d >/dev/null
for _ in $(seq 1 90); do
  n=$(docker compose ps --format '{{.Health}}' | grep -c healthy || true)
  [ "$n" -ge 4 ] && break
  sleep 2
done

WORK="$(pwd)/chaos/work"
mkdir -p "$WORK"
MOUNT="$WORK"; command -v cygpath >/dev/null 2>&1 && MOUNT="$(cygpath -w "$WORK")"
rm -f "$WORK/demo.stop"
docker rm -f demo-producer >/dev/null 2>&1 || true
docker run -d --name demo-producer --network logforge_cluster -v "$MOUNT:/work" logforge-bench \
  chaos-produce --bootstrap broker1:9092,broker2:9092,broker3:9092 --topic demo-orders \
  --acks /work/demo.acks --stop /work/demo.stop --rate 500 --partitions 3 --size 256 >/dev/null
sleep 20

curl -s -H "Authorization: Bearer $LOGFORGE_READ_TOKEN" http://127.0.0.1:8080/api/v1/cluster \
  > web/src/data/cluster-snapshot.json
touch "$WORK/demo.stop"
docker wait demo-producer >/dev/null; docker rm demo-producer >/dev/null
python -c "import json; d=json.load(open('web/src/data/cluster-snapshot.json')); print('snapshot:', len(d['brokers']), 'nodes,', len(d['topics']), 'topics')"
