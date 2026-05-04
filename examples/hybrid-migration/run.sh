#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# End-to-end hybrid migration demo.
#
# Scenario: Druid has 1,000 historical events older than the current Kafka
# topic retention. We need Pinot to end up with the FULL 1,500-event history
# (1,000 historical + 500 new) without losing the events that no longer
# exist in Kafka.
#
# Steps:
#   1. Boot the Druid + Pinot + Kafka stack from tests/docker.
#   2. Create a Kafka topic with very short retention (10s).
#   3. Produce 1,000 "old" events.
#   4. Submit Druid Kafka supervisor; wait for it to consume them.
#   5. Force Kafka to purge the consumed events (kafka-delete-records).
#   6. Produce 500 "new" events. Druid keeps consuming.
#   7. `dpm cutover` — one command runs extract-offsets → plan-hybrid →
#      deploy → backfill-batch → parity-check.
#
# Pre-v0.7.0 this script ran 5 separate dpm commands plus a hand-rolled
# curl-and-wait loop, totalling ~80 LOC of orchestration. v0.6.0
# introduced ``dpm cutover``; v0.7.0 made the parity phase reliable
# via the wait-for-segments fix. v0.8.0 (this PR) updates the example
# to use it.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
COMPOSE_FILE="$ROOT/tests/docker/docker-compose.yml"

DRUID_ROUTER="http://localhost:18888"
DRUID_OVERLORD="http://localhost:18081"
PINOT_CTRL="http://localhost:19000"
PINOT_BROKER="http://localhost:18099"
KAFKA_BOOTSTRAP="localhost:19092"
KAFKA_CONTAINER="migtest-kafka"

DATASOURCE="pageviews_hybrid"
TOPIC="hybrid_events_topic"
N_OLD=1000
N_NEW=500

OUT_DIR="$HERE/output"
STAGING_DIR="$HERE/staging"

bold() { printf "\033[1m%s\033[0m\n" "$*"; }
cyan() { printf "\033[36m%s\033[0m\n" "$*"; }

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }
}
require docker
require curl
require python3

if command -v dpm >/dev/null 2>&1; then DPM=(dpm); else DPM=(python3 -m migrator.cli.app); fi

# ── 1. Boot the cluster (only if not already up) ────────────────────────────
if ! docker ps --filter 'name=migtest-pinot-controller' --format '{{.Names}}' | grep -q .; then
  bold "[1/7] Booting Druid + Pinot + Kafka stack"
  docker compose -f "$COMPOSE_FILE" up -d --wait
fi

# ── 2. Topic with short retention ───────────────────────────────────────────
bold "[2/7] Create topic '$TOPIC' (retention.ms=10000)"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic "$TOPIC" \
  --partitions 2 --replication-factor 1 \
  --config retention.ms=10000 --config segment.ms=5000 \
  --config segment.bytes=1048576 --config file.delete.delay.ms=0 \
  >/dev/null 2>&1 || cyan "  topic exists"

# ── 3. Produce 1,000 historical events ──────────────────────────────────────
bold "[3/7] Producing $N_OLD old events (timestamps ~7 days ago)"
python3 "$HERE/data/produce.py" old --topic "$TOPIC" --bootstrap "$KAFKA_BOOTSTRAP" --n "$N_OLD"

# ── 4. Submit Druid Kafka supervisor; wait for ingestion ────────────────────
bold "[4/7] Submitting Druid Kafka supervisor"
curl -sf -X POST -H "Content-Type: application/json" \
  --data @"$HERE/specs/druid-supervisor.json" \
  "$DRUID_ROUTER/druid/indexer/v1/supervisor" >/dev/null

cyan "  waiting for Druid to consume $N_OLD events..."
DEADLINE=$(( $(date +%s) + 240 ))
while [[ $(date +%s) -lt $DEADLINE ]]; do
  CNT=$(curl -sf -X POST -H "Content-Type: application/json" \
    --data "{\"query\":\"SELECT COUNT(*) AS c FROM \\\"$DATASOURCE\\\"\"}" \
    "$DRUID_ROUTER/druid/v2/sql" 2>/dev/null \
    | python3 -c "import json,sys
try: d=json.load(sys.stdin); print(int(d[0]['c']) if d else 0)
except: print(0)")
  [[ "$CNT" -ge $N_OLD ]] && break
  sleep 5
done
cyan "  druid has $CNT rows"

# ── 5. Force Kafka to purge consumed events ─────────────────────────────────
bold "[5/7] Forcing Kafka retention purge (kafka-delete-records)"
LATEST=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" --time -1)
P0_OFFSET=$(echo "$LATEST" | grep ":0:" | cut -d: -f3)
P1_OFFSET=$(echo "$LATEST" | grep ":1:" | cut -d: -f3)
DELETE_JSON="/tmp/delete-records-$$.json"
cat > "$DELETE_JSON" <<EOF
{"partitions":[{"topic":"$TOPIC","partition":0,"offset":$P0_OFFSET},{"topic":"$TOPIC","partition":1,"offset":$P1_OFFSET}],"version":1}
EOF
docker cp "$DELETE_JSON" "$KAFKA_CONTAINER:/tmp/delete-records.json"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-delete-records.sh \
  --bootstrap-server localhost:9092 \
  --offset-json-file /tmp/delete-records.json
cyan "  kafka earliest=latest=$P0_OFFSET/$P1_OFFSET — historical events purged from Kafka"

# ── 6. Produce the new events that Pinot REALTIME will pick up ─────────────
# We produce these BEFORE the cutover so that by the time
# extract-offsets captures the watermark, the supervisor has consumed
# them and Druid+Pinot are in steady state. (In a real cutover the
# new events are arriving continuously; this script just simulates that
# with one batch.)
bold "[6/7] Producing $N_NEW new events"
python3 "$HERE/data/produce.py" new --topic "$TOPIC" --bootstrap "$KAFKA_BOOTSTRAP" --n "$N_NEW"

# ── 7. Cutover — one command does extract-offsets → plan-hybrid →           ─
#                deploy → backfill-batch → parity-check                       ─
# Pre-v0.7.0 this was 5 separate dpm invocations + curls + a hand-rolled
# wait loop (~80 LOC of run.sh scaffolding). v0.6.0 introduced
# `dpm cutover`; v0.7.0 added the wait-for-segments fix that makes the
# parity phase reliable. Now it's one command.
bold "[7/7] dpm cutover"
mkdir -p "$OUT_DIR"
rm -rf "$STAGING_DIR"
"${DPM[@]}" cutover \
  --supervisor-id "$DATASOURCE" \
  --datasource    "$DATASOURCE" \
  --pinot-table   "$DATASOURCE" \
  --spec "$HERE/specs/druid-supervisor.json" \
  --druid-overlord    "$DRUID_OVERLORD" \
  --druid-router      "$DRUID_ROUTER" \
  --pinot-controller  "$PINOT_CTRL" \
  --pinot-broker      "$PINOT_BROKER" \
  --backfill-time-column timestamp \
  --out         "$OUT_DIR" \
  --staging-dir "$STAGING_DIR"

bold "✓ Hybrid demo complete."
cyan "  Druid Web Console:  $DRUID_ROUTER (browse via http://localhost:18888)"
cyan "  Pinot Web UI:       $PINOT_CTRL"
cyan "  Cutover report:     $OUT_DIR/cutover-report.json"
cyan "  Tear down with:     docker compose -f $COMPOSE_FILE down -v"
