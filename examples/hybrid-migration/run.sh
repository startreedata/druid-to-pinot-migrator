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
#   3. Produce 1,000 "old" events; submit Druid Kafka supervisor.
#   4. Force Kafka to purge the consumed events (kafka-delete-records).
#   5. Capture the watermark via `dpm extract-offsets`.
#   6. Produce 500 "new" events. Druid keeps consuming.
#   7. `dpm plan-hybrid` → OFFLINE + REALTIME table configs aligned at watermark.
#   8. Patch the realtime table (add transformConfigs for raw→rolled metrics).
#   9. Deploy schemas + tables; REALTIME picks up at the watermark via
#      Kafka offsetsForTimes.
#  10. `dpm backfill-batch` → Druid SQL paged → Pinot ingestFromFile.
#  11. Convert __time → schema time column in the staging file
#      (workaround for a known dpm gap; tracked in the README).
#  12. Re-ingest the converted staging.
#  13. Validate parity. Druid total == Pinot hybrid total.
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
  bold "[1/13] Booting Druid + Pinot + Kafka stack"
  docker compose -f "$COMPOSE_FILE" up -d --wait
fi

# ── 2. Topic with short retention ───────────────────────────────────────────
bold "[2/13] Create topic '$TOPIC' (retention.ms=10000)"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic "$TOPIC" \
  --partitions 2 --replication-factor 1 \
  --config retention.ms=10000 --config segment.ms=5000 \
  --config segment.bytes=1048576 --config file.delete.delay.ms=0 \
  >/dev/null 2>&1 || cyan "  topic exists"

# ── 3. Produce 1,000 historical events ──────────────────────────────────────
bold "[3/13] Producing $N_OLD old events (timestamps ~7 days ago)"
python3 "$HERE/data/produce.py" old --topic "$TOPIC" --bootstrap "$KAFKA_BOOTSTRAP" --n "$N_OLD"

# ── 4. Submit Druid Kafka supervisor; wait for ingestion ────────────────────
bold "[4/13] Submitting Druid Kafka supervisor"
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
bold "[5/13] Forcing Kafka retention purge (kafka-delete-records)"
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

# ── 6. Capture watermark ────────────────────────────────────────────────────
bold "[6/13] Capturing watermark via dpm extract-offsets"
mkdir -p "$OUT_DIR"
"${DPM[@]}" extract-offsets \
  --supervisor-id "$DATASOURCE" \
  --overlord-url "$DRUID_OVERLORD" \
  --out "$OUT_DIR/offsets.json"

# ── 7. Produce 500 new events ───────────────────────────────────────────────
bold "[7/13] Producing $N_NEW new events"
python3 "$HERE/data/produce.py" new --topic "$TOPIC" --bootstrap "$KAFKA_BOOTSTRAP" --n "$N_NEW"

# ── 8. Plan hybrid ──────────────────────────────────────────────────────────
bold "[8/13] dpm plan-hybrid"
rm -rf "$OUT_DIR/hybrid"
"${DPM[@]}" plan-hybrid "$HERE/specs/druid-supervisor.json" \
  --offset-map "$OUT_DIR/offsets.json" \
  --out "$OUT_DIR/hybrid"

# ── 9. Apply our local override (transformConfigs for realtime) ─────────────
bold "[9/13] Applying realtime transformConfigs override"
cp "$HERE/pinot-overrides/table-realtime.json" "$OUT_DIR/hybrid/table-realtime.json"

# ── 10. Deploy to Pinot ─────────────────────────────────────────────────────
bold "[10/13] Deploying schema + OFFLINE + REALTIME to Pinot"
curl -sS -X POST -H "Content-Type: application/json" \
  --data @"$OUT_DIR/hybrid/schema.json" "$PINOT_CTRL/schemas" >/dev/null
curl -sS -X POST -H "Content-Type: application/json" \
  --data @"$OUT_DIR/hybrid/table-offline.json" "$PINOT_CTRL/tables" >/dev/null
curl -sS -X POST -H "Content-Type: application/json" \
  --data @"$OUT_DIR/hybrid/table-realtime.json" "$PINOT_CTRL/tables" >/dev/null

cyan "  waiting for REALTIME to consume the 500 new events..."
DEADLINE=$(( $(date +%s) + 180 ))
while [[ $(date +%s) -lt $DEADLINE ]]; do
  P=$(curl -sf -X POST -H "Content-Type: application/json" \
    --data "{\"sql\":\"SELECT COUNT(*) FROM ${DATASOURCE}_REALTIME\"}" \
    "$PINOT_BROKER/query/sql" 2>/dev/null \
    | python3 -c "import json,sys
try: d=json.load(sys.stdin); rows=d.get('resultTable',{}).get('rows',[]); print(int(rows[0][0]) if rows else 0)
except: print(0)")
  [[ "$P" -ge $N_NEW ]] && break
  sleep 5
done

# ── 11. Backfill historical Druid → Pinot OFFLINE ───────────────────────────
bold "[11/13] dpm backfill-batch (Druid history → Pinot OFFLINE)"
WATERMARK_ISO=$(python3 -c "import json; print(json.load(open('$OUT_DIR/offsets.json'))['watermark_iso'])")
rm -rf "$STAGING_DIR"
"${DPM[@]}" backfill-batch \
  --datasource "$DATASOURCE" \
  --pinot-table "$DATASOURCE" \
  --start-iso '1970-01-01T00:00:00.000Z' \
  --end-iso "$WATERMARK_ISO" \
  --druid-router "$DRUID_ROUTER" \
  --pinot-controller "$PINOT_CTRL" \
  --staging-dir "$STAGING_DIR" || cyan "  initial backfill ingest may fail; will fix-and-retry below"

# ── 12. Workaround: Druid returns __time as ISO; Pinot wants epoch millis ───
bold "[12/13] Renaming __time → timestamp in staging files (known dpm gap)"
for f in "$STAGING_DIR"/page-*.json; do
  python3 "$HERE/data/fix_staging.py" "$f" "${f%.json}-fixed.json"
done

cyan "  dropping bad OFFLINE segments and re-ingesting fixed files"
curl -sS -X DELETE "$PINOT_CTRL/segments/${DATASOURCE}?type=offline" >/dev/null
sleep 2
BATCH_ENC=$(python3 -c 'import urllib.parse; print(urllib.parse.quote("{\"inputFormat\":\"json\"}"))')
for f in "$STAGING_DIR"/page-*-fixed.json; do
  curl -sS -X POST \
    -F "file=@$f;type=application/octet-stream" \
    "$PINOT_CTRL/ingestFromFile?tableNameWithType=${DATASOURCE}_OFFLINE&batchConfigMapStr=$BATCH_ENC" \
    >/dev/null
done

# ── 13. Validate parity ─────────────────────────────────────────────────────
bold "[13/13] Validating Druid vs Pinot parity"
sleep 3
python3 "$HERE/validate.py"

bold "✓ Hybrid demo complete."
cyan "  Druid Web Console:  $DRUID_ROUTER (browse via http://localhost:18888)"
cyan "  Pinot Web UI:       $PINOT_CTRL"
cyan "  dpm artifacts:      $OUT_DIR/hybrid/"
cyan "  Tear down with:     docker compose -f $COMPOSE_FILE down -v"
