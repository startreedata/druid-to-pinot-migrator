#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Druid → Pinot quickstart driver
#
# End-to-end demonstration of the migration tool against live clusters:
#   1. Bring up Druid + Pinot via docker-compose
#   2. Generate a deterministic sample dataset
#   3. Ingest it into Druid using druid-spec.json
#   4. Run `dpm generate` to produce Pinot artifacts
#   5. Push the schema + table config to Pinot
#   6. Ingest the same source data into Pinot's OFFLINE table
#   7. Run validate.py to compare aggregate query results
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT_DIR="$HERE/output"
DRUID_ROUTER="http://localhost:8888"
DRUID_COORD="http://localhost:8081"
PINOT_CTRL="http://localhost:9000"
PINOT_BROKER="http://localhost:8099"
DATASOURCE="pageviews"
TABLE="pageviews"

bold()  { printf "\033[1m%s\033[0m\n" "$*"; }
cyan()  { printf "\033[36m%s\033[0m\n" "$*"; }
green() { printf "\033[32m%s\033[0m\n" "$*"; }
red()   { printf "\033[31m%s\033[0m\n" "$*"; }

require() {
  command -v "$1" >/dev/null 2>&1 || { red "Missing required command: $1"; exit 1; }
}
require docker
require curl
require python3

# Resolve the dpm command — prefer the installed console script, fall back to
# `python -m`.  Either way, typer + pydantic must be importable.
if command -v dpm >/dev/null 2>&1; then
  DPM=(dpm)
else
  DPM=(python3 -m migrator.cli.app)
fi
if ! python3 -c "import typer, pydantic" 2>/dev/null; then
  red "Migrator dependencies not installed."
  red "Run 'pip install -e .' from the repo root first."
  exit 1
fi

# Pick docker compose vs docker-compose
if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  red "Need 'docker compose' (v2) or 'docker-compose' (v1) on PATH"
  exit 1
fi

cleanup() {
  if [[ "${KEEP_RUNNING:-0}" == "1" ]]; then
    cyan "KEEP_RUNNING=1 — leaving the cluster up"
    cyan "  Druid Web Console: http://localhost:8888"
    cyan "  Pinot Web UI:      http://localhost:9000"
    cyan "Tear down later with:  $DC -f $HERE/docker-compose.yml down -v"
    return
  fi
  cyan "Tearing down docker stack..."
  ( cd "$HERE" && $DC down -v --remove-orphans ) || true
}
trap cleanup EXIT

# ───── 1. Bring up cluster ───────────────────────────────────────────────────
bold "[1/9] Starting Druid + Pinot docker stack"
( cd "$HERE" && $DC up -d --wait )

# ───── 1b. Preflight (dpm doctor) ────────────────────────────────────────────
# Demonstrates the v0.10.0 doctor command — fast HTTP-only probes that
# catch cluster-config problems before a long migration would discover
# them mid-flight. Same probes the operator would run on a real env.
bold "[2/9] Preflight (dpm doctor)"
( cd "$ROOT" && "${DPM[@]}" doctor \
    --druid-router "$DRUID_ROUTER" \
    --druid-coordinator "$DRUID_COORD" \
    --pinot-controller "$PINOT_CTRL" \
    --pinot-broker "$PINOT_BROKER" )

# ───── 2. Generate sample data ───────────────────────────────────────────────
bold "[3/9] Generating sample dataset (5,000 events)"
python3 "$HERE/data/generate_data.py"

# ───── 3. Ingest into Druid ──────────────────────────────────────────────────
bold "[4/9] Submitting Druid ingestion task"
TASK_ID=$(curl -sf -X POST -H "Content-Type: application/json" \
  --data @"$HERE/druid-spec.json" \
  "$DRUID_ROUTER/druid/indexer/v1/task" | python3 -c "import json,sys; print(json.load(sys.stdin)['task'])")
cyan "  task_id = $TASK_ID"

# Poll task to terminal state
DEADLINE=$(( $(date +%s) + 600 ))
while [[ $(date +%s) -lt $DEADLINE ]]; do
  STATUS=$(curl -sf "$DRUID_ROUTER/druid/indexer/v1/task/$TASK_ID/status" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['status']['status'])")
  case "$STATUS" in
    SUCCESS) green "  task SUCCESS"; break ;;
    FAILED)  red "  task FAILED"; exit 1 ;;
    *)       echo "  task status: $STATUS"; sleep 5 ;;
  esac
done

# Wait for the datasource to be queryable
cyan "  waiting for datasource '$DATASOURCE' to be queryable..."
DEADLINE=$(( $(date +%s) + 180 ))
while [[ $(date +%s) -lt $DEADLINE ]]; do
  CNT=$(curl -sf -X POST -H "Content-Type: application/json" \
    --data "{\"query\":\"SELECT COUNT(*) AS c FROM \\\"$DATASOURCE\\\"\"}" \
    "$DRUID_ROUTER/druid/v2/sql" 2>/dev/null \
    | python3 -c "import json,sys
try:
  d=json.load(sys.stdin); v=d[0].get('c') if d else 0
  print(int(v or 0))
except Exception:
  print(0)" || echo 0)
  if [[ "$CNT" =~ ^[0-9]+$ ]] && (( CNT > 0 )); then
    green "  Druid datasource has $CNT rows (post-rollup)"
    break
  fi
  sleep 5
done

# ───── 4. Run dpm generate ───────────────────────────────────────────────────
bold "[5/9] Running 'dpm generate' to produce Pinot artifacts"
rm -rf "$OUT_DIR"
( cd "$ROOT" && "${DPM[@]}" generate "$HERE/druid-spec.json" --out "$OUT_DIR" )
cyan "  generated files:"
ls -1 "$OUT_DIR"

# ───── 4b. Show recommendations (dpm recommend) ──────────────────────────────
# v0.10.0 recommend command — surfaces star-tree, sketch-aggregator,
# range-index, and id-like inverted/bloom suggestions derived from the
# canonical model. Operators paste the config_hint snippets into their
# generated table config to lift query latency.
bold "[6/9] Pinot indexing recommendations (dpm recommend)"
( cd "$ROOT" && "${DPM[@]}" recommend "$HERE/druid-spec.json" )

# ───── 5. Push schema + table to Pinot ───────────────────────────────────────
bold "[7/9] Deploying schema and table to Pinot"
curl -sf -X POST -H "Content-Type: application/json" \
  --data @"$OUT_DIR/schema.json" \
  "$PINOT_CTRL/schemas" >/dev/null && green "  schema created"

curl -sf -X POST -H "Content-Type: application/json" \
  --data @"$OUT_DIR/table-offline.json" \
  "$PINOT_CTRL/tables" >/dev/null && green "  table created"

# ───── 6. Ingest into Pinot ──────────────────────────────────────────────────
bold "[8/9] Ingesting source data into Pinot"
BATCH_CFG='{"inputFormat":"json","recordReaderSpec":{"dataFormat":"json","className":"org.apache.pinot.plugin.inputformat.json.JSONRecordReader"}}'
BATCH_CFG_ENC=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$BATCH_CFG")
curl -sf -X POST \
  -F "file=@$HERE/data/pageviews.json;type=application/octet-stream" \
  "$PINOT_CTRL/ingestFromFile?tableNameWithType=${TABLE}_OFFLINE&batchConfigMapStr=$BATCH_CFG_ENC" \
  >/dev/null && green "  ingestFromFile accepted"

# ───── 7. Validate ───────────────────────────────────────────────────────────
bold "[9/9] Running parity validation"
python3 "$HERE/validate.py"

green "\n✓ Quickstart complete."
green "  Druid Web Console:  http://localhost:8888"
green "  Pinot Web UI:       http://localhost:9000"
green "  Generated files:    $OUT_DIR"
green "  Migration summary:  $OUT_DIR/reports/migration-summary.md"
echo ""
echo "Tip: re-run with KEEP_RUNNING=1 to leave the cluster up after validation."
