# Hybrid migration: Druid history older than Kafka retention

A self-contained, end-to-end demonstration of migrating a Druid Kafka
datasource to a Pinot **hybrid (OFFLINE + REALTIME)** table when the
Kafka topic no longer retains the historical events Druid has in its
segments.

This is the most common production cutover shape — your Druid datasource
has been running for months, your Kafka topic only retains the last few
hours, and you can't just point Pinot at the topic and replay history.

The whole flow is wrapped in `./run.sh` so you can see every step from
raw data to validated migration in one command.

## Prerequisites

| Tool | Version | Why |
|------|---------|-----|
| Docker + Docker Compose v2 | recent | Druid + Pinot + Kafka containers |
| Python | 3.11+ | dpm + the validator |
| `pip install -e .` (from repo root) | once | Installs `dpm` |
| Free RAM | ~6 GB | Druid + Pinot + Kafka + Postgres + ZK |

The demo reuses the integration-test compose stack
(`tests/docker/docker-compose.yml`) which uses non-conflicting ports:

```
ZooKeeper:         12181
Kafka:             19092
Druid coordinator: 18081 (overlord lives here too)
Druid broker:      18082
Druid router:      18888
Pinot controller:  19000
Pinot broker:      18099
```

## Run it

```bash
pip install -e .
./examples/hybrid-migration/run.sh
```

What you'll see:

```
[1/13] Booting Druid + Pinot + Kafka stack
[2/13] Create topic 'hybrid_events_topic' (retention.ms=10000)
[3/13] Producing 1000 old events (timestamps ~7 days ago)
[4/13] Submitting Druid Kafka supervisor
[5/13] Forcing Kafka retention purge (kafka-delete-records)
[6/13] Capturing watermark via dpm extract-offsets
[7/13] Producing 500 new events
[8/13] dpm plan-hybrid
[9/13] Applying realtime transformConfigs override
[10/13] Deploying schema + OFFLINE + REALTIME to Pinot
[11/13] dpm backfill-batch (Druid history → Pinot OFFLINE)
[12/13] Renaming __time → timestamp in staging files (known dpm gap)
[13/13] Validating Druid vs Pinot parity

=== HYBRID: pageviews_hybrid (1000 hist + 500 new = 1500 events) ===
  PASS  Total event count                druid=1500  pinot=1500
  PASS  SUM(events)                      druid=1500  pinot=1500
  PASS  SUM(session_ms_sum)              druid=445768332  pinot=445768332
  PASS  SUM(bytes_sent_sum)              druid=73684277   pinot=73684277
  PASS  Distinct user_id (exact)         druid=200  pinot=200
  PASS  MIN(timestamp)                   druid=…  pinot=…
  PASS  MAX(timestamp)                   druid=…  pinot=…
  PASS  events by region                 (4 groups)
  PASS  events by platform               (3 groups)
  PASS  session_ms_sum by page           (5 groups)
  PASS  bytes_sent_sum by region         (4 groups)

Result: 11 passed, 0 failed (out of 11)
```

## What's in this directory

```
examples/hybrid-migration/
├── README.md                     # This file
├── run.sh                        # End-to-end driver
├── specs/
│   └── druid-supervisor.json     # Druid Kafka supervisor (raw, no rollup)
├── data/
│   └── produce.py                # Event producer with `old` / `new` modes
├── pinot-overrides/
│   └── table-realtime.json       # Override that adds transformConfigs (see below)
└── validate.py                   # Druid ↔ Pinot parity checks (11 cases)
```

## The migration model

### Why a hybrid table?

When Druid has more history than Kafka can replay:

```
   Druid datasource
   ──┬─────────────────────────────────────────┬──>
     │     1,000 historical events             │
     │       (only in Druid segments)          │ 500 new events
     │                                         │ (in Druid + Kafka)
     ↓                                         ↓
                                             watermark = "now" at cutover
                                             (captured via dpm extract-offsets)
```

A pure REALTIME-from-Kafka migration would lose the 1,000 historical
events forever. The fix is the standard Pinot hybrid pattern:

- **OFFLINE table**: holds events with `timestamp < watermark`. Backfilled
  from Druid SQL (which still has the segments).
- **REALTIME table**: holds events with `timestamp >= watermark`.
  Configured to start consuming from the offset corresponding to the
  watermark via Kafka's `consumer.offsetsForTimes()` API.
- **Pinot's broker** routes queries by a time boundary computed from the
  OFFLINE table's max timestamp:
  - `SELECT … FROM pageviews_hybrid` → OFFLINE answers `<= boundary`,
    REALTIME answers `> boundary`. Each event is counted exactly once.

### How dpm drives this

```bash
# 1. Snapshot Druid's current Kafka offsets + watermark timestamp.
dpm extract-offsets --supervisor-id pageviews_hybrid \
                    --overlord-url http://overlord:8081 \
                    --out offsets.json

# 2. Generate OFFLINE + REALTIME table configs aligned at the watermark.
dpm plan-hybrid druid-supervisor.json \
                --offset-map offsets.json \
                --out plan/

# 3. Backfill historical Druid → Pinot OFFLINE.
dpm backfill-batch --datasource pageviews_hybrid \
                   --pinot-table pageviews_hybrid \
                   --start-iso '1970-01-01T00:00:00.000Z' \
                   --end-iso   <watermark> \
                   --druid-router http://druid-router:8888 \
                   --pinot-controller http://pinot-controller:9000 \
                   --staging-dir staging/
```

The interesting bit is in the generated REALTIME table:

```jsonc
"stream.kafka.consumer.prop.auto.offset.reset": "<watermark ISO 8601>"
```

Pinot's Kafka consumer accepts an ISO timestamp here and resolves it via
`consumer.offsetsForTimes(...)`. Even when the historical events have
been purged from Kafka, this resolves to the offset of the **first
surviving record** (the one right after the purge cliff) — so Pinot
REALTIME consumes exactly the new events and nothing else.

## Known gaps in dpm (worked around in this example)

Two rough edges remain in the hybrid path. (A third — automatic
`__time` → schema time-column rename + ISO→ms conversion in
`backfill-batch` — was fixed in #11 / v0.4.0; the example now uses
`--time-column timestamp` instead of post-processing the staging files.)

| # | Issue | Workaround in this example |
|---|-------|---------------------------|
| 1 | `dpm normalize` requires `ioConfig.type: "kafka"` to classify a Druid Kafka spec as `stream` (Druid itself accepts the spec without it). | `specs/druid-supervisor.json` includes the field. |
| 2 | The realtime table generated by `plan-hybrid` is missing `transformConfigs` mapping raw Kafka fields (`session_ms`, `bytes_sent`) to the Druid-rolled metric names (`session_ms_sum`, `bytes_sent_sum`). Without these, the REALTIME half receives 0 for every metric. | `pinot-overrides/table-realtime.json` adds them; `run.sh` step 9 swaps it in. |

## Inspecting the running cluster

While the cluster is up, you can poke at it directly:

```bash
# Druid SQL (what's in the source datasource)
curl -s -X POST -H 'Content-Type: application/json' http://localhost:18888/druid/v2/sql \
  -d '{"query":"SELECT COUNT(*) FROM \"pageviews_hybrid\""}'

# Pinot OFFLINE only
curl -s -X POST -H 'Content-Type: application/json' http://localhost:18099/query/sql \
  -d '{"sql":"SELECT COUNT(*) FROM pageviews_hybrid_OFFLINE"}'

# Pinot REALTIME only
curl -s -X POST -H 'Content-Type: application/json' http://localhost:18099/query/sql \
  -d '{"sql":"SELECT COUNT(*) FROM pageviews_hybrid_REALTIME"}'

# Pinot HYBRID (broker-routed)
curl -s -X POST -H 'Content-Type: application/json' http://localhost:18099/query/sql \
  -d '{"sql":"SELECT COUNT(*) FROM pageviews_hybrid"}'

# What time boundary does Pinot's broker use?
curl -s http://localhost:18099/debug/timeBoundary/pageviews_hybrid
```

## Modifying the example

| Change | What to edit |
|--------|--------------|
| Different historical depth | `data/produce.py` (`--n` for the `old` phase) and the `--start-iso` of `dpm backfill-batch` |
| Different Kafka retention window | `run.sh` step 2 (`retention.ms`) |
| Different supervisor metrics | `specs/druid-supervisor.json::metricsSpec`. If you change metric names, update `pinot-overrides/table-realtime.json::transformConfigs` to match. |
| Real (not synthetic) production cluster | Skip step 1 of `run.sh` (boot) and pass real overlord/router/controller URLs to the dpm commands. |

## Tear down

```bash
docker compose -f tests/docker/docker-compose.yml down -v
```
