# Druid → Pinot Migration Quickstart

A self-contained, end-to-end demonstration of the `druid-pinot-migrator`:

1. Spins up Apache Druid and Apache Pinot in Docker.
2. Ingests a deterministic sample dataset into Druid.
3. Runs `dpm generate` to translate the Druid spec into Pinot artifacts.
4. Deploys those artifacts to the live Pinot cluster.
5. Re-ingests the source data into Pinot.
6. Validates query parity between Druid and Pinot.

The whole flow is wrapped in a single shell script — `./run-quickstart.sh` —
so you can see every step from raw data to validated migration in one command.

---

## Prerequisites

| Tool | Version | Why |
|------|---------|-----|
| Docker + Docker Compose v2 | recent | Run the Druid + Pinot containers |
| Python | 3.11+ | Run `dpm` and the validation script |
| `pip install -e .` (in repo root) | once | Installs `dpm` and its dependencies |
| Free RAM | ~6 GB | Druid + Pinot + Postgres + ZK |
| Free disk | ~3 GB | Container images and segment storage |

The compose stack uses these host ports — make sure they're free:

```
2181  ZooKeeper
5432  Postgres (Druid metadata)
8081  Druid coordinator
8082  Druid broker
8083  Druid historical
8091  Druid middleManager
8888  Druid Web Console / router
9000  Pinot Web UI / controller
8099  Pinot broker
8097  Pinot server admin
8098  Pinot server netty
```

---

## Run it

From the repo root:

```bash
pip install -e .
cd examples/quickstart
./run-quickstart.sh
```

What you'll see:

```
[1/7] Starting Druid + Pinot docker stack
[2/7] Generating sample dataset (5,000 events)
[3/7] Submitting Druid ingestion task
[4/7] Running 'dpm generate' to produce Pinot artifacts
[5/7] Deploying schema and table to Pinot
[6/7] Ingesting source data into Pinot
[7/7] Running parity validation

=== Druid → Pinot Quickstart Parity Check ===

Scalar aggregate checks:
  PASS  Total event count                      druid=5000  pinot=5000
  PASS  Total session_ms (SUM)                 druid=...   pinot=...
  PASS  Total bytes_sent (SUM)                 druid=...   pinot=...
  PASS  Max session_ms                         druid=...   pinot=...
  PASS  Min bytes_sent                         druid=...   pinot=...

GROUP BY aggregate checks:
  PASS  events grouped by region               (4 groups)
  PASS  events grouped by platform             (3 groups)
  PASS  session_ms sum grouped by page         (5 groups)

Result: 8 passed, 0 failed
```

By default the script tears down the cluster after validation. Pass
`KEEP_RUNNING=1` to keep Druid + Pinot up so you can browse the UIs:

```bash
KEEP_RUNNING=1 ./run-quickstart.sh
# Druid Web Console: http://localhost:8888
# Pinot Web UI:      http://localhost:9000
```

To tear down after `KEEP_RUNNING=1`:

```bash
docker compose -f examples/quickstart/docker-compose.yml down -v
```

---

## What's in this directory

```
examples/quickstart/
├── docker-compose.yml        # Druid + Pinot single-host cluster
├── druid.env                 # Druid container env (matches Apache micro-quickstart)
├── data/
│   ├── generate_data.py      # Produces 5,000 deterministic NDJSON events
│   └── pageviews.json        # (generated) the sample dataset
├── druid-spec.json           # Druid index_parallel ingestion spec
├── run-quickstart.sh         # End-to-end driver script
├── validate.py               # Cross-cluster parity checker
├── output/                   # (generated) dpm artifacts land here
└── README.md                 # This file
```

---

## The dataset

5,000 deterministic web-pageview events across **2024-01-01 → 2024-01-07**:

| Field | Type | Notes |
|-------|------|-------|
| `timestamp`   | epoch millis | `timestampSpec` source |
| `region`      | string       | one of 4 regions |
| `platform`    | string       | desktop / mobile / tablet |
| `page`        | string       | one of 5 paths |
| `user_id`     | string       | `user_<1..500>` |
| `session_ms`  | long         | session duration |
| `bytes_sent`  | long         | per-event byte count |

The dataset is generated from `data/generate_data.py` with a fixed seed
(`random.Random(42)`), so the values are byte-for-byte reproducible across runs.

---

## The Druid spec (`druid-spec.json`)

```jsonc
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "pageviews",
      "timestampSpec": {"column": "timestamp", "format": "millis"},
      "dimensionsSpec": {
        "dimensions": ["region", "platform", "page", "user_id"]
      },
      "metricsSpec": [
        {"type": "count",   "name": "events"},
        {"type": "longSum", "name": "session_ms_sum", "fieldName": "session_ms"},
        {"type": "longSum", "name": "bytes_sent_sum", "fieldName": "bytes_sent"},
        {"type": "longMax", "name": "session_ms_max", "fieldName": "session_ms"},
        {"type": "longMin", "name": "bytes_sent_min", "fieldName": "bytes_sent"}
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "HOUR",
        "rollup": true,
        "intervals": ["2024-01-01/2024-01-08"]
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "local",
        "baseDir": "/quickstart-data",
        "filter": "pageviews.json"
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

This spec exercises several patterns the migrator handles:

- **Rollup** with `queryGranularity: HOUR` (raises `ROLLUP_SEMANTIC_MISMATCH`).
- **count metric** distinct from `COUNT(*)` — Tutorial 03 covers this.
- **Sum / Min / Max** metrics with separate `name` and `fieldName`.
- **Local file input** mounted from the host (`./data` → `/quickstart-data`).

---

## The migration step

`dpm generate` reads the spec and writes the following into `output/`:

```
output/
├── schema.json              # Pinot schema
├── table-offline.json       # Pinot OFFLINE table config
├── batch-job.json           # Reference batch ingestion job
├── canonical.json           # Normalised intermediate model
└── reports/
    ├── migration-report.json
    ├── migration-summary.md
    ├── risks.json
    └── warnings.json
```

A typical `migration-summary.md` for this spec lists one risk:

> **HIGH — `ROLLUP_SEMANTIC_MISMATCH`**: Druid rollup is enabled. Replace
> `COUNT(*)` with `SUM(events)` in queries that previously counted raw events.

This is exactly what the validation script does (see below).

---

## The validation step

Druid pre-aggregates events at HOUR granularity (because of `rollup=true,
queryGranularity=HOUR`). Pinot ingests the **raw** events from the same source
file. Therefore a row-by-row comparison is meaningless — but **aggregates over
the source fields** must match exactly.

`validate.py` checks:

| Druid query | Pinot query |
|-------------|-------------|
| `SUM(events)` | `COUNT(*)` |
| `SUM(session_ms_sum)` | `SUM(session_ms)` |
| `SUM(bytes_sent_sum)` | `SUM(bytes_sent)` |
| `MAX(session_ms_max)` | `MAX(session_ms)` |
| `MIN(bytes_sent_min)` | `MIN(bytes_sent)` |

Plus the same aggregates broken out by `GROUP BY region`, `GROUP BY platform`,
and `GROUP BY page`.

If everything is wired up correctly, all 8 checks pass.

---

## Inspecting the running cluster

While the cluster is up (use `KEEP_RUNNING=1`), you can poke at it directly:

### Druid

```bash
# Web Console
open http://localhost:8888

# Native SQL via the router
curl -s -X POST -H "Content-Type: application/json" http://localhost:8888/druid/v2/sql \
  -d '{"query":"SELECT region, SUM(events) FROM \"pageviews\" GROUP BY region"}'
```

### Pinot

```bash
# Web UI
open http://localhost:9000

# Query via the broker
curl -s -X POST -H "Content-Type: application/json" http://localhost:8099/query/sql \
  -d '{"sql":"SELECT region, COUNT(*) FROM pageviews GROUP BY region"}'
```

### Run the validator on demand

```bash
python3 examples/quickstart/validate.py
```

---

## Modifying the example

The whole point of this directory is to be hackable. Some things to try:

| Change | What to edit |
|--------|--------------|
| Different dimensions / metrics | `data/generate_data.py` + `druid-spec.json` |
| Disable rollup, generate raw events | Set `rollup: false` in `druid-spec.json` |
| Add a sketch metric (HLL / theta) | Adds a `BLOCKING` risk — see Tutorial 12 |
| Change segmentGranularity to HOUR | `druid-spec.json::granularitySpec` |
| Different timestamp format | `timestampSpec.format` (try `iso`) |
| Streaming Kafka source | Replace `ioConfig` with `kafka` type |

After any spec change, re-run `./run-quickstart.sh`. The compose stack rebuilds
state from scratch on each run (volumes are removed at teardown).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `Migrator dependencies not installed` | `pip install -e .` not run | Install from repo root |
| Port conflict on 8888 / 9000 | Other Druid/Pinot already running | Stop them, or edit `docker-compose.yml` ports |
| Druid task FAILED | OOM in middleManager | Increase Docker RAM allocation to ≥ 6 GB |
| Pinot table empty after ingest | `ingestFromFile` runs async | Validator waits up to 120s — re-run if you canceled early |
| `dpm: command not found` | Console script not on PATH | Script falls back to `python3 -m migrator.cli.app` automatically |

For deeper migration patterns and edge cases, see the
[full tutorial set](../../docs/index.md).
