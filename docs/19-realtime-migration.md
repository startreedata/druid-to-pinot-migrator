# Tutorial 19 — Realtime (Hybrid) Migration from a Druid Kafka Datasource

The `dpm generate` command handles a single-source migration. When the
Druid datasource is a Kafka **realtime** ingestion, however, you usually
want both sides at once:

- An **OFFLINE** Pinot table holding everything Druid had already published
  before the cutover (the *historical* portion).
- A **REALTIME** Pinot table picking up *exactly* where Druid stopped, so
  no events are lost and no events are double-counted.

Pinot supports this natively as a **hybrid table** — same name in both the
OFFLINE and REALTIME variants; the broker routes time ranges automatically.
The migrator generates both halves plus a runbook with one command.

This tutorial walks the full flow.

---

## The shape of the migration

```
                 ┌────────────────────────────┐
   Kafka topic ──┤ Druid Kafka supervisor      │── stops here ─▶ T (watermark)
                 │ (consumes events 0…T)       │
                 └────────────────────────────┘
                                                 ▼
                 ┌────────────────────────────┐
   Druid deep ──▶│ Pinot OFFLINE table         │   data with __time < T
   storage       │ ‹{datasource}_OFFLINE›       │
   (backfill)    └────────────────────────────┘
                                                 ▼
   Kafka topic ──┐
                 │
                 ▼
                 ┌────────────────────────────┐
                 │ Pinot REALTIME table        │   data with __time ≥ T
                 │ ‹{datasource}_REALTIME›     │
                 │ stream config:              │
                 │   auto.offset.reset = T(iso)│
                 └────────────────────────────┘
```

Pinot's `stream.kafka.consumer.prop.auto.offset.reset` accepts an
ISO-8601 timestamp; the broker will resolve that to the right offset on
each partition via Kafka's `offsetsForTimes` API. So the **watermark is
the seed** — embedded statically in the REALTIME table config, no runtime
offset-seeding API call needed.

---

## Three CLI commands

The migrator splits the work along the natural boundaries:

| Command | What it does | Where it runs |
|---------|--------------|---------------|
| `dpm extract-offsets` | Snapshot a Druid Kafka supervisor's offsets + watermark | Reads from Druid Overlord |
| `dpm plan-hybrid`     | Generate OFFLINE + REALTIME table configs, backfill plan, runbook | Pure (no cluster contact) |
| `dpm backfill-batch`  | Page Druid SQL → NDJSON → Pinot OFFLINE | Reads Druid, writes Pinot |

Each is independently runnable. The `plan-hybrid` step is **pure** — no
network — so it's perfect for CI-driven migration plans, code-review,
and reproducible artifact generation.

---

## Step-by-step

### 1. Snapshot the watermark from Druid

```bash
dpm extract-offsets \
  --supervisor-id my_kafka_supervisor \
  --overlord-url  http://druid-overlord:8081 \
  --out           offsets.json
```

This calls `/druid/indexer/v1/supervisor/{id}/status`, parses the
`latestOffsets` per partition, and resolves a watermark timestamp from
either `lastIngestedTimestamp` or `aggregateLag.timestamp`. The captured
JSON looks like:

```json
{
  "platform": "kafka",
  "topic": "events",
  "supervisor_id": "my_kafka_supervisor",
  "datasource": "events",
  "captured_at_iso": "2024-04-25T22:00:00.000+00:00",
  "watermark_iso": "2024-04-25T21:59:30.000+00:00",
  "watermark_ms": 1714081170000,
  "offsets": [
    {"partition": 0, "offset": 1948203},
    {"partition": 1, "offset": 1948010}
  ]
}
```

The **watermark timestamp** is what Pinot uses. The per-partition
**offsets** are informational — they end up in the runbook so an operator
can verify or use `kafka-consumer-groups.sh` independently.

### 2. Stop the Druid supervisor

This is a one-line cutover. After it returns, Druid is no longer
ingesting from Kafka:

```bash
curl -X POST http://druid-overlord:8081/druid/indexer/v1/supervisor/my_kafka_supervisor/terminate
```

### 3. Generate the hybrid plan

```bash
dpm plan-hybrid path/to/druid-spec.json \
  --offset-map offsets.json \
  --out        ./hybrid-output
```

The output directory contains:

```
hybrid-output/
├── schema.json              # Pinot schema (shared OFFLINE + REALTIME)
├── table-offline.json       # OFFLINE table config
├── table-realtime.json      # REALTIME table config WITH watermark
├── backfill-job.json        # Pinot LaunchDataIngestionJob spec
├── hybrid-plan.json         # Full plan (machine-readable)
├── watermark.json           # Copy of the offset map for traceability
└── runbook.md               # Step-by-step guide for operators
```

The REALTIME config has the watermark embedded:

```json
"streamConfigs": {
  "stream.kafka.consumer.prop.auto.offset.reset": "2024-04-25T21:59:30.000+00:00",
  ...
}
```

### 4. Deploy schema + tables to Pinot

```bash
PINOT=http://pinot-controller:9000

curl -X POST -H 'Content-Type: application/json' \
  --data @hybrid-output/schema.json         "$PINOT/schemas"
curl -X POST -H 'Content-Type: application/json' \
  --data @hybrid-output/table-offline.json  "$PINOT/tables"
curl -X POST -H 'Content-Type: application/json' \
  --data @hybrid-output/table-realtime.json "$PINOT/tables"
```

As soon as the REALTIME table is created, Pinot starts consuming from
Kafka beginning at the watermark timestamp. **No runtime offset-seeding
step is needed** — the seed is in the static config.

### 5. Backfill the OFFLINE half

Two paths:

#### A. Tooling path — small to medium datasets

```bash
dpm backfill-batch \
  --datasource       events \
  --pinot-table      events \
  --start-iso        2024-04-01T00:00:00.000Z \
  --end-iso          2024-04-25T21:59:30.000+00:00 \
  --druid-router     http://druid-router:8888 \
  --pinot-controller http://pinot-controller:9000 \
  --staging-dir      /tmp/events-backfill
```

This pages rows out of Druid (`SELECT * FROM "events" WHERE __time < <watermark>`),
writes each page to a local NDJSON file, then POSTs each file to Pinot's
`/ingestFromFile` endpoint. Best for datasets that fit on one host's disk.

#### B. Runbook path — large datasets

For larger backfills, follow `runbook.md`. It walks through:

1. Run a parallelised Druid SQL → object store dump (Spark / Flink / Druid MSQ)
2. Use the included `backfill-job.json` with `pinot-admin LaunchDataIngestionJob`
3. Push the resulting tarballs to Pinot

The runbook path is recommended whenever you want to drive the dump in
parallel (which the tooling path cannot).

### 6. Verify hybrid query routing

```sql
-- Druid (the source of truth):
SELECT COUNT(*) FROM "events"
WHERE __time >= '2024-04-01T00:00:00Z'
  AND __time <  '2024-04-26T00:00:00Z'

-- Pinot:
SELECT COUNT(*) FROM events                       -- hybrid alias
WHERE timestamp >= '2024-04-01T00:00:00Z'
  AND timestamp <  '2024-04-26T00:00:00Z'
```

Pinot brokers route the time slice before the watermark to the OFFLINE
table and the slice after to the REALTIME table — automatically, no
client changes needed.

---

## Architecture

The realtime/hybrid functionality is structured around three reusable modules:

| Module | Responsibility | I/O? |
|--------|---------------|------|
| `migrator.realtime.models` | Data models (`KafkaOffsetMap`, `HybridMigrationPlan`, `StreamPlatform` enum) | None |
| `migrator.realtime.hybrid_planner` | Pure planner — canonical model + offsets → plan | None |
| `migrator.realtime.runbook_writer` | Markdown runbook from a plan | Writes one MD file |
| `migrator.druid.overlord_client` | Active client for `/supervisor/{id}/status` | HTTP |
| `migrator.realtime.backfill_runner` | Druid SQL pager + Pinot ingest sink (each behind a Protocol) | HTTP |

The `Protocol`-based interfaces in `backfill_runner` (`DruidSqlPager`,
`PinotIngestSink`) are dependency-injection seams: production callers
get the default HTTP implementations; tests use stubs to avoid live
clusters. This is the same pattern used to keep the unit-test suite
under a second.

For Kinesis support, the next pass extends `StreamPlatform.KINESIS`,
adds a Kinesis-flavoured offset model alongside `KafkaOffsetMap`, and
adds a Kinesis client that satisfies the same `DruidSqlPager` /
overlord-client protocols. No changes needed to `hybrid_planner`.

---

## Limitations and gotchas

- **Timestamp watermark vs. message offset**: Pinot's watermark seed
  uses `offsetsForTimes` against the Kafka broker. If event timestamps
  differ significantly from Kafka log timestamps (late-arriving data,
  out-of-order producers), the cutover boundary may be approximate —
  expect a small overlap or gap window of a few seconds. The hybrid
  query routing absorbs this transparently for queries.
- **Single supervisor**: `dpm extract-offsets` snapshots one supervisor
  at a time. For datasources with multiple supervisors (e.g., across
  Kafka clusters), run the command per-supervisor and merge the offset
  maps manually.
- **Offset map serialisation**: the captured JSON is the source of
  truth. Don't recompute it just before plan generation — capture once,
  feed both `plan-hybrid` and the Pinot deploy step from the same file.
- **No live offset seeding**: the design intentionally does NOT call
  Pinot REST APIs to override starting offsets. Pinot 1.5+ supports
  timestamp-based offset reset in static config, which is the single
  cleanest seeding mechanism.

---

## See also

- [Tutorial 04 — Kafka Streaming](04-kafka-streaming.md)
- [Tutorial 18 — Production Checklist](18-production-checklist.md)
- [Reference: CLI](reference/cli.md)
