# Tutorial 02 — Migrating a Raw Event Table

**Pattern:** `index_parallel`, no rollup, string dimensions, no pre-aggregation  
**Pinot table type:** OFFLINE  
**Classification:** `raw_event`  
**Typical use cases:** clickstream, page views, server logs, API request logs

---

## What This Pattern Looks Like in Druid

A raw event table ingests individual events without pre-aggregation. The `metricsSpec` is
either empty or contains only `count`-type metrics. `rollup` is `false`.

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "pageviews",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec": {
        "dimensions": ["page", "user", "region"]
      },
      "metricsSpec": [],
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE",
        "rollup": false,
        "intervals": ["2024-01-01/2025-01-01"]
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "local",
        "baseDir": "/data/pageviews",
        "filter": "*.json"
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate pageviews_spec.json --out ./output/pageviews
```

Expected output:

```
Generated 8 files in ./output/pageviews/
  schema.json
  table-offline.json
  batch-job.json
  canonical.json
  reports/migration-report.json
  reports/risks.json
  reports/warnings.json
  reports/migration-summary.md
```

The classification will be `raw_event` and no risks should be detected for a simple string
dimension table.

---

## What Gets Generated

### schema.json

```json
{
  "schemaName": "pageviews",
  "dimensionFieldSpecs": [
    {"name": "page",   "dataType": "STRING"},
    {"name": "region", "dataType": "STRING"},
    {"name": "user",   "dataType": "STRING"}
  ],
  "metricFieldSpecs": [],
  "dateTimeFieldSpecs": [
    {
      "name": "timestamp",
      "dataType": "LONG",
      "format": "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "granularity": "1:MILLISECONDS"
    }
  ]
}
```

Key points:
- Dimensions are sorted alphabetically (Pinot convention).
- `metricFieldSpecs` is empty since the Druid spec had no metrics.
- The ISO timestamp format maps to `SIMPLE_DATE_FORMAT` in Pinot.

### table-offline.json (excerpt)

```json
{
  "tableName": "pageviews_OFFLINE",
  "tableType": "OFFLINE",
  "segmentsConfig": {
    "timeColumnName": "timestamp",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "365",
    "replication": "1"
  },
  "tenants": {
    "broker": "DefaultTenant",
    "server": "DefaultTenant"
  },
  "tableIndexConfig": {
    "loadMode": "MMAP"
  },
  "ingestionConfig": {
    "batchIngestionConfig": {
      "segmentIngestionType": "APPEND",
      "segmentIngestionFrequency": "DAILY"
    }
  }
}
```

### batch-job.json (excerpt)

```json
{
  "jobType": "SegmentCreationAndTarPush",
  "inputDirURI": "/data/pageviews",
  "outputDirURI": "hdfs:///pinot/output/pageviews",
  "overwriteOutput": true,
  "pinotFSSpecs": [
    {"scheme": "hdfs", "className": "org.apache.pinot.plugin.filesystem.HadoopPinotFS"}
  ],
  "recordReaderSpec": {
    "dataFormat": "json",
    "className": "org.apache.pinot.plugin.inputformat.json.JSONRecordReader"
  },
  "tableSpec": {
    "tableName": "pageviews",
    "schemaURI": "http://controller:9000/schemas/pageviews",
    "tableConfigURI": "http://controller:9000/tables/pageviews"
  }
}
```

---

## Deploying to Pinot

```bash
# 1. Create the schema
curl -X POST http://pinot-controller:9000/schemas \
  -H "Content-Type: application/json" \
  -d @output/pageviews/schema.json

# 2. Create the table
curl -X POST http://pinot-controller:9000/tables \
  -H "Content-Type: application/json" \
  -d @output/pageviews/table-offline.json

# 3. Ingest data using the batch job spec
# (Adjust inputDirURI and outputDirURI to match your environment)
pinot-admin.sh LaunchDataIngestionJob \
  -jobSpecFile output/pageviews/batch-job.json
```

---

## What to Adjust for Production

The generated config uses safe defaults. Before deploying to production, review:

| Setting | Default | Recommendation |
|---------|---------|----------------|
| `replication` | `1` | Set to `3` for production clusters |
| `retentionTimeValue` | `365` | Match your actual data retention policy |
| `segmentIngestionFrequency` | `DAILY` | Match your batch cadence |
| `loadMode` | `MMAP` | `MMAP` is usually correct; use `HEAP` only for tiny tables |
| Index types | None configured | Consider `inverted`, `range`, or `sorted` indexes for frequently filtered columns |

---

## Verifying the Migration

After ingestion, run equivalent queries on both systems:

```sql
-- Total row count
-- Druid:
SELECT COUNT(*) FROM "pageviews"

-- Pinot:
SELECT COUNT(*) FROM pageviews_OFFLINE
```

```sql
-- Counts by region
-- Druid:
SELECT region, COUNT(*) AS cnt
FROM "pageviews"
GROUP BY region
ORDER BY region

-- Pinot:
SELECT region, COUNT(*) AS cnt
FROM pageviews_OFFLINE
GROUP BY region
ORDER BY region
```

For a raw event table with no rollup, row counts and aggregation results should be
**exactly equal**.

---

## Common Issues

**Problem:** Schema creation returns 400 — invalid dateTimeFieldSpec format  
**Cause:** Older Pinot versions use `EPOCH|MILLISECONDS|1` format strings; modern Pinot (1.x) requires `1:MILLISECONDS:EPOCH`.  
**Fix:** The tool generates the modern format. If targeting an older cluster (≤ 0.11), update the format field manually.

**Problem:** Ingestion fails — field not found in schema  
**Cause:** Your JSON records may have fields not declared in `dimensionsSpec`. Druid is permissive by default.  
**Fix:** Either add the missing fields to the Pinot schema, or configure `inputFormat.skipUnknownProperties: true` in the ingestion job.

**Problem:** `user` column appears empty after ingestion  
**Cause:** `user` is a reserved word in some Pinot versions.  
**Fix:** Rename to `user_id` or wrap in backticks when querying: `` `user` ``.

---

## See Also

- [Tutorial 03 — Rolled-Up Metrics](03-rolled-up-metrics.md) — when your Druid table uses `rollup: true`
- [Tutorial 06 — Typed Dimensions](06-typed-dimensions.md) — when your dimensions include `long`, `float`, or `double` types
- [Reference: Artifacts](reference/artifacts.md) — full description of every generated file
