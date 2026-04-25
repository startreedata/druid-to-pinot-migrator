# Tutorial 12 — Sketch Aggregators (HLL, Theta, Histogram)

**Pattern:** `thetaSketch`, `HLLSketchBuild`, `HLLSketchMerge`, `hyperUnique`,  
  `quantilesDoublesSketch`, `momentSketch`, `fixedBucketsHistogram`  
**Risk:** `APPROX_AGGREGATOR_MISMATCH` (BLOCKING)  
**Typical use cases:** distinct user counts, cardinality estimation, percentile queries, histograms

---

## Why This Is BLOCKING

Druid stores sketch aggregators as opaque binary blobs. When you migrate a Druid segment
to Pinot:

1. The serialized sketch bytes from Druid's implementation are **incompatible** with Pinot's
   own sketch implementations.
2. Pinot has its own HLL and Theta sketch support (`DISTINCTCOUNTHLL`,
   `DISTINCTCOUNTTHETASKETCH`), but they use a different binary format internally.
3. You cannot convert one format to the other without access to the raw events.

**The only correct migration path is re-ingestion from raw source events.**

---

## Sample Druid Spec

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "user_activity",
      "timestampSpec": {
        "column": "event_time",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": ["country", "platform", "feature"]
      },
      "metricsSpec": [
        {"type": "count",           "name": "event_count"},
        {"type": "thetaSketch",     "name": "unique_users",  "fieldName": "user_id"},
        {"type": "HLLSketchBuild",  "name": "approx_users",  "fieldName": "user_id"},
        {"type": "hyperUnique",     "name": "hll_users",     "fieldName": "user_id"},
        {"type": "quantilesDoublesSketch", "name": "latency_sketch", "fieldName": "latency_ms"}
      ],
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE",
        "rollup": true
      }
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate user_activity_spec.json --out ./output/user_activity
```

```
Classification : complex_aggregated
Confidence     : 0.55

Risks detected: 2
  [BLOCKING] APPROX_AGGREGATOR_MISMATCH
    Druid sketch aggregators (thetaSketch, HLLSketchBuild, hyperUnique) serialize
    to opaque BYTES in Pinot. Pinot has its own HLL/Theta sketch implementations
    but the serialized formats are incompatible. Re-ingest raw events and rebuild
    sketches in Pinot.
    Evidence: Complex aggregators found: unique_users, approx_users, hll_users
    Remediation: Re-ingest raw events into Pinot and define DISTINCTCOUNTHLL or
    DISTINCTCOUNTTHETASKETCH aggregations on the raw field values.

  [HIGH] UNSUPPORTED_COMPLEX_FIELD
    Fields mapped to BYTES: unique_users, approx_users, hll_users, latency_sketch
```

---

## What Gets Generated

The tool maps sketch fields to `BYTES` as a placeholder in the schema:

```json
{
  "schemaName": "user_activity",
  "dimensionFieldSpecs": [
    {"name": "country",  "dataType": "STRING"},
    {"name": "feature",  "dataType": "STRING"},
    {"name": "platform", "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "approx_users",   "dataType": "BYTES"},
    {"name": "event_count",    "dataType": "LONG"},
    {"name": "hll_users",      "dataType": "BYTES"},
    {"name": "latency_sketch", "dataType": "BYTES"},
    {"name": "unique_users",   "dataType": "BYTES"}
  ]
}
```

**The BYTES schema is a scaffold, not a deployable artifact.** You must redesign the
metric columns before deploying to Pinot.

---

## The Re-Ingestion Strategy

Since you cannot migrate pre-computed sketches, you must ingest from raw events and
let Pinot build its own sketches.

### Step 1: Identify the raw source fields

From the Druid spec, extract the `fieldName` of each sketch metric:

| Sketch metric | fieldName (raw source) |
|-------------|----------------------|
| `unique_users` (thetaSketch) | `user_id` |
| `approx_users` (HLLSketchBuild) | `user_id` |
| `hll_users` (hyperUnique) | `user_id` |
| `latency_sketch` (quantilesDoublesSketch) | `latency_ms` |

### Step 2: Design the Pinot schema with raw fields

Replace BYTES sketch columns with the raw source field. Pinot will compute cardinality
at query time using its own approximate functions:

```json
{
  "schemaName": "user_activity",
  "dimensionFieldSpecs": [
    {"name": "country",    "dataType": "STRING"},
    {"name": "feature",    "dataType": "STRING"},
    {"name": "platform",   "dataType": "STRING"},
    {"name": "user_id",    "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "event_count", "dataType": "LONG"},
    {"name": "latency_ms",  "dataType": "DOUBLE"}
  ],
  "dateTimeFieldSpecs": [...]
}
```

### Step 3: Ingest raw events (no rollup in Pinot)

Ingest the raw, un-aggregated events into Pinot. Since there is no rollup at ingest time,
Pinot stores every event individually. Approximate distinct counts are computed at query time.

### Step 4: Query using Pinot's approximate functions

```sql
-- Distinct users (Theta sketch — most accurate, higher memory)
SELECT country,
       DISTINCTCOUNTTHETASKETCH(user_id) AS unique_users
FROM user_activity_OFFLINE
GROUP BY country
ORDER BY unique_users DESC

-- Distinct users (HLL — lower memory, slightly less accurate)
SELECT country,
       DISTINCTCOUNTHLL(user_id) AS unique_users
FROM user_activity_OFFLINE
GROUP BY country

-- Percentile latency (Pinot's TDigest sketch)
SELECT feature,
       PERCENTILETDIGEST(latency_ms, 50) AS p50,
       PERCENTILETDIGEST(latency_ms, 95) AS p95,
       PERCENTILETDIGEST(latency_ms, 99) AS p99
FROM user_activity_OFFLINE
GROUP BY feature
```

---

## Choosing the Right Distinct Count Function

Pinot offers several approximate distinct count functions with different accuracy/memory trade-offs:

| Function | Algorithm | Error rate | Memory | When to use |
|----------|---------|-----------|--------|-------------|
| `DISTINCTCOUNT(col)` | Exact | 0% | High | Small cardinality or exact result required |
| `DISTINCTCOUNTHLL(col)` | HyperLogLog | ~2% | Low | Default for large cardinality |
| `DISTINCTCOUNTTHETASKETCH(col)` | Theta Sketch | ~0.5% | Medium | When you need set operations (union/intersection) |
| `DISTINCTCOUNTBITMAP(col)` | Roaring Bitmap | ~0% | Variable | Integer columns with contiguous ranges |

For most user-count use cases, `DISTINCTCOUNTHLL` offers the best performance.
For funnel analysis and set operations (e.g., "users who did A but not B"), prefer
`DISTINCTCOUNTTHETASKETCH`.

---

## Pre-Built Sketch Columns in Pinot (Advanced)

If you want to pre-compute sketches at ingest time to speed up queries, Pinot supports
this via `aggregateMetrics` and sketch column types in batch ingestion.

To pre-build a Theta sketch during segment creation:

```json
{
  "tableIndexConfig": {
    "aggregateMetrics": true
  }
}
```

And in the schema, declare the sketch column:

```json
{
  "name": "unique_users_sketch",
  "dataType": "BYTES",
  "transformFunction": "distinctCountThetaSketch(user_id)"
}
```

Note: This requires Pinot 0.12+ and specific configuration. Consult your Pinot version's
ingestion documentation for the exact syntax.

---

## Handling `quantilesDoublesSketch` and `momentSketch`

These are Druid-specific sketch types for percentile/quantile computation:

| Druid type | Migration strategy |
|-----------|-------------------|
| `quantilesDoublesSketch` | Re-ingest raw values; use `PERCENTILETDIGEST` or `PERCENTILEEST` at query time |
| `momentSketch` | Re-ingest raw values; use `PERCENTILETDIGEST` |
| `fixedBucketsHistogram` | Re-ingest raw values; use histogram functions if available, else compute manually |

Druid's `quantilesDoublesSketch` provides exact quantiles from pre-built sketches.
Pinot's `PERCENTILETDIGEST` uses TDigest (approximate) and `PERCENTILEEST` uses KLL sketch.
The results will be approximately equal for a large sample but may differ by a few percent.

---

## Migrating the Query Layer

| Druid function | Pinot equivalent |
|---------------|-----------------|
| `APPROX_COUNT_DISTINCT(user_id)` | `DISTINCTCOUNTHLL(user_id)` |
| `DS_THETA(unique_users, 0.5)` | `DISTINCTCOUNTTHETASKETCH(user_id)` (from raw) |
| `DS_QUANTILES_SKETCH(latency_sketch, 0.99)` | `PERCENTILETDIGEST(latency_ms, 99)` (from raw) |
| `THETASKETCH_INTERSECT(sketch1, sketch2)` | Not directly available; use `DISTINCTCOUNTTHETASKETCH` with filters |

---

## Confidence Score Impact

A BLOCKING risk reduces confidence by 0.30 and a HIGH risk by 0.15:

```
Initial score: 1.00
BLOCKING:       -0.30  (APPROX_AGGREGATOR_MISMATCH)
HIGH:           -0.15  (UNSUPPORTED_COMPLEX_FIELD)
Final:           0.55
```

A confidence of 0.55 correctly signals that this migration requires significant manual work.

---

## See Also

- [Tutorial 03 — Rolled-Up Metrics](03-rolled-up-metrics.md) — migrating simpler SUM/COUNT metrics
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — full risk scoring explanation
- [Tutorial 18 — Production Checklist](18-production-checklist.md) — verification before cutover
