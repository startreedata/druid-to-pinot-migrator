# Tutorial 03 — Migrating a Rolled-Up Metrics Table

**Pattern:** `rollup: true`, count + longSum + doubleSum metrics  
**Pinot table type:** OFFLINE  
**Classification:** `rolled_up_additive`  
**Risk:** `ROLLUP_SEMANTIC_MISMATCH` (HIGH)  
**Typical use cases:** ad impression/click aggregates, daily revenue summaries, IoT sensor rollups

---

## The Core Challenge

Druid rollup **pre-aggregates rows at ingestion time**. Two events with the same timestamp
bucket and dimension values are merged into one row, with metrics summed.

Pinot supports rollup-on-merge but its **query semantics differ in important ways**:
- `COUNT(*)` in Pinot counts **segment rows**, not original events
- The column that carries the event count is a named metric (e.g., `impressions`), not `COUNT(*)`
- `SUM(impressions)` is the correct way to count original events

Understanding this distinction is the most important thing to internalise before migrating
a rolled-up table.

---

## Sample Druid Spec

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "ad_metrics",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec": {
        "dimensions": ["campaign_id", "ad_group_id", "country"]
      },
      "metricsSpec": [
        {"type": "count",     "name": "impressions"},
        {"type": "longSum",   "name": "clicks",   "fieldName": "click_count"},
        {"type": "doubleSum", "name": "revenue",  "fieldName": "revenue_usd"}
      ],
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": "HOUR",
        "rollup": true,
        "intervals": ["2024-01-01/2025-01-01"]
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "uris": ["s3://data-bucket/ad_metrics/dt=*/data.json"]
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

Key things to note:
- `rollup: true` with `queryGranularity: "HOUR"` — Druid merges rows that share the same
  truncated-to-hour timestamp and the same `(campaign_id, ad_group_id, country)` tuple.
- The `count` metric named `impressions` stores the original row count per merged group.
- `clicks` stores the sum of the source field `click_count`.
- The source field `click_count` does **not** appear in Druid after rollup — only `clicks` does.

---

## Running the Migration

```bash
dpm generate ad_metrics_spec.json --out ./output/ad_metrics
```

You will see a risk in the output:

```
Risks detected: 1
  [HIGH] ROLLUP_SEMANTIC_MISMATCH
    Druid roll-up pre-aggregates rows at ingestion time. COUNT(*) in Pinot will
    return segment row count, not the original event count.
    Remediation: Validate that COUNT(*) and SUM() results match expected values
    against a reference dataset.
```

---

## What Gets Generated

### schema.json

```json
{
  "schemaName": "ad_metrics",
  "dimensionFieldSpecs": [
    {"name": "ad_group_id", "dataType": "STRING"},
    {"name": "campaign_id", "dataType": "STRING"},
    {"name": "country",     "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "clicks",      "dataType": "LONG"},
    {"name": "impressions", "dataType": "LONG"},
    {"name": "revenue",     "dataType": "DOUBLE"}
  ],
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

Note: metric names come from the `name` field in `metricsSpec`, not from `fieldName`.
This is critical for data ingestion: the column you write into Pinot must use the
metric name (`clicks`), not the source field name (`click_count`).

---

## The Metric Name vs. fieldName Distinction

This is the most common source of ingestion errors for rolled-up tables.

| Druid metricsSpec | Source field | Stored column | Pinot schema column |
|-------------------|-------------|---------------|---------------------|
| `{"type": "count", "name": "impressions"}` | *(event count)* | `impressions` | `impressions` (LONG) |
| `{"type": "longSum", "name": "clicks", "fieldName": "click_count"}` | `click_count` | `clicks` | `clicks` (LONG) |
| `{"type": "doubleSum", "name": "revenue", "fieldName": "revenue_usd"}` | `revenue_usd` | `revenue` | `revenue` (DOUBLE) |

**When ingesting into Pinot**, your records must use the stored column names:

```json
{"timestamp": 1709251200000, "campaign_id": "spring_sale", "country": "US",
 "impressions": 1, "clicks": 10, "revenue": 25.50}
```

Not:

```json
{"timestamp": 1709251200000, "campaign_id": "spring_sale", "country": "US",
 "click_count": 10, "revenue_usd": 25.50}
```

---

## Query Translation

### Total event count

```sql
-- Druid: COUNT(*) returns original event count because of rollup
SELECT COUNT(*) AS total_events FROM "ad_metrics"

-- Pinot: COUNT(*) returns row count (NOT original events for rolled-up data)
-- Use SUM of the count metric instead:
SELECT SUM(impressions) AS total_events FROM ad_metrics_OFFLINE
```

### Total clicks

```sql
-- Druid:
SELECT SUM(clicks) AS total_clicks FROM "ad_metrics"

-- Pinot (same):
SELECT SUM(clicks) AS total_clicks FROM ad_metrics_OFFLINE
```

### Revenue by country

```sql
-- Druid:
SELECT country, SUM(revenue) AS rev
FROM "ad_metrics"
GROUP BY country
ORDER BY country

-- Pinot (same):
SELECT country, SUM(revenue) AS rev
FROM ad_metrics_OFFLINE
GROUP BY country
ORDER BY country
```

### Clicks per campaign

```sql
-- Druid:
SELECT campaign_id, SUM(clicks) AS clicks
FROM "ad_metrics"
GROUP BY campaign_id
ORDER BY SUM(clicks) DESC

-- Pinot (same):
SELECT campaign_id, SUM(clicks) AS clicks
FROM ad_metrics_OFFLINE
GROUP BY campaign_id
ORDER BY SUM(clicks) DESC
```

The `SUM` queries are portable because they are invariant to how many rows Druid merged.
`COUNT(*)` queries are **not portable** for rolled-up data.

---

## Validating the Migration

Run queries that must produce identical results:

```sql
-- This must match:
SELECT SUM(impressions) FROM "ad_metrics"        -- Druid
SELECT SUM(impressions) FROM ad_metrics_OFFLINE  -- Pinot

-- This must match:
SELECT SUM(clicks) FROM "ad_metrics"             -- Druid
SELECT SUM(clicks) FROM ad_metrics_OFFLINE       -- Pinot

-- This must match:
SELECT SUM(revenue) FROM "ad_metrics"            -- Druid
SELECT SUM(revenue) FROM ad_metrics_OFFLINE      -- Pinot
```

```sql
-- This will NOT match if there was any rollup merging:
SELECT COUNT(*) FROM "ad_metrics"                -- Druid: original events
SELECT COUNT(*) FROM ad_metrics_OFFLINE          -- Pinot: segment rows
```

If you need to validate `COUNT(*)` equivalence, check whether Druid actually merged any rows:
```sql
-- Druid: if any timestamp+dimension combination had >1 event, these will differ
SELECT COUNT(*) FROM "ad_metrics"
SELECT SUM(impressions) FROM "ad_metrics"
```

If they are equal, no rows were merged and `COUNT(*)` is portable.

---

## Pinot Rollup-on-Merge (Optional)

Pinot supports rollup at segment merge time via `tableIndexConfig.aggregateMetrics`.
This can reduce storage but is **not required** for query correctness — raw rows can be
stored and queries use the aggregation functions regardless.

```json
"tableIndexConfig": {
  "aggregateMetrics": true
}
```

Enable this only if storage reduction is a priority and you have validated query parity first.

---

## Production Tuning Checklist

- [ ] Rename `click_count` → `clicks` (or whatever the metric names are) in your ETL pipeline
- [ ] Verify `SUM(impressions)` in Pinot equals `COUNT(*)` in Druid for the reference period
- [ ] Verify `SUM(revenue)` totals match within acceptable tolerance
- [ ] Verify `GROUP BY` queries on all key dimensions match
- [ ] Update all dashboards and queries to use Pinot metric names
- [ ] Set production replication to 3
- [ ] Tune retention to match Druid's data lifecycle

---

## See Also

- [Tutorial 07 — Min/Max Metrics](07-minmax-metrics.md) — when your rollup includes MIN/MAX aggregators
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — full explanation of ROLLUP_SEMANTIC_MISMATCH
- [Tutorial 17 — Validating the Migration](17-validation.md) — systematic parity verification
