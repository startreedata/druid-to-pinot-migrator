# Tutorial 07 — Min/Max Metrics

**Pattern:** `doubleMin`, `doubleMax`, `longMin`, `longMax`, `floatSum`, `floatMin`, `floatMax`  
**Pinot table type:** OFFLINE  
**Classification:** `raw_event` (without rollup) or `rolled_up_additive` (with rollup)  
**Typical use cases:** IoT sensor statistics, price range tracking, latency histograms

---

## The Pattern

Min/max metrics in Druid are pre-aggregators: for each time bucket and dimension group,
Druid stores the minimum and maximum observed value. They are used alongside `count` and
`sum` to compute range statistics without storing raw events.

Example: a `sensor_readings` table that tracks temperature extremes per sensor per day.

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "sensor_readings",
      "timestampSpec": {
        "column": "ts",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "sensor_id",
          "location",
          {"type": "double", "name": "temperature"},
          {"type": "double", "name": "humidity"}
        ]
      },
      "metricsSpec": [
        {"type": "count",     "name": "reading_count"},
        {"type": "doubleMin", "name": "temp_min",  "fieldName": "temperature"},
        {"type": "doubleMax", "name": "temp_max",  "fieldName": "temperature"},
        {"type": "doubleSum", "name": "temp_sum",  "fieldName": "temperature"},
        {"type": "doubleMin", "name": "hum_min",   "fieldName": "humidity"},
        {"type": "doubleMax", "name": "hum_max",   "fieldName": "humidity"}
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
        "prefixes": ["s3://iot-bucket/sensor_readings/"]
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

---

## Type Mapping for Min/Max Metrics

All min/max metric types map cleanly:

| Druid type | Pinot type | Aggregation |
|-----------|-----------|-------------|
| `doubleMin` | `DOUBLE` | `MIN` |
| `doubleMax` | `DOUBLE` | `MAX` |
| `doubleSum` | `DOUBLE` | `SUM` |
| `longMin` | `LONG` | `MIN` |
| `longMax` | `LONG` | `MAX` |
| `longSum` | `LONG` | `SUM` |
| `floatMin` | `DOUBLE` | `MIN` |
| `floatMax` | `DOUBLE` | `MAX` |
| `floatSum` | `DOUBLE` | `SUM` |

Note: `float*` types map to `DOUBLE` in Pinot (there is no `FLOAT` metric type in Pinot).
Storage will use more space than float, but arithmetic is safer.

---

## Running the Migration

```bash
dpm generate sensor_readings_spec.json --out ./output/sensor_readings
```

With `rollup: true` and only simple min/max aggregators, the classification will be
`rolled_up_additive` and the only risk will be `ROLLUP_SEMANTIC_MISMATCH` (HIGH):

```
Classification : rolled_up_additive
Risks: 1
  [HIGH] ROLLUP_SEMANTIC_MISMATCH
    ...COUNT(*) returns row count not original event count...
```

---

## What Gets Generated

### schema.json

```json
{
  "schemaName": "sensor_readings",
  "dimensionFieldSpecs": [
    {"name": "humidity",    "dataType": "DOUBLE"},
    {"name": "location",    "dataType": "STRING"},
    {"name": "sensor_id",   "dataType": "STRING"},
    {"name": "temperature", "dataType": "DOUBLE"}
  ],
  "metricFieldSpecs": [
    {"name": "hum_max",       "dataType": "DOUBLE"},
    {"name": "hum_min",       "dataType": "DOUBLE"},
    {"name": "reading_count", "dataType": "LONG"},
    {"name": "temp_max",      "dataType": "DOUBLE"},
    {"name": "temp_min",      "dataType": "DOUBLE"},
    {"name": "temp_sum",      "dataType": "DOUBLE"}
  ],
  "dateTimeFieldSpecs": [
    {
      "name": "ts",
      "dataType": "LONG",
      "format": "1:MILLISECONDS:EPOCH",
      "granularity": "1:MILLISECONDS"
    }
  ]
}
```

---

## Ingesting into Pinot

When ingesting rolled-up data into Pinot, the records must use the **metric names**, not
the source field names:

```json
{
  "ts": 1709251200000,
  "sensor_id": "T001",
  "location": "building-a",
  "temperature": 22.5,
  "humidity": 0.65,
  "reading_count": 1,
  "temp_min": 22.5,
  "temp_max": 22.5,
  "temp_sum": 22.5,
  "hum_min": 0.65,
  "hum_max": 0.65
}
```

For un-aggregated raw events (rollup=false), you just need the source field names and
Pinot will store them as-is.

---

## Query Translation

### Average temperature (derived from sum/count)

```sql
-- Druid:
SELECT sensor_id, SUM(temp_sum) / SUM(reading_count) AS avg_temp
FROM "sensor_readings"
GROUP BY sensor_id
ORDER BY sensor_id

-- Pinot (same):
SELECT sensor_id, SUM(temp_sum) / SUM(reading_count) AS avg_temp
FROM sensor_readings_OFFLINE
GROUP BY sensor_id
ORDER BY sensor_id
```

Note: `AVG(temperature)` would be incorrect for rolled-up data because each row has
already been pre-aggregated. Use `SUM(sum) / SUM(count)` for a true weighted average.

### Min/max temperature range

```sql
-- Druid:
SELECT sensor_id,
       MIN(temp_min) AS daily_low,
       MAX(temp_max) AS daily_high
FROM "sensor_readings"
WHERE location = 'building-a'
GROUP BY sensor_id
ORDER BY sensor_id

-- Pinot (same):
SELECT sensor_id,
       MIN(temp_min) AS daily_low,
       MAX(temp_max) AS daily_high
FROM sensor_readings_OFFLINE
WHERE location = 'building-a'
GROUP BY sensor_id
ORDER BY sensor_id
```

`MIN(temp_min)` applies `MIN` across rows that already hold per-bucket minimums —
this correctly computes the global minimum without storing raw events.

### Total readings with temperature extremes

```sql
-- Druid:
SELECT DATE_TRUNC('day', __time) AS day,
       SUM(reading_count) AS total_readings,
       MIN(temp_min) AS low,
       MAX(temp_max) AS high
FROM "sensor_readings"
GROUP BY DATE_TRUNC('day', __time)
ORDER BY day

-- Pinot:
SELECT DATETIMECONVERT(ts, '1:MILLISECONDS:EPOCH', '1:DAYS:EPOCH', '1:DAYS') AS day,
       SUM(reading_count) AS total_readings,
       MIN(temp_min) AS low,
       MAX(temp_max) AS high
FROM sensor_readings_OFFLINE
GROUP BY day
ORDER BY day
```

---

## Common Pitfall: AVG on Pre-Aggregated Data

This is a frequent bug when migrating min/max metric tables:

```sql
-- WRONG for rolled-up data:
SELECT sensor_id, AVG(temp_sum) AS avg_temp
FROM sensor_readings_OFFLINE
GROUP BY sensor_id

-- This computes the average of daily sums, not the average temperature.

-- CORRECT:
SELECT sensor_id, SUM(temp_sum) / SUM(reading_count) AS avg_temp
FROM sensor_readings_OFFLINE
GROUP BY sensor_id
```

The same caution applies to `AVG(temp_min)` — it would give the average of daily minimum
values, not the true global minimum. Use `MIN(temp_min)` instead.

---

## Float Metrics and Numeric Precision

`floatSum`, `floatMin`, `floatMax` all map to `DOUBLE` in Pinot. This means your data will
be stored with 64-bit precision even if Druid stored it at 32-bit. This is safe — you will
not lose precision going from float to double. Queries may return slightly different string
representations (e.g., `9.990000724792480` vs `9.99`) but arithmetic comparisons will be
within normal IEEE 754 double tolerance.

---

## See Also

- [Tutorial 03 — Rolled-Up Metrics](03-rolled-up-metrics.md) — rollup semantics and COUNT(*) caveats
- [Tutorial 06 — Typed Dimensions](06-typed-dimensions.md) — numeric dimension types
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — ROLLUP_SEMANTIC_MISMATCH
