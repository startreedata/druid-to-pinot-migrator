# Reference: Type Mapping

Complete mapping of Druid types to Pinot types used by the migration tool.

---

## Dimension / Column Types

| Druid `type` | Pinot `dataType` | Notes |
|-------------|-----------------|-------|
| `string` (default) | `STRING` | |
| `long` | `LONG` | 64-bit signed integer |
| `float` | `FLOAT` | 32-bit IEEE 754 |
| `double` | `DOUBLE` | 64-bit IEEE 754 |
| `complex` | `BYTES` | Triggers HIGH risk; placeholder only |
| `hyperUnique` | `BYTES` | Triggers BLOCKING risk |
| `thetaSketch` | `BYTES` | Triggers BLOCKING risk |
| `HLLSketchBuild` | `BYTES` | Triggers BLOCKING risk |
| `HLLSketchMerge` | `BYTES` | Triggers BLOCKING risk |
| `quantilesDoublesSketch` | `BYTES` | Triggers BLOCKING risk |
| `momentSketch` | `BYTES` | Triggers BLOCKING risk |
| `fixedBucketsHistogram` | `BYTES` | Triggers BLOCKING risk |
| `mv_enum` | `STRING`, `singleValueField: false` | Multi-value handling |

Multi-value dimensions (any type with `multiValueHandling` set) receive
`"singleValueField": false` in the Pinot schema regardless of the base type.

---

## Metric (Aggregation) Types

| Druid `type` | Pinot `dataType` | Pinot aggregation function | Notes |
|-------------|-----------------|---------------------------|-------|
| `count` | `LONG` | `COUNT` | |
| `longSum` | `LONG` | `SUM` | |
| `longMin` | `LONG` | `MIN` | |
| `longMax` | `LONG` | `MAX` | |
| `doubleSum` | `DOUBLE` | `SUM` | |
| `doubleMin` | `DOUBLE` | `MIN` | |
| `doubleMax` | `DOUBLE` | `MAX` | |
| `floatSum` | `DOUBLE` | `SUM` | Druid float → Pinot DOUBLE |
| `floatMin` | `DOUBLE` | `MIN` | Druid float → Pinot DOUBLE |
| `floatMax` | `DOUBLE` | `MAX` | Druid float → Pinot DOUBLE |
| `thetaSketch` | `BYTES` | `DISTINCTCOUNTTHETASKETCH` | Incompatible binary format; re-ingest required |
| `HLLSketchBuild` | `BYTES` | `DISTINCTCOUNTHLL` | Incompatible binary format; re-ingest required |
| `HLLSketchMerge` | `BYTES` | `DISTINCTCOUNTHLL` | Incompatible binary format; re-ingest required |
| `hyperUnique` | `BYTES` | `DISTINCTCOUNTHLL` | Incompatible binary format; re-ingest required |
| `quantilesDoublesSketch` | `BYTES` | `PERCENTILEEST` | Incompatible binary format; re-ingest required |
| `momentSketch` | `BYTES` | `PERCENTILEEST` | Incompatible binary format; re-ingest required |
| `fixedBucketsHistogram` | `BYTES` | `HISTOGRAM` | Incompatible binary format; re-ingest required |
| *(unknown type)* | `DOUBLE` | `SUM` | Fallback for unrecognised types |

---

## Timestamp Formats

| Druid `format` | Pinot `format` | Pinot `granularity` | Data example |
|---------------|---------------|-------------------|--------------|
| `millis` | `1:MILLISECONDS:EPOCH` | `1:MILLISECONDS` | `1709251200000` |
| `auto` | `1:MILLISECONDS:EPOCH` | `1:MILLISECONDS` | (epoch ms or ISO, auto-detected) |
| `seconds` | `1:SECONDS:EPOCH` | `1:SECONDS` | `1709251200` |
| `posix` | `1:SECONDS:EPOCH` | `1:SECONDS` | `1709251200` |
| `micro` | `1:MICROSECONDS:EPOCH` | `1:MICROSECONDS` | `1709251200000000` |
| `nano` | `1:NANOSECONDS:EPOCH` | `1:NANOSECONDS` | `1709251200000000000` |
| `iso` | `1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss.SSSZ` | `1:MILLISECONDS` | `2024-03-01T00:00:00.000+0000` |
| *(any other string)* | `1:MILLISECONDS:SIMPLE_DATE_FORMAT:{format}` | `1:MILLISECONDS` | Triggers CUSTOM_TIMESTAMP_FORMAT risk |

---

## Source Kind Detection

| Condition | Source kind |
|-----------|------------|
| `ioConfig.type` contains `kafka` or `kinesis` | `stream` |
| `ioConfig.type` is `kinesis` | `stream` + STREAM_SOURCE_MISMATCH risk |
| All other ioConfig types | `batch` |

---

## Table Type Selection

| Source kind | Generated table type |
|------------|---------------------|
| `batch` | `OFFLINE` (`table-offline.json`) |
| `stream` | `REALTIME` (`table-realtime.json`) |

---

## Classification Rules

| Condition | Classification |
|-----------|---------------|
| Any sketch metric type | `complex_aggregated` |
| Any metric with `pinot_type == "BYTES"` | `complex_aggregated` |
| `rollup=true` + all simple additive metric types | `rolled_up_additive` |
| `rollup=true` + non-additive non-sketch types | `complex_aggregated` |
| `rollup=false` + no metrics | `raw_event` |
| `rollup=false` + all simple metric types | `raw_event` |

Simple additive types: `count`, `longSum`, `doubleSum`, `floatSum`, `floatMin`, `floatMax`,
`longMin`, `longMax`, `doubleMin`, `doubleMax`.
