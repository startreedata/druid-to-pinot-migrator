# Reference: Risk Taxonomy

All risk IDs, severity levels, triggers, and remediations in one place.

---

## Quick Reference Table

| Risk ID | Severity | Confidence | Score penalty | Tutorial |
|---------|----------|-----------|--------------|---------|
| `APPROX_AGGREGATOR_MISMATCH` | blocking | certain | -0.30 | [Tutorial 12](../12-sketch-aggregators.md) |
| `ROLLUP_SEMANTIC_MISMATCH` | high | certain | -0.15 | [Tutorial 03](../03-rolled-up-metrics.md) |
| `UNSUPPORTED_COMPLEX_FIELD` | high | certain | -0.15 | [Tutorial 12](../12-sketch-aggregators.md) |
| `FLATTEN_SPEC_NOT_PORTABLE` | high | certain | -0.15 | [Tutorial 10](../10-nested-json.md) |
| `TRANSFORM_PORTABILITY_RISK` | medium | likely | -0.05 | [Tutorial 09](../09-transforms.md) |
| `MULTIVALUE_AMBIGUITY` | medium | likely | -0.05 | [Tutorial 08](../08-multivalue-dimensions.md) |
| `PARTITIONING_CONFIG_REQUIRED` | medium | certain | -0.05 | [Tutorial 13](../13-partitioned-tables.md) |
| `CUSTOM_TIMESTAMP_FORMAT` | medium | likely | -0.05 | [Tutorial 11](../11-custom-timestamps.md) |
| `TIME_SEMANTICS_MISMATCH` | low | possible | -0.01 | [Tutorial 11](../11-custom-timestamps.md) |
| `INGESTION_BEHAVIOR_MISMATCH` | info | certain | -0.00 | [Tutorial 14](../14-append-mode.md) |
| `NULL_DEFAULT_MISMATCH` | info | — | -0.00 | (not currently triggered) |
| `RETENTION_GRANULARITY_MISMATCH` | info | — | -0.00 | (not currently triggered) |
| `QUERY_COMPATIBILITY_RISK` | medium | — | -0.05 | (not currently triggered) |
| `INDEX_ADVISORY_ONLY` | info | — | -0.00 | (not currently triggered) |

---

## APPROX_AGGREGATOR_MISMATCH

**Severity:** BLOCKING  
**Confidence:** CERTAIN  
**Score penalty:** -0.30 per occurrence  

**What triggers it:**  
Any metric in `metricsSpec` with type: `thetaSketch`, `HLLSketchBuild`, `HLLSketchMerge`,
`hyperUnique`, `quantilesDoublesSketch`, `momentSketch`, `fixedBucketsHistogram`.

**Why it matters:**  
Druid serialises sketch aggregators as opaque binary blobs using Druid's internal sketch
libraries. Pinot has its own HLL and Theta sketch implementations but the binary wire
formats are incompatible. Loading Druid sketch bytes into Pinot's sketch columns produces
garbage results or errors.

**Evidence format:**  
`Complex aggregators found: {metric_name_1}, {metric_name_2}`

**Remediation:**  
1. Identify the source fields (`fieldName` in each sketch metric).
2. Re-ingest raw event data into Pinot (not Druid's pre-aggregated segments).
3. Define `DISTINCTCOUNTHLL(field)` or `DISTINCTCOUNTTHETASKETCH(field)` queries.
4. For quantile queries: use `PERCENTILETDIGEST(field, percentile)`.

---

## ROLLUP_SEMANTIC_MISMATCH

**Severity:** HIGH  
**Confidence:** CERTAIN  
**Score penalty:** -0.15  

**What triggers it:**  
`granularitySpec.rollup: true`.

**Why it matters:**  
After Druid rollup, `COUNT(*)` returns the number of merged rows (post-aggregation),
not the original event count. In Pinot, `COUNT(*)` returns segment rows, which also equals
post-aggregation rows. However, applications that use `COUNT(*)` expecting original event
counts will get different results depending on how much merging occurred.

Additionally, Druid's `queryGranularity` controls the time bucket for merging. Pinot does
not apply rollup at ingest time in the same way — all rows are stored as-is unless
`aggregateMetrics: true` is configured.

**Evidence format:**  
`rollup=True in granularitySpec`, `queryGranularity={value}`

**Remediation:**  
Replace `COUNT(*)` with `SUM(count_metric_name)` everywhere in queries and dashboards.
Validate all aggregate queries return identical results to Druid for a reference period.

---

## UNSUPPORTED_COMPLEX_FIELD

**Severity:** HIGH  
**Confidence:** CERTAIN  
**Score penalty:** -0.15  

**What triggers it:**  
Any metric where `pinot_type == "BYTES"` (which happens for all complex/sketch aggregator
types that are not covered by a specific Pinot equivalent).

**Why it matters:**  
The generated schema has `"dataType": "BYTES"` for these fields. Querying a `BYTES` column
as a sketch or as a normal numeric column will produce incorrect or null results.

**Evidence format:**  
`BYTES-type fields: {field_name_1}, {field_name_2}`

**Remediation:**  
Replace the BYTES columns with the raw source fields and redesign aggregation queries.
Follow the APPROX_AGGREGATOR_MISMATCH remediation path.

---

## FLATTEN_SPEC_NOT_PORTABLE

**Severity:** HIGH  
**Confidence:** CERTAIN  
**Score penalty:** -0.15  

**What triggers it:**  
`inputFormat.flattenSpec` is present in the ioConfig.

**Why it matters:**  
Druid's `flattenSpec` performs JSONPath or `jq`-style field extraction at ingest time.
Pinot has no equivalent mechanism. Without implementing the extraction, the nested fields
will not be available as columns.

**Evidence format:**  
`flattenSpec with path expressions detected in inputFormat`

**Remediation:**  
1. Pre-flatten the JSON upstream in your ETL pipeline.
2. Or configure Pinot ingestion `transformFunctionSpec` with `jsonPath()` function.
3. `jq`-style transforms must be implemented upstream.

---

## TRANSFORM_PORTABILITY_RISK

**Severity:** MEDIUM  
**Confidence:** LIKELY  
**Score penalty:** -0.05  

**What triggers it:**  
One or more transforms in `transformSpec.transforms` with expressions matching patterns:
`case`, `if(`, `coalesce`, `concat`, `nvl`, `regexp`, `timestamp_parse`, `unix_timestamp`,
`->` (JSON path), `[` (array/JSON index).

**Why it matters:**  
Druid's expression language evaluates these at ingest time. Pinot has no built-in equivalent
at ingest time. The derived columns will be missing from ingested data unless the logic is
moved upstream.

**Evidence format:**  
`Non-trivial transforms: {name_1}, {name_2}`

**Remediation:**  
1. Move transform logic to an upstream ETL pipeline.
2. Or use Pinot's `transformFunctionSpec` for simple cases.
3. Query-time computation (`REGEXP_EXTRACT`, `CASE WHEN` in SQL) is viable for low-volume
   queries but expensive for large scans.

---

## MULTIVALUE_AMBIGUITY

**Severity:** MEDIUM  
**Confidence:** LIKELY  
**Score penalty:** -0.05  

**What triggers it:**  
One or more dimensions with `multiValueHandling` set, or with `type: "mv_enum"`.

**Why it matters:**  
MV columns have different behaviours in edge cases: null handling, empty arrays,
correlated multi-column GROUP BY, and certain aggregation functions.

**Evidence format:**  
`Multi-value dimensions: {dim_name_1}, {dim_name_2}`

**Remediation:**  
Set `singleValueField: false` in the Pinot schema (done automatically by the tool).
Run the full GROUP BY and DISTINCTCOUNT validation suite after migration.

---

## PARTITIONING_CONFIG_REQUIRED

**Severity:** MEDIUM  
**Confidence:** CERTAIN  
**Score penalty:** -0.05  

**What triggers it:**  
`tuningConfig.partitionsSpec` is present in the Druid spec.

**Why it matters:**  
The generated table config does not include partition configuration. The table deploys
correctly but queries that would benefit from partition pruning will scan all segments.

**Evidence format:**  
`Druid partitionsSpec type='{type}' detected in tuningConfig`

**Remediation:**  
Add `segmentPartitionConfig` to the table config. Match `numPartitions` to Druid's
`numShards`. Use `Murmur` for hash or `BoundedColumnValue` for range.

---

## CUSTOM_TIMESTAMP_FORMAT

**Severity:** MEDIUM  
**Confidence:** LIKELY  
**Score penalty:** -0.05  

**What triggers it:**  
`timestampSpec.format` is not one of: `auto`, `iso`, `millis`, `seconds`, `posix`,
`micro`, `nano`, `milliseconds`.

**Why it matters:**  
The tool generates a best-effort `SIMPLE_DATE_FORMAT` mapping. If the pattern is invalid
or locale-sensitive, timestamps may parse as epoch 0 or fail.

**Evidence format:**  
`Column '{col}' uses format '{format}'`

**Remediation:**  
Verify the generated `dateTimeFieldSpec.format` in `schema.json`. Test with a sample
value. If unreliable, pre-convert to epoch milliseconds upstream.

---

## TIME_SEMANTICS_MISMATCH

**Severity:** LOW  
**Confidence:** POSSIBLE  
**Score penalty:** -0.01  

**What triggers it:**  
Timestamp format is one of: `posix`, `auto`, `custom`, `ruby`.

**Why it matters:**  
These formats have edge cases in timezone handling or sub-second precision across systems.

**Remediation:**  
Verify the `dateTimeFieldSpec` format produces correct epoch values for your data.

---

## INGESTION_BEHAVIOR_MISMATCH

**Severity:** INFO  
**Confidence:** CERTAIN  
**Score penalty:** 0.00  

**What triggers it:**  
`ioConfig.appendToExisting: true`.

**Why it matters:**  
Advisory: the generated config uses `APPEND` ingestion type (correct equivalent), but
Pinot's segment management (compaction, deduplication) may differ from Druid's.

**Remediation:**  
Review Pinot segment compaction and upsert documentation for your specific use case.

---

## Risks Not Currently Triggered

These risk IDs exist in the taxonomy but are not triggered by the current analyzer:

| Risk ID | When it would fire |
|---------|-------------------|
| `NULL_DEFAULT_MISMATCH` | When null handling config differs |
| `RETENTION_GRANULARITY_MISMATCH` | When Druid retention rules are detected |
| `QUERY_COMPATIBILITY_RISK` | When specific incompatible SQL patterns detected |
| `INDEX_ADVISORY_ONLY` | Advisory on all migrations (currently not emitted) |
