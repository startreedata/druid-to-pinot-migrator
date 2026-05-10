# Tutorial 16 — Understanding Risks and Confidence Scores

This tutorial explains how the tool's risk analysis system works, what each risk means,
and how to interpret and act on confidence scores.

---

## The Risk Model

Every migration analysis produces a list of **risk annotations**. Each annotation has:

| Field | Description |
|-------|-------------|
| `risk_id` | A unique identifier (e.g., `ROLLUP_SEMANTIC_MISMATCH`) |
| `severity` | How bad the problem is: `blocking`, `high`, `medium`, `low`, `info` |
| `confidence` | How certain we are this risk applies: `certain`, `likely`, `possible` |
| `description` | What the risk is and why it matters |
| `evidence` | What was found in the spec that triggered this risk |
| `remediation` | Concrete steps to address the risk |

---

## Severity Levels

| Severity | Meaning | Action required |
|---------|---------|----------------|
| `blocking` | Migration is not possible without significant rework | **Stop. Address before proceeding.** |
| `high` | Migration will produce incorrect results or require major manual work | Address before production cutover |
| `medium` | Migration may produce subtly different behaviour; manual verification needed | Verify during testing |
| `low` | Minor differences; unlikely to affect most workloads | Review and accept if appropriate |
| `info` | Advisory notice; no direct correctness issue | Review for operational awareness |

---

## Confidence Score

The confidence score starts at 1.0 and is penalised for each risk:

```
score = 1.0
      - 0.30 × (number of BLOCKING risks)
      - 0.15 × (number of HIGH risks)
      - 0.05 × (number of MEDIUM risks)
      - 0.01 × (number of LOW risks)
score = max(0.0, min(1.0, score))
```

### Example scores

| Scenario | Risks | Score |
|---------|-------|-------|
| Clean raw event table | None | 1.00 |
| Rolled-up additive | 1 HIGH (ROLLUP_SEMANTIC_MISMATCH) | 0.85 |
| Transform portability | 1 MEDIUM (TRANSFORM_PORTABILITY_RISK) | 0.95 |
| Sketch aggregators | 1 BLOCKING + 1 HIGH | 0.55 |
| Complex + partitioned | 1 BLOCKING + 2 HIGH + 1 MEDIUM | 0.35 |

A score below 0.7 warrants careful review. A score below 0.5 indicates the migration
requires significant manual work and re-design.

---

## Complete Risk Reference

### APPROX_AGGREGATOR_MISMATCH — BLOCKING

**Trigger:** Any metric with type `thetaSketch`, `HLLSketchBuild`, `HLLSketchMerge`,
`hyperUnique`, `quantilesDoublesSketch`, `momentSketch`, or `fixedBucketsHistogram`.

**Why it blocks:** Druid stores these as opaque binary sketch blobs. The binary format
is incompatible with Pinot's own sketch implementations. You cannot deploy the Druid data
to Pinot and expect sketch queries to return correct results — the bytes mean different
things to each system.

**What to do:**
1. Identify the raw source field (from `fieldName` in the metric spec).
2. Re-ingest raw events into Pinot.
3. Define `DISTINCTCOUNTHLL(field)` or `DISTINCTCOUNTTHETASKETCH(field)` queries
   rather than reading a pre-built sketch column.

See [Tutorial 12 — Sketch Aggregators](12-sketch-aggregators.md) for the full strategy.

---

### ROLLUP_SEMANTIC_MISMATCH — HIGH

**Trigger:** `granularitySpec.rollup: true`.

**Why it's high:** Druid merges rows at ingest time. After rollup, `COUNT(*)` in Druid
returns the number of merged rows (which equals the number of original events because
rollup counts are stored in the count metric). In Pinot, `COUNT(*)` returns the number of
segment rows (after merge), which can be much lower than the original event count.

**What to do:**
1. Replace `COUNT(*) FROM table` with `SUM(impressions)` (or whatever your count metric is named).
2. Use `SUM(metric)` for all pre-aggregated values — not `AVG`, `COUNT`, or any aggregation
   that assumes one-event-per-row semantics.
3. Validate `SUM(count_metric)` in Pinot equals `COUNT(*)` in Druid for your reference period.

See [Tutorial 03 — Rolled-Up Metrics](03-rolled-up-metrics.md).

---

### UNSUPPORTED_COMPLEX_FIELD — HIGH

**Trigger:** Any metric or dimension where the Pinot mapped type is `BYTES` (i.e., a
complex Druid type with no direct Pinot equivalent).

**Why it's high:** The generated schema has `BYTES` as a placeholder. Deploying and
querying a `BYTES` column as if it contains a sketch will return garbage or errors.

**What to do:**
1. Review which columns have `"dataType": "BYTES"` in the generated `schema.json`.
2. Replace them with the appropriate raw field type (e.g., `STRING` for user IDs,
   `DOUBLE` for latency values).
3. If you need the approximate aggregation, follow the APPROX_AGGREGATOR_MISMATCH
   remediation (re-ingest raw events).

---

### FLATTEN_SPEC_NOT_PORTABLE — HIGH

**Trigger:** `flattenSpec` is present in `inputFormat`.

**Why it's high:** Druid's flattenSpec extracts nested JSON fields at ingest time. Pinot
has no equivalent. Without implementation, the nested fields will not be available as
columns in Pinot.

**What to do:**
1. Pre-flatten the JSON upstream using your ETL pipeline.
2. Or configure Pinot ingestion transform functions for simple `$.path.to.field` extractions.
3. `jq`-style transforms must be implemented upstream.

See [Tutorial 10 — Nested JSON](10-nested-json.md).

---

### TRANSFORM_PORTABILITY_RISK — MEDIUM

**Trigger:** One or more transforms in `transformSpec` use non-trivial expressions
(containing `case`, `if`, `regexp`, `concat`, `coalesce`, `->`, or `[`).

**Why it's medium:** The transforms are detected at the spec level. If they produce
columns that downstream queries depend on, missing transforms will cause incorrect results.
However, the correctness impact depends on whether those columns are actually queried.

**What to do:**
1. Identify which transform output columns are used in downstream queries.
2. Re-implement those transforms upstream or via Pinot ingestion transforms.
3. Simple renames can use `transformFunctionSpec` in the Pinot ingestion job.

See [Tutorial 09 — Transforms](09-transforms.md).

---

### MULTIVALUE_AMBIGUITY — MEDIUM

**Trigger:** One or more dimensions have `multiValueHandling` set.

**Why it's medium:** MV dimension query semantics (GROUP BY, COUNT DISTINCT) may differ
between Druid and Pinot in edge cases involving nulls, empty arrays, and correlated MV columns.

**What to do:**
1. Run the validation query suite against both systems after migration.
2. Pay special attention to `COUNT(DISTINCT mv_col)` and `GROUP BY mv_col` results.
3. Verify null/empty array handling matches your application's expectations.

See [Tutorial 08 — Multi-Value Dimensions](08-multivalue-dimensions.md).

---

### PARTITIONING_CONFIG_REQUIRED — MEDIUM

**Trigger:** `tuningConfig.partitionsSpec` is present (type: `hashed`, `range`, or `dynamic`).

**Why it's medium:** The generated table config does not include partition configuration.
The table will be deployed and queryable, but without partitioning, query performance may
be significantly worse for large tables with partition-key filters.

**What to do:**
1. Add `segmentPartitionConfig` to the generated table config.
2. Match `numPartitions` to the Druid `numShards` value.
3. Choose `Murmur` for hash partitioning or `BoundedColumnValue` for range.

See [Tutorial 13 — Partitioned Tables](13-partitioned-tables.md).

---

### CUSTOM_TIMESTAMP_FORMAT — MEDIUM

**Trigger:** `timestampSpec.format` is a non-standard string (not `auto`, `iso`, `millis`,
`seconds`, `posix`, `micro`, `nano`).

**Why it's medium:** The tool generates a best-effort `SIMPLE_DATE_FORMAT` pattern for Pinot.
If the pattern is invalid or locale-dependent, timestamps will parse as 0 or epoch.

**What to do:**
1. Verify the generated format string in `schema.json`.
2. Test with a sample value using Pinot's `DATETIMECONVERT` function.
3. If the format cannot be expressed in Pinot's `SIMPLE_DATE_FORMAT`, pre-convert
   timestamps to epoch milliseconds upstream.

See [Tutorial 11 — Custom Timestamps](11-custom-timestamps.md).

---

### TIME_SEMANTICS_MISMATCH — LOW

**Trigger:** The timestamp format is `posix`, `auto`, `custom`, or `ruby`.

**Why it's low:** These formats are handled by the tool but may have subtle differences
in timezone handling or sub-second precision between the two systems.

**What to do:**
Verify the `dateTimeFieldSpec` format in the generated schema matches the actual data.
Run a spot check on a few known timestamps to confirm epoch values are correct.

---

### INGESTION_BEHAVIOR_MISMATCH — INFO

**Trigger:** `ioConfig.appendToExisting: true`.

**Why it's info:** The generated table config uses `APPEND` ingestion type, which is the
correct equivalent. This risk is advisory — it reminds you to review Pinot's segment
compaction and deduplication behaviour, which differs from Druid's.

**What to do:**
1. Review Pinot's `MergeRollupTask` for compaction.
2. Verify whether your workload requires deduplication (upsert) or pure append.

See [Tutorial 14 — Append Mode](14-append-mode.md).

---

## Reading the risks.json File

After `dpm generate`, `reports/risks.json` contains the full structured risk data:

```json
{
  "risks": [
    {
      "risk_id": "ROLLUP_SEMANTIC_MISMATCH",
      "severity": "high",
      "confidence": "certain",
      "description": "Druid roll-up pre-aggregates rows at ingestion time...",
      "evidence": [
        "rollup=True in granularitySpec",
        "queryGranularity=HOUR"
      ],
      "remediation": "After migration, validate that COUNT(*) and SUM() results match..."
    }
  ]
}
```

You can process this programmatically to build migration dashboards or gate CI/CD pipelines:

```bash
# Check for blocking risks — fail if any exist
python3 -c "
import json, sys
risks = json.load(open('output/reports/risks.json'))['risks']
blocking = [r for r in risks if r['severity'] == 'blocking']
if blocking:
    print(f'FAIL: {len(blocking)} blocking risk(s)')
    for r in blocking:
        print(f'  {r[\"risk_id\"]}: {r[\"description\"][:100]}')
    sys.exit(1)
print('OK: no blocking risks')
"
```

---

## Confidence Score in CI

Use the confidence score as a migration gate in your CI/CD pipeline:

```bash
SCORE=$(dpm inspect spec.json --json | python3 -c "import sys,json; print(json.load(sys.stdin).get('confidence_score', 1.0))")
echo "Confidence: $SCORE"
if python3 -c "exit(0 if float('$SCORE') >= 0.8 else 1)"; then
  echo "Confidence OK — proceeding with migration"
else
  echo "Confidence too low — review risks before proceeding"
  exit 1
fi
```

---

## See Also

- [Tutorial 17 — Validating the Migration](17-validation.md) — post-generation checks
- [Tutorial 18 — Production Checklist](18-production-checklist.md) — full verification workflow
- [Reference: Risks](reference/risks.md) — machine-readable risk table
