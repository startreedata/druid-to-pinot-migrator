# Tutorial 17 — Validating the Migration

After generating Pinot artifacts, `dpm validate` runs a suite of checks to verify that
the generated files are consistent, complete, and ready to deploy.

---

## Two Phases of Validation

Validation runs in two phases:

1. **Static checks** — Run against the canonical model (parsed from the Druid spec).
   These do not require generated files.

2. **Artifact checks** — Run against the generated `schema.json` and `table-*.json`.
   These require `--generated-dir`.

---

## Running Validation

### Phase 1 only (static checks)

```bash
dpm validate path/to/druid-spec.json
```

### Phase 1 + Phase 2 (full validation)

```bash
dpm validate path/to/druid-spec.json --generated-dir ./output/my_table
```

### JSON output

```bash
dpm validate path/to/druid-spec.json --generated-dir ./output/my_table --json
```

---

## Static Checks

These run regardless of whether `--generated-dir` is provided:

| Check ID | What it verifies | Fail condition |
|----------|-----------------|----------------|
| `static.datasource_name_present` | `datasource_name` is non-empty | Missing or empty datasource name |
| `static.time_field_present` | A time field was extracted | No `timestampSpec` found or empty `column` |
| `static.field_names_unique` | No two columns share a name across dimensions + metrics | Duplicate field name detected |
| `static.metric_names_valid` | All metric entries have non-empty names | A metric with empty `name` |
| `static.classification_assigned` | Classification is not `unknown` | Spec could not be classified |

---

## Artifact Checks

These run only when `--generated-dir` is specified:

| Check ID | What it verifies | Fail condition |
|----------|-----------------|----------------|
| `artifact.schema_has_datetime` | `schema.json` has at least one `dateTimeFieldSpec` | No datetime field in schema |
| `artifact.schema_no_duplicate_fields` | No duplicate names across all field specs | Duplicate detected in generated schema |
| `artifact.table_type_valid` | `tableType` is `OFFLINE` or `REALTIME` | Invalid or missing table type |
| `artifact.time_column_match` | Schema time column matches `segmentsConfig.timeColumnName` | Mismatch between schema and table |
| `artifact.realtime_has_stream_configs` | REALTIME tables have `streamConfigs` | Missing stream configuration |

---

## Example: Passing Validation

```
$ dpm validate pageviews_spec.json --generated-dir ./output/pageviews

datasource     : pageviews
overall_status : pass
confidence     : 1.00

Checks (7):
  ✓ static.datasource_name_present
  ✓ static.time_field_present
  ✓ static.field_names_unique
  ✓ static.metric_names_valid
  ✓ static.classification_assigned
  ✓ artifact.schema_has_datetime
  ✓ artifact.schema_no_duplicate_fields
  ✓ artifact.table_type_valid
  ✓ artifact.time_column_match
```

---

## Example: Failing Validation

```
$ dpm validate broken_spec.json --generated-dir ./output/broken

datasource     : broken_table
overall_status : fail
confidence     : 0.70

Checks (7):
  ✓ static.datasource_name_present
  ✓ static.time_field_present
  ✗ static.field_names_unique
      Duplicate field name 'user_id' in dimensions and metrics
  ✓ static.metric_names_valid
  ✓ static.classification_assigned
  ✓ artifact.schema_has_datetime
  ✗ artifact.time_column_match
      Schema time column 'event_ts' != table timeColumnName 'timestamp'
```

---

## What the Checks Don't Cover

These checks verify structural correctness of the generated artifacts. They do **not**:

- Verify data parity between Druid and Pinot (use query comparison for that)
- Validate that your source data format matches the schema
- Check Pinot cluster connectivity or table deployment success
- Verify that index configurations are optimal

---

## Using Validation in CI

```yaml
# Example GitHub Actions step
- name: Validate migration artifacts
  run: |
    dpm generate "${{ inputs.spec_path }}" --out ./pinot-artifacts
    dpm validate "${{ inputs.spec_path }}" \
      --generated-dir ./pinot-artifacts \
      --json > validation-report.json
    
    # Fail if overall_status is not 'pass'
    STATUS=$(jq -r '.overall_status' validation-report.json)
    if [ "$STATUS" != "pass" ]; then
      echo "Validation failed: $STATUS"
      jq '.checks | map(select(.status == "fail"))' validation-report.json
      exit 1
    fi
```

---

## Validation Report JSON Structure

```json
{
  "datasource_name": "pageviews",
  "overall_status": "pass",
  "confidence_score": 1.0,
  "checks": [
    {
      "check_id": "static.datasource_name_present",
      "status": "pass",
      "message": "datasource_name='pageviews'"
    },
    {
      "check_id": "artifact.time_column_match",
      "status": "pass",
      "message": "Schema and table agree on time column 'timestamp'"
    }
  ]
}
```

Status values: `pass`, `warn`, `fail`.

Overall status:
- `fail` if any check has status `fail`
- `warn` if all checks pass but at least one is `warn`
- `pass` if all checks pass

---

## Fixing Common Validation Failures

### `artifact.time_column_match`

The time column name in the schema and the table config must match exactly.

```json
// schema.json:
"dateTimeFieldSpecs": [{"name": "event_ts", ...}]

// table-offline.json:
"segmentsConfig": {"timeColumnName": "timestamp"}  // MISMATCH
```

Fix: make sure your `timestampSpec.column` value is consistent, then regenerate. Or
manually update one of the two files.

### `static.field_names_unique`

A dimension and a metric share the same name. This commonly happens when a field is
both a dimension in `dimensionsSpec` and a metric in `metricsSpec`.

```json
"dimensionsSpec": {"dimensions": ["user_id", "event_count"]},
"metricsSpec": [{"type": "count", "name": "event_count"}]
```

Fix: rename either the dimension or the metric. Usually, the metric (`event_count`) should
be removed from `dimensionsSpec` since it is a pre-aggregation output, not a raw dimension.

### `static.classification_assigned`

The spec could not be classified — it has rollup=true with aggregator types that are
neither simple additive nor complex sketches. Check `canonical.json` for the exact
metric types that caused the unknown classification.

### `artifact.realtime_has_stream_configs`

The tool generated a REALTIME table but could not extract stream configuration from
the Druid ioConfig. This can happen if the Druid spec uses an unusual streaming format.

Fix: manually add `streamConfigs` to the generated `table-realtime.json` following
the pattern in [Tutorial 04 — Kafka Streaming](04-kafka-streaming.md).

---

## Data Parity Verification (Beyond the Tool)

After structural validation passes, verify data correctness by comparing query results:

```python
import requests

def druid_count(datasource):
    r = requests.post("http://druid-broker:8082/druid/v2/sql",
                      json={"query": f'SELECT COUNT(*) AS cnt FROM "{datasource}"'})
    return r.json()[0]["cnt"]

def pinot_count(table):
    r = requests.post("http://pinot-broker:8099/query/sql",
                      json={"sql": f"SELECT COUNT(*) AS cnt FROM {table}_OFFLINE"})
    return r.json()["resultTable"]["rows"][0][0]

druid_cnt = druid_count("pageviews")
pinot_cnt = pinot_count("pageviews")

assert druid_cnt == pinot_cnt, \
    f"Row count mismatch: Druid={druid_cnt}, Pinot={pinot_cnt}"
```

---

## See Also

- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md)
- [Tutorial 18 — Production Checklist](18-production-checklist.md)
- [Reference: CLI](reference/cli.md) — validate command options
