# Reference: CLI Commands

Complete reference for all `dpm` commands, flags, and output formats.

---

## dpm inspect

Quick summary of a Druid ingestion spec. No files are written.

```
dpm inspect <spec> [--json]
```

| Argument | Description |
|----------|-------------|
| `<spec>` | Path to Druid spec file (JSON or YAML) |
| `--json` | Output as JSON instead of human-readable text |

**Output fields:**

| Field | Type | Description |
|-------|------|-------------|
| `datasource_name` | string | Druid datasource name |
| `source_kind` | string | `batch` or `stream` |
| `classification` | string | `raw_event`, `rolled_up_additive`, `complex_aggregated`, or `unknown` |
| `dimensions` | int | Number of dimension fields |
| `metrics` | int | Number of metric fields |
| `transforms` | int | Number of transform expressions |
| `rollup` | bool | Whether rollup is enabled |
| `risk_count` | int | Number of risks detected |
| `warnings` | int | Number of normalisation warnings |

**Example:**

```bash
$ dpm inspect tests/fixtures/rolled_up/spec.json
datasource     : ad_metrics
source_kind    : batch
classification : rolled_up_additive
dimensions     : 3
metrics        : 3
transforms     : 0
rollup         : True
risks          : 1
warnings       : 0

$ dpm inspect tests/fixtures/rolled_up/spec.json --json
{
  "datasource_name": "ad_metrics",
  "source_kind": "batch",
  "classification": "rolled_up_additive",
  "dimensions": 3,
  "metrics": 3,
  "transforms": 0,
  "rollup": true,
  "risk_count": 1,
  "warnings": 0
}
```

---

## dpm normalize

Parse and normalise a spec to the canonical migration model. Useful for debugging parse
issues or inspecting the intermediate representation.

```
dpm normalize <spec> [--out <path>] [--json]
```

| Argument | Description |
|----------|-------------|
| `<spec>` | Path to Druid spec file |
| `--out PATH` | Write canonical model JSON to this path |
| `--json` | Print canonical model as JSON to stdout |

**Output:** The canonical model JSON, which includes `datasource_name`, `source_kind`,
`classification`, `dimensions[]`, `metrics[]`, `transforms[]`, `granularity`,
`unsupported_features[]`, `risk_annotations[]`, and `notes[]`.

**Example:**

```bash
$ dpm normalize tests/fixtures/raw_batch/spec.json --json | python3 -m json.tool | head -30
{
  "datasource_name": "pageviews",
  "source_kind": "batch",
  "classification": "raw_event",
  "time_field": {
    "column_name": "timestamp",
    "format": "iso",
    "timezone": "UTC"
  },
  "dimensions": [
    {"name": "page",   "druid_type": "string", "pinot_type": "STRING", "multi_value": false},
    {"name": "region", "druid_type": "string", "pinot_type": "STRING", "multi_value": false},
    {"name": "user",   "druid_type": "string", "pinot_type": "STRING", "multi_value": false}
  ],
  ...
}
```

---

## dpm generate

Full pipeline: parse → normalise → classify → generate Pinot artifacts → risk-analyse →
validate → write reports.

```
dpm generate <spec> [--out <dir>] [--dry-run] [--json]
```

| Argument | Description | Default |
|----------|-------------|---------|
| `<spec>` | Path to Druid spec file | (required) |
| `--out DIR` | Output directory for generated files | `./output` |
| `--dry-run` | Run full analysis without writing files | false |
| `--json` | Output result summary as JSON | false |

**Files written (batch):**

| File | Description |
|------|-------------|
| `schema.json` | Pinot schema |
| `table-offline.json` | OFFLINE table configuration |
| `batch-job.json` | Batch ingestion job spec |
| `canonical.json` | Normalised canonical model |
| `reports/migration-report.json` | Full migration report |
| `reports/risks.json` | Risk annotations array |
| `reports/warnings.json` | Normalisation warnings |
| `reports/migration-summary.md` | Human-readable Markdown summary |

**Files written (streaming):**

| File | Description |
|------|-------------|
| `schema.json` | Pinot schema |
| `table-realtime.json` | REALTIME table configuration |
| `stream-config.json` | Streaming configuration snippet |
| `canonical.json` | Normalised canonical model |
| `reports/migration-report.json` | Full migration report |
| `reports/risks.json` | Risk annotations array |
| `reports/warnings.json` | Normalisation warnings |
| `reports/migration-summary.md` | Human-readable Markdown summary |

**JSON output fields:**

| Field | Type | Description |
|-------|------|-------------|
| `success` | bool | Whether generation succeeded |
| `output_dir` | string | Absolute path to output directory |
| `files_written` | list[string] | Absolute paths of all written files |
| `errors` | list[string] | Fatal errors (empty on success) |
| `warnings` | list[string] | Non-fatal warnings |

**Example:**

```bash
$ dpm generate tests/fixtures/raw_batch/spec.json --out /tmp/pageviews
Generated 8 files in /tmp/pageviews/
  schema.json
  table-offline.json
  batch-job.json
  canonical.json
  reports/migration-report.json
  reports/risks.json
  reports/warnings.json
  reports/migration-summary.md

$ dpm generate tests/fixtures/raw_batch/spec.json --json
{
  "success": true,
  "output_dir": "/tmp/pageviews",
  "files_written": [...],
  "errors": [],
  "warnings": []
}
```

---

## dpm validate

Validate a Druid spec (static checks) and optionally validate generated Pinot artifacts
(artifact checks).

```
dpm validate <spec> [--generated-dir <dir>] [--json]
```

| Argument | Description |
|----------|-------------|
| `<spec>` | Path to Druid spec file |
| `--generated-dir DIR` | Path to directory with generated artifacts |
| `--json` | Output validation report as JSON |

**Static checks** (always run):

| Check ID | Description |
|----------|-------------|
| `static.datasource_name_present` | datasource_name is non-empty |
| `static.time_field_present` | time_field was successfully extracted |
| `static.field_names_unique` | No duplicate field names across dimensions + metrics |
| `static.metric_names_valid` | All metric names are non-empty strings |
| `static.classification_assigned` | Classification is not `unknown` |

**Artifact checks** (only when `--generated-dir` is provided):

| Check ID | Description |
|----------|-------------|
| `artifact.schema_has_datetime` | schema.json has ≥1 dateTimeFieldSpec |
| `artifact.schema_no_duplicate_fields` | No duplicate names in generated schema |
| `artifact.table_type_valid` | tableType is OFFLINE or REALTIME |
| `artifact.time_column_match` | Schema time column = table segmentsConfig.timeColumnName |
| `artifact.realtime_has_stream_configs` | REALTIME tables have streamConfigs |

**JSON output:**

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
    }
  ]
}
```

`overall_status` values: `"pass"`, `"warn"`, `"fail"`.

**Example:**

```bash
$ dpm validate tests/fixtures/raw_batch/spec.json \
    --generated-dir /tmp/pageviews

datasource     : pageviews
overall_status : pass
confidence     : 1.00
checks         : 9 passed, 0 warned, 0 failed
```

---

## Input Formats

The tool accepts both JSON and YAML input files. The file extension is not checked;
the tool attempts JSON first and falls back to YAML.

```bash
dpm generate my_spec.yml    # YAML spec
dpm generate my_spec.json   # JSON spec
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Generation or validation failed (errors reported) |
| `2` | Invalid arguments or file not found |

---

## Environment

No environment variables are required. The tool uses only the spec file and the
output directory path. Cloud storage credentials are not managed by the CLI —
they are embedded in the generated `batch-job.json` based on your spec's `inputSource`.
