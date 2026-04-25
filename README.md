# druid-pinot-migrator

A CLI tool for migrating Apache Druid ingestion specs to Apache Pinot artifacts.

## Overview

`druid-pinot-migrator` (`dpm`) parses Druid ingestion specs (batch `index_parallel`, Kafka streaming, and related formats), normalises them into a canonical model, and generates:

- Pinot schema JSON
- Pinot table config JSON (OFFLINE or REALTIME)
- Pinot batch ingestion job spec
- Risk analysis report (JSON + Markdown)
- Validation report

## Installation

```bash
pip install -e ".[dev]"
```

Requires Python 3.11+.

## Quick Start

```bash
# Inspect a spec without generating any files
dpm inspect tests/fixtures/raw_batch/spec.json

# Full generation into ./output/
dpm generate tests/fixtures/rolled_up/spec.json --out ./output

# Validate spec only
dpm validate tests/fixtures/raw_batch/spec.json

# Validate spec and generated artifacts together
dpm validate tests/fixtures/raw_batch/spec.json --generated-dir ./output
```

## Commands

### `dpm inspect <spec>`

Parse and summarise a Druid ingestion spec. Prints datasource name, source kind, classification, field counts, risk count, and warnings.

```
Options:
  --json    Output as JSON
```

### `dpm normalize <spec>`

Parse and normalise a spec to the canonical migration model.

```
Options:
  --out PATH   Write canonical model JSON to this path
  --json        Print canonical model as JSON
```

### `dpm generate <spec>`

Run the full pipeline: parse → normalise → classify → generate → risk-analyse → validate → write reports.

```
Options:
  --out PATH   Output directory (default: ./output)
  --dry-run    Simulate without writing files
  --json        Output result summary as JSON
```

Generated files:

| File | Description |
|------|-------------|
| `schema.json` | Pinot schema |
| `table-offline.json` / `table-realtime.json` | Pinot table config |
| `batch-job.json` / `stream-config.json` | Ingestion job spec |
| `canonical.json` | Normalised canonical model |
| `reports/migration-report.json` | Full migration report |
| `reports/risks.json` | Risk annotations |
| `reports/warnings.json` | Normalisation warnings |
| `reports/migration-summary.md` | Human-readable summary |

### `dpm validate <spec>`

Validate a Druid spec and optionally validate generated artifacts.

```
Options:
  --generated-dir PATH   Directory with generated Pinot artifacts
  --json                  Output validation report as JSON
```

## Package Layout

```
migrator/
  cli/               CLI commands (typer)
  core/              Enums, errors, models, result types
  druid/             Druid spec models, parser, normaliser, classifier
  pinot/             Pinot schema/table/ingestion generators
  risks/             Risk taxonomy, analyser, formatters
  validation/        Static checks, artifact checks, scoring
  reports/           JSON and Markdown report writers
  translators/       Type mapping rules, naming utilities, pipeline
  utils/             IO, JSON, YAML, logging helpers
  templates/         Jinja2 templates for batch/stream configs
tests/
  fixtures/          Five representative Druid spec fixtures
  unit/              Unit tests for each major component
  integration/       End-to-end pipeline and CLI tests
  golden/            Golden output files (for future snapshot tests)
```

## Risk Categories

| Risk ID | Severity | Description |
|---------|----------|-------------|
| `APPROX_AGGREGATOR_MISMATCH` | BLOCKING | Sketch aggregators (thetaSketch, HLL, hyperUnique) cannot be directly migrated |
| `ROLLUP_SEMANTIC_MISMATCH` | HIGH | Druid rollup semantics differ from Pinot; COUNT(*) semantics change |
| `UNSUPPORTED_COMPLEX_FIELD` | HIGH | Fields mapped to BYTES require manual migration planning |
| `TRANSFORM_PORTABILITY_RISK` | MEDIUM | Druid expression transforms are not supported at Pinot ingestion time |
| `MULTIVALUE_AMBIGUITY` | MEDIUM | MV column query semantics differ between Druid and Pinot |
| `TIME_SEMANTICS_MISMATCH` | LOW | Non-standard time format may need verification |
| `INGESTION_BEHAVIOR_MISMATCH` | INFO | `appendToExisting` and compaction semantics differ |

## Confidence Score

The migration confidence score starts at 1.0 and is reduced by:
- `-0.30` per BLOCKING risk
- `-0.15` per HIGH risk
- `-0.05` per MEDIUM risk
- `-0.01` per LOW risk

Clamped to `[0.0, 1.0]`.

## Development

```bash
# Run tests
.venv/bin/pytest tests/ -v

# Run tests with coverage
.venv/bin/pytest tests/ --cov=migrator --cov-report=term-missing
```

## Supported Druid Spec Formats

- `index_parallel` (native batch)
- `kafka` (Kafka indexing service)
- `kinesis` (Kinesis indexing service)
- Top-level `dataSchema` (legacy format)
- Nested `spec.dataSchema` (current format)

## Known Limitations

1. Druid sketch aggregators (`thetaSketch`, `HLLSketchBuild`, `hyperUnique`) cannot be directly migrated. Re-ingestion from raw events is required.
2. Druid expression-based transforms (`transformSpec`) have no direct Pinot equivalent; they must be applied upstream.
3. Druid multi-value dimensions require careful validation of query semantics after migration.
4. The generated Pinot configs use conservative defaults; review and tune for production workloads.
