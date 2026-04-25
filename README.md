# druid-pinot-migrator

[![CI](https://github.com/startreedata/druid-to-pinot-migrator/actions/workflows/ci.yml/badge.svg)](https://github.com/startreedata/druid-to-pinot-migrator/actions/workflows/ci.yml)
[![Version Matrix](https://github.com/startreedata/druid-to-pinot-migrator/actions/workflows/version-matrix.yml/badge.svg)](https://github.com/startreedata/druid-to-pinot-migrator/actions/workflows/version-matrix.yml)

A CLI tool for migrating Apache Druid ingestion specs to Apache Pinot artifacts.

## Overview

`druid-pinot-migrator` (`dpm`) parses Druid ingestion specs (batch `index_parallel`, Kafka streaming, and related formats), normalises them into a canonical model, and generates:

- Pinot schema JSON
- Pinot table config JSON (OFFLINE or REALTIME)
- Pinot batch ingestion job spec
- Risk analysis report (JSON + Markdown)
- Validation report

## Compatibility

The tool operates at the ingestion-spec JSON layer and does not connect to running clusters during translation, so compatibility is governed by the spec / artifact formats on each side.

### Tested combination

| Component | Version | Notes |
|-----------|---------|-------|
| Apache Druid (source) | **31.0.0** | Live integration suite (`tests/docker/`) and `examples/quickstart/` |
| Apache Pinot (target) | **1.5.0** | Live integration suite (`tests/docker/`) and `examples/quickstart/` |
| Python | 3.11+ | Runtime requirement |

### Version compatibility matrix

The cells indicate the expected outcome of translating a Druid spec from the row version into Pinot artifacts deployable on the column version.

|                       | Pinot 1.5.x   | Pinot 1.0.x – 1.4.x | Pinot 0.12.x        | Pinot ≤ 0.11.x      |
|-----------------------|---------------|---------------------|---------------------|---------------------|
| **Druid 30.x – 31.x** | ✅ Tested     | ✅ Supported        | ⚠️ Mostly works     | ❌ Not supported     |
| **Druid 24.x – 29.x** | ✅ Supported  | ✅ Supported        | ⚠️ Mostly works     | ❌ Not supported     |
| **Druid 0.20 – 23.x** | ✅ Supported  | ✅ Supported        | ⚠️ Mostly works     | ❌ Not supported     |
| **Druid < 0.20**      | ❌ Not supported | ❌ Not supported | ❌ Not supported    | ❌ Not supported     |

Legend:
- ✅ **Tested** — covered by the live Docker integration suite that boots both clusters and validates query parity.
- ✅ **Supported** — same Druid spec layout and same Pinot artifact format as the tested cell; expected to work without changes.
- ⚠️ **Mostly works** — generated artifacts deploy, but newer fields (e.g., index plug-in flags) may be silently ignored. Review after deploy.
- ❌ **Not supported** — spec or artifact format predates what the tool emits / parses; deployment will likely fail.

### Why the bounds

**Druid ≥ 0.20** — the tool models the modern ingestion API (`index_parallel`, `kafka`, `kinesis` ioConfigs) and both the legacy top-level `dataSchema` and the current nested `spec.dataSchema` layouts. Pre-0.20 specs (legacy `index` task, deprecated firehoses) are not modeled and will fail to parse cleanly.

**Pinot ≥ 0.12** — generated `schema.json` uses the compact `dateTimeFieldSpec` format (`"1:MILLISECONDS:EPOCH"` / `"1:MILLISECONDS:SIMPLE_DATE_FORMAT:..."`) and `table-realtime.json` uses the modern `tableIndexConfig.streamConfigs` block. Earlier Pinot versions (≤ 0.11.x) used split `format` / `granularity` fields and a different stream-config shape, and will reject the generated artifacts.

**Note on Pinot 1.5** — Pinot 1.5.0 removed the `pinot-kafka-2.0` plugin in favour of `pinot-kafka-3.0` (Kafka 3/4 clients). Generated REALTIME tables emit the `kafka30` consumer factory class name (`org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory`), which is shipped by both Pinot 1.4 and 1.5 with the same FQCN, so the same generated artifact deploys cleanly on either release. (Pinot also keeps a backward-compat alias for the old `kafka20` class name, but new artifacts use the canonical name.)

### Spec-feature support snapshot

| Druid feature | Pinot equivalent in generated artifact | Risk emitted |
|---------------|----------------------------------------|--------------|
| `index_parallel` (batch) | `OFFLINE` table + `batch-job.json` | — |
| `kafka` ioConfig | `REALTIME` table + Kafka `streamConfigs` | — |
| `kinesis` ioConfig | `REALTIME` table + Kafka defaults | `STREAM_SOURCE_MISMATCH` (HIGH) |
| `rollup: true` + additive metrics | `OFFLINE` table | `ROLLUP_SEMANTIC_MISMATCH` (HIGH) |
| `count`, `longSum`, `doubleSum`, min/max | Pinot SUM / MIN / MAX columns | — |
| Sketch metrics (`thetaSketch`, `HLL*`, `hyperUnique`, …) | `BYTES` column | `APPROX_AGGREGATOR_MISMATCH` (BLOCKING) |
| `multiValueHandling` dimensions | `singleValueField: false` | `MULTIVALUE_AMBIGUITY` (MEDIUM) |
| `transformSpec.transforms` | — | `TRANSFORM_PORTABILITY_RISK` (MEDIUM) |
| `flattenSpec` | — | `FLATTEN_SPEC_NOT_PORTABLE` (HIGH) |
| `partitionsSpec` (hash / range) | — | `PARTITIONING_CONFIG_REQUIRED` (MEDIUM) |
| Custom `timestampSpec.format` | `SIMPLE_DATE_FORMAT` mapping | `CUSTOM_TIMESTAMP_FORMAT` (MEDIUM) |
| `appendToExisting: true` | `APPEND` ingestion type | `INGESTION_BEHAVIOR_MISMATCH` (info) |
| Cloud `inputSource` (S3, GCS, Azure) | URI propagated to `batch-job.json` | — (review `pinotFSSpecs`) |

See [docs/reference/risks.md](docs/reference/risks.md) for the full risk taxonomy.

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

### Live Docker integration tests

`tests/docker/` boots a real Druid + Pinot cluster via docker-compose and validates
the full pipeline. They are gated behind `LIVE_DOCKER_TESTS=1` so they do not run
by default. Versions are overridable via env vars:

```bash
DRUID_VERSION=30.0.0 PINOT_VERSION=1.4.0 LIVE_DOCKER_TESTS=1 \
  .venv/bin/pytest tests/docker -v
```

If unset, the defaults from the README's tested combination apply.

## Continuous Integration

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| [`ci.yml`](.github/workflows/ci.yml) | every push / PR | Unit + integration tests on Python 3.11 and 3.12; CLI smoke; verifies generated kafka FQCN |
| [`version-matrix.yml`](.github/workflows/version-matrix.yml) | weekly + manual + relevant-paths push | Live Druid × Pinot integration suite over a curated matrix of versions; aggregates a single check for branch protection |

The version-matrix workflow exercises the compatibility table in
[Compatibility](#compatibility). To test a single cell on demand:

> *Actions → Version Matrix (Live Docker) → Run workflow → fill in `druid_version` and/or `pinot_version`*

Leave both inputs blank to run the full curated matrix.

## Known Limitations

1. Druid sketch aggregators (`thetaSketch`, `HLLSketchBuild`, `hyperUnique`) cannot be directly migrated. Re-ingestion from raw events is required.
2. Druid expression-based transforms (`transformSpec`) have no direct Pinot equivalent; they must be applied upstream.
3. Druid multi-value dimensions require careful validation of query semantics after migration.
4. The generated Pinot configs use conservative defaults; review and tune for production workloads.
