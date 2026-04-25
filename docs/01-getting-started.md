# Getting Started

This guide walks you through installing the tool, understanding its commands, and running
your first migration in under five minutes.

---

## Installation

```bash
git clone <repo-url> druid-pinot-migration-tool
cd druid-pinot-migration-tool
pip install -e ".[dev]"
```

Requires Python 3.11+. After installation the `dpm` command is available on your PATH.

---

## The Four Commands

| Command | What it does | When to use it |
|---------|-------------|----------------|
| `dpm inspect` | Prints a summary of the spec without writing any files | First look at a spec |
| `dpm normalize` | Parses and normalises into the canonical model | Debugging parse issues |
| `dpm generate` | Full pipeline: parse → normalise → generate → report | The main migration command |
| `dpm validate` | Validates spec and optionally checks generated artifacts | Post-generation verification |

---

## Your First Migration

### Step 1 — Inspect

Before generating anything, get a quick summary of the datasource:

```bash
dpm inspect path/to/druid-spec.json
```

Example output:

```
datasource     : pageviews
source_kind    : batch
classification : raw_event
dimensions     : 3
metrics        : 0
transforms     : 0
rollup         : False
risks          : 0
warnings       : 0
```

If risks > 0, add `--json` to see the full risk list before proceeding.

### Step 2 — Generate

Run the full pipeline. Artifacts are written to `./output/` by default:

```bash
dpm generate path/to/druid-spec.json --out ./output
```

On success you will see:

```
Generated 8 files in ./output/
  schema.json
  table-offline.json
  batch-job.json
  canonical.json
  reports/migration-report.json
  reports/risks.json
  reports/warnings.json
  reports/migration-summary.md
```

### Step 3 — Read the Summary

```bash
cat ./output/reports/migration-summary.md
```

This is a human-readable overview of the migration including the datasource name,
classification, all detected risks with remediation steps, and a confidence score.

### Step 4 — Validate

```bash
dpm validate path/to/druid-spec.json --generated-dir ./output
```

This runs static checks on the canonical model and then verifies the generated artifacts
(schema time column matches table time column, table type is valid, etc.).

A clean pass looks like:

```
overall_status : pass
confidence     : 0.95
checks         : 7 passed, 0 warned, 0 failed
```

---

## Dry Run

To run the full analysis without writing any files:

```bash
dpm generate path/to/druid-spec.json --dry-run
```

Warnings and risks are still computed and printed. This is useful for quickly assessing a
spec inside a CI pipeline.

---

## JSON Output

Every command supports `--json` for machine-readable output:

```bash
dpm inspect path/to/spec.json --json
dpm validate path/to/spec.json --json
dpm generate path/to/spec.json --json
```

---

## Understanding the Output Directory

After `dpm generate`, the output directory contains:

```
output/
├── schema.json              # Pinot schema
├── table-offline.json       # Table config (OFFLINE) or table-realtime.json
├── batch-job.json           # Batch ingestion job spec (or stream-config.json)
├── canonical.json           # Normalised intermediate representation
└── reports/
    ├── migration-report.json   # Full structured report
    ├── risks.json              # Risk annotations array
    ├── warnings.json           # Non-fatal warnings from parsing/normalisation
    └── migration-summary.md    # Human-readable summary
```

`schema.json` and `table-*.json` are the files you deploy to Pinot.
`batch-job.json` (or `stream-config.json`) is used to configure data ingestion.
`canonical.json` is the tool's internal normalised view, useful for debugging.
`reports/` exists to help your team understand and review the migration.

---

## What Gets Skipped

The tool does **not** move any data. It produces configuration artifacts only. You are
responsible for:

1. Running the Pinot batch/stream ingestion job.
2. Validating query parity between Druid and Pinot.
3. Adjusting the generated configs for your production environment (replicas, retention,
   segment size targets, index types, etc.).

---

## Next Steps

- Migrating a **simple batch table**: [Tutorial 02 — Raw Event Table](02-raw-event-table.md)
- Migrating a **rolled-up metrics table**: [Tutorial 03 — Rolled-Up Metrics](03-rolled-up-metrics.md)
- Migrating a **Kafka stream**: [Tutorial 04 — Kafka Streaming](04-kafka-streaming.md)
- Understanding all **risks**: [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md)
