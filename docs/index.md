# Druid → Pinot Migration Documentation

This directory contains tutorials, reference guides, and stories for migrating Apache Druid
datasources to Apache Pinot using the `druid-pinot-migrator` (`dpm`) tool.

## How to Read This

If you are **new to the tool**, start with the [Getting Started](01-getting-started.md) guide,
then follow the tutorials in order. Each tutorial is self-contained but builds on the
concepts introduced earlier.

If you want a **runnable end-to-end demo** that brings up Druid + Pinot, ingests sample
data into Druid, runs the migration, and validates parity in Pinot, see the
[Quickstart Example](../examples/quickstart/README.md).

If you are looking for a **specific migration pattern**, jump directly to the relevant tutorial.
Each one maps to a real-world Druid datasource archetype.

If you hit a risk or warning during migration, consult the
[Risk Reference](reference/risks.md) for detailed explanations and remediation steps.

---

## Tutorials

| # | Tutorial | Druid Pattern | Key Concepts |
|---|----------|--------------|--------------|
| 01 | [Getting Started](01-getting-started.md) | Any spec | Installation, CLI commands, first run |
| 02 | [Migrating a Raw Event Table](02-raw-event-table.md) | `index_parallel`, no rollup | OFFLINE table, batch ingestion |
| 03 | [Migrating a Rolled-Up Metrics Table](03-rolled-up-metrics.md) | rollup=true, SUM/COUNT | ROLLED_UP_ADDITIVE, semantic differences |
| 04 | [Migrating a Kafka Streaming Table](04-kafka-streaming.md) | `kafka` ioConfig | REALTIME table, stream configs |
| 05 | [Migrating a Kinesis Streaming Table](05-kinesis-streaming.md) | `kinesis` ioConfig | STREAM_SOURCE_MISMATCH risk |
| 06 | [Typed Dimensions: Long, Float, Double](06-typed-dimensions.md) | Typed dimensionsSpec | Type mapping, precision considerations |
| 07 | [Min/Max Metrics](07-minmax-metrics.md) | doubleMin/Max, longMin/Max | MIN/MAX aggregations, rollup with range stats |
| 08 | [Multi-Value Dimensions](08-multivalue-dimensions.md) | `multiValueHandling` | MV columns in Pinot, query caveats |
| 09 | [Ingestion-Time Transforms](09-transforms.md) | `transformSpec` | Portability risk, upstream ETL alternatives |
| 10 | [Nested JSON with flattenSpec](10-nested-json.md) | `flattenSpec` | JSON path extraction, Pinot alternatives |
| 11 | [Custom Timestamp Formats](11-custom-timestamps.md) | Java `SimpleDateFormat` | SIMPLE_DATE_FORMAT mapping |
| 12 | [Sketch Aggregators (HLL, Theta, Histogram)](12-sketch-aggregators.md) | thetaSketch, HLL, hyperUnique | BLOCKING risk, re-ingestion strategy |
| 13 | [Partitioned Tables](13-partitioned-tables.md) | `partitionsSpec` hash/range | Pinot segmentPartitionConfig |
| 14 | [Append-Mode Ingestion](14-append-mode.md) | `appendToExisting=true` | Druid vs. Pinot segment semantics |
| 15 | [Cloud Storage Sources (GCS, Azure)](15-cloud-storage.md) | `google`, `azure` inputSource | Pinot pinotFS plugins |
| 16 | [Understanding Risks and Confidence Scores](16-risks-and-confidence.md) | All patterns | Risk taxonomy, scoring, remediation |
| 17 | [Validating the Migration](17-validation.md) | Post-generate workflow | Validation checks, artifact parity |
| 18 | [Production Migration Checklist](18-production-checklist.md) | End-to-end | Pre/post migration verification |

---

## Reference

- [Risk Reference](reference/risks.md) — All risk IDs, severity levels, evidence, remediation
- [Type Mapping Reference](reference/type-mapping.md) — Druid → Pinot type conversions
- [CLI Reference](reference/cli.md) — All commands, flags, and output formats
- [Generated Artifact Reference](reference/artifacts.md) — Schema, table, ingestion job formats
- [Configuration Defaults Reference](reference/defaults.md) — Default values in generated configs

---

## Design Philosophy

The tool is built around **safe, guided migration** rather than fully automatic conversion.
Three principles guide every design decision:

1. **Surface all risks explicitly** — Nothing is silently dropped. Every non-portable pattern
   becomes a named risk or unsupported-feature annotation with a severity level and remediation
   guidance.

2. **Generate runnable defaults, not perfect configs** — The generated Pinot artifacts are
   valid and deployable, but tuned for safety (1 replica, 365-day retention, MMAP load mode).
   Production operators must review and adjust.

3. **Enable incremental migration** — The `inspect` and `validate` commands let teams assess
   migration complexity before committing to a full migration, and verify parity after.
