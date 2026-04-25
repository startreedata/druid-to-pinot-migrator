# Reference: Generated Artifact Formats

Complete documentation of every file written by `dpm generate`.

---

## Output Directory Layout

### Batch (OFFLINE) table

```
output-dir/
├── schema.json
├── table-offline.json
├── batch-job.json
├── canonical.json
└── reports/
    ├── migration-report.json
    ├── risks.json
    ├── warnings.json
    └── migration-summary.md
```

### Streaming (REALTIME) table

```
output-dir/
├── schema.json
├── table-realtime.json
├── stream-config.json
├── canonical.json
└── reports/
    ├── migration-report.json
    ├── risks.json
    ├── warnings.json
    └── migration-summary.md
```

---

## schema.json

Pinot schema definition. Submitted to the Controller at `POST /schemas`.

### Structure

```json
{
  "schemaName": "string",
  "dimensionFieldSpecs": [ DimensionFieldSpec, ... ],
  "metricFieldSpecs":    [ MetricFieldSpec, ... ],
  "dateTimeFieldSpecs":  [ DateTimeFieldSpec, ... ]
}
```

### DimensionFieldSpec

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Column name |
| `dataType` | string | `STRING`, `LONG`, `FLOAT`, `DOUBLE`, or `BYTES` |
| `singleValueField` | bool | Only present when `false` (multi-value column) |

```json
{"name": "country",   "dataType": "STRING"},
{"name": "user_id",   "dataType": "LONG"},
{"name": "tags",      "dataType": "STRING", "singleValueField": false}
```

Dimensions are sorted alphabetically by name.

### MetricFieldSpec

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Column name |
| `dataType` | string | `LONG`, `DOUBLE`, or `BYTES` (for sketches) |

```json
{"name": "revenue",  "dataType": "DOUBLE"},
{"name": "cnt",      "dataType": "LONG"}
```

Metrics are sorted alphabetically by name.

### DateTimeFieldSpec

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Time column name (from `timestampSpec.column`) |
| `dataType` | string | Always `LONG` |
| `format` | string | Pinot time format string |
| `granularity` | string | Pinot granularity string |

Format strings by Druid source format:

| Druid format | Pinot `format` | Pinot `granularity` |
|-------------|----------------|---------------------|
| `millis` | `1:MILLISECONDS:EPOCH` | `1:MILLISECONDS` |
| `auto` | `1:MILLISECONDS:EPOCH` | `1:MILLISECONDS` |
| `seconds` / `posix` | `1:SECONDS:EPOCH` | `1:SECONDS` |
| `micro` | `1:MICROSECONDS:EPOCH` | `1:MICROSECONDS` |
| `nano` | `1:NANOSECONDS:EPOCH` | `1:NANOSECONDS` |
| `iso` | `1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss.SSSZ` | `1:MILLISECONDS` |
| *(custom)* | `1:MILLISECONDS:SIMPLE_DATE_FORMAT:{pattern}` | `1:MILLISECONDS` |

```json
{
  "name": "timestamp",
  "dataType": "LONG",
  "format": "1:MILLISECONDS:EPOCH",
  "granularity": "1:MILLISECONDS"
}
```

### Complete example

```json
{
  "schemaName": "pageviews",
  "dimensionFieldSpecs": [
    {"name": "page",   "dataType": "STRING"},
    {"name": "region", "dataType": "STRING"},
    {"name": "user",   "dataType": "STRING"}
  ],
  "metricFieldSpecs": [],
  "dateTimeFieldSpecs": [
    {
      "name": "timestamp",
      "dataType": "LONG",
      "format": "1:MILLISECONDS:EPOCH",
      "granularity": "1:MILLISECONDS"
    }
  ]
}
```

---

## table-offline.json

Pinot OFFLINE table configuration. Submitted to the Controller at `POST /tables`.

### Structure

```json
{
  "tableName": "{datasource}_OFFLINE",
  "tableType": "OFFLINE",
  "segmentsConfig":  { ... },
  "tenants":         { ... },
  "tableIndexConfig":{ ... },
  "ingestionConfig": { ... },
  "metadata":        { "customConfigs": {} }
}
```

### segmentsConfig

| Field | Default | Description |
|-------|---------|-------------|
| `timeColumnName` | *(from spec)* | Must match schema's `dateTimeFieldSpec.name` |
| `timeType` | `"MILLISECONDS"` | Time unit for the time column |
| `replication` | `"1"` | **Change to `"3"` for production** |
| `segmentAssignmentStrategy` | `"BalanceNumSegmentAssignmentStrategy"` | Segment placement policy |
| `retentionTimeUnit` | `"DAYS"` | Retention period unit |
| `retentionTimeValue` | `"365"` | **Set to your actual retention requirement** |

### tenants

| Field | Default | Description |
|-------|---------|-------------|
| `broker` | `"DefaultTenant"` | Broker tenant tag |
| `server` | `"DefaultTenant"` | Server tenant tag |

### tableIndexConfig

| Field | Default | Description |
|-------|---------|-------------|
| `loadMode` | `"MMAP"` | Memory-mapped I/O (default) or `"HEAP"` |

### ingestionConfig

| Field | Default | Description |
|-------|---------|-------------|
| `batchIngestionConfig.segmentIngestionType` | `"APPEND"` | `"APPEND"` or `"REFRESH"` |
| `batchIngestionConfig.segmentIngestionFrequency` | `"DAILY"` | `"HOURLY"`, `"DAILY"`, `"WEEKLY"`, `"MONTHLY"` |

### Complete example

```json
{
  "tableName": "pageviews_OFFLINE",
  "tableType": "OFFLINE",
  "segmentsConfig": {
    "timeColumnName": "timestamp",
    "timeType": "MILLISECONDS",
    "replication": "1",
    "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "365"
  },
  "tenants": {
    "broker": "DefaultTenant",
    "server": "DefaultTenant"
  },
  "tableIndexConfig": {
    "loadMode": "MMAP"
  },
  "ingestionConfig": {
    "batchIngestionConfig": {
      "segmentIngestionType": "APPEND",
      "segmentIngestionFrequency": "DAILY"
    }
  },
  "metadata": {
    "customConfigs": {}
  }
}
```

---

## table-realtime.json

Pinot REALTIME table configuration. Submitted to the Controller at `POST /tables`.

### Structure

```json
{
  "tableName": "{datasource}_REALTIME",
  "tableType": "REALTIME",
  "segmentsConfig":   { ... },
  "tenants":          { ... },
  "tableIndexConfig": { "loadMode": "MMAP", "streamConfigs": { ... } },
  "metadata":         { "customConfigs": {} }
}
```

### segmentsConfig (REALTIME)

| Field | Default | Description |
|-------|---------|-------------|
| `timeColumnName` | *(from spec)* | Must match schema's `dateTimeFieldSpec.name` |
| `timeType` | `"MILLISECONDS"` | Time unit |
| `replication` | `"1"` | **Change to `"3"` for production** |
| `retentionTimeUnit` | `"DAYS"` | Retention period unit |
| `retentionTimeValue` | `"365"` | **Set to your actual retention requirement** |

### streamConfigs

Kafka consumer configuration nested inside `tableIndexConfig`.

| Key | Extracted from Druid spec | Default |
|-----|--------------------------|---------|
| `streamType` | — | `"kafka"` |
| `stream.kafka.topic.name` | `ioConfig.topic` | datasource name |
| `stream.kafka.broker.list` | `ioConfig.consumerProperties["bootstrap.servers"]` | `"localhost:9092"` |
| `stream.kafka.consumer.type` | — | `"lowlevel"` |
| `stream.kafka.consumer.factory.class.name` | — | `"org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory"` |
| `stream.kafka.decoder.class.name` | — | `"org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"` |
| `realtime.segment.flush.threshold.rows` | — | `"1000000"` |
| `realtime.segment.flush.threshold.time` | — | `"1h"` |

> **Note:** For Kinesis sources, the tool still generates Kafka defaults and raises a
> `STREAM_SOURCE_MISMATCH` risk. Update `streamConfigs` manually before deployment.

### Complete example

```json
{
  "tableName": "events_REALTIME",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "event_time",
    "timeType": "MILLISECONDS",
    "replication": "1",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "365"
  },
  "tenants": {
    "broker": "DefaultTenant",
    "server": "DefaultTenant",
    "tagOverrideConfig": {}
  },
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "streamConfigs": {
      "streamType": "kafka",
      "stream.kafka.topic.name": "events",
      "stream.kafka.broker.list": "kafka-broker:9092",
      "stream.kafka.consumer.type": "lowlevel",
      "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory",
      "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder",
      "realtime.segment.flush.threshold.rows": "1000000",
      "realtime.segment.flush.threshold.time": "1h"
    }
  },
  "metadata": {
    "customConfigs": {}
  }
}
```

---

## batch-job.json

Pinot batch ingestion job spec. Used with `pinot-admin.sh LaunchDataIngestionJob`.

### Structure

```json
{
  "jobType": "SegmentCreationAndTarPush",
  "inputDirURI": "string",
  "outputDirURI": "string",
  "overwriteOutput": true,
  "pinotFSSpecs": [ ... ],
  "recordReaderSpec": { ... },
  "tableSpec": { ... },
  "pinotClusterSpecs": [ ... ]
}
```

### Fields

| Field | Default | Description |
|-------|---------|-------------|
| `jobType` | `"SegmentCreationAndTarPush"` | Creates segments and pushes them to the cluster |
| `inputDirURI` | *(from Druid `inputSource`)* | Source data URI |
| `outputDirURI` | `/tmp/pinot-output/{datasource}` | Temporary segment storage |
| `overwriteOutput` | `true` | Overwrite existing output |

### pinotFSSpecs

Array of filesystem plugin configurations. The default spec configures the local filesystem:

```json
[
  {
    "scheme": "file",
    "className": "org.apache.pinot.spi.filesystem.LocalPinotFS"
  }
]
```

For cloud storage, add additional specs. See [Tutorial 15 — Cloud Storage](../15-cloud-storage.md).

### recordReaderSpec

| Field | Default | Description |
|-------|---------|-------------|
| `dataFormat` | `"json"` | Input data format |
| `className` | `"org.apache.pinot.plugin.inputformat.json.JSONRecordReader"` | Reader class |

### tableSpec

| Field | Value | Description |
|-------|-------|-------------|
| `tableName` | datasource name | Without `_OFFLINE` suffix |
| `schemaURI` | `http://localhost:9000/schemas/{datasource}` | Controller endpoint |
| `tableConfigURI` | `http://localhost:9000/tables/{datasource}` | Controller endpoint |

**Update these URIs to your actual cluster address before running.**

### pinotClusterSpecs

```json
[{"controllerURI": "http://localhost:9000"}]
```

**Update to your actual Controller URI before running.**

### Complete example

```json
{
  "jobType": "SegmentCreationAndTarPush",
  "inputDirURI": "s3://my-bucket/pageviews/",
  "outputDirURI": "/tmp/pinot-output/pageviews",
  "overwriteOutput": true,
  "pinotFSSpecs": [
    {
      "scheme": "file",
      "className": "org.apache.pinot.spi.filesystem.LocalPinotFS"
    }
  ],
  "recordReaderSpec": {
    "dataFormat": "json",
    "className": "org.apache.pinot.plugin.inputformat.json.JSONRecordReader"
  },
  "tableSpec": {
    "tableName": "pageviews",
    "schemaURI": "http://localhost:9000/schemas/pageviews",
    "tableConfigURI": "http://localhost:9000/tables/pageviews"
  },
  "pinotClusterSpecs": [
    {"controllerURI": "http://localhost:9000"}
  ]
}
```

---

## stream-config.json

Kafka/stream configuration snippet for REALTIME tables. This contains only the
`streamConfigs` object, extracted as a standalone file for reference. It is a copy
of the `tableIndexConfig.streamConfigs` block from `table-realtime.json`.

```json
{
  "streamType": "kafka",
  "stream.kafka.topic.name": "events",
  "stream.kafka.broker.list": "kafka-broker:9092",
  "stream.kafka.consumer.type": "lowlevel",
  "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory",
  "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder",
  "realtime.segment.flush.threshold.rows": "1000000",
  "realtime.segment.flush.threshold.time": "1h"
}
```

---

## canonical.json

The normalised intermediate representation produced by the parser and normaliser.
Useful for debugging and for programmatic consumption of migration metadata.

### Top-level fields

| Field | Type | Description |
|-------|------|-------------|
| `datasource_name` | string | Druid datasource name |
| `source_kind` | string | `"batch"` or `"stream"` |
| `classification` | string | `"raw_event"`, `"rolled_up_additive"`, `"complex_aggregated"`, or `"unknown"` |
| `time_field` | object | Time column spec (see below) |
| `dimensions` | array | Dimension column definitions |
| `metrics` | array | Metric column definitions |
| `transforms` | array | Ingestion-time transform definitions |
| `granularity` | object | Segment/query granularity and rollup flag |
| `retention_hint` | object | Retention period hint (if present in spec) |
| `unsupported_features` | array | Features the tool cannot convert |
| `risk_annotations` | array | Risk objects (same as `reports/risks.json`) |
| `raw_io_config` | object | Raw ioConfig block from Druid spec |
| `notes` | array | Normalisation warnings |

### time_field

```json
{
  "column_name": "timestamp",
  "format": "millis",
  "timezone": "UTC",
  "notes": ""
}
```

### dimensions[]

```json
{
  "name": "country",
  "druid_type": "string",
  "pinot_type": "STRING",
  "multi_value": false,
  "notes": ""
}
```

### metrics[]

```json
{
  "name": "revenue",
  "druid_type": "doubleSum",
  "field_name": "revenue",
  "pinot_type": "DOUBLE",
  "aggregation": "SUM",
  "notes": ""
}
```

### transforms[]

```json
{
  "name": "user_country",
  "expression": "concat(user_id, '_', country)",
  "output_type": "string",
  "notes": ""
}
```

### granularity

```json
{
  "segment_granularity": "DAY",
  "query_granularity": "HOUR",
  "rollup": true,
  "intervals": ["2024-01-01/2024-04-01"]
}
```

### unsupported_features[]

```json
{
  "feature": "flattenSpec",
  "reason": "Druid flattenSpec has no Pinot ingest-time equivalent",
  "severity": "high"
}
```

---

## reports/migration-report.json

Combined report combining classification, risks, validation checks, and unsupported
features in a single document.

```json
{
  "datasource_name": "string",
  "source_kind": "string",
  "classification": "string",
  "confidence_score": 0.0,
  "overall_status": "string",
  "risks": [ RiskAnnotation, ... ],
  "validation_checks": [ ValidationCheck, ... ],
  "unsupported_features": [ UnsupportedFeature, ... ],
  "notes": [ "string", ... ]
}
```

### RiskAnnotation

```json
{
  "risk_id": "ROLLUP_SEMANTIC_MISMATCH",
  "severity": "high",
  "confidence": "certain",
  "description": "...",
  "evidence": ["rollup=True in granularitySpec"],
  "remediation": "..."
}
```

### ValidationCheck

```json
{
  "check_id": "static.datasource_name_present",
  "status": "pass",
  "message": "datasource_name='pageviews'",
  "details": {}
}
```

`status` values: `"pass"`, `"warn"`, `"fail"`.

---

## reports/risks.json

Risk annotations array, extracted from the migration report for easy consumption.

```json
{
  "risks": [
    {
      "risk_id": "APPROX_AGGREGATOR_MISMATCH",
      "severity": "blocking",
      "confidence": "certain",
      "description": "Sketch aggregators present...",
      "evidence": ["Complex aggregators found: user_sketch"],
      "remediation": "Re-ingest raw data into Pinot..."
    }
  ]
}
```

---

## reports/warnings.json

Normalisation warnings emitted during parsing and normalisation.

```json
{
  "warnings": [
    "Metric 'sketch_col' has BYTES type (sketch/complex); manual migration required for this field"
  ]
}
```

An empty `warnings` array means the spec normalised cleanly.

---

## reports/migration-summary.md

Human-readable Markdown report. Contains:

1. **Source Summary** — datasource name, source kind, time column, dimension/metric/transform counts, rollup status
2. **Classification** — inferred datasource classification
3. **Generated Artifacts** — table listing which files were produced
4. **Validation** — overall status and confidence score
5. **Warnings** — normalisation warnings (if any)
6. **Risks** — formatted risk list with severity, confidence, evidence, and remediation
7. **Next Steps** — deployment instructions tailored to the source kind

---

## File Size Expectations

| File | Typical size |
|------|-------------|
| `schema.json` | 1–10 KB |
| `table-offline.json` | 1–3 KB |
| `table-realtime.json` | 1–3 KB |
| `batch-job.json` | 1–2 KB |
| `canonical.json` | 5–50 KB |
| `reports/migration-report.json` | 5–30 KB |
| `reports/risks.json` | 1–10 KB |
| `reports/warnings.json` | < 1 KB |
| `reports/migration-summary.md` | 2–10 KB |

---

## See Also

- [Configuration Defaults Reference](defaults.md) — All default values and when to change them
- [CLI Reference](cli.md) — `dpm generate` options
- [Tutorial 02 — Raw Event Table](../02-raw-event-table.md) — Walkthrough of batch artifacts
- [Tutorial 04 — Kafka Streaming](../04-kafka-streaming.md) — Walkthrough of streaming artifacts
