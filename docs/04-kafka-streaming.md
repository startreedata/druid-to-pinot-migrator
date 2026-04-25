# Tutorial 04 — Migrating a Kafka Streaming Table

**Pattern:** `kafka` ioConfig, Kafka indexing service  
**Pinot table type:** REALTIME  
**Classification:** `raw_event`  
**Typical use cases:** real-time event streams, live user activity, sensor telemetry

---

## What This Pattern Looks Like in Druid

Druid Kafka supervisor specs have an `ioConfig.type: "kafka"` with a `topic` and
`consumerProperties` block. The `dataSchema` is identical to batch specs.

```json
{
  "type": "kafka",
  "spec": {
    "dataSchema": {
      "dataSource": "clickstream",
      "timestampSpec": {
        "column": "event_time",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "user_id",
          "session_id",
          "page",
          "action",
          "platform"
        ]
      },
      "metricsSpec": [],
      "granularitySpec": {
        "segmentGranularity": "HOUR",
        "queryGranularity": "NONE",
        "rollup": false
      }
    },
    "ioConfig": {
      "type": "kafka",
      "topic": "clickstream-events",
      "consumerProperties": {
        "bootstrap.servers": "kafka-broker-1:9092,kafka-broker-2:9092"
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate clickstream_spec.json --out ./output/clickstream
```

The tool detects `ioConfig.type: "kafka"` and generates a **REALTIME table config**.

```
Generated 8 files in ./output/clickstream/
  schema.json
  table-realtime.json          <-- REALTIME instead of OFFLINE
  stream-config.json
  canonical.json
  reports/migration-report.json
  reports/risks.json
  reports/warnings.json
  reports/migration-summary.md
```

---

## What Gets Generated

### schema.json

```json
{
  "schemaName": "clickstream",
  "dimensionFieldSpecs": [
    {"name": "action",     "dataType": "STRING"},
    {"name": "page",       "dataType": "STRING"},
    {"name": "platform",   "dataType": "STRING"},
    {"name": "session_id", "dataType": "STRING"},
    {"name": "user_id",    "dataType": "STRING"}
  ],
  "metricFieldSpecs": [],
  "dateTimeFieldSpecs": [
    {
      "name": "event_time",
      "dataType": "LONG",
      "format": "1:MILLISECONDS:EPOCH",
      "granularity": "1:MILLISECONDS"
    }
  ]
}
```

### table-realtime.json (key sections)

```json
{
  "tableName": "clickstream_REALTIME",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "event_time",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "365",
    "replication": "1",
    "replicasPerPartition": "1"
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
      "stream.kafka.topic.name": "clickstream-events",
      "stream.kafka.broker.list": "kafka-broker-1:9092,kafka-broker-2:9092",
      "stream.kafka.consumer.type": "lowlevel",
      "stream.kafka.consumer.factory.class.name":
        "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
      "stream.kafka.decoder.class.name":
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
      "realtime.segment.flush.threshold.rows": "1000000",
      "realtime.segment.flush.threshold.time": "1h"
    }
  }
}
```

The topic name (`clickstream-events`) is extracted from the Druid ioConfig and populated
into `stream.kafka.topic.name`. Broker list is carried over from `consumerProperties`.

### stream-config.json

```json
{
  "streamType": "kafka",
  "stream.kafka.topic.name": "clickstream-events",
  "stream.kafka.broker.list": "kafka-broker-1:9092,kafka-broker-2:9092",
  "stream.kafka.consumer.type": "lowlevel",
  "stream.kafka.decoder.class.name":
    "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
  "realtime.segment.flush.threshold.rows": "1000000",
  "realtime.segment.flush.threshold.time": "1h"
}
```

---

## Deploying to Pinot

```bash
# 1. Create the schema
curl -X POST http://pinot-controller:9000/schemas \
  -H "Content-Type: application/json" \
  -d @output/clickstream/schema.json

# 2. Create the REALTIME table
curl -X POST http://pinot-controller:9000/tables \
  -H "Content-Type: application/json" \
  -d @output/clickstream/table-realtime.json
```

Pinot will immediately begin consuming from Kafka. No separate ingestion command is needed
for streaming tables.

---

## What to Adjust for Production

The generated config uses conservative defaults. For a production Kafka stream, review and
adjust:

### Consumer Group and Offsets

```json
"stream.kafka.consumer.prop.auto.offset.reset": "latest"
```

Add this to `streamConfigs` if you want to start consuming from the latest offset (skip
historical messages). Use `"earliest"` if you need to backfill.

### Parallelism (replicasPerPartition)

```json
"replication": "3",
"replicasPerPartition": "1"
```

`replicasPerPartition` controls how many Pinot server threads consume from each Kafka
partition. For high-throughput topics, increase this.

### Segment Flush Thresholds

```json
"realtime.segment.flush.threshold.rows": "1000000",
"realtime.segment.flush.threshold.time": "1h"
```

The defaults flush a segment when it hits 1M rows OR 1 hour, whichever comes first.
For low-volume topics you may want to lower the row threshold; for very high-volume topics
you may want a smaller time window.

### SSL/SASL Authentication

If your Kafka cluster requires authentication, add the consumer properties:

```json
"stream.kafka.consumer.prop.security.protocol": "SASL_SSL",
"stream.kafka.consumer.prop.sasl.mechanism": "PLAIN",
"stream.kafka.consumer.prop.sasl.jaas.config":
  "org.apache.kafka.common.security.plain.PlainLoginModule required username='...' password='...';",
"stream.kafka.consumer.prop.ssl.truststore.location": "/path/to/truststore.jks"
```

---

## Query Translation

Queries against REALTIME tables use the table name directly (without type suffix in most
Pinot versions):

```sql
-- Druid:
SELECT page, COUNT(*) AS views
FROM "clickstream"
WHERE action = 'pageview'
GROUP BY page
ORDER BY views DESC
LIMIT 10

-- Pinot:
SELECT page, COUNT(*) AS views
FROM clickstream_REALTIME
WHERE action = 'pageview'
GROUP BY page
ORDER BY views DESC
LIMIT 10
```

Note: if you have both an OFFLINE and REALTIME table for the same schema (hybrid table),
query `clickstream` (without suffix) and Pinot will federate across both.

---

## REALTIME vs. OFFLINE: When to Use a Hybrid Table

A common pattern is to combine REALTIME ingestion with daily OFFLINE segments for history:

- **REALTIME table** consumes from Kafka, holding the recent window (e.g., last 3 days).
- **OFFLINE table** holds historical segments generated by daily batch jobs.
- **Querying `clickstream`** (base name) automatically combines both.

This requires creating both table types with the same schema. The tool generates only the
REALTIME config for a streaming spec; you'll need to add the OFFLINE config manually for
the hybrid pattern.

---

## Troubleshooting

**Segments not appearing / consumer not progressing**

Check the server logs and verify the Kafka broker list is reachable from Pinot server pods.
Also verify the topic exists and has data:

```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group pinot-clickstream-REALTIME --describe
```

**Schema validation errors during consumption**

Pinot validates each message against the schema. Fields not in the schema are silently
dropped by default. Fields declared in the schema but missing from the message get the
column's default value (empty string for STRING, 0 for numeric types).

If you want strict validation, set:
```json
"stream.kafka.decoder.prop.missingFieldsHandling": "FAIL"
```

**Time column parse errors**

If `event_time` carries epoch milliseconds (LONG) and the schema declares
`"format": "1:MILLISECONDS:EPOCH"`, no conversion is needed. If the field arrives as an
ISO string, either transform it upstream or change the format in the schema.

---

## See Also

- [Tutorial 05 — Kinesis Streaming](05-kinesis-streaming.md) — if your source is Kinesis, not Kafka
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — streaming-specific risks
- [Reference: Artifacts](reference/artifacts.md) — full table-realtime.json structure
