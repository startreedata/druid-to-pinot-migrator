# Tutorial 05 — Migrating a Kinesis Streaming Table

**Pattern:** `kinesis` ioConfig  
**Pinot table type:** REALTIME  
**Risk:** `STREAM_SOURCE_MISMATCH` (HIGH)  
**Typical use cases:** AWS-native real-time pipelines, payment events, IoT streams on AWS

---

## The Problem

Druid supports Kinesis natively via the Kinesis indexing service. Pinot does not have a
built-in Kinesis consumer in most deployments. The tool generates a **Kafka-based REALTIME
config** as a best-effort scaffold, and raises `STREAM_SOURCE_MISMATCH` (HIGH severity)
to flag that manual work is required.

You have two options:
1. **Kinesis-to-Kafka bridge** — Route Kinesis events into Kafka using Kafka MirrorMaker 2,
   Kafka Connect (Kinesis Source Connector), or AWS MSK.
2. **Pinot Kinesis plugin** — Use the `pinot-kinesis` plugin (available but not default in
   all Pinot distributions).

---

## Sample Druid Spec

```json
{
  "type": "kinesis",
  "spec": {
    "dataSchema": {
      "dataSource": "payment_events",
      "timestampSpec": {
        "column": "event_time",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "payment_id", "user_id", "merchant_id",
          "payment_method", "currency", "status"
        ]
      },
      "metricsSpec": [
        {"type": "count",     "name": "tx_count"},
        {"type": "doubleSum", "name": "amount_usd", "fieldName": "amount_usd"},
        {"type": "longSum",   "name": "failure_count", "fieldName": "is_failure"}
      ],
      "granularitySpec": {
        "segmentGranularity": "HOUR",
        "queryGranularity": "MINUTE",
        "rollup": false
      }
    },
    "ioConfig": {
      "type": "kinesis",
      "stream": "payment-events-prod",
      "endpoint": "kinesis.us-east-1.amazonaws.com",
      "taskCount": 2,
      "replicas": 1,
      "taskDuration": "PT1H",
      "useEarliestSequenceNumber": false
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate payment_events_kinesis.json --out ./output/payment_events
```

You will see a high-severity risk:

```
Risks detected: 1
  [HIGH] STREAM_SOURCE_MISMATCH
    The source datasource uses Kinesis as the streaming source. The generated
    Pinot REALTIME table config uses Kafka defaults.
    Evidence: ioConfig.type=kinesis; REALTIME table generated with Kafka defaults
    Remediation: Replace streamConfigs with Kinesis consumer factory settings or
    bridge Kinesis to Kafka before Pinot ingestion.
```

A REALTIME table config and stream-config.json are still generated — they are usable
as templates but require manual stream configuration.

---

## What Gets Generated

### schema.json

```json
{
  "schemaName": "payment_events",
  "dimensionFieldSpecs": [
    {"name": "currency",       "dataType": "STRING"},
    {"name": "merchant_id",    "dataType": "STRING"},
    {"name": "payment_id",     "dataType": "STRING"},
    {"name": "payment_method", "dataType": "STRING"},
    {"name": "status",         "dataType": "STRING"},
    {"name": "user_id",        "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "amount_usd",    "dataType": "DOUBLE"},
    {"name": "failure_count", "dataType": "LONG"},
    {"name": "tx_count",      "dataType": "LONG"}
  ],
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

### table-realtime.json (streamConfigs section — needs update)

The generated streamConfigs uses Kafka as a placeholder. You **must** replace this:

```json
"streamConfigs": {
  "streamType": "kafka",
  "stream.kafka.topic.name": "payment-events-prod",
  "stream.kafka.broker.list": "localhost:9092",
  ...
}
```

---

## Option A: Kinesis-to-Kafka Bridge

This approach reuses the generated Kafka config with no Pinot plugin changes.

### Using Kafka Connect (Kinesis Source Connector)

```json
{
  "name": "kinesis-payment-events-source",
  "config": {
    "connector.class": "io.confluent.connect.aws.kinesis.KinesisSourceConnector",
    "tasks.max": "2",
    "kinesis.stream.name": "payment-events-prod",
    "kinesis.region": "us-east-1",
    "kafka.topic": "payment-events-pinot",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

Then update `stream.kafka.topic.name` in the generated table config to `payment-events-pinot`.

### Using Amazon MSK Connect

```bash
aws kafkaconnect create-connector \
  --connector-configuration file://connector-config.json \
  --connector-name kinesis-payment-bridge \
  ...
```

---

## Option B: Pinot Kinesis Plugin

If your Pinot cluster includes the `pinot-kinesis` plugin, update the streamConfigs:

```json
"streamConfigs": {
  "streamType": "kinesis",
  "stream.kinesis.topic.name": "payment-events-prod",
  "stream.kinesis.region": "us-east-1",
  "stream.kinesis.consumer.factory.class.name":
    "org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory",
  "stream.kinesis.decoder.class.name":
    "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder",
  "stream.kinesis.shard.iterator.type": "LATEST",
  "realtime.segment.flush.threshold.rows": "100000",
  "realtime.segment.flush.threshold.time": "1h"
}
```

For IAM authentication:

```json
"stream.kinesis.consumer.prop.aws.accessKeyId": "AKIAIOSFODNN7EXAMPLE",
"stream.kinesis.consumer.prop.aws.secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

Or, if running on EC2/ECS with an IAM role, leave these out — the SDK will use the
instance/task role automatically.

---

## Kinesis-Specific Ingestion Parameters

Map Druid Kinesis supervisor settings to Pinot equivalents:

| Druid `ioConfig` | Pinot streamConfig equivalent |
|-----------------|-------------------------------|
| `taskCount: 2` | `tasks.max` in connector / consumer thread count |
| `replicas: 1` | `replicasPerPartition` in segmentsConfig |
| `taskDuration: "PT1H"` | `realtime.segment.flush.threshold.time: "1h"` |
| `useEarliestSequenceNumber: false` | `stream.kinesis.shard.iterator.type: "LATEST"` |
| `useEarliestSequenceNumber: true` | `stream.kinesis.shard.iterator.type: "TRIM_HORIZON"` |

---

## Sequence Number vs. Offset

Kinesis uses **sequence numbers** (per-shard monotonic strings) rather than Kafka-style
integer offsets. Pinot's Kinesis plugin tracks these automatically. If you're using a
Kafka bridge, the Kafka connector handles the offset bookkeeping — you just interact with
Kafka offsets on the Pinot side.

---

## Deploying

After updating the streamConfigs:

```bash
# Create schema
curl -X POST http://pinot-controller:9000/schemas \
  -H "Content-Type: application/json" \
  -d @output/payment_events/schema.json

# Create REALTIME table (with updated stream config)
curl -X POST http://pinot-controller:9000/tables \
  -H "Content-Type: application/json" \
  -d @output/payment_events/table-realtime.json
```

---

## Query Translation

Queries are identical to the Kafka streaming pattern:

```sql
-- Druid:
SELECT status, COUNT(*) AS cnt, SUM(amount_usd) AS total
FROM "payment_events"
WHERE currency = 'USD'
GROUP BY status

-- Pinot:
SELECT status, COUNT(*) AS cnt, SUM(amount_usd) AS total
FROM payment_events_REALTIME
WHERE currency = 'USD'
GROUP BY status
```

---

## See Also

- [Tutorial 04 — Kafka Streaming](04-kafka-streaming.md) — full streaming table walkthrough
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — STREAM_SOURCE_MISMATCH explanation
