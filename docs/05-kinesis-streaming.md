# Tutorial 05 — Migrating a Kinesis Streaming Table

**Pattern:** `kinesis` ioConfig
**Pinot table type:** REALTIME
**Risks:** none specific to Kinesis (general streaming risks may still fire)
**Typical use cases:** AWS-native real-time pipelines, payment events, IoT streams on AWS

---

## The Pattern

Druid's Kinesis indexing service has a direct counterpart in Pinot:
[`KinesisConsumerFactory`](https://github.com/apache/pinot/tree/master/pinot-plugins/pinot-stream-ingestion/pinot-kinesis),
which ships in every Pinot 1.x distribution under the
`pinot-kinesis` plugin and is enabled by default.

`dpm` emits a complete `streamConfigs` block targeting that plugin. There is
no manual stream-config rewrite required to deploy — only AWS credentials, which
should be supplied via IAM instance profiles or environment variables on the
Pinot servers (deliberately **not** committed to the table config).

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

No HIGH-severity risk fires for the Kinesis source itself. The generated
`table-realtime.json` is deploy-ready once AWS credentials are in place on
the Pinot side.

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

### table-realtime.json — `streamConfigs` block

```json
"streamConfigs": {
  "streamType": "kinesis",
  "stream.kinesis.topic.name": "payment-events-prod",
  "stream.kinesis.consumer.type": "lowlevel",
  "stream.kinesis.consumer.factory.class.name":
    "org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory",
  "stream.kinesis.decoder.class.name":
    "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder",
  "stream.kinesis.consumer.prop.auto.offset.reset": "largest",
  "stream.kinesis.endpoint": "kinesis.us-east-1.amazonaws.com",
  "region": "us-east-1",
  "realtime.segment.flush.threshold.rows": "1000000",
  "realtime.segment.flush.threshold.time": "1h"
}
```

Notes:

- **`region`** — auto-extracted from the Druid `endpoint` when it follows
  the canonical `kinesis.<region>.amazonaws.com` form. For non-AWS endpoints
  (kinesalite, custom proxies) the field is left blank and **must** be set
  manually before deploy — Pinot's Kinesis plugin requires it.
- **`stream.kinesis.endpoint`** — only emitted when the Druid spec has an
  explicit endpoint. For real AWS deployments where the SDK can resolve the
  region by itself you can drop this field.
- **`auto.offset.reset`** — derived from `useEarliestSequenceNumber`:
  `false` ⇒ `largest` (Kinesis `LATEST`), `true` ⇒ `smallest` (Kinesis
  `TRIM_HORIZON`). When this table is the OFFLINE-side of a hybrid
  migration, the planner overrides this with the watermark ISO timestamp.
- **AWS credentials are deliberately omitted.** Production Pinot deployments
  source them from IAM instance profiles (EKS / ECS / EC2 task roles) or the
  standard AWS env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  `AWS_SESSION_TOKEN`). Putting them in the table config commits a secret to
  source control.

---

## Druid → Pinot Kinesis-config Mapping

| Druid `ioConfig` | Pinot streamConfig (auto-emitted) |
|------------------|-----------------------------------|
| `stream: "payment-events-prod"` | `stream.kinesis.topic.name: "payment-events-prod"` |
| `endpoint: "kinesis.us-east-1.amazonaws.com"` | `stream.kinesis.endpoint: "..."` + `region: "us-east-1"` |
| `useEarliestSequenceNumber: false` | `stream.kinesis.consumer.prop.auto.offset.reset: "largest"` |
| `useEarliestSequenceNumber: true` | `stream.kinesis.consumer.prop.auto.offset.reset: "smallest"` |
| `taskDuration: "PT1H"` | `realtime.segment.flush.threshold.time: "1h"` |
| `replicas: 1` | `replicasPerPartition` in segmentsConfig |

---

## Sequence Number vs. Offset

Kinesis uses **sequence numbers** (per-shard monotonic strings) rather than
Kafka-style integer offsets. The Pinot Kinesis plugin tracks these
automatically per shard, and exposes them through Pinot's segment metadata
the same way Kafka offsets are exposed for Kafka tables.

For hybrid (OFFLINE + REALTIME) migrations, the watermark snapshot still
captures a UTC ISO timestamp — Pinot's Kinesis plugin honours
`auto.offset.reset` set to a timestamp, just like Kafka.

---

## Deploying

```bash
# Create schema
curl -X POST http://pinot-controller:9000/schemas \
  -H "Content-Type: application/json" \
  -d @output/payment_events/schema.json

# Create REALTIME table
curl -X POST http://pinot-controller:9000/tables \
  -H "Content-Type: application/json" \
  -d @output/payment_events/table-realtime.json
```

Pinot servers must have AWS credentials available at process start (IAM
role on the host, or env vars on the container). If the table goes ERROR
shortly after creation with `kinesis.AmazonClientException: Unable to load
AWS credentials`, the credential chain is the cause — table config does not
need to change.

---

## Query Translation

Queries are identical to the Kafka streaming pattern; the streamType is an
ingestion-side concern, not a query-side one.

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

- [Tutorial 04 — Kafka Streaming](04-kafka-streaming.md) — Kafka equivalent
- [Reference — Risks](reference/risks.md) — full risk taxonomy
