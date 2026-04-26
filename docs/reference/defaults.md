# Reference: Configuration Defaults

All default values embedded in generated Pinot artifacts, why they exist, and when to change them.

The tool generates **safe, deployable defaults** — not production-ready configurations.
Every default below is intentionally conservative. Review each one before deploying.

---

## Quick Reference

| Setting | Default | When to change |
|---------|---------|----------------|
| `segmentsConfig.replication` | `"1"` | Always change to `"3"` for production |
| `segmentsConfig.retentionTimeValue` | `"365"` | Match your data retention policy |
| `segmentsConfig.retentionTimeUnit` | `"DAYS"` | Rarely need to change |
| `segmentsConfig.timeType` | `"MILLISECONDS"` | Change to match actual time column type |
| `tableIndexConfig.loadMode` | `"MMAP"` | Change to `"HEAP"` for low-latency small tables |
| `ingestionConfig.segmentIngestionType` | `"APPEND"` | Change to `"REFRESH"` for full-rewrite workflows |
| `ingestionConfig.segmentIngestionFrequency` | `"DAILY"` | Match your actual ingestion cadence |
| `tenants.broker` | `"DefaultTenant"` | Change if your cluster uses custom tenants |
| `tenants.server` | `"DefaultTenant"` | Change if your cluster uses custom tenants |
| `streamConfigs.realtime.segment.flush.threshold.rows` | `"1000000"` | Tune for throughput/latency |
| `streamConfigs.realtime.segment.flush.threshold.time` | `"1h"` | Tune for commit frequency |
| `batch-job.json controllerURI` | `"http://localhost:9000"` | Always set to actual cluster address |
| `batch-job.json outputDirURI` | `/tmp/pinot-output/{datasource}` | Set to persistent storage |

---

## OFFLINE Table Defaults

### replication: "1"

**Default:** `"1"` (single copy)

**Why the tool generates this:** Safe for local testing and development. A single replica
avoids the need to provision multiple server nodes just to run `dpm generate`.

**When to change:** Always change to `"3"` for production. A replication factor of 1
means any server failure loses the segment data.

```bash
jq '.segmentsConfig.replication = "3"' table-offline.json > tmp.json
mv tmp.json table-offline.json
```

---

### retentionTimeValue: "365" / retentionTimeUnit: "DAYS"

**Default:** 365 days.

**Why the tool generates this:** A one-year default matches common data warehouse practices
without risking immediate data deletion. Pinot enforces retention at the segment level.

**When to change:** Set to match your organisation's data retention policy. Common values:
- 7 (one week of hot data)
- 30 (monthly rolling window)
- 90 (quarterly)
- 730 (two years)
- 0 (disabled — Pinot never deletes segments)

```bash
# Set retention to 90 days
jq '.segmentsConfig.retentionTimeValue = "90"' table-offline.json > tmp.json
mv tmp.json table-offline.json
```

> **Note:** `retentionTimeValue = "0"` disables retention. Pinot will keep segments
> indefinitely. Use this when you manage data lifecycle externally.

---

### timeType: "MILLISECONDS"

**Default:** `"MILLISECONDS"` in `segmentsConfig.timeType`.

**Why the tool generates this:** Milliseconds is the most common epoch unit in web and
application data. The generated `dateTimeFieldSpec` format is derived from the Druid spec
and will already match.

**When to change:** Only if you need `"SECONDS"`, `"MICROSECONDS"`, or `"NANOSECONDS"`.
If the time column format is `seconds` or `posix`, the schema will use `1:SECONDS:EPOCH`
and `segmentsConfig.timeType` should match.

---

### segmentAssignmentStrategy: "BalanceNumSegmentAssignmentStrategy"

**Default:** Balance segments evenly across servers by count.

**Why the tool generates this:** The balanced strategy works correctly for all cluster
sizes including single-node deployments.

**When to change:** Replace with `"ReplicaGroupSegmentAssignmentStrategy"` if you need
routing guarantees for deterministic fan-out (required for replica-group routing).

---

### loadMode: "MMAP"

**Default:** `"MMAP"` — memory-mapped I/O. Segments are stored on disk and paged into
memory on demand.

**Why the tool generates this:** MMAP minimises JVM heap usage, which prevents out-of-memory
errors on any server size.

**When to change:** Change to `"HEAP"` if:
- The table is small (< 1 GB total data)
- You need the lowest possible query latency
- The server has enough JVM heap to hold the full dataset

```json
"tableIndexConfig": {
  "loadMode": "HEAP"
}
```

---

### segmentIngestionType: "APPEND"

**Default:** `"APPEND"` — each batch run adds new segments without replacing old ones.

**Why the tool generates this:** APPEND is always safe. It prevents accidental data deletion
and works for both new data loads and backfills.

**When to change:** Change to `"REFRESH"` when each batch run replaces the full dataset
(e.g., snapshot tables, daily full-dump ingestion). REFRESH deletes existing segments for
the time range before writing new ones.

> **Caution:** REFRESH with incorrect time ranges can delete more data than intended.
> Test in a non-production environment first.

---

### segmentIngestionFrequency: "DAILY"

**Default:** `"DAILY"` — the Minion task scheduler runs once per day.

**Why the tool generates this:** Daily is a conservative default that avoids creating
excessive segments.

**When to change:** Set to match your actual ingestion cadence:
- `"HOURLY"` — data arrives hourly, segment per hour
- `"DAILY"` — default, segment per day
- `"WEEKLY"` — weekly batch loads
- `"MONTHLY"` — monthly loads

---

## REALTIME Table Defaults

### replication: "1"

Same as OFFLINE. Change to `"3"` for production.

---

### retentionTimeValue: "365" / retentionTimeUnit: "DAYS"

Same as OFFLINE. Set to match your retention policy.

---

### streamConfigs flush.threshold.rows: "1000000"

**Default:** Flush (commit) a segment after 1,000,000 rows.

**Why the tool generates this:** 1M rows is a reasonable starting point that keeps segment
files in the 50–500 MB range for typical JSON event sizes.

**When to change:**
- Decrease for lower-latency requirements (data becomes queryable sooner after ingestion)
- Increase for high-throughput topics to reduce segment count
- Typical production values: 100,000 (low latency) to 5,000,000 (high throughput)

```json
"realtime.segment.flush.threshold.rows": "500000"
```

---

### streamConfigs flush.threshold.time: "1h"

**Default:** Flush a segment after 1 hour regardless of row count.

**Why the tool generates this:** The time-based threshold acts as a safety net. Even for
low-volume topics, segments are committed within 1 hour so data becomes queryable.

**When to change:**
- `"6h"` or `"12h"` for low-volume topics with hourly SLAs
- `"15m"` or `"30m"` for real-time dashboards requiring fresh data
- Must be combined with the row threshold; whichever triggers first wins

```json
"realtime.segment.flush.threshold.time": "30m"
```

---

### streamConfigs broker.list: "localhost:9092"

**Default:** Single local Kafka broker.

**Why the tool generates this:** The tool extracts `bootstrap.servers` from
`ioConfig.consumerProperties` when present. If missing (or for Kinesis sources),
the fallback is `localhost:9092`.

**When to change:** Always — replace with your actual Kafka broker addresses:

```json
"stream.kafka.broker.list": "kafka-1:9092,kafka-2:9092,kafka-3:9092"
```

---

### streamConfigs consumer.type: "lowlevel"

**Default:** `"lowlevel"` — each Pinot server instance manages its own partition offsets.

**Why the tool generates this:** Low-level consumer provides the most control and is the
standard for production Pinot deployments.

**When to change:** Change to `"highlevel"` only if using the deprecated Kafka high-level
consumer API (Kafka consumer groups). The low-level consumer is preferred for all new
deployments.

---

### streamConfigs decoder: JSONMessageDecoder

**Default:** `org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder`

**Why the tool generates this:** JSON is the most common message format in Druid streaming
specs.

**When to change:** If your Kafka topic uses a different format:
- Avro: `org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder`
- Thrift: `org.apache.pinot.plugin.inputformat.thrift.ThriftMessageDecoder`
- Protobuf: `org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder`

---

## Batch Job Defaults

### controllerURI: "http://localhost:9000"

**Default:** Local Controller.

**Why the tool generates this:** Placeholder that makes the job spec syntactically valid.

**When to change:** Always — set to your actual Controller address before running any
ingestion job:

```bash
jq '.pinotClusterSpecs[0].controllerURI = "http://pinot-controller:9000"' \
  batch-job.json > tmp.json && mv tmp.json batch-job.json
```

---

### outputDirURI: "/tmp/pinot-output/{datasource}"

**Default:** Temporary local directory.

**Why the tool generates this:** `/tmp` is always present and writable. Valid for local
testing.

**When to change:** For production runs, set to a persistent location with enough disk
space to hold generated segments:

```json
"outputDirURI": "s3://my-pinot-segments/output/pageviews"
```

---

### jobType: "SegmentCreationAndTarPush"

**Default:** Creates segments and pushes them to the cluster in a single job.

**Why the tool generates this:** The most common workflow — creates segments locally
(or in a distributed executor) and pushes the tar-compressed segments to the Controller.

**When to change:**
- `"SegmentCreationAndUriPush"` — upload segment URIs to the Controller (segments remain
  at their source URI; Controller fetches them)
- `"SegmentCreationAndMetadataPush"` — only push metadata, segments served from their
  storage location

---

## Tenant Defaults

### broker: "DefaultTenant" / server: "DefaultTenant"

**Default:** Routes to all available brokers and servers.

**Why the tool generates this:** `DefaultTenant` is the pre-created tenant in every Pinot
cluster. It requires no additional configuration.

**When to change:** If your cluster uses tenant isolation (separate server groups for
different tables), specify the tenant tag:

```json
"tenants": {
  "broker": "analytics_tenant",
  "server": "analytics_tenant"
}
```

---

## Schema Defaults

### Dimension sort order: alphabetical

Dimensions are sorted alphabetically by name in the generated `dimensionFieldSpecs`.

**Why:** Reproducibility — the same spec always produces the same schema regardless of
input ordering.

**When to change:** Never required. Field order in the Pinot schema has no performance
impact. Pinot uses columnar storage; the schema order does not affect query execution.

---

### Metric sort order: alphabetical

Same as dimensions.

---

### dateTimeFieldSpec dataType: "LONG"

All generated time columns use `dataType: "LONG"`.

**Why:** Pinot requires time columns to be numeric. Even ISO string timestamps are parsed
to epoch millis at ingest time.

**When to change:** Never — this is a Pinot requirement, not a default.

---

## See Also

- [Generated Artifact Reference](artifacts.md) — Full structure of each file
- [CLI Reference](cli.md) — `dpm generate` options
- [Tutorial 18 — Production Checklist](../18-production-checklist.md) — Pre-deployment review
