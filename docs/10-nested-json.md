# Tutorial 10 — Nested JSON with flattenSpec

**Pattern:** `flattenSpec` in `inputFormat`  
**Risk:** `FLATTEN_SPEC_NOT_PORTABLE` (HIGH, CERTAIN)  
**Typical use cases:** API request logs, auth event logs, nested event payloads

---

## What flattenSpec Does

Druid's `flattenSpec` extracts nested fields from JSON payloads at ingest time using
JSONPath expressions. It allows you to pull values from deeply nested objects or
transform them using `jq`-style expressions without modifying the source data.

Example JSON event:
```json
{
  "request_time": 1709251200000,
  "request_id": "req-abc-123",
  "auth_context": {
    "userId": "user-42",
    "tenantId": "acme-corp"
  },
  "network": {
    "remoteAddress": "203.0.113.55"
  },
  "request": {
    "method": "POST",
    "path": "/api/v2/events?source=mobile&format=json"
  },
  "response": {
    "status": 201
  }
}
```

---

## Sample Druid Spec with flattenSpec

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "api_requests",
      "timestampSpec": {
        "column": "request_time",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "request_id", "endpoint", "method",
          "user_id", "tenant_id", "client_ip", "response_status"
        ]
      },
      "metricsSpec": [
        {"type": "count",   "name": "request_count"},
        {"type": "longSum", "name": "response_time_ms_sum", "fieldName": "response_time_ms"},
        {"type": "longMax", "name": "response_time_ms_max", "fieldName": "response_time_ms"}
      ],
      "granularitySpec": {
        "segmentGranularity": "HOUR",
        "queryGranularity": "MINUTE",
        "rollup": false
      },
      "transformSpec": {
        "transforms": [
          {"type": "expression", "name": "user_id",
           "expression": "json_value(auth_context, '$.userId')"},
          {"type": "expression", "name": "tenant_id",
           "expression": "json_value(auth_context, '$.tenantId')"}
        ]
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "prefixes": ["s3://api-logs-bucket/requests/"]
      },
      "inputFormat": {
        "type": "json",
        "flattenSpec": {
          "useFieldDiscovery": false,
          "fields": [
            {"type": "path", "name": "user_id",   "expr": "$.auth_context.userId"},
            {"type": "path", "name": "tenant_id", "expr": "$.auth_context.tenantId"},
            {"type": "path", "name": "client_ip", "expr": "$.network.remoteAddress"},
            {"type": "jq",   "name": "endpoint",  "expr": ".request.path | split(\"?\")[0]"}
          ]
        }
      }
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate api_requests_spec.json --out ./output/api_requests
```

You will see a HIGH risk:

```
Risks detected: 2
  [HIGH] FLATTEN_SPEC_NOT_PORTABLE
    Druid's flattenSpec extracts nested JSON fields at ingestion time using path
    expressions. Pinot does not support flattenSpec; nested field extraction must
    be implemented via Pinot's ingestion transform functions or upstream ETL.
    Evidence: flattenSpec with path expressions detected in inputFormat
    Remediation: Implement field extraction via Pinot ingestion transformations
    (e.g., JsonPathTransformer) or pre-flatten the JSON upstream before ingest.

  [MEDIUM] TRANSFORM_PORTABILITY_RISK
    Evidence: Non-trivial transforms: user_id, tenant_id
```

---

## What Gets Generated

The schema is generated from the declared dimension names (after the flattenSpec
has conceptually been applied):

```json
{
  "schemaName": "api_requests",
  "dimensionFieldSpecs": [
    {"name": "client_ip",       "dataType": "STRING"},
    {"name": "endpoint",        "dataType": "STRING"},
    {"name": "method",          "dataType": "STRING"},
    {"name": "request_id",      "dataType": "STRING"},
    {"name": "response_status", "dataType": "STRING"},
    {"name": "tenant_id",       "dataType": "STRING"},
    {"name": "user_id",         "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "request_count",        "dataType": "LONG"},
    {"name": "response_time_ms_max", "dataType": "LONG"},
    {"name": "response_time_ms_sum", "dataType": "LONG"}
  ]
}
```

The schema is correct — the problem is getting the nested fields extracted and
available as flat fields when the data reaches Pinot.

---

## Migration Strategy: Pre-Flatten Upstream

The cleanest approach is to flatten the JSON before data reaches Pinot. This removes any
dependency on Pinot-specific transformation capabilities.

### Using jq (batch preprocessing)

```bash
jq '{
  request_time: .request_time,
  request_id:   .request_id,
  user_id:      .auth_context.userId,
  tenant_id:    .auth_context.tenantId,
  client_ip:    .network.remoteAddress,
  method:       .request.method,
  endpoint:     (.request.path | split("?")[0]),
  response_status: .response.status
}' < raw_events.json > flat_events.json
```

### Using Python / Spark

```python
import re

def flatten_api_event(event: dict) -> dict:
    path = event.get("request", {}).get("path", "")
    return {
        "request_time":    event["request_time"],
        "request_id":      event["request_id"],
        "user_id":         event.get("auth_context", {}).get("userId"),
        "tenant_id":       event.get("auth_context", {}).get("tenantId"),
        "client_ip":       event.get("network", {}).get("remoteAddress"),
        "method":          event.get("request", {}).get("method"),
        "endpoint":        path.split("?")[0],
        "response_status": str(event.get("response", {}).get("status")),
        "response_time_ms": event.get("response_time_ms", 0),
    }
```

```python
# Spark version
from pyspark.sql import functions as F

df = df.withColumn(
    "user_id",   F.col("auth_context.userId")
).withColumn(
    "tenant_id", F.col("auth_context.tenantId")
).withColumn(
    "client_ip", F.col("network.remoteAddress")
).withColumn(
    "method",    F.col("request.method")
).withColumn(
    "endpoint",  F.split(F.col("request.path"), "\\?")[0]
).withColumn(
    "response_status", F.col("response.status").cast("string")
)
```

---

## Migration Strategy: Pinot JsonPathTransformer

For Pinot batch ingestion (segment creation), you can configure a `JsonPathTransformer`
in the ingestion job spec. This handles simple path extractions without pre-flattening:

```json
{
  "inputFormat": {
    "recordReaderSpec": {
      "dataFormat": "json",
      "className": "org.apache.pinot.plugin.inputformat.json.JSONRecordReader"
    }
  },
  "transformFunctionSpec": [
    {
      "columnName": "user_id",
      "transformFunction": "jsonPath(auth_context, '$.userId')"
    },
    {
      "columnName": "tenant_id",
      "transformFunction": "jsonPath(auth_context, '$.tenantId')"
    },
    {
      "columnName": "client_ip",
      "transformFunction": "jsonPath(network, '$.remoteAddress')"
    },
    {
      "columnName": "method",
      "transformFunction": "jsonPath(request, '$.method')"
    }
  ]
}
```

Note: `jq`-style transforms (like the `split("?")[0]` endpoint extraction) are not
supported by `JsonPathTransformer`. These must be handled upstream.

---

## Handling jq-Style Expressions

Druid supports `jq` expression type in `flattenSpec`:
```json
{"type": "jq", "name": "endpoint", "expr": ".request.path | split(\"?\")[0]"}
```

Pinot has no direct equivalent. Options:
1. **Pre-process upstream** — apply the split before writing to Pinot (recommended)
2. **Store full path, query-time split** — store the raw `request.path` and use Pinot's
   `REGEXP_EXTRACT` at query time:
   ```sql
   SELECT REGEXP_EXTRACT(request_path, '^([^?]+)') AS endpoint,
          COUNT(*) AS cnt
   FROM api_requests_OFFLINE
   GROUP BY endpoint
   ```

---

## Schema Validation After Flattening

After flattening, run the validation check to confirm the schema matches:

```bash
dpm validate api_requests_spec.json --generated-dir ./output/api_requests
```

If your flattened records match the schema field names exactly, the tool will report
no artifact mismatches.

---

## Query Translation

After the nested fields are flattened (by whatever method), queries work identically:

```sql
-- Requests per tenant
-- Druid:
SELECT tenant_id, COUNT(*) AS cnt
FROM "api_requests"
GROUP BY tenant_id
ORDER BY cnt DESC

-- Pinot:
SELECT tenant_id, COUNT(*) AS cnt
FROM api_requests_OFFLINE
GROUP BY tenant_id
ORDER BY cnt DESC
```

```sql
-- P99 endpoint latency (if response_time_ms is stored)
-- Druid:
SELECT endpoint, APPROX_QUANTILE_DS(response_time_ms, 0.99) AS p99
FROM "api_requests"
GROUP BY endpoint

-- Pinot (no direct APPROX_QUANTILE; use PERCENTILEEST or PERCENTILETDIGEST):
SELECT endpoint, PERCENTILEEST(response_time_ms_max, 99) AS p99
FROM api_requests_OFFLINE
GROUP BY endpoint
```

---

## See Also

- [Tutorial 09 — Transforms](09-transforms.md) — ingestion-time transform portability
- [Tutorial 12 — Sketch Aggregators](12-sketch-aggregators.md) — when using Druid sketches for percentile queries
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — FLATTEN_SPEC_NOT_PORTABLE
