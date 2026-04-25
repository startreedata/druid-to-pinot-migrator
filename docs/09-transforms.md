# Tutorial 09 — Ingestion-Time Transforms

**Pattern:** `transformSpec` in dataSchema  
**Risk:** `TRANSFORM_PORTABILITY_RISK` (MEDIUM, LIKELY)  
**Typical use cases:** field renaming, type coercion, conditional enrichment, URL normalisation

---

## What Druid Transforms Do

Druid's `transformSpec` lets you compute derived fields at ingestion time using Druid's
expression language. Transforms run before dimensions are extracted and after raw events
are parsed. Common uses:

- Renaming a field from a source name to a cleaner column name
- Extracting a sub-field from a JSON path
- Normalising a URL by stripping query parameters
- Computing a category label from raw enum values
- Applying conditional logic (`if`, `case`) to map raw codes to human-readable values

---

## The Portability Problem

**Pinot does not support ingestion-time expression transforms** in the same way. Pinot's
ingestion pipeline can apply simple field renames and type casts, but arbitrary expression
evaluation (regex, conditionals, string functions) is not available at segment creation time.

The tool detects non-trivial transforms and raises `TRANSFORM_PORTABILITY_RISK`:
- Simple renames (field A → field B) are low-risk.
- Complex expressions (`regexp_replace`, `if`, `case`, `concat`, `coalesce`) are flagged
  as MEDIUM risk and require upstream implementation.

---

## Sample Druid Spec

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "enriched_clicks",
      "timestampSpec": {
        "column": "click_time",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "session_id",
          "user_id",
          "device_type",
          "normalized_url",
          "campaign_label",
          "risk_tier"
        ]
      },
      "metricsSpec": [
        {"type": "count",     "name": "click_count"},
        {"type": "doubleSum", "name": "bid_sum", "fieldName": "bid_price"}
      ],
      "granularitySpec": {
        "segmentGranularity": "HOUR",
        "queryGranularity": "NONE",
        "rollup": false
      },
      "transformSpec": {
        "transforms": [
          {
            "type": "expression",
            "name": "normalized_url",
            "expression": "regexp_replace(url, '\\?.*$', '')"
          },
          {
            "type": "expression",
            "name": "campaign_label",
            "expression": "if(campaign_id != null, concat(campaign_id, '_', channel), 'organic')"
          },
          {
            "type": "expression",
            "name": "risk_tier",
            "expression": "case(fraud_score < 0.3, 'low', fraud_score < 0.7, 'medium', 'high')"
          }
        ],
        "filter": {
          "type": "not",
          "field": {
            "type": "selector",
            "dimension": "bot_flag",
            "value": "true"
          }
        }
      }
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate enriched_clicks_spec.json --out ./output/enriched_clicks
```

Output:

```
Classification : raw_event
Risks: 1
  [MEDIUM] TRANSFORM_PORTABILITY_RISK
    Druid transform expressions use Druid's built-in expression language. Pinot
    does not support ingestion-time expression transforms directly.
    Evidence: Non-trivial transforms: normalized_url, campaign_label, risk_tier
    Remediation: Re-implement transform logic upstream in the ETL pipeline or use
    Pinot's groovy/SQL transform plugins if supported.
```

---

## What Gets Generated

The schema reflects the **output** of the transforms — the target column names:

```json
{
  "schemaName": "enriched_clicks",
  "dimensionFieldSpecs": [
    {"name": "campaign_label",  "dataType": "STRING"},
    {"name": "device_type",     "dataType": "STRING"},
    {"name": "normalized_url",  "dataType": "STRING"},
    {"name": "risk_tier",       "dataType": "STRING"},
    {"name": "session_id",      "dataType": "STRING"},
    {"name": "user_id",         "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "bid_sum",    "dataType": "DOUBLE"},
    {"name": "click_count","dataType": "LONG"}
  ]
}
```

---

## Migration Strategies

Choose the strategy that fits your architecture:

### Strategy 1: Upstream ETL (Recommended)

Apply the transforms in your data pipeline **before** data reaches Pinot. The transformed
fields land directly in the source records, requiring no per-column logic at ingest time.

**Kafka / Flink:**
```python
# Flink or Kafka Streams transform function
def enrich(event: dict) -> dict:
    # normalized_url: strip query string
    event["normalized_url"] = re.sub(r"\?.*$", "", event.get("url", ""))

    # campaign_label: conditional
    cid = event.get("campaign_id")
    ch  = event.get("channel", "")
    event["campaign_label"] = f"{cid}_{ch}" if cid else "organic"

    # risk_tier: case expression
    score = event.get("fraud_score", 0.0)
    event["risk_tier"] = "low" if score < 0.3 else ("medium" if score < 0.7 else "high")

    return event
```

**Spark (batch):**
```python
from pyspark.sql import functions as F

df = df.withColumn(
    "normalized_url", F.regexp_replace("url", r"\?.*$", "")
).withColumn(
    "campaign_label",
    F.when(F.col("campaign_id").isNotNull(),
           F.concat(F.col("campaign_id"), F.lit("_"), F.col("channel"))
    ).otherwise("organic")
).withColumn(
    "risk_tier",
    F.when(F.col("fraud_score") < 0.3, "low")
     .when(F.col("fraud_score") < 0.7, "medium")
     .otherwise("high")
)
```

After transformation, write the enriched records directly to Pinot via OFFLINE batch ingest.

### Strategy 2: Pinot Ingestion Transform

Pinot supports limited transform functions in the ingestion spec for batch jobs. These are
applied at segment creation time via `transformFunctionSpec`:

```json
{
  "fieldTypeMap": {
    "normalized_url": "DIMENSION",
    "campaign_label": "DIMENSION"
  },
  "transformFunctionSpec": [
    {
      "columnName": "normalized_url",
      "transformFunction": "regexp_replace(url, '\\?.*$', '')"
    }
  ]
}
```

Note: Pinot's transform functions are more limited than Druid's expression language.
`regexp_replace`, `concat`, and basic arithmetic are supported; complex branching (`case`,
`if`) may not be available depending on your Pinot version. Check your version's
[transform function documentation](https://docs.pinot.apache.org/developers/advanced/ingestion-level-transformations).

### Strategy 3: Query-Time Computation

Instead of storing the derived field, compute it in the SQL query:

```sql
-- Instead of storing normalized_url, query it:
SELECT REGEXP_REPLACE(url, '\?.*$', '') AS normalized_url,
       COUNT(*) AS cnt
FROM enriched_clicks_OFFLINE
GROUP BY normalized_url
```

This avoids the transform entirely but adds CPU cost to every query. Suitable for
infrequently queried derived fields.

---

## Handling the ingest-time Filter

The spec above also has a `filter` in `transformSpec` that excludes bot traffic:

```json
"filter": {
  "type": "not",
  "field": {"type": "selector", "dimension": "bot_flag", "value": "true"}
}
```

Pinot has no equivalent ingest-time filter. Implement this in your ETL:

```python
# Upstream filter — drop bot events before writing to Pinot
events = [e for e in events if e.get("bot_flag") != "true"]
```

Or add a `WHERE bot_flag != 'true'` clause to every query. The latter approach
retains bot events in storage — choose based on your storage and compliance requirements.

---

## Simple Renames Are Low-Risk

Not all transforms are risky. A simple field rename is portable:

```json
{
  "type": "expression",
  "name": "user_id",
  "expression": "userId"
}
```

This renames the source field `userId` to `user_id`. In Pinot, you can handle this with:
- A field rename in your ETL pipeline
- A `transformFunctionSpec` entry: `{"columnName": "user_id", "transformFunction": "userId"}`

The tool will still report `TRANSFORM_PORTABILITY_RISK` for non-trivial expressions but
simple renames will not appear in the evidence list.

---

## Query Translation After Upstream Transform

Once transforms are applied upstream, queries translate cleanly:

```sql
-- Druid:
SELECT normalized_url, COUNT(*) AS clicks
FROM "enriched_clicks"
GROUP BY normalized_url
ORDER BY clicks DESC
LIMIT 10

-- Pinot (same — normalized_url is now a stored column):
SELECT normalized_url, COUNT(*) AS clicks
FROM enriched_clicks_OFFLINE
GROUP BY normalized_url
ORDER BY clicks DESC
LIMIT 10
```

---

## See Also

- [Tutorial 10 — Nested JSON with flattenSpec](10-nested-json.md) — similar transform portability concerns
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — TRANSFORM_PORTABILITY_RISK details
