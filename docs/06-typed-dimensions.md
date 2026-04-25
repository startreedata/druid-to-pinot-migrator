# Tutorial 06 — Typed Dimensions: Long, Float, Double

**Pattern:** `dimensionsSpec` with typed dimension entries  
**Pinot table type:** OFFLINE  
**Classification:** `raw_event`  
**Typical use cases:** product catalogs, user profiles, sensor data with numeric attributes

---

## Why Dimension Types Matter

In Druid, dimensions default to `string` unless explicitly typed. When you declare a
dimension as `long`, `float`, or `double`, Druid stores it with native numeric encoding —
this matters for range filters (`WHERE price > 10`), numeric ordering, and storage.

Pinot schema requires explicit `dataType` for every field. The tool maps Druid dimension
types directly:

| Druid type | Pinot dataType |
|-----------|---------------|
| `string` (default) | `STRING` |
| `long` | `LONG` |
| `float` | `FLOAT` |
| `double` | `DOUBLE` |
| `complex` / sketch types | `BYTES` (triggers HIGH risk) |

---

## Sample Druid Spec

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "product_catalog",
      "timestampSpec": {
        "column": "last_updated",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "product_id",
          "category",
          {"type": "long",   "name": "user_id"},
          {"type": "float",  "name": "price"},
          {"type": "double", "name": "rating"},
          {"type": "long",   "name": "stock_count"}
        ]
      },
      "metricsSpec": [],
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE",
        "rollup": false,
        "intervals": ["2024-01-01/2025-01-01"]
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "uris": ["s3://catalog-bucket/products/"]
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate product_catalog_spec.json --out ./output/product_catalog
```

No risks should be detected for a pure typed-dimension spec.

---

## What Gets Generated

### schema.json

```json
{
  "schemaName": "product_catalog",
  "dimensionFieldSpecs": [
    {"name": "category",    "dataType": "STRING"},
    {"name": "price",       "dataType": "FLOAT"},
    {"name": "product_id",  "dataType": "STRING"},
    {"name": "rating",      "dataType": "DOUBLE"},
    {"name": "stock_count", "dataType": "LONG"},
    {"name": "user_id",     "dataType": "LONG"}
  ],
  "metricFieldSpecs": [],
  "dateTimeFieldSpecs": [
    {
      "name": "last_updated",
      "dataType": "LONG",
      "format": "1:MILLISECONDS:EPOCH",
      "granularity": "1:MILLISECONDS"
    }
  ]
}
```

---

## Float vs. Double Precision

In Pinot, `FLOAT` is single-precision (32-bit) and `DOUBLE` is double-precision (64-bit).
Druid's `float` dimension is also 32-bit. This mapping is faithful.

For price fields, be aware that `FLOAT` can represent values only to about 7 significant
decimal digits. If you store `9.99` as a float, it may round to `9.99000072479248` when
retrieved. If exact decimal representation is important (billing, financial), use `DOUBLE`
or store as STRING.

```sql
-- Float precision loss example in Pinot:
SELECT price FROM product_catalog_OFFLINE WHERE product_id = 'P001'
-- May return: 9.990000724792480 instead of 9.99
```

**Recommendation:** Upgrade `float` dimensions that carry financial values to `double` in
the Pinot schema. Edit `schema.json` before deploying:

```json
{"name": "price", "dataType": "DOUBLE"}  -- Changed from FLOAT
```

---

## Long vs. Integer

Pinot does not have an `INT`/`INTEGER` type in the schema spec — `LONG` covers all
64-bit integers. Druid's `long` dimension maps cleanly to Pinot's `LONG`.

If your source data sends `int32` values and storage is a concern, you can still declare
`LONG` in Pinot — it will store correctly but use slightly more space than a native int
column would.

---

## Query Translation

Typed dimensions work identically for range filters and aggregations:

```sql
-- Price range filter
-- Druid:
SELECT product_id, price FROM "product_catalog" WHERE price BETWEEN 10.0 AND 50.0

-- Pinot (same):
SELECT product_id, price FROM product_catalog_OFFLINE WHERE price BETWEEN 10.0 AND 50.0
```

```sql
-- Average rating by category
-- Druid:
SELECT category, AVG(rating) AS avg_rating
FROM "product_catalog"
GROUP BY category
ORDER BY avg_rating DESC

-- Pinot (same):
SELECT category, AVG(rating) AS avg_rating
FROM product_catalog_OFFLINE
GROUP BY category
ORDER BY avg_rating DESC
```

```sql
-- Stock count aggregation
-- Druid:
SELECT category, SUM(stock_count) AS total_stock
FROM "product_catalog"
GROUP BY category

-- Pinot (same):
SELECT category, SUM(stock_count) AS total_stock
FROM product_catalog_OFFLINE
GROUP BY category
```

---

## String Dimensions with Numeric Values

A common anti-pattern in Druid is storing numeric IDs as strings because Druid defaults
to `string` when no type is specified:

```json
"dimensions": ["product_id", "user_id", "order_id"]
```

If these are actually numeric IDs, they will be `STRING` in both Druid and the generated
Pinot schema. This is usually fine for lookups and joins. If you need to perform numeric
operations (range filters, arithmetic), you should either:
1. Declare them with explicit types in `dimensionsSpec`.
2. Use `CAST(user_id AS LONG)` in queries.
3. Edit the generated schema to change the type, then ensure your data has no non-numeric values.

---

## Null Handling in Numeric Dimensions

Druid uses `0` as the default value for missing numeric dimensions (unless configured
with `useDefaultValueForNull: false`). Pinot also uses `0` for missing numeric columns
by default.

If your data can have true nulls and you rely on null-sensitive queries (e.g.,
`WHERE price IS NULL`), test carefully — behaviour may differ between the two systems.

---

## Indexed Numeric Columns

Pinot supports additional index types for numeric columns that Druid does not:

- **Range index** (`rangeIndexColumns`) — significantly speeds up `BETWEEN` and `>/<`
  filters on high-cardinality numeric columns.
- **Sorted index** — if data arrives sorted by a numeric column (e.g., timestamp),
  you can designate it as sorted for optimal range queries.

Add these to the table config after migration:

```json
"tableIndexConfig": {
  "rangeIndexColumns": ["price", "rating", "stock_count"]
}
```

---

## See Also

- [Tutorial 07 — Min/Max Metrics](07-minmax-metrics.md) — numeric aggregation functions
- [Tutorial 02 — Raw Event Table](02-raw-event-table.md) — baseline batch ingestion
- [Reference: Type Mapping](reference/type-mapping.md) — full Druid → Pinot type table
