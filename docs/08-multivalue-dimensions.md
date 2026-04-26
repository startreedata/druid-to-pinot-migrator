# Tutorial 08 — Multi-Value Dimensions

**Pattern:** `multiValueHandling` in dimensionsSpec  
**Pinot table type:** OFFLINE  
**Risk:** `MULTIVALUE_AMBIGUITY` (MEDIUM)  
**Typical use cases:** content tags, user interests, product categories, skill sets

---

## What Multi-Value Dimensions Are

In Druid, a multi-value (MV) dimension stores an array of strings for a single column in
a single event. The classic example is a `tags` column that holds `["python", "java", "go"]`
for an article that has been labelled with multiple programming languages.

Druid declares MV dimensions explicitly using `multiValueHandling`:

```json
{
  "type": "string",
  "name": "tags",
  "multiValueHandling": "SORTED_ARRAY"
}
```

Pinot also supports MV columns but their **query semantics differ**:
- `GROUP BY tags` in Druid explodes each row once per tag value.
- `GROUP BY tags` in Pinot behaves similarly but the treatment of NULL entries and
  empty arrays may differ.
- `COUNT(DISTINCT tags)` in Druid counts distinct tag values across all rows.
- Pinot's MV `DISTINCTCOUNT` works per-column but the behaviour across correlated
  MV columns (e.g., `GROUP BY tags, categories`) may differ.

---

## Sample Druid Spec

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "content_tags",
      "timestampSpec": {
        "column": "published_at",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "content_id",
          "author",
          {
            "type": "string",
            "name": "tags",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "categories",
            "multiValueHandling": "SORTED_SET"
          },
          "language"
        ]
      },
      "metricsSpec": [
        {"type": "count", "name": "view_count"}
      ],
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
        "type": "local",
        "baseDir": "/data/content",
        "filter": "*.json"
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate content_tags_spec.json --out ./output/content_tags
```

You will see a MEDIUM risk:

```
Risks detected: 1
  [MEDIUM] MULTIVALUE_AMBIGUITY
    Multi-value dimensions in Druid are arrays of strings ingested and indexed
    as MV columns. Pinot supports MV columns but query semantics for aggregations
    over MV columns can differ. Verify COUNT DISTINCT and GROUP BY behavior.
    Evidence: Multi-value dimensions: tags, categories
    Remediation: Set singleValueField=false in Pinot schema for MV columns and
    validate GROUP BY / COUNT DISTINCT query results.
```

---

## What Gets Generated

### schema.json

```json
{
  "schemaName": "content_tags",
  "dimensionFieldSpecs": [
    {"name": "author",      "dataType": "STRING"},
    {"name": "categories",  "dataType": "STRING", "singleValueField": false},
    {"name": "content_id",  "dataType": "STRING"},
    {"name": "language",    "dataType": "STRING"},
    {"name": "tags",        "dataType": "STRING", "singleValueField": false}
  ],
  "metricFieldSpecs": [
    {"name": "view_count", "dataType": "LONG"}
  ],
  "dateTimeFieldSpecs": [
    {
      "name": "published_at",
      "dataType": "LONG",
      "format": "1:MILLISECONDS:EPOCH",
      "granularity": "1:MILLISECONDS"
    }
  ]
}
```

The tool automatically adds `"singleValueField": false` for dimensions that have
`multiValueHandling` set. This is what tells Pinot to treat the column as multi-value.

---

## Data Format for MV Columns

Both Druid and Pinot accept MV column values as JSON arrays:

```json
{
  "published_at": 1709251200000,
  "content_id": "art-001",
  "author": "alice",
  "tags": ["python", "tutorial", "beginner"],
  "categories": ["programming", "education"],
  "language": "en",
  "view_count": 1
}
```

Pinot also accepts delimited strings (e.g., `"python,tutorial,beginner"`) with a
field delimiter configured in the ingestion spec. JSON arrays are the recommended format.

---

## multiValueHandling Options

Druid's `multiValueHandling` controls how the array is stored internally:

| Druid option | Behaviour | Pinot equivalent |
|-------------|----------|-----------------|
| `SORTED_ARRAY` | Values sorted, duplicates retained | `singleValueField: false` |
| `SORTED_SET` | Values sorted, duplicates removed | `singleValueField: false` |
| `ARRAY` | Values in original order | `singleValueField: false` |

Pinot does not preserve insertion order for MV dimensions internally (values are stored
in dictionary-encoded form). The `SORTED_SET` de-duplication behaviour should be
replicated in your ETL pipeline before ingest if required.

---

## Query Translation

### Count content items per tag

```sql
-- Druid:
SELECT tags, COUNT(*) AS cnt
FROM "content_tags"
GROUP BY 1
ORDER BY cnt DESC

-- Pinot (same, because GROUP BY on MV column explodes by value):
SELECT tags, COUNT(*) AS cnt
FROM content_tags_OFFLINE
GROUP BY tags
ORDER BY cnt DESC
```

### Content items tagged with 'python'

```sql
-- Druid:
SELECT content_id, author
FROM "content_tags"
WHERE MV_CONTAINS(tags, 'python')

-- Pinot:
SELECT content_id, author
FROM content_tags_OFFLINE
WHERE ARRAYLENGTH(FILTER(x -> x = 'python', tags)) > 0
-- or use the text match function in Pinot:
WHERE tags = 'python'   -- Pinot uses equality match across MV columns
```

Pinot's filter syntax for MV columns uses equality directly — `WHERE tags = 'python'`
returns rows where `'python'` is one of the values in the `tags` array.

### Distinct tag count

```sql
-- Druid:
SELECT COUNT(DISTINCT tags) AS tag_count FROM "content_tags"

-- Pinot:
SELECT DISTINCTCOUNT(tags) AS tag_count FROM content_tags_OFFLINE
```

Note: `DISTINCTCOUNT` in Pinot counts distinct values across all rows for an MV column,
which may differ from Druid depending on how null/empty arrays are handled.

---

## Known Semantic Differences

| Query type | Druid behaviour | Pinot behaviour | Risk |
|-----------|----------------|----------------|------|
| `GROUP BY mv_col` | One row per distinct value | One row per distinct value | Same |
| `COUNT(DISTINCT mv_col)` | Distinct values across all rows | `DISTINCTCOUNT(mv_col)` — same | Same |
| `WHERE mv_col = 'value'` | Rows containing that value | Rows containing that value | Same |
| `COUNT(*)` per group | Event count | Row count (segment rows) | Differ if rolled-up |
| Correlated MV GROUP BY | `GROUP BY tags, categories` — each (tag, category) pair | Implementation may differ | Verify |
| NULL / empty arrays | Empty array → null in GROUP BY | May vary | Test edge cases |

---

## Testing MV Query Parity

After migration, run these verification queries:

```sql
-- Tag distribution should match:
-- Druid:
SELECT tags, COUNT(*) AS cnt FROM "content_tags" GROUP BY 1 ORDER BY 1
-- Pinot:
SELECT tags, COUNT(*) AS cnt FROM content_tags_OFFLINE GROUP BY tags ORDER BY tags

-- Total distinct tags should match:
-- Druid:
SELECT COUNT(DISTINCT tags) FROM "content_tags"
-- Pinot:
SELECT DISTINCTCOUNT(tags) FROM content_tags_OFFLINE

-- Specific tag filter should return same row count:
-- Druid:
SELECT COUNT(*) FROM "content_tags" WHERE MV_CONTAINS(tags, 'python')
-- Pinot:
SELECT COUNT(*) FROM content_tags_OFFLINE WHERE tags = 'python'
```

---

## Production Considerations

1. **Bitmap indexes**: Druid creates bitmap indexes for MV columns by default
   (`createBitmapIndex: true`). Pinot also builds inverted indexes for MV columns
   automatically when `invertedIndexColumns` includes the column name.

2. **Cardinality**: High-cardinality MV columns (e.g., user IDs in a `liked_by` column)
   can cause memory pressure in Pinot because MV columns are dictionary-encoded per segment.
   Profile cardinality before migrating large MV columns.

3. **Null handling**: Pinot returns `null` for missing MV columns by default.
   Druid returns an empty string. Adjust downstream consumers accordingly.

---

## See Also

- [Tutorial 02 — Raw Event Table](02-raw-event-table.md) — baseline batch pattern
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — MULTIVALUE_AMBIGUITY details
