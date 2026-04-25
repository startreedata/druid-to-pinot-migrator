# Tutorial 14 — Append-Mode Ingestion

**Pattern:** `appendToExisting: true` in `ioConfig`  
**Risk:** `INGESTION_BEHAVIOR_MISMATCH` (INFO, CERTAIN)  
**Typical use cases:** audit trails, immutable event logs, incremental batch loads

---

## What appendToExisting Does in Druid

When `appendToExisting: true` is set in Druid's ioConfig, new segments are **added to**
the existing datasource rather than overwriting overlapping time intervals. This is used
for incremental ingestion — each batch adds new segments without touching the existing ones.

Druid's default is `appendToExisting: false`, which replaces segments for the same
interval on each run (useful for reprocessing). Setting it to `true` means you are
accumulating data across many batch runs.

---

## Sample Druid Spec

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "audit_trail",
      "timestampSpec": {
        "column": "created_at",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "entity_type", "entity_id", "action", "actor_id", "tenant_id"
        ]
      },
      "metricsSpec": [
        {"type": "count", "name": "event_count"}
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
        "type": "s3",
        "prefixes": ["s3://audit-bucket/trail/2024/"]
      },
      "inputFormat": {"type": "json"},
      "appendToExisting": true
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate audit_trail_spec.json --out ./output/audit_trail
```

```
Risks detected: 1
  [INFO] INGESTION_BEHAVIOR_MISMATCH
    Druid and Pinot differ in how they handle late-arriving data, segment
    compaction, and upserts. If the source pipeline relies on Druid-specific
    behaviors (e.g. appendToExisting, compaction tasks), review the equivalent
    Pinot mechanisms.
    Evidence: appendToExisting=true in ioConfig
    Remediation: Review Pinot segment compaction and upsert documentation to
    replicate the desired behavior.
```

The INFO severity means this is **advisory**, not a blocker. The generated Pinot artifacts
are valid and deployable. You need to adjust your ingestion process, not the schema.

---

## Pinot's Equivalent: APPEND Mode

Pinot's batch ingestion table config supports two ingestion types:

```json
"ingestionConfig": {
  "batchIngestionConfig": {
    "segmentIngestionType": "APPEND",   -- or "REFRESH"
    "segmentIngestionFrequency": "DAILY"
  }
}
```

The generated config uses `APPEND` by default, which is the correct equivalent for
`appendToExisting: true`.

| Druid `appendToExisting` | Pinot `segmentIngestionType` | Behaviour |
|-------------------------|------------------------------|----------|
| `true` | `APPEND` | New segments are added; existing segments not touched |
| `false` (default) | `REFRESH` | Segments for the interval are replaced each run |

The generated table config already has `APPEND`:

```json
"ingestionConfig": {
  "batchIngestionConfig": {
    "segmentIngestionType": "APPEND",
    "segmentIngestionFrequency": "DAILY"
  }
}
```

---

## Segment Accumulation Over Time

In append mode, each batch run creates new segments for the same time window. Over time,
you accumulate many small segments that overlap in time. Both Druid and Pinot have compaction
mechanisms to merge small segments into larger ones for query efficiency.

### Pinot Segment Compaction

Pinot's Controller has a built-in minion task framework for compaction:

```json
{
  "tableName": "audit_trail_OFFLINE",
  "task": {
    "taskTypeConfigsMap": {
      "MergeRollupTask": {
        "audit_trail_OFFLINE.merge.type": "concat",
        "audit_trail_OFFLINE.merge.mergeLevel.0.maxNumRecordsPerSegment": "10000000",
        "audit_trail_OFFLINE.merge.mergeLevel.0.bucketTimePeriod": "1d"
      }
    }
  }
}
```

Configure this in the table config's `task` section. The Controller will automatically
trigger merge tasks on the Minion nodes.

### Manual Segment Compaction

For manual compaction, use the Admin API:

```bash
curl -X POST "http://pinot-controller:9000/tables/audit_trail_OFFLINE/tasks" \
  -H "Content-Type: application/json" \
  -d '{"taskType": "MergeRollupTask"}'
```

---

## Preventing Duplicates in Append Mode

Append mode can introduce duplicates if the same source files are processed twice
(e.g., after a failed job retry). Two strategies:

### 1. Idempotent segment naming

Pinot segment names are based on the input file path by default in many ingestion jobs.
If the same file is reprocessed, the segment name collides and Pinot replaces the old
segment with the new one (same name = same segment → replace).

Verify your ingestion job configuration generates deterministic segment names from
input paths:

```json
{
  "segmentNameGeneratorSpec": {
    "type": "normalizedDate",
    "columnName": "created_at",
    "dateFormat": "yyyy-MM-dd"
  }
}
```

### 2. Upsert table (for update workloads)

If your audit trail can have updated records (same `entity_id` + `action` with a newer
state), consider a Pinot UPSERT table:

```json
{
  "upsertConfig": {
    "mode": "PARTIAL",
    "partialUpsertStrategies": {
      "action": "OVERWRITE",
      "entity_id": "OVERWRITE"
    },
    "comparisonColumn": "created_at"
  }
}
```

Note: Upsert requires a REALTIME table and a primary key column. For pure event logs
(immutable), append mode without upsert is correct.

---

## Monitoring Segment Count

In append mode, monitor segment count to ensure compaction is keeping up with growth:

```bash
# Check segment count via Pinot API
curl "http://pinot-controller:9000/tables/audit_trail_OFFLINE/segments/metadata" \
  | jq 'length'
```

High segment counts (> 1000) hurt query performance. Schedule regular compaction tasks.

---

## Difference from Druid Compaction

Druid's compaction rewrites segments in the same time range and can also apply rollup
during compaction. Pinot's `MergeRollupTask` merges segments but applies rollup only
if `aggregateMetrics: true` is set in the table config.

For an audit trail (no rollup), use `merge.type: "concat"` — this concatenates
segments without any metric aggregation.

---

## See Also

- [Tutorial 02 — Raw Event Table](02-raw-event-table.md) — base table config
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — INGESTION_BEHAVIOR_MISMATCH
- [Tutorial 18 — Production Checklist](18-production-checklist.md) — operational verification
