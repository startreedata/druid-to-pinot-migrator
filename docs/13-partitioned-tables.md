# Tutorial 13 — Partitioned Tables

**Pattern:** `partitionsSpec` in `tuningConfig`  
**Risk:** `PARTITIONING_CONFIG_REQUIRED` (MEDIUM, CERTAIN)  
**Typical use cases:** large tables that benefit from segment pruning, data locality for joins

---

## Druid Partitioning vs. Pinot Partitioning

Both systems support partitioning data across segments/shards, but the terminology and
configuration paths differ.

| Concept | Druid | Pinot |
|---------|-------|-------|
| Config location | `tuningConfig.partitionsSpec` | `tableIndexConfig.segmentPartitionConfig` |
| Hash partitioning | `type: "hashed"` with `numShards` | `columnPartitionMap` with `Murmur` function |
| Range partitioning | `type: "range"` with `partitionDimensions` | `columnPartitionMap` with `BoundedColumnValue` |
| Dynamic (auto) | `type: "dynamic"` (default) | Not applicable — use `numPartitions` tuning |
| Effect | Determines which historical node serves a segment | Enables partition pruning in queries |

---

## Sample Druid Spec (Hash Partitioned)

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "orders",
      "timestampSpec": {
        "column": "order_time",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "order_id", "customer_id", "product_id", "status"
        ]
      },
      "metricsSpec": [
        {"type": "count",     "name": "order_count"},
        {"type": "doubleSum", "name": "total_amount", "fieldName": "amount"}
      ],
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "rollup": false
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {"type": "s3", "uris": ["s3://data-bucket/orders/"]},
      "inputFormat": {"type": "json"}
    },
    "tuningConfig": {
      "type": "index_parallel",
      "partitionsSpec": {
        "type": "hashed",
        "numShards": 4,
        "partitionDimensions": ["customer_id", "product_id"]
      },
      "maxNumConcurrentSubTasks": 4
    }
  }
}
```

---

## Running the Migration

```bash
dpm generate orders_spec.json --out ./output/orders
```

```
Risks detected: 1
  [MEDIUM] PARTITIONING_CONFIG_REQUIRED
    Druid partitionsSpec (hash, range, or dynamic) controls segment distribution
    across historicals. Pinot uses tableIndexConfig.segmentPartitionConfig for
    similar behavior. Configure Pinot partitioning manually.
    Evidence: Druid partitionsSpec type='hashed' detected in tuningConfig
    Remediation: Configure tableIndexConfig.segmentPartitionConfig in the Pinot
    table config with equivalent columnPartitionMap entries.
```

---

## What Gets Generated

A standard table config is generated without partition configuration. You must add
`segmentPartitionConfig` manually.

```json
{
  "tableName": "orders_OFFLINE",
  "tableType": "OFFLINE",
  "tableIndexConfig": {
    "loadMode": "MMAP"
  }
}
```

---

## Adding Pinot Partition Configuration

### Hash Partitioning (equivalent to Druid `hashed`)

Add `segmentPartitionConfig` to the table config:

```json
{
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "segmentPartitionConfig": {
      "columnPartitionMap": {
        "customer_id": {
          "functionName": "Murmur",
          "numPartitions": 4
        }
      }
    }
  }
}
```

Notes:
- Druid's `numShards: 4` maps to Pinot's `numPartitions: 4`.
- Druid supports multi-column hash partitioning (`partitionDimensions`). Pinot's `Murmur`
  function partitions on a single column. If Druid uses multiple partition dimensions,
  choose the highest-cardinality one as the Pinot partition key (usually the primary
  dimension used in filter predicates).
- Pinot uses Murmur2 hash by default. If exact data distribution parity is needed,
  verify the hash function compatibility.

### Range Partitioning (equivalent to Druid `range`)

For range-partitioned tables, use `BoundedColumnValue`:

```json
{
  "tableIndexConfig": {
    "segmentPartitionConfig": {
      "columnPartitionMap": {
        "sensor_id": {
          "functionName": "BoundedColumnValue",
          "numPartitions": 8,
          "partitionFunctionConfig": {
            "columnValues": ["sensor_100", "sensor_200", "sensor_300",
                             "sensor_400", "sensor_500", "sensor_600", "sensor_700"]
          }
        }
      }
    }
  }
}
```

For numeric ranges, `BoundedColumnValue` uses sorted string comparison. Pre-determine
the bucket boundaries from your data distribution before configuring.

---

## How Partitioning Helps Performance

### Partition pruning

When you filter on the partition key in a query, Pinot will only scan segments that
could contain matching rows:

```sql
SELECT order_id, total_amount
FROM orders_OFFLINE
WHERE customer_id = 'CU-12345'   -- Pinot prunes to the partition containing CU-12345
ORDER BY order_time DESC
LIMIT 20
```

Without partitioning, this query scans all segments. With partitioning on `customer_id`,
only `1/numPartitions` of segments are scanned.

### Data locality

Segments belonging to the same partition are co-located on the same server nodes,
improving join performance when using Pinot's multi-stage query engine.

---

## Verifying Partitioning is Active

After deploying the table with `segmentPartitionConfig`, check that segments are labelled
with their partition:

```bash
curl http://pinot-controller:9000/tables/orders_OFFLINE/segments
```

Each segment should include partition metadata in its name or metadata fields.

You can also verify with an `EXPLAIN PLAN`:
```sql
EXPLAIN PLAN FOR
SELECT COUNT(*) FROM orders_OFFLINE
WHERE customer_id = 'CU-12345'
```

Look for `PARTITION_ID` in the plan output to confirm pruning is occurring.

---

## When Partitioning Is Optional

Partitioning adds operational complexity: segment creation jobs must hash records
consistently, and the `numPartitions` value is difficult to change after the fact
(requires re-ingestion).

Consider skipping partitioning if:
- Your table is small (< 50M rows)
- Most queries scan the full table (no partition-key filters)
- Your queries already benefit from time-based pruning (usually sufficient)

Consider enabling partitioning if:
- Queries frequently filter on a single high-cardinality dimension (user_id, customer_id)
- You need data locality for large join operations
- Table is very large (> 500M rows) and full scans are too slow

---

## Range Partitioning Spec Example

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "sensor_readings",
      ...
    },
    "tuningConfig": {
      "type": "index_parallel",
      "partitionsSpec": {
        "type": "range",
        "partitionDimensions": ["sensor_id"],
        "targetRowsPerSegment": 5000000
      }
    }
  }
}
```

Pinot equivalent (manual range boundaries needed):

```json
{
  "tableIndexConfig": {
    "segmentPartitionConfig": {
      "columnPartitionMap": {
        "sensor_id": {
          "functionName": "BoundedColumnValue",
          "numPartitions": 16,
          "partitionFunctionConfig": {
            "columnValues": ["T0100", "T0200", "T0300", "T0400",
                             "T0500", "T0600", "T0700", "T0800",
                             "T0900", "T1000", "T1100", "T1200",
                             "T1300", "T1400", "T1500"]
          }
        }
      }
    }
  }
}
```

The 15 boundary values create 16 buckets. Values ≤ T0100 go to bucket 0, T0101–T0200
go to bucket 1, etc.

---

## See Also

- [Tutorial 02 — Raw Event Table](02-raw-event-table.md) — table config structure
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — PARTITIONING_CONFIG_REQUIRED
- [Reference: Artifacts](reference/artifacts.md) — full table config structure
