# Tutorial 18 — Production Migration Checklist

A step-by-step checklist for safely migrating a Druid datasource to Pinot in production.
Follow this in order. Each phase has explicit go/no-go criteria.

---

## Phase 0: Pre-Migration Assessment

### Spec Analysis

```bash
# Run full inspection and capture output
dpm inspect druid-spec.json --json > inspection.json
dpm generate druid-spec.json --out ./migration-output --dry-run
cat ./migration-output/reports/migration-summary.md
```

- [ ] Confidence score ≥ 0.80
- [ ] No BLOCKING risks, OR a clear remediation plan for each blocking risk
- [ ] All HIGH risks reviewed and accepted or mitigated
- [ ] Datasource correctly classified (`raw_event`, `rolled_up_additive`, or `complex_aggregated`)
- [ ] Source data location confirmed (S3, GCS, Azure, etc.)

### Data Inventory

- [ ] Total row count documented (Druid: `SELECT COUNT(*) FROM "datasource"`)
- [ ] Time range of data confirmed (`SELECT MIN(__time), MAX(__time) FROM "datasource"`)
- [ ] Key dimensions and metrics documented
- [ ] Top 10 most frequent queries identified and collected
- [ ] Dashboard / BI tool query inventory taken

### Infrastructure

- [ ] Target Pinot cluster provisioned and healthy
- [ ] Storage allocated for segments (estimate: Druid segment size × 0.8–1.5)
- [ ] Pinot Controller, Broker, Server, and Minion reachable
- [ ] Cloud storage credentials configured (if GCS/Azure)
- [ ] Kafka/Kinesis connectivity confirmed (for streaming tables)

---

## Phase 1: Generate Artifacts

```bash
dpm generate druid-spec.json --out ./migration-output
```

- [ ] Generation completes with `result.success = true`
- [ ] `schema.json` exists in output directory
- [ ] `table-offline.json` or `table-realtime.json` exists
- [ ] `reports/migration-summary.md` reviewed by team

### Review Generated Schema

Open `migration-output/schema.json`:

- [ ] `schemaName` matches the expected Pinot table name
- [ ] All expected dimension columns are present with correct types
- [ ] All expected metric columns are present with correct types
- [ ] `dateTimeFieldSpecs` has exactly one entry with the correct column name
- [ ] Time format matches the source data (epoch millis vs. ISO vs. custom)
- [ ] No `BYTES`-type columns unless intentional (sketches → must be re-designed)
- [ ] Multi-value columns have `"singleValueField": false`

### Review Generated Table Config

Open `migration-output/table-offline.json` or `table-realtime.json`:

- [ ] `tableName` is `{datasource}_OFFLINE` or `{datasource}_REALTIME`
- [ ] `tableType` is correct (`OFFLINE` or `REALTIME`)
- [ ] `segmentsConfig.timeColumnName` matches schema time column
- [ ] `retentionTimeValue` and `retentionTimeUnit` set correctly for production
- [ ] `replication` set to `3` (not the default `1`)
- [ ] For REALTIME: `streamConfigs` updated for your actual Kafka/Kinesis cluster
- [ ] Tenant configuration correct for your Pinot cluster setup

### Apply Production Adjustments

Edit the generated files before deployment:

```bash
# Example: increase replication
jq '.segmentsConfig.replication = "3"' \
  migration-output/table-offline.json > migration-output/table-offline.tmp.json
mv migration-output/table-offline.tmp.json migration-output/table-offline.json

# Example: set production retention
jq '.segmentsConfig.retentionTimeValue = "730"' \
  migration-output/table-offline.json > tmp.json && mv tmp.json migration-output/table-offline.json
```

---

## Phase 2: Validate Artifacts

```bash
dpm validate druid-spec.json --generated-dir ./migration-output
```

- [ ] `overall_status: pass`
- [ ] All static checks pass
- [ ] All artifact checks pass
- [ ] No `fail` checks remain

---

## Phase 3: Deploy Schema and Table to Pinot

```bash
# Deploy schema
curl -X POST http://pinot-controller:9000/schemas \
  -H "Content-Type: application/json" \
  -d @migration-output/schema.json

# Deploy table
curl -X POST http://pinot-controller:9000/tables \
  -H "Content-Type: application/json" \
  -d @migration-output/table-offline.json
```

- [ ] Schema creation returns HTTP 200
- [ ] Table creation returns HTTP 200
- [ ] Schema appears in Pinot UI: `http://pinot-controller:9000/#/tables`
- [ ] Table appears in Pinot UI

---

## Phase 4: Ingest Data

### Batch table

Run the batch ingestion job:

```bash
# Update batch-job.json with correct inputDirURI and outputDirURI first
pinot-admin.sh LaunchDataIngestionJob \
  -jobSpecFile migration-output/batch-job.json
```

Monitor progress:

```bash
# Check segment upload status
watch -n 10 'curl -s "http://pinot-controller:9000/tables/my_table_OFFLINE/segments/metadata" | python3 -m json.tool | grep -c "segmentName"'
```

### Streaming table (Kafka/Kinesis)

For REALTIME tables, ingestion starts automatically after table creation. Verify:

```bash
# Check that consumers are active
curl http://pinot-controller:9000/tables/my_table_REALTIME/liveInstances
```

### Wait for data to be queryable

```bash
# Repeat until count > 0
curl -s "http://pinot-broker:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) AS cnt FROM my_table_OFFLINE"}' \
  | python3 -c "import sys,json; data=json.load(sys.stdin); print(data['resultTable']['rows'][0][0])"
```

- [ ] `SELECT COUNT(*)` returns a positive number
- [ ] Segment count matches expectations (at least 1 segment per day of data)

---

## Phase 5: Data Parity Verification

Run each of the following queries on both Druid and Pinot and compare results.

### Row count (raw event tables)

```sql
-- Druid:
SELECT COUNT(*) AS cnt FROM "datasource"

-- Pinot:
SELECT COUNT(*) AS cnt FROM my_table_OFFLINE
```

- [ ] Counts match exactly

### Row count (rolled-up tables)

```sql
-- Druid (use count metric, not COUNT(*)):
SELECT SUM(impressions) AS total_events FROM "datasource"

-- Pinot:
SELECT SUM(impressions) AS total_events FROM my_table_OFFLINE
```

- [ ] `SUM(count_metric)` matches between systems

### Time range

```sql
-- Druid:
SELECT MIN(__time) AS min_ts, MAX(__time) AS max_ts FROM "datasource"

-- Pinot:
SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM my_table_OFFLINE
```

- [ ] Time ranges cover the same period

### Aggregate metrics (for tables with metricsSpec)

For each key metric, run:

```sql
-- Druid:
SELECT SUM(revenue) AS total FROM "datasource"

-- Pinot:
SELECT SUM(revenue) AS total FROM my_table_OFFLINE
```

- [ ] SUM values match within acceptable tolerance (< 0.01% for floating point)

### GROUP BY parity

For each primary GROUP BY dimension:

```sql
-- Druid:
SELECT country, SUM(revenue) AS rev FROM "datasource"
GROUP BY country ORDER BY country

-- Pinot:
SELECT country, SUM(revenue) AS rev FROM my_table_OFFLINE
GROUP BY country ORDER BY country
```

- [ ] Same number of rows in result
- [ ] Same dimension values
- [ ] Same metric values (within tolerance)

### Filter parity

Test at least 2-3 representative filter predicates:

```sql
-- Druid:
SELECT COUNT(*) FROM "datasource"
WHERE platform = 'mobile' AND region = 'us-east'

-- Pinot:
SELECT COUNT(*) FROM my_table_OFFLINE
WHERE platform = 'mobile' AND region = 'us-east'
```

- [ ] Filter results match

---

## Phase 6: Application Query Validation

Replay the top 10 queries from your inventory against both systems:

- [ ] Each query returns the same number of rows
- [ ] Numeric values match within tolerance (0.01% for large aggregates)
- [ ] Query latencies are acceptable (within 2× of Druid baseline, or better)
- [ ] No SQL syntax errors in Pinot (function names may differ — see below)

### Common query translation issues

| Druid SQL | Pinot equivalent |
|---------|-----------------|
| `TIME_FLOOR(__time, 'PT1H')` | `DATETIMECONVERT(ts, '1:MILLIS:EPOCH', '1:HOURS:EPOCH', '1:HOURS')` |
| `APPROX_COUNT_DISTINCT(user_id)` | `DISTINCTCOUNTHLL(user_id)` |
| `TIMESTAMPADD(HOUR, 1, __time)` | `DATEADD('hour', 1, ts)` |
| `MV_CONTAINS(tags, 'python')` | `tags = 'python'` |
| `MILLIS_TO_TIMESTAMP(ts)` | `DATETIMECONVERT(ts, ...)` |

---

## Phase 7: Dashboard Migration

- [ ] Identify all dashboards and BI tools that query this datasource
- [ ] Update data source connections from Druid to Pinot Broker
- [ ] Update SQL queries to use Pinot table name and function names
- [ ] Run each dashboard against Pinot and visually compare with Druid version
- [ ] Get sign-off from dashboard owners

---

## Phase 8: Traffic Cut-Over

### Gradual cut-over (recommended)

1. **Dual-write period** — Run both Druid and Pinot in parallel. New data goes to both.
2. **Shadow mode** — Route a percentage of read traffic to Pinot; compare results.
3. **Full cut-over** — Route all read traffic to Pinot.
4. **Monitoring period** — Monitor for 1–2 weeks before decommissioning Druid.

### Monitoring

- [ ] Error rate on Pinot queries is < 0.1%
- [ ] P99 query latency is within SLA
- [ ] Segment ingestion is keeping up with incoming data (for streaming)
- [ ] Disk usage is within capacity planning estimates

---

## Phase 9: Post-Migration Cleanup

After the monitoring period:

- [ ] Archive Druid ingestion spec (for reference)
- [ ] Archive migration-output/ artifacts
- [ ] Remove Druid datasource and free historical node capacity
- [ ] Update runbooks and oncall documentation
- [ ] Document any query translation changes made during migration

---

## Go / No-Go Criteria Summary

| Gate | Pass | No-Go |
|------|------|-------|
| Confidence score | ≥ 0.80 | < 0.80 |
| Blocking risks | None | Any unresolved |
| Artifact validation | all pass | Any fail |
| Row count parity | exact match | > 0.1% difference |
| Aggregate parity | < 0.01% diff | > 0.1% difference |
| Query error rate | < 0.1% | > 0.1% |
| Dashboard sign-off | received | pending |

---

## See Also

- [Tutorial 16 — Risks and Confidence](16-risks-and-confidence.md) — interpreting risks
- [Tutorial 17 — Validation](17-validation.md) — artifact validation details
- [Reference: CLI](reference/cli.md) — all tool commands
