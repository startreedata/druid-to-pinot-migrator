from __future__ import annotations

ROLLUP_SEMANTIC_MISMATCH = "ROLLUP_SEMANTIC_MISMATCH"
APPROX_AGGREGATOR_MISMATCH = "APPROX_AGGREGATOR_MISMATCH"
NULL_DEFAULT_MISMATCH = "NULL_DEFAULT_MISMATCH"
TRANSFORM_PORTABILITY_RISK = "TRANSFORM_PORTABILITY_RISK"
MULTIVALUE_AMBIGUITY = "MULTIVALUE_AMBIGUITY"
TIME_SEMANTICS_MISMATCH = "TIME_SEMANTICS_MISMATCH"
UNSUPPORTED_COMPLEX_FIELD = "UNSUPPORTED_COMPLEX_FIELD"
QUERY_COMPATIBILITY_RISK = "QUERY_COMPATIBILITY_RISK"
INDEX_ADVISORY_ONLY = "INDEX_ADVISORY_ONLY"
INGESTION_BEHAVIOR_MISMATCH = "INGESTION_BEHAVIOR_MISMATCH"
RETENTION_GRANULARITY_MISMATCH = "RETENTION_GRANULARITY_MISMATCH"
PARTITIONING_CONFIG_REQUIRED = "PARTITIONING_CONFIG_REQUIRED"
FLATTEN_SPEC_NOT_PORTABLE = "FLATTEN_SPEC_NOT_PORTABLE"
CUSTOM_TIMESTAMP_FORMAT = "CUSTOM_TIMESTAMP_FORMAT"
STREAM_SOURCE_MISMATCH = "STREAM_SOURCE_MISMATCH"
BATCH_AGGREGATION_NOT_REPLAYED = "BATCH_AGGREGATION_NOT_REPLAYED"

RISK_DESCRIPTIONS: dict[str, str] = {
    ROLLUP_SEMANTIC_MISMATCH: (
        "Druid roll-up pre-aggregates rows at ingestion time. Pinot supports rollup-on-merge "
        "but query semantics differ: COUNT(*) in Pinot will return segment row count, not the "
        "original event count. Validate that metric columns (especially 'count') carry the "
        "expected semantics after migration."
    ),
    APPROX_AGGREGATOR_MISMATCH: (
        "Druid sketch aggregators (thetaSketch, HLLSketchBuild, hyperUnique) serialize to "
        "opaque BYTES in Pinot. Pinot has its own HLL/Theta sketch implementations "
        "(DISTINCTCOUNTHLL, DISTINCTCOUNTTHETASKETCH) but the serialized formats are "
        "incompatible. Re-ingest raw events and rebuild sketches in Pinot."
    ),
    NULL_DEFAULT_MISMATCH: (
        "Druid treats missing numeric values as 0 by default (unless 'useDefaultValueForNull' "
        "is false). Pinot also defaults to 0 for numerics but NULL handling for GROUP BY "
        "and filters differs. Verify query results for rows with null/missing values."
    ),
    TRANSFORM_PORTABILITY_RISK: (
        "Druid transform expressions use Druid's built-in expression language. Pinot does not "
        "support ingestion-time expression transforms directly; equivalent logic must be "
        "implemented upstream (ETL) or via Pinot's groovy/SQL transform plugins."
    ),
    MULTIVALUE_AMBIGUITY: (
        "Multi-value dimensions in Druid are arrays of strings ingested and indexed as MV columns. "
        "Pinot supports MV columns but the query semantics for aggregations over MV columns can "
        "differ. Verify COUNT DISTINCT and GROUP BY behavior."
    ),
    TIME_SEMANTICS_MISMATCH: (
        "Druid uses a special '__time' column with millisecond epoch semantics. Pinot time columns "
        "are flexible but must be explicitly configured with the correct granularity and format. "
        "Verify that the dateTimeFieldSpec format matches the actual data format."
    ),
    UNSUPPORTED_COMPLEX_FIELD: (
        "One or more fields use Druid complex types (e.g. sketches, histograms) that cannot be "
        "directly migrated to Pinot. These fields have been mapped to BYTES as a placeholder "
        "and require manual migration planning."
    ),
    QUERY_COMPATIBILITY_RISK: (
        "Some Druid SQL/native query patterns are not directly portable to Pinot SQL. "
        "In particular: APPROX_COUNT_DISTINCT, TIME_FLOOR, TIMESTAMPADD, and certain "
        "aggregation functions have different names or semantics in Pinot."
    ),
    INDEX_ADVISORY_ONLY: (
        "Druid uses bitmap indexes by default for all dimensions. Pinot also uses bitmap "
        "indexes but offers additional index types (sorted, range, text, FST, JSON, H3). "
        "Review the column access patterns and configure appropriate indexes for optimal "
        "query performance."
    ),
    INGESTION_BEHAVIOR_MISMATCH: (
        "Druid and Pinot differ in how they handle late-arriving data, segment compaction, "
        "and upserts. If the source pipeline relies on Druid-specific behaviors (e.g. "
        "appendToExisting, compaction tasks), review the equivalent Pinot mechanisms."
    ),
    RETENTION_GRANULARITY_MISMATCH: (
        "Druid retention rules operate at segment granularity. Pinot retention is configured "
        "at the table level in time units. Verify that the Pinot retention window covers "
        "the same data range as the Druid retention policy."
    ),
    PARTITIONING_CONFIG_REQUIRED: (
        "Druid partitionsSpec (hash, range, or dynamic) controls segment distribution across "
        "historicals. Pinot uses tableIndexConfig.segmentPartitionConfig for similar behavior. "
        "Configure Pinot partitioning manually to match the Druid sharding strategy for "
        "query pruning and data locality."
    ),
    FLATTEN_SPEC_NOT_PORTABLE: (
        "Druid's flattenSpec extracts nested JSON fields at ingestion time using path expressions. "
        "Pinot does not support flattenSpec; nested field extraction must be implemented via "
        "Pinot's ingestion transform functions (e.g., JsonPathTransformer) or upstream ETL."
    ),
    CUSTOM_TIMESTAMP_FORMAT: (
        "A non-standard timestamp format string is used in the Druid timestampSpec. "
        "The generated Pinot dateTimeFieldSpec uses a best-effort mapping; verify that "
        "the format pattern is valid in Pinot's SIMPLE_DATE_FORMAT and produces correct "
        "epoch millisecond values."
    ),
    STREAM_SOURCE_MISMATCH: (
        "The source datasource uses Kinesis as the streaming source. The generated Pinot "
        "REALTIME table config uses Kafka defaults. Update streamConfigs to point to the "
        "correct Kinesis endpoint and credentials, or set up a Kinesis-to-Kafka bridge."
    ),
    BATCH_AGGREGATION_NOT_REPLAYED: (
        "Druid roll-up pre-aggregates rows at ingest time (TIME_FLOOR + GROUP BY in MSQ, "
        "or rollup=true with metricsSpec in the classic spec). Pinot's batch ingestion is "
        "row-oriented — it reads source records via a RecordReader but does NOT execute "
        "GROUP BY or any other SQL operator. Three options for the operator: "
        "(a) pre-aggregate upstream and feed the rolled-up output to Pinot; "
        "(b) ingest raw rows + configure star-tree to pre-aggregate the same SUM/COUNT/MIN/MAX "
        "combinations at segment-build time (``dpm recommend`` suggests this); "
        "(c) ingest raw rows and rely on query-time aggregation (slower but simplest)."
    ),
}
