from __future__ import annotations

from migrator.core.enums import SourceKind
from migrator.core.models import CanonicalMigrationModel


# ─────────────────────────────────────────────────────────────────────────────
# Default-tunable Pinot keys; lifted to module constants so callers can
# inspect or override them and tests can assert against them stably.
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_CONSUMER_FACTORY = "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory"
"""
The ``kafka20`` package (Kafka 2.0 client plugin) is what we emit by default.

Compatibility rationale:
  - Pinot 1.0 – 1.4 ship the ``pinot-kafka-2.0`` plugin; the FQCN works directly.
  - Pinot 1.5 removed the ``pinot-kafka-2.0`` plugin in favour of
    ``pinot-kafka-3.0``, but adds a backward-compatibility alias in
    ``PluginManager`` that transparently maps ``kafka20.KafkaConsumerFactory``
    → ``kafka30.KafkaConsumerFactory`` at deserialise time.

Therefore emitting ``kafka20`` works on every Pinot 1.x release we support,
while ``kafka30`` would break Pinot ≤ 1.3 (the plugin doesn't exist there).
Operators who target Pinot 1.4+ exclusively can rewrite the FQCN by hand
or via a sed / jq post-processing step; the tool defaults to maximum
compatibility.
"""
KAFKA_JSON_DECODER = "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"
DEFAULT_OFFSET_RESET = "largest"
"""Used when no migration watermark is supplied. Matches the historical
Pinot default for new REALTIME tables."""


def build_realtime_transform_configs(
    canonical: CanonicalMigrationModel,
) -> list[dict]:
    """
    Build the Pinot ``transformConfigs`` for a REALTIME table from the
    canonical Druid metricsSpec.

    Druid's ``metricsSpec`` declares pre-aggregated metric columns, e.g.

        {"type": "count",   "name": "events"}
        {"type": "longSum", "name": "session_ms_sum", "fieldName": "session_ms"}

    Druid applies these aggregations at ingest time: a row with
    ``session_ms=12345`` becomes a row with ``session_ms_sum=12345`` and
    ``events=1``. dpm's generated Pinot schema follows Druid's lead and
    declares the **stored** column names (``events``, ``session_ms_sum``).

    But the raw Kafka stream still contains the *source* fields
    (``session_ms``), not the rolled-up names. Without a Pinot ingestion
    transform mapping raw → rolled, the REALTIME table receives 0 for
    every metric (the rolled column has no matching JSON field, and the
    source field has no matching schema column, so it's silently dropped).

    This function emits exactly the right transformConfigs:

    - ``count`` → ``{columnName: name, transformFunction: "1"}``
    - any other metric where ``field_name`` differs from ``name`` →
      ``{columnName: name, transformFunction: field_name}`` (alias copy)
    - metrics where ``field_name`` is empty (or equal to ``name``) get no
      transform — Druid's pass-through semantics.

    Returns an empty list if no transforms are needed (e.g. no rollup,
    or every metric's stored name matches its source field).
    """
    transforms: list[dict] = []
    for m in canonical.metrics:
        # ``count`` is special: there is no source field, every input row
        # contributes 1 to the metric.
        if m.druid_type.lower() == "count":
            transforms.append({
                "columnName": m.name,
                "transformFunction": "1",
            })
            continue
        # All other aggregations carry a source field name. Emit an alias
        # only when the names differ (a Druid spec is allowed to use
        # name == fieldName for pure rollup with no rename).
        if m.field_name and m.field_name != m.name:
            transforms.append({
                "columnName": m.name,
                "transformFunction": m.field_name,
            })
    return transforms


def build_kafka_stream_configs(
    *,
    topic: str,
    broker_list: str,
    offset_criteria: str = DEFAULT_OFFSET_RESET,
    decoder_class: str = KAFKA_JSON_DECODER,
    flush_threshold_rows: str = "1000000",
    flush_threshold_time: str = "1h",
) -> dict[str, str]:
    """
    Build a Pinot ``streamConfigs`` dict for a Kafka REALTIME table.

    ``offset_criteria`` accepts any value Pinot ``OffsetCriteria`` understands:

    - ``"smallest"`` / ``"largest"`` — start at topic head/tail.
    - An ISO-8601 timestamp like ``"2024-03-01T00:00:00.000Z"`` — Pinot's
      TIMESTAMP offset criterion. **This is the migration-watermark mode**:
      Pinot starts consumption at this timestamp, so it picks up exactly
      where Druid's REALTIME ingestion stopped.
    - A relative period like ``"7d"`` or ``"4h30m"`` — Pinot's PERIOD
      offset criterion (relative to broker request time).

    Pulled out as a free function so the hybrid planner and the existing
    PinotTableGenerator can share one definition.
    """
    return {
        "streamType": "kafka",
        "stream.kafka.topic.name": topic,
        "stream.kafka.broker.list": broker_list,
        "stream.kafka.consumer.type": "lowlevel",
        "stream.kafka.consumer.factory.class.name": KAFKA_CONSUMER_FACTORY,
        "stream.kafka.decoder.class.name": decoder_class,
        "stream.kafka.consumer.prop.auto.offset.reset": offset_criteria,
        "realtime.segment.flush.threshold.rows": flush_threshold_rows,
        "realtime.segment.flush.threshold.time": flush_threshold_time,
    }


class PinotTableGenerator:
    """Generate Pinot offline and realtime table config dicts."""

    def generate_offline(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate an OFFLINE table configuration."""
        time_column = canonical.time_field.column_name if canonical.time_field else "__time"
        table_name = f"{canonical.datasource_name}_OFFLINE"

        return {
            "tableName": table_name,
            "tableType": "OFFLINE",
            "segmentsConfig": {
                "timeColumnName": time_column,
                "timeType": "MILLISECONDS",
                "replication": "1",
                "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {
                "broker": "DefaultTenant",
                "server": "DefaultTenant",
            },
            "tableIndexConfig": {
                "loadMode": "MMAP",
            },
            "ingestionConfig": {
                "batchIngestionConfig": {
                    "segmentIngestionType": "APPEND",
                    "segmentIngestionFrequency": "DAILY",
                }
            },
            "metadata": {
                "customConfigs": {}
            },
        }

    def generate_realtime(
        self,
        canonical: CanonicalMigrationModel,
        *,
        watermark_iso: str | None = None,
    ) -> dict:
        """
        Generate a REALTIME table configuration.

        If ``watermark_iso`` is provided, the generated stream config uses it
        as the ``auto.offset.reset`` value (Pinot's TIMESTAMP offset criterion),
        so Pinot starts consumption from that point. Use this for hybrid
        Druid → Pinot migrations where Druid has already ingested everything
        before the watermark.
        """
        time_column = canonical.time_field.column_name if canonical.time_field else "__time"
        table_name = f"{canonical.datasource_name}_REALTIME"

        io = canonical.raw_io_config or {}
        consumer_props = io.get("consumerProperties", {})
        broker_list = consumer_props.get("bootstrap.servers", "localhost:9092")
        topic = io.get("topic", canonical.datasource_name)

        offset_criteria = watermark_iso or DEFAULT_OFFSET_RESET
        stream_configs = build_kafka_stream_configs(
            topic=topic,
            broker_list=broker_list,
            offset_criteria=offset_criteria,
        )

        table: dict = {
            "tableName": table_name,
            "tableType": "REALTIME",
            "segmentsConfig": {
                "timeColumnName": time_column,
                "timeType": "MILLISECONDS",
                "replication": "1",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {
                "broker": "DefaultTenant",
                "server": "DefaultTenant",
                "tagOverrideConfig": {},
            },
            "tableIndexConfig": {
                "loadMode": "MMAP",
                "streamConfigs": stream_configs,
            },
            "metadata": {
                "customConfigs": {}
            },
        }

        # transformConfigs map raw Kafka field names → rolled-up Druid metric
        # column names so the REALTIME table actually receives metric values.
        # Without this the realtime half ingests dimensions only and every
        # SUM(events)/SUM(session_ms_sum) returns 0 until enough events
        # accumulate to make the divergence obvious. Empty list = no rollup
        # (or no rename) → no ingestionConfig key emitted.
        transforms = build_realtime_transform_configs(canonical)
        if transforms:
            table["ingestionConfig"] = {"transformConfigs": transforms}

        return table

    def generate(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate the appropriate table config based on source kind."""
        if canonical.source_kind == SourceKind.STREAM.value:
            return self.generate_realtime(canonical)
        return self.generate_offline(canonical)
