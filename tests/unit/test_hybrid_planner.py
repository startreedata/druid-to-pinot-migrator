"""Unit tests for the pure hybrid planner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.core.errors import GenerationError
from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    GranularityInfo,
    MetricField,
    TimeField,
)
from migrator.realtime.hybrid_planner import (
    plan_hybrid_migration,
    write_hybrid_plan,
)
from migrator.realtime.models import (
    KafkaOffsetMap,
    KafkaPartitionOffset,
    StreamPlatform,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def stream_canonical() -> CanonicalMigrationModel:
    return CanonicalMigrationModel(
        datasource_name="events",
        source_kind="stream",
        classification="raw_event",
        time_field=TimeField(column_name="ts", format="millis", timezone="UTC"),
        dimensions=[DimensionField(name="region", druid_type="string", pinot_type="STRING")],
        metrics=[MetricField(
            name="count", druid_type="count", pinot_type="LONG", aggregation="COUNT",
        )],
        granularity=GranularityInfo(
            segment_granularity="HOUR",
            query_granularity="MINUTE",
            rollup=False,
            intervals=["2024-02-01T00:00:00.000Z/2024-04-01T00:00:00.000Z"],
        ),
        raw_io_config={
            "type": "kafka",
            "topic": "events",
            "consumerProperties": {"bootstrap.servers": "kafka:9092"},
        },
    )


@pytest.fixture
def watermark() -> KafkaOffsetMap:
    return KafkaOffsetMap(
        platform=StreamPlatform.KAFKA,
        topic="events",
        supervisor_id="events-supervisor",
        datasource="events",
        watermark_iso="2024-03-01T00:00:00.000+00:00",
        watermark_ms=1709251200000,
        offsets=[
            KafkaPartitionOffset(partition=0, offset=12345),
            KafkaPartitionOffset(partition=1, offset=12300),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pure-planner tests
# ─────────────────────────────────────────────────────────────────────────────


class TestPlanHybridMigration:
    def test_rejects_batch_source(self, stream_canonical, watermark):
        stream_canonical.source_kind = "batch"
        with pytest.raises(GenerationError):
            plan_hybrid_migration(stream_canonical, watermark)

    def test_returns_complete_plan(self, stream_canonical, watermark):
        plan = plan_hybrid_migration(stream_canonical, watermark)

        assert plan.datasource_name == "events"
        assert plan.schema_["schemaName"] == "events"
        assert plan.offline_table["tableType"] == "OFFLINE"
        assert plan.offline_table["tableName"] == "events_OFFLINE"
        assert plan.realtime_table["tableType"] == "REALTIME"
        assert plan.realtime_table["tableName"] == "events_REALTIME"

    def test_realtime_uses_watermark_as_offset_reset(self, stream_canonical, watermark):
        plan = plan_hybrid_migration(stream_canonical, watermark)
        sc = plan.realtime_table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kafka.consumer.prop.auto.offset.reset"] == watermark.watermark_iso

    def test_backfill_range_ends_at_watermark(self, stream_canonical, watermark):
        plan = plan_hybrid_migration(stream_canonical, watermark)
        assert plan.backfill_range.end_iso == watermark.watermark_iso

    def test_backfill_range_default_start_from_intervals(self, stream_canonical, watermark):
        plan = plan_hybrid_migration(stream_canonical, watermark)
        assert plan.backfill_range.start_iso == "2024-02-01T00:00:00.000Z"

    def test_backfill_range_explicit_override(self, stream_canonical, watermark):
        plan = plan_hybrid_migration(
            stream_canonical, watermark,
            backfill_start_iso="2024-02-15T00:00:00.000Z",
        )
        assert plan.backfill_range.start_iso == "2024-02-15T00:00:00.000Z"

    def test_backfill_page_rows_threaded_through(self, stream_canonical, watermark):
        plan = plan_hybrid_migration(
            stream_canonical, watermark, backfill_page_rows=12345
        )
        assert plan.backfill_range.page_rows == 12345

    def test_offline_and_realtime_share_time_column(self, stream_canonical, watermark):
        plan = plan_hybrid_migration(stream_canonical, watermark)
        offline_tc = plan.offline_table["segmentsConfig"]["timeColumnName"]
        realtime_tc = plan.realtime_table["segmentsConfig"]["timeColumnName"]
        assert offline_tc == realtime_tc == "ts"

    def test_planner_is_deterministic(self, stream_canonical, watermark):
        a = plan_hybrid_migration(stream_canonical, watermark)
        b = plan_hybrid_migration(stream_canonical, watermark)
        assert a.model_dump() == b.model_dump()


class TestWriteHybridPlan:
    def test_writes_all_expected_files(self, tmp_path, stream_canonical, watermark):
        plan = plan_hybrid_migration(stream_canonical, watermark)
        paths = write_hybrid_plan(plan, tmp_path)

        expected = {"schema", "offline_table", "realtime_table",
                    "backfill_job", "plan", "watermark", "runbook"}
        assert set(paths) == expected
        for p in paths.values():
            assert p.exists() and p.stat().st_size > 0

    def test_runbook_mentions_watermark(self, tmp_path, stream_canonical, watermark):
        plan = plan_hybrid_migration(stream_canonical, watermark)
        paths = write_hybrid_plan(plan, tmp_path)
        rb = paths["runbook"].read_text()
        assert watermark.watermark_iso in rb
        assert "events" in rb

    def test_realtime_table_json_contains_watermark(self, tmp_path, stream_canonical, watermark):
        plan = plan_hybrid_migration(stream_canonical, watermark)
        paths = write_hybrid_plan(plan, tmp_path)
        rt = json.loads(paths["realtime_table"].read_text())
        assert (
            rt["tableIndexConfig"]["streamConfigs"]
              ["stream.kafka.consumer.prop.auto.offset.reset"]
            == watermark.watermark_iso
        )


# ─────────────────────────────────────────────────────────────────────────────
# Kinesis end-to-end: a Kinesis canonical + Kinesis watermark must produce a
# Kinesis REALTIME table whose auto.offset.reset is the watermark timestamp.
# This is the load-bearing assertion for Kinesis hybrid cutover.
# ─────────────────────────────────────────────────────────────────────────────


from migrator.realtime.models import KinesisShardSequence, StreamOffsetMap


@pytest.fixture
def kinesis_canonical() -> CanonicalMigrationModel:
    return CanonicalMigrationModel(
        datasource_name="payments",
        source_kind="stream",
        classification="raw_event",
        time_field=TimeField(column_name="ts", format="millis", timezone="UTC"),
        dimensions=[DimensionField(name="region", druid_type="string", pinot_type="STRING")],
        metrics=[MetricField(
            name="count", druid_type="count", pinot_type="LONG", aggregation="COUNT",
        )],
        granularity=GranularityInfo(
            segment_granularity="HOUR",
            query_granularity="MINUTE",
            rollup=False,
            intervals=["2024-02-01T00:00:00.000Z/2024-04-01T00:00:00.000Z"],
        ),
        raw_io_config={
            "type": "kinesis",
            "stream": "payment-events",
            "endpoint": "kinesis.us-east-1.amazonaws.com",
        },
    )


@pytest.fixture
def kinesis_watermark() -> StreamOffsetMap:
    return StreamOffsetMap(
        platform=StreamPlatform.KINESIS,
        topic="payment-events",
        supervisor_id="payments-sup",
        datasource="payments",
        watermark_iso="2024-03-01T00:00:00.000+00:00",
        watermark_ms=1709251200000,
        shard_sequences=[
            KinesisShardSequence(shard_id="shardId-000000000000", sequence_number="100"),
        ],
    )


class TestPlanHybridMigrationKinesis:
    def test_realtime_is_kinesis_stream_config(self, kinesis_canonical, kinesis_watermark):
        plan = plan_hybrid_migration(kinesis_canonical, kinesis_watermark)
        sc = plan.realtime_table["tableIndexConfig"]["streamConfigs"]
        assert sc["streamType"] == "kinesis"
        assert sc["stream.kinesis.topic.name"] == "payment-events"
        # No Kafka keys leak into a Kinesis hybrid plan.
        assert not any(k.startswith("stream.kafka.") for k in sc)

    def test_realtime_uses_watermark_as_offset_reset(self, kinesis_canonical, kinesis_watermark):
        plan = plan_hybrid_migration(kinesis_canonical, kinesis_watermark)
        sc = plan.realtime_table["tableIndexConfig"]["streamConfigs"]
        assert (
            sc["stream.kinesis.consumer.prop.auto.offset.reset"]
            == kinesis_watermark.watermark_iso
        )

    def test_region_extracted_from_endpoint(self, kinesis_canonical, kinesis_watermark):
        plan = plan_hybrid_migration(kinesis_canonical, kinesis_watermark)
        sc = plan.realtime_table["tableIndexConfig"]["streamConfigs"]
        assert sc["region"] == "us-east-1"

    def test_backfill_range_ends_at_watermark(self, kinesis_canonical, kinesis_watermark):
        plan = plan_hybrid_migration(kinesis_canonical, kinesis_watermark)
        assert plan.backfill_range.end_iso == kinesis_watermark.watermark_iso

    def test_runbook_mentions_kinesis_stream(self, tmp_path, kinesis_canonical, kinesis_watermark):
        plan = plan_hybrid_migration(kinesis_canonical, kinesis_watermark)
        paths = write_hybrid_plan(plan, tmp_path)
        rb = paths["runbook"].read_text()
        assert "Kinesis stream" in rb
        assert "stream.kinesis.consumer.prop.auto.offset.reset" in rb
        # Per-shard sequence table rendered, not the partition table.
        assert "Per-shard sequence numbers" in rb
        assert "shardId-000000000000" in rb

    def test_watermark_json_round_trips_kinesis(self, tmp_path, kinesis_canonical, kinesis_watermark):
        plan = plan_hybrid_migration(kinesis_canonical, kinesis_watermark)
        paths = write_hybrid_plan(plan, tmp_path)
        wm = json.loads(paths["watermark"].read_text())
        assert wm["platform"] == "kinesis"
        assert wm["shard_sequences"][0]["shard_id"] == "shardId-000000000000"
