from __future__ import annotations

import json
from pathlib import Path

from migrator.core.models import CanonicalMigrationModel, MetricField
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.pinot.table_generator import (
    PinotTableGenerator,
    build_realtime_transform_configs,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _canonical_from_fixture(name: str):
    raw = json.loads((FIXTURES / name / "spec.json").read_text())
    parser = DruidSpecParser()
    parse_result = parser.parse(raw)
    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)
    return norm_result.canonical


class TestPinotTableGenerator:
    def setup_method(self):
        self.gen = PinotTableGenerator()

    def test_batch_generates_offline_table(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        assert table["tableType"] == "OFFLINE"

    def test_stream_generates_realtime_table(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        assert table["tableType"] == "REALTIME"

    def test_table_name_contains_datasource_offline(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        assert "pageviews" in table["tableName"]

    def test_table_name_contains_datasource_realtime(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        assert "clickstream" in table["tableName"]

    def test_offline_table_has_segments_config(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        assert "segmentsConfig" in table
        assert table["segmentsConfig"]["replication"] == "1"

    def test_offline_table_has_retention(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        seg_cfg = table["segmentsConfig"]
        assert "retentionTimeUnit" in seg_cfg
        assert "retentionTimeValue" in seg_cfg

    def test_realtime_table_has_stream_configs(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        stream_configs = table["tableIndexConfig"]["streamConfigs"]
        assert stream_configs["streamType"] == "kafka"
        assert "stream.kafka.topic.name" in stream_configs
        assert "stream.kafka.broker.list" in stream_configs
        assert "stream.kafka.consumer.type" in stream_configs
        assert "stream.kafka.decoder.class.name" in stream_configs

    def test_realtime_topic_from_io_config(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        stream_configs = table["tableIndexConfig"]["streamConfigs"]
        assert stream_configs["stream.kafka.topic.name"] == "clickstream-events"

    def test_generate_dispatches_to_offline_for_batch(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate(canonical)
        assert table["tableType"] == "OFFLINE"

    def test_generate_dispatches_to_realtime_for_stream(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate(canonical)
        assert table["tableType"] == "REALTIME"

    def test_offline_table_name_has_offline_suffix(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        assert table["tableName"].endswith("_OFFLINE")

    def test_realtime_table_name_has_realtime_suffix(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        assert table["tableName"].endswith("_REALTIME")

    def test_realtime_no_metricsspec_emits_no_transforms(self):
        # raw_stream's metricsSpec is empty (no rollup) → no transforms.
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        # ingestionConfig key must NOT be present when there's nothing to
        # transform — the absence is part of the contract: callers can
        # still tack their own ingestionConfig on without colliding with
        # an empty placeholder.
        assert "ingestionConfig" not in table


# ─────────────────────────────────────────────────────────────────────────────
# build_realtime_transform_configs — helper unit tests
# ─────────────────────────────────────────────────────────────────────────────


def _metric(name: str, druid_type: str, field_name: str = "") -> MetricField:
    """Construct a minimal MetricField for the helper tests."""
    return MetricField(
        name=name,
        druid_type=druid_type,
        field_name=field_name,
        pinot_type="LONG",
        aggregation="SUM",
    )


def _canonical_with_metrics(metrics: list[MetricField]) -> CanonicalMigrationModel:
    """Wrap a metrics list in just enough canonical model to call the helper."""
    return CanonicalMigrationModel(
        datasource_name="ds",
        source_kind="stream",
        metrics=metrics,
    )


class TestBuildRealtimeTransformConfigs:
    def test_count_emits_constant_one(self):
        c = _canonical_with_metrics([_metric("events", "count")])
        configs = build_realtime_transform_configs(c)
        assert configs == [{"columnName": "events", "transformFunction": "1"}]

    def test_long_sum_with_rename_emits_alias(self):
        # name != field_name → emit alias
        c = _canonical_with_metrics([
            _metric("session_ms_sum", "longSum", "session_ms"),
        ])
        configs = build_realtime_transform_configs(c)
        assert configs == [{
            "columnName": "session_ms_sum",
            "transformFunction": "session_ms",
        }]

    def test_metric_without_rename_emits_no_alias(self):
        # name == field_name → Druid is doing pure rollup, no rename needed.
        c = _canonical_with_metrics([
            _metric("session_ms", "longSum", "session_ms"),
        ])
        configs = build_realtime_transform_configs(c)
        assert configs == []

    def test_metric_with_empty_field_name_emits_nothing(self):
        # Defensive: a non-count metric with empty field_name is malformed
        # but the helper shouldn't crash — it just skips it.
        c = _canonical_with_metrics([_metric("orphan", "longSum", "")])
        configs = build_realtime_transform_configs(c)
        assert configs == []

    def test_full_metricsspec_round_trip(self):
        # The classic Druid pageviews shape: count + multiple longSums with
        # different source field names.
        c = _canonical_with_metrics([
            _metric("events",         "count"),
            _metric("session_ms_sum", "longSum",  "session_ms"),
            _metric("bytes_sent_sum", "longSum",  "bytes_sent"),
            _metric("session_ms_max", "longMax",  "session_ms"),
            _metric("bytes_sent_min", "longMin",  "bytes_sent"),
        ])
        configs = build_realtime_transform_configs(c)
        assert configs == [
            {"columnName": "events",         "transformFunction": "1"},
            {"columnName": "session_ms_sum", "transformFunction": "session_ms"},
            {"columnName": "bytes_sent_sum", "transformFunction": "bytes_sent"},
            {"columnName": "session_ms_max", "transformFunction": "session_ms"},
            {"columnName": "bytes_sent_min", "transformFunction": "bytes_sent"},
        ]

    def test_count_type_case_insensitive(self):
        # Druid spec parsers normalise to lowercase but defensive callers
        # might construct the canonical model directly with mixed case.
        c = _canonical_with_metrics([_metric("events", "Count")])
        configs = build_realtime_transform_configs(c)
        assert configs == [{"columnName": "events", "transformFunction": "1"}]

    def test_empty_metrics_list_emits_no_transforms(self):
        # The raw_stream-equivalent: no rollup → no metrics → no transforms.
        c = _canonical_with_metrics([])
        configs = build_realtime_transform_configs(c)
        assert configs == []


class TestRealtimeTableWithTransforms:
    def test_realtime_table_emits_ingestion_config_when_transforms_exist(self):
        c = _canonical_with_metrics([
            _metric("events",         "count"),
            _metric("session_ms_sum", "longSum", "session_ms"),
        ])
        table = PinotTableGenerator().generate_realtime(c)
        assert "ingestionConfig" in table
        assert table["ingestionConfig"]["transformConfigs"] == [
            {"columnName": "events",         "transformFunction": "1"},
            {"columnName": "session_ms_sum", "transformFunction": "session_ms"},
        ]

    def test_realtime_table_skips_ingestion_config_when_no_transforms(self):
        c = _canonical_with_metrics([])
        table = PinotTableGenerator().generate_realtime(c)
        # No transforms → no ingestionConfig key (don't emit an empty
        # placeholder; that would force callers who add their own
        # ingestionConfig to deep-merge instead of just setting it).
        assert "ingestionConfig" not in table


# ─────────────────────────────────────────────────────────────────────────────
# Kinesis streamConfigs
# ─────────────────────────────────────────────────────────────────────────────

from migrator.pinot.table_generator import (
    KINESIS_CONSUMER_FACTORY,
    _extract_kinesis_region,
    build_kinesis_stream_configs,
)


class TestExtractKinesisRegion:
    def test_extracts_from_canonical_aws_endpoint(self):
        assert _extract_kinesis_region("kinesis.us-east-1.amazonaws.com") == "us-east-1"
        assert _extract_kinesis_region("kinesis.eu-west-2.amazonaws.com") == "eu-west-2"

    def test_strips_protocol(self):
        assert (
            _extract_kinesis_region("https://kinesis.ap-southeast-1.amazonaws.com")
            == "ap-southeast-1"
        )

    def test_returns_none_for_non_aws_endpoints(self):
        # localhost-style proxies, kinesalite, or custom endpoints
        # don't follow the AWS canonical form — caller must supply
        # region explicitly.
        assert _extract_kinesis_region("localhost:4567") is None
        assert _extract_kinesis_region("http://kinesalite:4567") is None
        assert _extract_kinesis_region("kinesis.example.com") is None

    def test_returns_none_for_empty(self):
        assert _extract_kinesis_region(None) is None
        assert _extract_kinesis_region("") is None


class TestBuildKinesisStreamConfigs:
    def test_minimum_required_fields(self):
        cfg = build_kinesis_stream_configs(
            stream_name="payments", region="us-east-1",
        )
        assert cfg["streamType"] == "kinesis"
        assert cfg["stream.kinesis.topic.name"] == "payments"
        assert cfg["region"] == "us-east-1"
        assert cfg["stream.kinesis.consumer.factory.class.name"] == KINESIS_CONSUMER_FACTORY
        # No endpoint key when not supplied — Pinot defaults to AWS Kinesis.
        assert "stream.kinesis.endpoint" not in cfg

    def test_endpoint_threaded_through(self):
        cfg = build_kinesis_stream_configs(
            stream_name="payments", region="us-east-1",
            endpoint="https://kinesalite:4567",
        )
        assert cfg["stream.kinesis.endpoint"] == "https://kinesalite:4567"

    def test_offset_criteria_overridable(self):
        cfg = build_kinesis_stream_configs(
            stream_name="t", region="us-east-1",
            offset_criteria="2024-03-01T00:00:00.000Z",
        )
        assert (
            cfg["stream.kinesis.consumer.prop.auto.offset.reset"]
            == "2024-03-01T00:00:00.000Z"
        )

    def test_no_aws_credentials_in_output(self):
        # Critical: production Pinot deployments source AWS creds from
        # IAM / env, not from the table config (committing them would
        # leak secrets). Make sure the builder never accidentally adds
        # an access-key field.
        cfg = build_kinesis_stream_configs(stream_name="t", region="us-east-1")
        for key in cfg:
            assert "access" not in key.lower()
            assert "secret" not in key.lower()


class TestKinesisRealtimeGeneration:
    def setup_method(self):
        self.gen = PinotTableGenerator()

    def test_kinesis_spec_emits_kinesis_stream_type(self):
        canonical = _canonical_from_fixture("kinesis_stream")
        table = self.gen.generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["streamType"] == "kinesis"
        # No kafka keys leak into a kinesis config.
        assert not any(k.startswith("stream.kafka.") for k in sc)

    def test_kinesis_topic_name_from_stream_field(self):
        # Druid stores the stream name in ``ioConfig.stream`` (not
        # ``ioConfig.topic``); the generator must read the right key.
        canonical = _canonical_from_fixture("kinesis_stream")
        table = self.gen.generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kinesis.topic.name"] == "payment-events-prod"

    def test_kinesis_region_extracted_from_endpoint(self):
        canonical = _canonical_from_fixture("kinesis_stream")
        table = self.gen.generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["region"] == "us-east-1"
        assert sc["stream.kinesis.endpoint"] == "kinesis.us-east-1.amazonaws.com"

    def test_kinesis_offset_reset_largest_when_useEarliest_false(self):
        # Fixture has useEarliestSequenceNumber=false → 'largest'.
        canonical = _canonical_from_fixture("kinesis_stream")
        table = self.gen.generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kinesis.consumer.prop.auto.offset.reset"] == "largest"

    def test_kinesis_offset_reset_smallest_when_useEarliest_true(self):
        canonical = _canonical_from_fixture("kinesis_stream")
        # Mutate the canonical's raw_io_config to flip the flag — easier
        # than maintaining a parallel fixture.
        canonical.raw_io_config["useEarliestSequenceNumber"] = True
        table = self.gen.generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kinesis.consumer.prop.auto.offset.reset"] == "smallest"

    def test_kinesis_watermark_iso_overrides_offset(self):
        # Hybrid mode: the watermark ISO supplied explicitly always
        # wins over the spec's useEarliestSequenceNumber default.
        canonical = _canonical_from_fixture("kinesis_stream")
        table = self.gen.generate_realtime(
            canonical, watermark_iso="2024-03-01T00:00:00.000Z",
        )
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert (
            sc["stream.kinesis.consumer.prop.auto.offset.reset"]
            == "2024-03-01T00:00:00.000Z"
        )

    def test_kinesis_falls_back_to_default_region_with_warning_safe(self):
        # When endpoint is non-AWS and no explicit region in raw_io,
        # the generator emits us-east-1 as a placeholder rather than
        # failing — operators who care can override post-generation.
        canonical = _canonical_from_fixture("kinesis_stream")
        canonical.raw_io_config["endpoint"] = "kinesalite:4567"
        canonical.raw_io_config.pop("region", None)
        table = self.gen.generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        # Placeholder region — operators should set this.
        assert sc["region"] == "us-east-1"
