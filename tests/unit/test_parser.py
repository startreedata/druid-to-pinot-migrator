from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.core.errors import ParseError
from migrator.druid.parser import DruidSpecParser

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name / "spec.json").read_text())


class TestDruidSpecParser:
    def setup_method(self):
        self.parser = DruidSpecParser()

    def test_parse_raw_batch_no_errors(self):
        raw = _load("raw_batch")
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec is not None
        assert not result.errors

    def test_parse_raw_stream_no_errors(self):
        raw = _load("raw_stream")
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec is not None

    def test_parse_rolled_up_no_errors(self):
        raw = _load("rolled_up")
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec is not None

    def test_parse_transforms_no_errors(self):
        raw = _load("transforms")
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec is not None

    def test_parse_unsupported_complex_no_errors(self):
        raw = _load("unsupported_complex")
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec is not None

    def test_datasource_name_raw_batch(self):
        raw = _load("raw_batch")
        result = self.parser.parse(raw)
        assert result.parsed_spec.datasource_name == "pageviews"

    def test_datasource_name_raw_stream(self):
        raw = _load("raw_stream")
        result = self.parser.parse(raw)
        assert result.parsed_spec.datasource_name == "clickstream"

    def test_datasource_name_rolled_up(self):
        raw = _load("rolled_up")
        result = self.parser.parse(raw)
        assert result.parsed_spec.datasource_name == "ad_metrics"

    def test_datasource_name_transforms(self):
        raw = _load("transforms")
        result = self.parser.parse(raw)
        assert result.parsed_spec.datasource_name == "user_events"

    def test_datasource_name_unsupported_complex(self):
        raw = _load("unsupported_complex")
        result = self.parser.parse(raw)
        assert result.parsed_spec.datasource_name == "audience_segments"

    def test_missing_data_schema_raises_parse_error(self):
        raw = {"type": "index_parallel"}
        with pytest.raises(ParseError):
            self.parser.parse(raw)

    def test_kafka_io_type_detected(self):
        raw = _load("raw_stream")
        result = self.parser.parse(raw)
        assert result.parsed_spec.io_config.type == "kafka"

    def test_kafka_iotype_falls_back_to_top_level_type(self):
        """Druid itself accepts a Kafka supervisor spec without
        ``ioConfig.type`` — it infers from the top-level task ``type``.
        dpm should mirror that inference so users hand-writing
        supervisor JSON aren't forced to repeat ``"type": "kafka"``
        twice."""
        raw = {
            "type": "kafka",  # top-level
            "spec": {
                "dataSchema": {
                    "dataSource": "ds",
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": ["d"]},
                    "metricsSpec": [],
                    "granularitySpec": {"segmentGranularity": "HOUR", "rollup": False},
                },
                "ioConfig": {
                    # NOTE: no "type": "kafka" here — Druid still accepts it.
                    "topic": "events",
                    "consumerProperties": {"bootstrap.servers": "kafka:9092"},
                },
            },
        }
        result = self.parser.parse(raw)
        assert result.success
        # Inferred from the top-level task type.
        assert result.parsed_spec.io_config.type == "kafka"

    def test_iotype_when_present_wins_over_top_level(self):
        """If both top-level and ioConfig.type are present, ioConfig.type
        wins (it's the more specific signal)."""
        raw = {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "ds",
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": ["d"]},
                    "metricsSpec": [],
                    "granularitySpec": {"segmentGranularity": "HOUR", "rollup": False},
                },
                "ioConfig": {
                    "type": "kinesis",  # disagreeing inner type — wins
                    "stream": "my-stream",
                },
            },
        }
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec.io_config.type == "kinesis"

    def test_top_level_kinesis_inferred(self):
        """The same fallback works for Kinesis supervisors."""
        raw = {
            "type": "kinesis",
            "spec": {
                "dataSchema": {
                    "dataSource": "ds",
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": ["d"]},
                    "metricsSpec": [],
                    "granularitySpec": {"segmentGranularity": "HOUR", "rollup": False},
                },
                "ioConfig": {
                    "stream": "my-stream",
                },
            },
        }
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec.io_config.type == "kinesis"

    def test_no_top_level_no_inner_falls_back_to_index(self):
        """Defensive: when neither side declares a type, default to the
        legacy ``"index"`` (batch) — no behavioural change vs pre-fix."""
        raw = {
            "spec": {
                "dataSchema": {
                    "dataSource": "ds",
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": ["d"]},
                    "metricsSpec": [],
                    "granularitySpec": {"segmentGranularity": "DAY", "rollup": False},
                },
                "ioConfig": {
                    "inputSource": {"type": "local", "baseDir": "/data", "filter": "*.json"},
                    "inputFormat": {"type": "json"},
                },
            },
        }
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec.io_config.type == "index"

    def test_top_level_data_schema(self):
        """Support top-level dataSchema (not nested under spec)."""
        raw = {
            "dataSchema": {
                "dataSource": "toplevel_ds",
                "timestampSpec": {"column": "ts", "format": "millis"},
                "dimensionsSpec": {"dimensions": ["a"]},
                "metricsSpec": [],
                "granularitySpec": {"segmentGranularity": "DAY", "rollup": False},
            },
            "ioConfig": {"type": "index"},
        }
        result = self.parser.parse(raw)
        assert result.success
        assert result.parsed_spec.datasource_name == "toplevel_ds"

    def test_metrics_parsed_for_rolled_up(self):
        raw = _load("rolled_up")
        result = self.parser.parse(raw)
        assert len(result.parsed_spec.metrics_spec) == 3
        metric_names = [m.name for m in result.parsed_spec.metrics_spec]
        assert "impressions" in metric_names
        assert "clicks" in metric_names
        assert "revenue" in metric_names

    def test_transforms_parsed(self):
        raw = _load("transforms")
        result = self.parser.parse(raw)
        assert len(result.parsed_spec.transform_spec.transforms) == 1
        assert result.parsed_spec.transform_spec.transforms[0]["name"] == "event_category"

    def test_rollup_flag_parsed(self):
        raw = _load("rolled_up")
        result = self.parser.parse(raw)
        assert result.parsed_spec.granularity_spec.rollup is True

    def test_no_rollup_for_raw(self):
        raw = _load("raw_batch")
        result = self.parser.parse(raw)
        assert result.parsed_spec.granularity_spec.rollup is False
