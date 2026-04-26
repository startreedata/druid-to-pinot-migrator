"""Unit tests for migrator.druid.spec_extractor (pure logic + mocked clients)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from migrator.druid.coordinator_client import (
    DruidCoordinatorClient,
    SegmentMetadata,
)
from migrator.druid.overlord_client import DruidOverlordClient
from migrator.druid.spec_extractor import ExtractedSpec, extract_spec


# ─── Fixtures ──────────────────────────────────────────────────────────────


def _coord_mock(
    *,
    exists: bool = True,
    summary: dict | None = None,
    meta: SegmentMetadata | None = None,
) -> DruidCoordinatorClient:
    c = MagicMock(spec=DruidCoordinatorClient)
    c.datasource_exists.return_value = exists
    c.get_datasource_summary.return_value = summary or {}
    c.get_segment_metadata.return_value = meta or SegmentMetadata(
        columns={}, intervals=[]
    )
    return c


def _overlord_mock(
    *,
    supervisor_id: str | None = None,
    spec: dict | None = None,
) -> DruidOverlordClient:
    o = MagicMock(spec=DruidOverlordClient)
    o.find_supervisor_for_datasource.return_value = supervisor_id
    if spec is not None:
        o.get_supervisor_spec.return_value = spec
    return o


# ─── Datasource doesn't exist ──────────────────────────────────────────────


def test_unknown_datasource_raises():
    coord = _coord_mock(exists=False)
    with pytest.raises(ValueError, match="not found"):
        extract_spec("missing", coordinator=coord)


# ─── Stream extraction ─────────────────────────────────────────────────────


SUPERVISOR_SPEC = {
    "id": "events-sup",
    "type": "kafka",
    "spec": {
        "dataSchema": {
            "dataSource": "events",
            "timestampSpec": {"column": "ts", "format": "millis"},
            "dimensionsSpec": {"dimensions": ["country"]},
            "metricsSpec": [],
            "granularitySpec": {"type": "uniform", "rollup": False},
        },
        "ioConfig": {
            "topic": "events",
            "consumerProperties": {
                "bootstrap.servers": "kafka.internal:9092"
            },
        },
        "tuningConfig": {"type": "kafka"},
    },
}


class TestStreamExtraction:
    def test_auto_detect_picks_supervisor_when_present(self):
        coord = _coord_mock()
        overlord = _overlord_mock(supervisor_id="events-sup", spec=SUPERVISOR_SPEC)
        result = extract_spec("events", coordinator=coord, overlord=overlord)

        assert isinstance(result, ExtractedSpec)
        assert result.source_kind == "stream"
        assert result.supervisor_id == "events-sup"
        # Output is wrapped {"type": "kafka", "spec": <dataSchema/ioConfig/tuningConfig>}
        assert result.spec["type"] == "kafka"
        assert "spec" in result.spec
        assert result.spec["spec"]["dataSchema"]["dataSource"] == "events"
        assert result.spec["spec"]["ioConfig"]["topic"] == "events"

    def test_prefer_stream_without_overlord_raises(self):
        coord = _coord_mock()
        with pytest.raises(ValueError, match="Overlord"):
            extract_spec("events", coordinator=coord, prefer="stream")

    def test_prefer_stream_no_matching_supervisor_raises(self):
        coord = _coord_mock()
        overlord = _overlord_mock(supervisor_id=None)
        with pytest.raises(ValueError, match="No active supervisor"):
            extract_spec(
                "events", coordinator=coord, overlord=overlord, prefer="stream"
            )

    def test_warns_when_kafka_bootstrap_is_localhost(self):
        coord = _coord_mock()
        local_spec = {
            **SUPERVISOR_SPEC,
            "spec": {
                **SUPERVISOR_SPEC["spec"],
                "ioConfig": {
                    "topic": "events",
                    "consumerProperties": {
                        "bootstrap.servers": "localhost:9092"
                    },
                },
            },
        }
        overlord = _overlord_mock(supervisor_id="s", spec=local_spec)
        result = extract_spec("events", coordinator=coord, overlord=overlord)
        assert any("localhost" in w for w in result.warnings)

    def test_kinesis_supervisor_warns(self):
        coord = _coord_mock()
        kinesis_spec = {
            "type": "kinesis",
            "spec": SUPERVISOR_SPEC["spec"],
        }
        overlord = _overlord_mock(supervisor_id="s", spec=kinesis_spec)
        result = extract_spec("events", coordinator=coord, overlord=overlord)
        assert any("Kinesis" in w for w in result.warnings)


# ─── Batch extraction ──────────────────────────────────────────────────────


class TestBatchExtraction:
    def _meta_with_typical_columns(self) -> SegmentMetadata:
        return SegmentMetadata(
            columns={
                "__time": {"type": "LONG"},
                "country": {"type": "STRING"},
                "platform": {"type": "STRING"},
                "tags": {"type": "STRING", "hasMultipleValues": True},
                "revenue": {"type": "DOUBLE"},
                "clicks": {"type": "LONG"},
            },
            intervals=[
                "2024-03-01T00:00:00.000Z/2024-03-02T00:00:00.000Z",
                "2024-03-02T00:00:00.000Z/2024-03-03T00:00:00.000Z",
            ],
        )

    def test_falls_back_to_batch_when_no_overlord(self):
        coord = _coord_mock(meta=self._meta_with_typical_columns())
        result = extract_spec("events", coordinator=coord)
        assert result.source_kind == "batch"
        assert result.spec["type"] == "index_parallel"

    def test_dimensions_inferred_from_string_columns(self):
        coord = _coord_mock(meta=self._meta_with_typical_columns())
        result = extract_spec("events", coordinator=coord)
        dims = result.spec["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"]
        # `tags` is multi-value → expanded form; others are bare strings
        names = [d if isinstance(d, str) else d["name"] for d in dims]
        assert set(names) == {"country", "platform", "tags"}
        # The MV one has structured form
        tags = next(d for d in dims if isinstance(d, dict) and d["name"] == "tags")
        assert tags["multiValueHandling"] == "SORTED_ARRAY"

    def test_metrics_inferred_from_numeric_columns(self):
        coord = _coord_mock(meta=self._meta_with_typical_columns())
        result = extract_spec("events", coordinator=coord)
        mets = result.spec["spec"]["dataSchema"]["metricsSpec"]
        names = {m["name"] for m in mets}
        assert names == {"revenue", "clicks"}
        # Type-correct aggregators
        by_name = {m["name"]: m["type"] for m in mets}
        assert by_name["revenue"] == "doubleSum"
        assert by_name["clicks"] == "longSum"

    def test_intervals_from_segment_metadata(self):
        coord = _coord_mock(meta=self._meta_with_typical_columns())
        result = extract_spec("events", coordinator=coord)
        ivs = result.spec["spec"]["dataSchema"]["granularitySpec"]["intervals"]
        assert ivs == [
            "2024-03-01T00:00:00.000Z/2024-03-02T00:00:00.000Z",
            "2024-03-02T00:00:00.000Z/2024-03-03T00:00:00.000Z",
        ]

    def test_batch_emits_placeholder_warning_for_input_source(self):
        coord = _coord_mock(meta=self._meta_with_typical_columns())
        result = extract_spec("events", coordinator=coord)
        assert any("inputSource" in w for w in result.warnings)
        # Spec must still be syntactically valid
        assert result.spec["spec"]["ioConfig"]["inputSource"]["type"] == "local"

    def test_batch_warns_about_transforms_and_flatten(self):
        coord = _coord_mock(meta=self._meta_with_typical_columns())
        result = extract_spec("events", coordinator=coord)
        assert any(
            "transformSpec" in w or "flattenSpec" in w
            for w in result.warnings
        )

    def test_segment_granularity_inferred_to_DAY_for_daily_intervals(self):
        coord = _coord_mock(meta=self._meta_with_typical_columns())
        result = extract_spec("events", coordinator=coord)
        gran = result.spec["spec"]["dataSchema"]["granularitySpec"]
        assert gran["segmentGranularity"] == "DAY"

    def test_segment_granularity_inferred_to_HOUR(self):
        coord = _coord_mock(meta=SegmentMetadata(
            columns={"__time": {"type": "LONG"}, "x": {"type": "STRING"}},
            intervals=["2024-03-01T00:00:00.000Z/2024-03-01T01:00:00.000Z"],
        ))
        result = extract_spec("events", coordinator=coord)
        assert (
            result.spec["spec"]["dataSchema"]["granularitySpec"]["segmentGranularity"]
            == "HOUR"
        )

    def test_segment_granularity_default_DAY_when_unparseable(self):
        coord = _coord_mock(meta=SegmentMetadata(
            columns={"__time": {"type": "LONG"}, "x": {"type": "STRING"}},
            intervals=["garbage"],
        ))
        result = extract_spec("events", coordinator=coord)
        assert (
            result.spec["spec"]["dataSchema"]["granularitySpec"]["segmentGranularity"]
            == "DAY"
        )

    def test_no_dimensions_warns(self):
        coord = _coord_mock(meta=SegmentMetadata(
            columns={"__time": {"type": "LONG"}, "metric": {"type": "DOUBLE"}},
            intervals=["2024-03-01/2024-03-02"],
        ))
        result = extract_spec("events", coordinator=coord)
        assert any("dimension" in w.lower() for w in result.warnings)

    def test_no_metrics_warns(self):
        coord = _coord_mock(meta=SegmentMetadata(
            columns={
                "__time": {"type": "LONG"},
                "country": {"type": "STRING"},
            },
            intervals=["2024-03-01/2024-03-02"],
        ))
        result = extract_spec("events", coordinator=coord)
        assert any("metricsSpec" in w for w in result.warnings)


# ─── prefer flag ───────────────────────────────────────────────────────────


class TestPreferFlag:
    def test_prefer_batch_skips_overlord(self):
        coord = _coord_mock(meta=SegmentMetadata(
            columns={"__time": {"type": "LONG"}, "x": {"type": "STRING"}},
            intervals=["2024-03-01/2024-03-02"],
        ))
        overlord = _overlord_mock(supervisor_id="s", spec=SUPERVISOR_SPEC)
        result = extract_spec(
            "events",
            coordinator=coord,
            overlord=overlord,
            prefer="batch",
        )
        # Even though a supervisor exists, prefer="batch" must skip it
        assert result.source_kind == "batch"
        overlord.find_supervisor_for_datasource.assert_not_called()
