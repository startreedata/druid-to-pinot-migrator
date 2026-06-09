"""Unit tests for migrator.realtime.watermark."""

from __future__ import annotations

import pytest

from migrator.realtime.models import StreamOffsetMap, StreamPlatform
from migrator.realtime.watermark import (
    parse_epoch_ms,
    refine_watermark,
    to_pinot_iso,
)


def _estimated_map(**overrides) -> StreamOffsetMap:
    defaults = dict(
        platform=StreamPlatform.KINESIS,
        topic="payments",
        datasource="payments",
        watermark_iso="2026-06-09T00:00:00.000Z",
        watermark_ms=1_780_000_000_000,
        watermark_estimated=True,
    )
    defaults.update(overrides)
    return StreamOffsetMap(**defaults)


class TestParseEpochMs:
    def test_epoch_millis_int(self):
        ms, iso = parse_epoch_ms(1709251200123)
        assert ms == 1709251200123
        assert iso == "2024-03-01T00:00:00.123Z"

    def test_epoch_millis_numeric_string(self):
        ms, iso = parse_epoch_ms("1709251200000")
        assert ms == 1709251200000
        assert iso == "2024-03-01T00:00:00.000Z"

    def test_iso_string_with_z(self):
        ms, iso = parse_epoch_ms("2024-03-01T00:00:00.000Z")
        assert ms == 1709251200000
        assert iso == "2024-03-01T00:00:00.000Z"

    def test_iso_string_naive_treated_as_utc(self):
        ms, iso = parse_epoch_ms("2024-03-01T00:00:00")
        assert iso == "2024-03-01T00:00:00.000Z"

    def test_junk_returns_none(self):
        assert parse_epoch_ms("not-a-timestamp") is None
        assert parse_epoch_ms("") is None
        assert parse_epoch_ms(None) is None

    def test_bool_rejected(self):
        # bool is an int subclass — must not be read as epoch 0/1.
        assert parse_epoch_ms(True) is None


class TestToPinotIso:
    def test_millis_precision_and_z(self):
        from datetime import datetime, timezone
        dt = datetime(2024, 4, 25, 22, 0, 0, 123456, tzinfo=timezone.utc)
        assert to_pinot_iso(dt) == "2024-04-25T22:00:00.123Z"


class TestRefineWatermark:
    def test_refines_estimated_from_max_time(self):
        calls = []
        def q(sql):
            calls.append(sql)
            return [{"wm": 1709251200123}]
        out = refine_watermark(_estimated_map(), druid_sql_query=q)
        assert out.watermark_ms == 1709251200123
        assert out.watermark_iso == "2024-03-01T00:00:00.123Z"
        assert out.watermark_estimated is False
        # Queries MAX(__time) of the datasource, cast to epoch millis.
        assert 'MAX("__time")' in calls[0]
        assert '"payments"' in calls[0]

    def test_noop_when_not_estimated(self):
        called = []
        m = _estimated_map(watermark_estimated=False)
        out = refine_watermark(m, druid_sql_query=lambda s: called.append(s) or [])
        assert out is m
        assert called == []  # no query issued when the watermark is precise

    def test_noop_when_no_datasource(self):
        called = []
        m = _estimated_map(datasource="")
        out = refine_watermark(m, druid_sql_query=lambda s: called.append(s) or [])
        assert out.watermark_estimated is True
        assert called == []

    def test_keeps_estimate_on_query_error(self):
        def boom(sql):
            raise RuntimeError("broker down")
        out = refine_watermark(_estimated_map(), druid_sql_query=boom)
        assert out.watermark_estimated is True
        assert out.watermark_iso == "2026-06-09T00:00:00.000Z"

    def test_keeps_estimate_on_empty_rows(self):
        out = refine_watermark(_estimated_map(), druid_sql_query=lambda s: [])
        assert out.watermark_estimated is True

    def test_keeps_estimate_on_unparseable_value(self):
        out = refine_watermark(
            _estimated_map(), druid_sql_query=lambda s: [{"wm": None}],
        )
        assert out.watermark_estimated is True

    def test_handles_row_as_list(self):
        out = refine_watermark(
            _estimated_map(), druid_sql_query=lambda s: [[1709251200000]],
        )
        assert out.watermark_ms == 1709251200000
        assert out.watermark_estimated is False

    def test_preserves_other_fields(self):
        out = refine_watermark(
            _estimated_map(topic="payments", supervisor_id="sup-1"),
            druid_sql_query=lambda s: [{"wm": 1709251200000}],
        )
        assert out.topic == "payments"
        assert out.supervisor_id == "sup-1"
        assert out.platform == StreamPlatform.KINESIS
