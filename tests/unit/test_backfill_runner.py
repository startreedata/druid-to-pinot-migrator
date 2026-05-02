"""Unit tests for the backfill orchestrator with stub pager + sink."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import pytest

from migrator.realtime.backfill_runner import (
    BackfillResult,
    _iso_to_millis,
    _normalize_time_column,
    dump_to_ndjson,
    run_backfill,
)


# ─────────────────────────────────────────────────────────────────────────────
# Stubs
# ─────────────────────────────────────────────────────────────────────────────


class StubPager:
    """Pager that yields preconfigured rows in fixed-size pages."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.last_call: dict = {}

    def page_rows(self, datasource, *, start_iso, end_iso, page_rows) -> Iterator[list[dict]]:
        self.last_call = dict(
            datasource=datasource,
            start_iso=start_iso,
            end_iso=end_iso,
            page_rows=page_rows,
        )
        rows = list(self._rows)
        while rows:
            yield rows[:page_rows]
            rows = rows[page_rows:]


class CountingSink:
    """Records every ingest_file call."""

    def __init__(self) -> None:
        self.received: list[tuple[Path, str]] = []

    def ingest_file(self, ndjson_path, table_name) -> None:
        self.received.append((Path(ndjson_path), table_name))


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestRunBackfill:
    def test_pages_dumped_and_ingested(self, tmp_path):
        rows = [{"ts": i, "v": i * 2} for i in range(10)]
        pager = StubPager(rows)
        sink = CountingSink()

        result = run_backfill(
            datasource="ds",
            pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager,
            sink=sink,
            page_rows=4,
        )

        assert isinstance(result, BackfillResult)
        # 10 rows / 4 per page → 3 pages (4, 4, 2)
        assert result.pages_dumped == 3
        assert result.rows_dumped == 10
        assert result.files_ingested == 3

        # Sink saw three files, all routed to "ds"
        assert len(sink.received) == 3
        assert all(table == "ds" for _, table in sink.received)

        # Page files exist on disk and contain the right rows
        files = sorted(tmp_path.glob("page-*.json"))
        assert [p.name for p in files] == [
            "page-000000.json", "page-000001.json", "page-000002.json"
        ]
        all_rows = []
        for p in files:
            for line in p.read_text().splitlines():
                all_rows.append(json.loads(line))
        assert all_rows == rows

    def test_pager_arguments_threaded_through(self, tmp_path):
        pager = StubPager([{"ts": 1}])
        sink = CountingSink()
        run_backfill(
            datasource="my_ds",
            pinot_table="my_ds",
            start_iso="2024-03-01T00:00:00Z",
            end_iso="2024-04-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager,
            sink=sink,
            page_rows=999,
        )
        assert pager.last_call == {
            "datasource": "my_ds",
            "start_iso": "2024-03-01T00:00:00Z",
            "end_iso": "2024-04-01T00:00:00Z",
            "page_rows": 999,
        }

    def test_no_rows_no_pages_no_ingest(self, tmp_path):
        pager = StubPager([])
        sink = CountingSink()
        result = run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z", end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path, pager=pager, sink=sink, page_rows=10,
        )
        assert result.pages_dumped == 0
        assert result.rows_dumped == 0
        assert result.files_ingested == 0
        assert sink.received == []


class TestDumpToNdjson:
    def test_yields_paths_and_writes_ndjson(self, tmp_path):
        rows = [{"v": i} for i in range(5)]
        pager = StubPager(rows)

        paths = list(dump_to_ndjson(
            datasource="d",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager,
            page_rows=2,
        ))
        # 5 rows / 2 per page = 3 pages
        assert len(paths) == 3
        # Confirm round-trip
        seen = []
        for p in paths:
            for line in p.read_text().splitlines():
                seen.append(json.loads(line))
        assert seen == rows


# ─────────────────────────────────────────────────────────────────────────────
# Time-column normalisation: __time → schema time-column + ISO → epoch ms
# ─────────────────────────────────────────────────────────────────────────────


class TestIsoToMillis:
    @pytest.mark.parametrize("iso, expected", [
        # Druid SQL canonical form: ISO 8601, UTC, ms precision, trailing Z
        ("2026-04-23T00:15:22.967Z", 1776903322967),
        # Sub-second precision absent — exact-second epoch
        ("2024-01-01T00:00:00Z",     1704067200000),
        # Microsecond precision (some Druid result formats include it)
        # — sub-millisecond is truncated, matching Druid's own behaviour.
        ("2026-04-23T00:15:22.967123Z", 1776903322967),
        # No trailing Z (still UTC by Druid convention)
        ("2024-01-01T00:00:00",      1704067200000),
    ])
    def test_round_values(self, iso, expected):
        assert _iso_to_millis(iso) == expected


class TestNormalizeTimeColumn:
    def test_renames_string_time_to_millis(self):
        row = {
            "__time": "2024-01-01T00:00:00.000Z",
            "region": "us-east",
            "events": 1,
        }
        out = _normalize_time_column(row, "timestamp")
        assert "__time" not in out
        assert out["timestamp"] == 1704067200000
        assert out["region"] == "us-east"
        assert out["events"] == 1

    def test_passes_through_numeric_time(self):
        row = {"__time": 1704067200000, "events": 1}
        out = _normalize_time_column(row, "timestamp")
        assert out["timestamp"] == 1704067200000
        assert "__time" not in out

    def test_no_change_when_target_is_already_underscore_time(self):
        row = {"__time": "2024-01-01T00:00:00.000Z", "events": 1}
        out = _normalize_time_column(row, "__time")
        # No-op fast path: original dict is returned as-is.
        assert out is row
        assert out["__time"] == "2024-01-01T00:00:00.000Z"

    def test_no_change_when_time_field_absent(self):
        row = {"region": "us-east", "events": 1}
        out = _normalize_time_column(row, "timestamp")
        assert out is row

    def test_does_not_mutate_input(self):
        row = {"__time": "2024-01-01T00:00:00.000Z", "events": 1}
        original = dict(row)
        _ = _normalize_time_column(row, "timestamp")
        # The caller's dict is unchanged — only the returned copy carries
        # the rename. That keeps the page-iteration loop in run_backfill
        # safe for callers that retain references to their rows.
        assert row == original


class TestRunBackfillRenamesTime:
    """End-to-end via the orchestrator: __time rename flows into the
    NDJSON staging files."""

    def test_default_time_column_is_timestamp(self, tmp_path):
        rows = [
            {"__time": "2024-01-01T00:00:00.000Z", "region": "us-east"},
            {"__time": "2024-01-01T00:01:00.000Z", "region": "us-west"},
        ]
        pager = StubPager(rows)
        sink = CountingSink()

        run_backfill(
            datasource="ds",
            pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager,
            sink=sink,
            page_rows=10,
        )

        page_path, _ = sink.received[0]
        lines = page_path.read_text().splitlines()
        records = [json.loads(line) for line in lines]
        assert all("__time" not in r for r in records)
        assert records[0]["timestamp"] == 1704067200000
        assert records[1]["timestamp"] == 1704067260000

    def test_custom_time_column_name_is_honoured(self, tmp_path):
        rows = [{"__time": "2024-01-01T00:00:00.000Z", "v": 1}]
        pager = StubPager(rows)
        sink = CountingSink()

        run_backfill(
            datasource="ds",
            pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager,
            sink=sink,
            page_rows=10,
            time_column="event_ts",
        )

        page_path, _ = sink.received[0]
        record = json.loads(page_path.read_text().strip())
        assert "__time" not in record
        assert record["event_ts"] == 1704067200000

    def test_rows_without_time_column_are_passed_through(self, tmp_path):
        """Defensive: if upstream Druid SELECT omits __time (custom view,
        future Druid version, etc.), the orchestrator shouldn't blow up."""
        rows = [{"region": "us-east", "v": 1}]
        pager = StubPager(rows)
        sink = CountingSink()

        run_backfill(
            datasource="ds",
            pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager,
            sink=sink,
            page_rows=10,
        )

        page_path, _ = sink.received[0]
        record = json.loads(page_path.read_text().strip())
        assert record == {"region": "us-east", "v": 1}
