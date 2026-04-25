"""Unit tests for the backfill orchestrator with stub pager + sink."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from migrator.realtime.backfill_runner import (
    BackfillResult,
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
