"""Unit tests for the backfill orchestrator with stub pager + sink."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import pytest

from migrator.realtime.backfill_runner import (
    BackfillResult,
    PinotIngestFromUriSink,
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


# ─────────────────────────────────────────────────────────────────────────────
# PinotIngestFromUriSink — control-plane-only sink for large backfills
# ─────────────────────────────────────────────────────────────────────────────


class _FakeResp:
    """Duck-typed `requests.Response` for sink tests."""

    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


class _SpySession:
    """Records every POST and returns a canned response."""

    def __init__(self, status: int = 200, text: str = "") -> None:
        self.status = status
        self.text = text
        self.posts: list[tuple[str, dict]] = []
        self.headers: dict[str, str] = {}

    def post(self, url, *, timeout=None, **kwargs):
        self.posts.append((url, kwargs))
        return _FakeResp(self.status, self.text)


class TestPinotIngestFromUriSink:
    def test_emits_file_uri_when_no_prefix(self, tmp_path: Path):
        # Default behaviour — turn the local path into a file:// URI.
        page = tmp_path / "page-000000.json"
        page.write_text("{}\n")
        session = _SpySession()
        sink = PinotIngestFromUriSink(
            "http://pinot:9000", session=session,
        )

        sink.ingest_file(page, "ds")

        assert len(session.posts) == 1
        url, _ = session.posts[0]
        # Endpoint and table-name encoding
        assert "/ingestFromURI" in url
        assert "tableNameWithType=ds_OFFLINE" in url
        # Data is referenced by URI, not uploaded — no `files=` kwarg
        # was passed to the session.
        assert "sourceURIStr=" in url
        # The URI is file:// against the absolute path.
        from urllib.parse import parse_qs, urlparse
        qs = parse_qs(urlparse(url).query)
        assert qs["sourceURIStr"][0].startswith("file://")
        assert str(page.resolve()) in qs["sourceURIStr"][0]

    def test_emits_prefixed_uri_when_prefix_given(self, tmp_path: Path):
        # Object-store mode: the operator pre-uploads to the prefix; the
        # sink references the file by basename.
        page = tmp_path / "page-000000.json"
        page.write_text("{}\n")
        session = _SpySession()
        sink = PinotIngestFromUriSink(
            "http://pinot:9000",
            session=session,
            uri_prefix="s3://my-bucket/staging/",  # trailing slash tolerated
        )

        sink.ingest_file(page, "ds")

        from urllib.parse import parse_qs, urlparse
        url, _ = session.posts[0]
        qs = parse_qs(urlparse(url).query)
        # Trailing slash on prefix gets normalised; basename appended.
        assert qs["sourceURIStr"][0] == "s3://my-bucket/staging/page-000000.json"

    def test_does_not_upload_file_body(self, tmp_path: Path):
        # The control-plane-only contract: dpm POSTs URL+query only, no
        # body. Specifically, no `files=` (multipart upload) and no
        # `data=` (raw body).
        page = tmp_path / "page-000000.json"
        page.write_text("{}\n")
        session = _SpySession()
        sink = PinotIngestFromUriSink("http://pinot:9000", session=session)

        sink.ingest_file(page, "ds")

        _, kwargs = session.posts[0]
        assert "files" not in kwargs
        assert "data" not in kwargs

    def test_unsets_content_type_for_body_less_post(self, tmp_path: Path):
        # /ingestFromURI is a parameter-only endpoint; sending an empty
        # body with `Content-Type: application/json` makes Pinot's
        # content negotiation return 415 (verified across 1.0/1.4/1.5).
        # The sink must override any session-level Content-Type to None
        # so requests strips the header on this call.
        page = tmp_path / "page-000000.json"
        page.write_text("{}\n")
        session = _SpySession()
        sink = PinotIngestFromUriSink("http://pinot:9000", session=session)

        sink.ingest_file(page, "ds")

        _, kwargs = session.posts[0]
        assert kwargs.get("headers", {}).get("Content-Type") is None
        # Specifically the header dict must contain the key — so
        # requests' merge logic actually unsets it from the session.
        assert "Content-Type" in kwargs["headers"]

    def test_500_response_raises(self, tmp_path: Path):
        page = tmp_path / "page.json"
        page.write_text("{}\n")
        session = _SpySession(status=500, text="boom")
        sink = PinotIngestFromUriSink("http://pinot:9000", session=session)
        with pytest.raises(RuntimeError, match="ingestFromURI failed"):
            sink.ingest_file(page, "ds")

    def test_ingest_files_via_run_backfill(self, tmp_path: Path):
        # The orchestrator hands one file at a time to the sink; verify
        # the URI sink integrates cleanly via that loop. The pager
        # produces three pages, the sink should see three POSTs.
        rows = [{"a": i} for i in range(7)]
        pager = StubPager(rows)
        session = _SpySession()
        sink = PinotIngestFromUriSink("http://pinot:9000", session=session)

        run_backfill(
            datasource="ds",
            pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager,
            sink=sink,
            page_rows=3,  # 7 rows / 3 = 3 pages (3, 3, 1)
        )

        assert len(session.posts) == 3
        # Each POST references a distinct page-NNNNNN.json file.
        from urllib.parse import parse_qs, urlparse
        seen_uris = []
        for url, _ in session.posts:
            qs = parse_qs(urlparse(url).query)
            seen_uris.append(qs["sourceURIStr"][0])
        assert len(set(seen_uris)) == 3
        assert all("page-" in u for u in seen_uris)
