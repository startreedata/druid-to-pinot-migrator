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

    def page_rows(self, datasource, *, start_iso, end_iso, page_rows, start_offset=0) -> Iterator[list[dict]]:
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

    @pytest.mark.parametrize(
        "uri_prefix, expected",
        [
            # s3:// — exercised end-to-end against MinIO in the live suite.
            ("s3://my-bucket/staging/", "s3://my-bucket/staging/page-000000.json"),
            ("s3://my-bucket/staging",  "s3://my-bucket/staging/page-000000.json"),
            # gs:// — only unit-tested. The Pinot 1.x GcsPinotFS plugin
            # ignores STORAGE_EMULATOR_HOST and won't talk to a local
            # fake-gcs-server, so end-to-end coverage requires a real GCP
            # project. The dpm side is just URL composition though, and
            # this assertion locks down the contract: a gs:// prefix
            # produces a gs:// sourceURI with the file basename appended.
            ("gs://my-bucket/staging/", "gs://my-bucket/staging/page-000000.json"),
            ("gs://my-bucket/staging",  "gs://my-bucket/staging/page-000000.json"),
            # Bare http(s):// — the URI sink doesn't care about scheme;
            # whatever PinotFS plugin is registered for it on the
            # controller side handles the fetch.
            ("https://example.com/data/", "https://example.com/data/page-000000.json"),
            # Custom scheme — also passed through verbatim. Useful for
            # operator-deployed PinotFS plugins (azure://, hdfs://, …).
            ("azure://acct/container/", "azure://acct/container/page-000000.json"),
        ],
    )
    def test_uri_prefix_composition_is_scheme_agnostic(
        self, tmp_path: Path, uri_prefix: str, expected: str,
    ):
        page = tmp_path / "page-000000.json"
        page.write_text("{}\n")
        session = _SpySession()
        sink = PinotIngestFromUriSink(
            "http://pinot:9000", session=session, uri_prefix=uri_prefix,
        )
        sink.ingest_file(page, "ds")
        from urllib.parse import parse_qs, urlparse
        url, _ = session.posts[0]
        qs = parse_qs(urlparse(url).query)
        assert qs["sourceURIStr"][0] == expected

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


# ─────────────────────────────────────────────────────────────────────────────
# DruidHttpSqlPager — covers the SQL composer + pagination loop
# ─────────────────────────────────────────────────────────────────────────────


from migrator.realtime.backfill_runner import (
    DruidHttpSqlPager,
    PinotIngestFromFileSink,
)


class _PagerSpyResp:
    def __init__(self, payload) -> None:
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _PagerSpySession:
    def __init__(self, pages: list[list[dict]]) -> None:
        self._pages = pages
        self.calls: list[tuple[str, dict]] = []
        self.headers: dict[str, str] = {}

    def post(self, url, *, data=None, timeout=None, **kwargs):
        # The pager hits /druid/v2/sql; record the body so we can
        # assert SQL composition without scraping log output.
        self.calls.append((url, {"data": data, **kwargs}))
        idx = len(self.calls) - 1
        if idx < len(self._pages):
            return _PagerSpyResp(self._pages[idx])
        return _PagerSpyResp([])  # empty signals end of pagination


class TestDruidHttpSqlPager:
    def test_pages_until_empty(self):
        # Two pages of 3 rows each, third is empty → loop exits.
        session = _PagerSpySession([
            [{"a": i} for i in range(3)],
            [{"a": i} for i in range(3, 6)],
        ])
        pager = DruidHttpSqlPager("http://druid:8888/", session=session)
        pages = list(pager.page_rows(
            "ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            page_rows=3,
        ))
        assert len(pages) == 2
        assert pages[0][0]["a"] == 0
        # Three POSTs: pages 1, 2, then the trailing empty fetch that
        # tells the loop pagination is done.
        assert len(session.calls) == 3

    def test_short_last_page_terminates_without_extra_fetch(self):
        # When a page returns fewer rows than `page_rows`, the loop
        # short-circuits — no extra "is the cursor empty" round-trip.
        session = _PagerSpySession([
            [{"a": i} for i in range(2)],  # short: only 2 of 3 requested
        ])
        pager = DruidHttpSqlPager("http://druid:8888", session=session)
        pages = list(pager.page_rows(
            "ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            page_rows=3,
        ))
        assert len(pages) == 1
        assert len(session.calls) == 1   # no probe-for-empty fetch

    def test_sql_uses_druid_timestamp_format_not_iso(self):
        session = _PagerSpySession([[]])
        pager = DruidHttpSqlPager("http://druid:8888", session=session)
        list(pager.page_rows(
            "ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00.123Z",
            page_rows=10,
        ))
        # Druid SQL TIMESTAMP literals must use 'yyyy-MM-dd HH:mm:ss' —
        # not ISO 8601. The pager rewrites T→space and strips Z.
        body = session.calls[0][1]["data"]
        assert "TIMESTAMP '2024-01-01 00:00:00'" in body
        assert "TIMESTAMP '2024-02-01 00:00:00.123'" in body
        assert "T00:00:00Z" not in body

    def test_offset_advances_per_page(self):
        session = _PagerSpySession([
            [{"a": i} for i in range(3)],
            [{"a": i} for i in range(3, 6)],
        ])
        pager = DruidHttpSqlPager("http://druid:8888", session=session)
        list(pager.page_rows(
            "ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            page_rows=3,
        ))
        # Page 1: OFFSET 0; page 2: OFFSET 3; page 3 (probe): OFFSET 6
        offsets_seen = [
            "OFFSET 0" in c[1]["data"] for c in session.calls
        ]
        assert any("OFFSET 0 ROWS" in c[1]["data"] for c in session.calls)
        assert any("OFFSET 3 ROWS" in c[1]["data"] for c in session.calls)

    def test_default_session_built_when_none_given(self):
        # Smoke test: __init__ doesn't blow up without a session.
        pager = DruidHttpSqlPager("http://druid:8888")
        assert pager._session is not None


# ─────────────────────────────────────────────────────────────────────────────
# PinotIngestFromFileSink — multipart upload sink
# ─────────────────────────────────────────────────────────────────────────────


class _FileSinkResp:
    def __init__(self, status: int = 200, text: str = "") -> None:
        self.status_code = status
        self.text = text


class _FileSinkSession:
    def __init__(self, status: int = 200, text: str = "") -> None:
        self.posts: list[tuple[str, dict]] = []
        self.headers: dict[str, str] = {}
        self._status = status
        self._text = text

    def post(self, url, *, files=None, timeout=None, **kwargs):
        self.posts.append((url, {"files": files, **kwargs}))
        return _FileSinkResp(self._status, self._text)


class TestPinotIngestFromFileSink:
    def test_upload_uses_multipart(self, tmp_path: Path):
        page = tmp_path / "page-000000.json"
        page.write_text('{"a": 1}\n')
        session = _FileSinkSession(status=200)
        sink = PinotIngestFromFileSink("http://pinot:9000/", session=session)

        sink.ingest_file(page, "ds")

        assert len(session.posts) == 1
        url, kwargs = session.posts[0]
        assert "/ingestFromFile" in url
        assert "tableNameWithType=ds_OFFLINE" in url
        assert kwargs["files"] is not None
        # The actual file body — name is preserved on the multipart part.
        name, fh, mime = kwargs["files"]["file"]
        assert name == "page-000000.json"
        assert mime == "application/octet-stream"

    def test_201_is_accepted(self, tmp_path: Path):
        # Pinot returns 201 on first-create, 200 on idempotent re-ingest.
        page = tmp_path / "p.json"
        page.write_text("{}\n")
        session = _FileSinkSession(status=201)
        sink = PinotIngestFromFileSink("http://pinot:9000", session=session)
        sink.ingest_file(page, "ds")  # must not raise

    def test_500_raises_runtime_error(self, tmp_path: Path):
        page = tmp_path / "p.json"
        page.write_text("{}\n")
        session = _FileSinkSession(status=500, text="server burped")
        sink = PinotIngestFromFileSink("http://pinot:9000", session=session)
        with pytest.raises(RuntimeError, match="ingestFromFile failed"):
            sink.ingest_file(page, "ds")

    def test_default_session_built_when_none_given(self, tmp_path: Path):
        # Without a session injected, the sink falls back to module-level
        # `requests.post`. Verify __init__ accepts the no-session path.
        sink = PinotIngestFromFileSink("http://pinot:9000")
        assert sink._session is None


# ─────────────────────────────────────────────────────────────────────────────
# Page-level resume
# ─────────────────────────────────────────────────────────────────────────────


from migrator.realtime.backfill_runner import (
    _RESUME_FINGERPRINT_FILENAME,
    _scan_completed_pages,
)


class _FailingIngestSink:
    """Sink that records every call and raises on the Nth ingest_file
    invocation so we can simulate mid-backfill failures."""

    def __init__(self, fail_on: int) -> None:
        self.fail_on = fail_on
        self.received: list = []

    def ingest_file(self, ndjson_path, table_name) -> None:
        self.received.append((Path(ndjson_path), table_name))
        if len(self.received) == self.fail_on:
            raise RuntimeError(f"simulated failure on ingest #{self.fail_on}")


class _RecordingPager:
    """Pager that yields preset pages but also records the start_offset
    it received — so a resume test can prove the offset bumped."""

    def __init__(self, pages: list[list[dict]]) -> None:
        self._pages = pages
        self.last_start_offset: int | None = None

    def page_rows(self, datasource, *, start_iso, end_iso, page_rows, start_offset=0):
        self.last_start_offset = start_offset
        # Skip the first ``start_offset // page_rows`` pages — the
        # real DruidHttpSqlPager would do this via OFFSET in SQL.
        skip = start_offset // page_rows
        for p in self._pages[skip:]:
            yield p


class TestRunBackfillPageResume:
    def test_first_run_writes_markers_for_each_ingested_page(self, tmp_path):
        rows = [{"v": i} for i in range(7)]
        pager = StubPager(rows)
        sink = CountingSink()
        run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager, sink=sink,
            page_rows=3,
        )
        # 7 rows / 3 = 3 pages. Each page produces a sibling
        # ``.ingested`` marker after the sink call returns.
        for i in range(3):
            assert (tmp_path / f"page-{i:06d}.json.ingested").exists(), (
                f"missing marker for page {i}"
            )
        # Fingerprint is also written so a resume can validate it.
        assert (tmp_path / _RESUME_FINGERPRINT_FILENAME).exists()

    def test_failure_mid_backfill_leaves_partial_markers(self, tmp_path):
        # Sink errors on the 2nd ingest. Page 0 should have a marker,
        # page 1 should NOT (the marker is written *after* the sink
        # returns successfully).
        rows = [{"v": i} for i in range(7)]
        pager = StubPager(rows)
        sink = _FailingIngestSink(fail_on=2)
        with pytest.raises(RuntimeError, match="simulated failure"):
            run_backfill(
                datasource="ds", pinot_table="ds",
                start_iso="2024-01-01T00:00:00Z",
                end_iso="2024-02-01T00:00:00Z",
                staging_dir=tmp_path,
                pager=pager, sink=sink,
                page_rows=3,
            )
        assert (tmp_path / "page-000000.json.ingested").exists()
        assert not (tmp_path / "page-000001.json.ingested").exists()

    def test_resume_skips_completed_pages(self, tmp_path):
        # First run: ingest 1 page, then fail.
        rows = [{"v": i} for i in range(7)]
        run1_pager = StubPager(rows)
        run1_sink = _FailingIngestSink(fail_on=2)
        with pytest.raises(RuntimeError):
            run_backfill(
                datasource="ds", pinot_table="ds",
                start_iso="2024-01-01T00:00:00Z",
                end_iso="2024-02-01T00:00:00Z",
                staging_dir=tmp_path,
                pager=run1_pager, sink=run1_sink,
                page_rows=3,
            )
        # Second run: page 0's marker exists. Resume should pass
        # start_offset=3 (=1 page * 3 rows) so the pager skips that
        # page entirely. Pages 1 and 2 ingest cleanly this time.
        run2_pager = _RecordingPager([
            [{"v": i} for i in range(0, 3)],   # already done — should be skipped
            [{"v": i} for i in range(3, 6)],
            [{"v": i} for i in range(6, 7)],
        ])
        run2_sink = CountingSink()
        result = run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=run2_pager, sink=run2_sink,
            page_rows=3,
        )
        assert run2_pager.last_start_offset == 3
        assert result.pages_resumed == 1
        # Sink only saw the un-ingested pages (pages 1 + 2).
        assert len(run2_sink.received) == 2
        # All three markers now exist.
        for i in range(3):
            assert (tmp_path / f"page-{i:06d}.json.ingested").exists()

    def test_resume_disabled_replays_from_zero(self, tmp_path):
        # Stage a marker for page 0 manually.
        (tmp_path / "page-000000.json.ingested").write_text("{}")
        pager = StubPager([{"v": 1}, {"v": 2}, {"v": 3}])
        sink = CountingSink()
        result = run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager, sink=sink,
            page_rows=3,
            resume=False,
        )
        # resume=False: ignore the existing marker; pages_resumed=0.
        assert result.pages_resumed == 0

    def test_fingerprint_mismatch_invalidates_resume(self, tmp_path):
        # Stage a fingerprint that doesn't match the current run's
        # identity (different datasource). Resume must NOT skip
        # pages — using a stale fingerprint would apply the wrong
        # source to the wrong target.
        (tmp_path / _RESUME_FINGERPRINT_FILENAME).write_text(json.dumps({
            "datasource": "OTHER_DS",
            "start_iso": "2024-01-01T00:00:00Z",
            "end_iso": "2024-02-01T00:00:00Z",
            "page_rows": 3,
            "time_column": "timestamp",
        }))
        # Stage a marker that would otherwise fool resume.
        (tmp_path / "page-000000.json.ingested").write_text("{}")
        pager = _RecordingPager([
            [{"v": 1}], [{"v": 2}],
        ])
        sink = CountingSink()
        run_backfill(
            datasource="MY_DS", pinot_table="MY_DS",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager, sink=sink,
            page_rows=3,
        )
        # Pager started from offset 0 — fingerprint mismatch means
        # the existing marker can't be trusted.
        assert pager.last_start_offset == 0

    def test_scan_completed_pages_stops_at_first_gap(self, tmp_path):
        # Scenario: pages 0, 1, 3 done but 2 missing. The orchestrator
        # only counts the contiguous run from 0 — re-doing 2 onward
        # is safer than gambling on out-of-order resume.
        for i in (0, 1, 3):
            (tmp_path / f"page-{i:06d}.json.ingested").write_text("{}")
        assert _scan_completed_pages(tmp_path) == 2


# ─────────────────────────────────────────────────────────────────────────────
# Progress streaming
# ─────────────────────────────────────────────────────────────────────────────


from migrator.realtime.backfill_runner import BackfillProgress


class TestProgressCallback:
    def test_callback_fires_once_per_page(self, tmp_path):
        rows = [{"v": i} for i in range(7)]
        pager = StubPager(rows)
        sink = CountingSink()
        events: list[BackfillProgress] = []

        run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager, sink=sink,
            page_rows=3,
            progress_callback=events.append,
        )
        # 7 / 3 = 3 pages → 3 progress events.
        assert len(events) == 3

    def test_callback_payload_fields_populated(self, tmp_path):
        rows = [{"v": i} for i in range(5)]
        pager = StubPager(rows)
        sink = CountingSink()
        events: list[BackfillProgress] = []

        run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager, sink=sink,
            page_rows=2,
            progress_callback=events.append,
        )
        # 5 rows / 2 = 3 pages (2, 2, 1).
        assert events[0].page_index == 0
        assert events[0].rows_in_page == 2
        assert events[0].rows_total_so_far == 2
        assert events[0].pages_done == 1

        assert events[1].page_index == 1
        assert events[1].rows_in_page == 2
        assert events[1].rows_total_so_far == 4

        # Last page — short by design.
        assert events[2].page_index == 2
        assert events[2].rows_in_page == 1
        assert events[2].rows_total_so_far == 5

        # Sanity on derived rate fields: must be positive (or 0 for an
        # absurdly-fast empty page) and elapsed must be non-decreasing.
        assert all(e.rows_per_sec >= 0 for e in events)
        assert all(e.elapsed_s >= 0 for e in events)
        assert events[-1].elapsed_s >= events[0].elapsed_s

    def test_callback_fires_after_marker_is_written(self, tmp_path):
        # Operator monitoring: when a progress event arrives, the
        # corresponding marker is already on disk. This guarantees the
        # event accurately represents work that survived a crash.
        rows = [{"v": i} for i in range(3)]
        pager = StubPager(rows)
        sink = CountingSink()
        marker_existed_at_callback: list[bool] = []

        def check_marker(p: BackfillProgress) -> None:
            marker_path = tmp_path / f"page-{p.page_index:06d}.json.ingested"
            marker_existed_at_callback.append(marker_path.exists())

        run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager, sink=sink,
            page_rows=10,
            progress_callback=check_marker,
        )
        assert marker_existed_at_callback == [True]

    def test_callback_exception_does_not_abort_backfill(self, tmp_path):
        # Misbehaving callbacks are an operator-side bug — they must
        # not abort the migration. Backfill continues; data + markers
        # land normally.
        def bad_callback(_p: BackfillProgress) -> None:
            raise RuntimeError("operator's progress UI exploded")

        rows = [{"v": i} for i in range(5)]
        pager = StubPager(rows)
        sink = CountingSink()
        result = run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager, sink=sink,
            page_rows=2,
            progress_callback=bad_callback,
        )
        # All pages still ingested.
        assert result.pages_dumped == 3
        assert result.rows_dumped == 5
        # Markers all written.
        assert (tmp_path / "page-000002.json.ingested").exists()

    def test_no_callback_means_no_events_no_overhead(self, tmp_path):
        # Backward compat: ``progress_callback`` is optional; the
        # default of None must be a true no-op (not even a "default
        # callback" doing anything).
        rows = [{"v": i} for i in range(3)]
        pager = StubPager(rows)
        sink = CountingSink()
        result = run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=pager, sink=sink,
            page_rows=10,
        )
        assert result.pages_dumped == 1

    def test_resumed_run_progress_uses_absolute_page_index(self, tmp_path):
        # On resume after pages 0-1 completed previously, the FIRST
        # progress event of the new run should report page_index=2,
        # not 0. This matches operator mental-model ("we're now on
        # page 2 of the original 5") rather than "page 0 of this run".
        # Stage marker files directly to fake a previous run's state.
        for i in (0, 1):
            (tmp_path / f"page-{i:06d}.json.ingested").write_text("{}")
        # Stage matching fingerprint so resume kicks in.
        from migrator.realtime.backfill_runner import (
            _RESUME_FINGERPRINT_FILENAME,
            _backfill_fingerprint,
        )
        fp = _backfill_fingerprint(
            "ds", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z", 2, "timestamp",
        )
        (tmp_path / _RESUME_FINGERPRINT_FILENAME).write_text(json.dumps(fp))

        # Pager that yields rows for the un-skipped pages.
        from migrator.realtime.backfill_runner import BackfillProgress

        class _SkipAwarePager:
            def page_rows(self, ds, *, start_iso, end_iso, page_rows, start_offset=0):
                # 5 rows total, 2 per page, start at offset (skipped 4 = 2 pages).
                # Yield the un-ingested remainder.
                yield [{"v": i} for i in range(start_offset, start_offset + 2)]
                yield [{"v": i} for i in range(start_offset + 2, start_offset + 3)]

        events: list[BackfillProgress] = []
        result = run_backfill(
            datasource="ds", pinot_table="ds",
            start_iso="2024-01-01T00:00:00Z",
            end_iso="2024-02-01T00:00:00Z",
            staging_dir=tmp_path,
            pager=_SkipAwarePager(), sink=CountingSink(),
            page_rows=2,
            progress_callback=events.append,
        )
        assert result.pages_resumed == 2
        # First event of the new run is page_index=2, not 0.
        assert events[0].page_index == 2
        assert events[1].page_index == 3
