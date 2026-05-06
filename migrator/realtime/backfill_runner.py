"""
Tooling-path implementation of the OFFLINE backfill phase.

Use case: small-to-medium datasets where running a Druid SQL pager and
pushing the resulting NDJSON to Pinot's ``/ingestFromFile`` endpoint is
fast enough. Larger datasets should use the runbook (Druid → object store
→ Pinot ``LaunchDataIngestionJob``) which this module's runbook companion
generates.

The orchestrator is structured around two thin interfaces — a Druid SQL
pager and a Pinot ingest sink — so unit tests can swap in stubs and avoid
needing live clusters.
"""

from __future__ import annotations

import datetime
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Protocol


# ─────────────────────────────────────────────────────────────────────────────
# Interfaces — clean seams for testability and future Kinesis support
# ─────────────────────────────────────────────────────────────────────────────


class DruidSqlPager(Protocol):
    """Page rows out of a Druid datasource via SQL."""

    def page_rows(
        self,
        datasource: str,
        *,
        start_iso: str,
        end_iso: str,
        page_rows: int,
        start_offset: int = 0,
    ) -> Iterator[list[dict]]:
        """Yield successive lists of rows (each at most ``page_rows`` long).

        ``start_offset`` is the row-OFFSET into the underlying SQL —
        the orchestrator passes ``rows_already_ingested`` here on a
        resume so the pager skips pages already covered by markers.
        Defaults to 0 for fresh runs.
        """


class PinotIngestSink(Protocol):
    """Sink that accepts a local NDJSON file for a Pinot OFFLINE table."""

    def ingest_file(self, ndjson_path: str | Path, table_name: str) -> None: ...


# ─────────────────────────────────────────────────────────────────────────────
# Default DruidSqlPager (active; uses the existing DruidClient pattern)
# ─────────────────────────────────────────────────────────────────────────────


class DruidHttpSqlPager:
    """
    Default ``DruidSqlPager`` implementation that talks to the Druid Router.

    Reuses the same shape as ``tests/docker/cluster_clients.DruidClient``
    but kept independent so production callers don't depend on test code.
    """

    def __init__(
        self,
        router_url: str,
        *,
        timeout: float = 60.0,
        session: "requests.Session | None" = None,
    ) -> None:
        import requests

        self._url = router_url.rstrip("/")
        self._timeout = timeout
        if session is None:
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
        self._session = session

    def page_rows(
        self,
        datasource: str,
        *,
        start_iso: str,
        end_iso: str,
        page_rows: int,
        start_offset: int = 0,
    ) -> Iterator[list[dict]]:
        # Druid SQL TIMESTAMP literals require 'yyyy-MM-dd HH:mm:ss[.SSS]'
        # format — not ISO 8601 with 'T'/'Z'. Convert.
        def _druid_ts(s: str) -> str:
            return s.replace("T", " ").rstrip("Z").rstrip()
        start_druid = _druid_ts(start_iso)
        end_druid = _druid_ts(end_iso)
        offset = start_offset
        while True:
            sql = (
                f'SELECT * FROM "{datasource}" '
                f"WHERE __time >= TIMESTAMP '{start_druid}' "
                f"AND __time <  TIMESTAMP '{end_druid}' "
                f"ORDER BY __time "
                f"OFFSET {offset} ROWS FETCH NEXT {page_rows} ROWS ONLY"
            )
            resp = self._session.post(
                f"{self._url}/druid/v2/sql",
                data=json.dumps({"query": sql, "resultFormat": "object"}),
                timeout=self._timeout,
            )
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                return
            yield rows
            if len(rows) < page_rows:
                return
            offset += len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Default PinotIngestSink
# ─────────────────────────────────────────────────────────────────────────────


class PinotIngestFromFileSink:
    """Default sink: HTTPS POST to ``/ingestFromFile`` on the Pinot controller.

    Uploads the NDJSON file body via multipart/form-data — the data
    travels controller-side, then Pinot builds a segment from it. Fine
    for small backfills (≤ ~1M rows per page) but doesn't scale: the
    file body is in-memory on the controller during the upload.

    For larger datasets prefer ``PinotIngestFromUriSink``: control-plane
    only, the controller pulls the data from a URI it can read directly
    (file://, s3://, gs://). One round-trip per file regardless of size.
    """

    def __init__(
        self,
        controller_url: str,
        *,
        timeout: float = 600.0,
        session: "requests.Session | None" = None,
    ) -> None:
        self._url = controller_url.rstrip("/")
        self._timeout = timeout
        self._session = session  # may be None — created lazily in ingest_file

    def ingest_file(self, ndjson_path: str | Path, table_name: str) -> None:
        import urllib.parse

        import requests

        # Pinot 1.5+ rejects nested JSON objects in batchConfigMapStr —
        # the values must be primitives. The simple form below is auto-resolved
        # by the controller using its default JSONRecordReader.
        batch_cfg = {"inputFormat": "json"}
        url = (
            f"{self._url}/ingestFromFile?tableNameWithType={table_name}_OFFLINE"
            f"&batchConfigMapStr={urllib.parse.quote(json.dumps(batch_cfg))}"
        )
        path = Path(ndjson_path)
        # multipart upload sets its own Content-Type; using a session is fine
        # — requests strips the JSON Content-Type when `files=` is supplied.
        post = self._session.post if self._session is not None else requests.post
        with path.open("rb") as fh:
            resp = post(
                url,
                files={"file": (path.name, fh, "application/octet-stream")},
                timeout=self._timeout,
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Pinot ingestFromFile failed: {resp.status_code} {resp.text[:300]}"
            )


class PinotIngestFromUriSink:
    """Streaming-style sink: HTTPS POST to ``/ingestFromURI``.

    Unlike ``PinotIngestFromFileSink``, this is a **control-plane**
    call — the dpm POST contains only the URI, not the data. The
    Pinot controller reads the source file directly via the URI's
    scheme (``file://``, ``s3://``, ``gs://`` — depending on which
    PinotFS plugins are installed).

    Use this for large backfills where uploading file bodies through
    the controller would be wasteful or exceed the request-size limit.

    URI translation:
      - The pager hands the sink a local filesystem path
        (e.g. ``/tmp/staging/page-000000.json``).
      - The sink turns that into a URI the controller can resolve.
      - Default: ``file://<absolute path>``. This works when the
        staging directory is on a filesystem the controller can see
        — typically a Kubernetes shared volume, or local-localhost.
      - For object storage, pass a ``uri_prefix`` like
        ``s3://my-bucket/staging/`` and arrange for the staging files
        to be uploaded there *before* calling ``ingest_file``. The
        sink will combine ``uri_prefix`` with the file's basename.

    Each call is one HTTP round-trip — the data transfer happens on
    the Pinot side, not over the dpm → controller link.
    """

    def __init__(
        self,
        controller_url: str,
        *,
        timeout: float = 600.0,
        session: "requests.Session | None" = None,
        uri_prefix: str | None = None,
    ) -> None:
        self._url = controller_url.rstrip("/")
        self._timeout = timeout
        self._uri_prefix = uri_prefix
        if session is None:
            import requests
            session = requests.Session()
        self._session = session

    def _build_source_uri(self, path: Path) -> str:
        if self._uri_prefix is None:
            # Local file://; the controller must share a filesystem.
            return path.resolve().as_uri()
        # Object-store mode: caller has already uploaded file under the
        # prefix; we reference it by basename.
        prefix = self._uri_prefix.rstrip("/")
        return f"{prefix}/{path.name}"

    def ingest_file(self, ndjson_path: str | Path, table_name: str) -> None:
        import urllib.parse

        path = Path(ndjson_path)
        source_uri = self._build_source_uri(path)
        batch_cfg = {"inputFormat": "json"}

        url = (
            f"{self._url}/ingestFromURI"
            f"?tableNameWithType={table_name}_OFFLINE"
            f"&batchConfigMapStr={urllib.parse.quote(json.dumps(batch_cfg))}"
            f"&sourceURIStr={urllib.parse.quote(source_uri)}"
        )
        # /ingestFromURI is a control-plane-only call: no body, parameters
        # in the querystring. Setting Content-Type: application/json with
        # an empty body trips Pinot's content negotiation and returns 415
        # across all tested versions; passing None here unsets any value
        # the caller may have set on the shared session.
        resp = self._session.post(
            url,
            timeout=self._timeout,
            headers={"Content-Type": None},
        )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Pinot ingestFromURI failed for {source_uri}: "
                f"{resp.status_code} {resp.text[:300]}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Time-column normalisation
# ─────────────────────────────────────────────────────────────────────────────
#
# Druid SQL ``SELECT *`` returns the time column as ``__time`` (Druid's
# internal name) typed as an ISO 8601 string. The Pinot schema's time
# column is whatever the user named it (typically the original
# ``timestampSpec.column`` — ``timestamp`` is dpm's generator default)
# and is typed ``LONG/MILLISECONDS:EPOCH``.
#
# Without this normalisation step, Pinot would silently drop ``__time``
# and the OFFLINE segment's ``[startTime, endTime]`` would collapse to
# the single millisecond at which the segment was generated, breaking
# hybrid-table time-boundary routing.
#
# The conversion is intentionally tolerant: numeric ``__time`` values
# (which some Druid result formats produce) are passed through as-is.


def _iso_to_millis(s: str) -> int:
    """Convert a Druid SQL ISO 8601 timestamp into epoch milliseconds.

    Druid's SQL layer emits ``"2026-04-23T00:15:22.967Z"`` (always UTC,
    always millisecond-precision or below). This bypasses
    ``datetime.fromisoformat`` so we keep working on Python 3.10 where
    that builtin doesn't accept a trailing ``Z``.
    """
    s = s.rstrip("Z").replace("T", " ")
    if "." in s:
        head, frac = s.split(".")
        # Pad / truncate to exactly 6 digits so ``%f`` is happy.
        frac = (frac + "000000")[:6]
        dt = datetime.datetime.strptime(
            f"{head}.{frac}", "%Y-%m-%d %H:%M:%S.%f"
        )
    else:
        dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    # int(...) truncates toward zero, which matches Druid's own behaviour
    # when sub-millisecond precision is dropped.
    return int(dt.timestamp() * 1000)


def _normalize_time_column(row: dict, time_column: str) -> dict:
    """Rename ``__time`` → ``time_column`` (in-place semantically).

    Returns a row dict that's safe to JSON-serialise. The input dict is
    not mutated when no rename is needed, so callers can still rely on
    ``row is unchanged`` for performance-sensitive paths.
    """
    if time_column == "__time" or "__time" not in row:
        return row
    out = dict(row)
    raw = out.pop("__time")
    if isinstance(raw, str):
        out[time_column] = _iso_to_millis(raw)
    else:
        # Already numeric (epoch ms) — pass through.
        out[time_column] = raw
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class BackfillResult:
    pages_dumped: int
    rows_dumped: int
    files_ingested: int
    staging_dir: Path
    # Pages skipped because their ``.ingested`` marker already existed
    # from a previous run. Sum of (pages_dumped + pages_resumed) is
    # the total pages the operator's interval covers.
    pages_resumed: int = 0


@dataclass
class BackfillProgress:
    """One progress event, emitted to the progress callback after each
    page is fully processed (write → ingest → marker).

    Operators care about three things during a long backfill: am I
    making progress, how fast, and how much longer. ``rows_total_so_far``
    + ``elapsed_s`` answers the first two; ``rows_per_sec`` is derived
    so dashboards don't have to compute it.

    ``page_index`` is the absolute index across the whole interval
    (NOT the index within this run) — so on a resume of a 250-page
    backfill that previously completed 200, the first event of the
    new run reports page_index=200, not 0.
    """
    page_index: int
    rows_in_page: int
    rows_total_so_far: int
    pages_done: int
    pages_resumed: int
    elapsed_s: float
    rows_per_sec: float


# ─────────────────────────────────────────────────────────────────────────────
# Page-level resume helpers
# ─────────────────────────────────────────────────────────────────────────────


_RESUME_FINGERPRINT_FILENAME = "_backfill_fingerprint.json"


def _backfill_fingerprint(
    datasource: str, start_iso: str, end_iso: str,
    page_rows: int, time_column: str,
) -> dict:
    """Top-level fingerprint of the backfill identity.

    Persisted to staging_dir on the first page write; on resume we
    refuse to honour markers from a stale fingerprint (different
    datasource / interval / page size) — better to redo work than
    silently apply someone else's NDJSON to the wrong table.
    """
    return {
        "datasource": datasource,
        "start_iso": start_iso,
        "end_iso": end_iso,
        "page_rows": page_rows,
        "time_column": time_column,
    }


def _scan_completed_pages(staging: Path) -> int:
    """Return the highest contiguous page index N such that
    ``page-NNNNNN.json.ingested`` exists for all 0..N. Pages with
    a hole (e.g. 0,1,3 done but 2 missing) count only the initial
    contiguous run — we re-do the rest because the gap suggests a
    partial failure mid-page."""
    n = 0
    while (staging / f"page-{n:06d}.json.ingested").exists():
        n += 1
    return n


def _write_marker(page_path: Path, page_index: int, rows: int) -> None:
    """Atomically write the marker sidecar after a successful ingest.
    Tmp + rename guarantees a crash mid-write doesn't leave a
    half-formed marker that resume would mistakenly trust."""
    marker = page_path.with_name(page_path.name + ".ingested")
    tmp = marker.with_suffix(marker.suffix + ".tmp")
    tmp.write_text(json.dumps({
        "page_index": page_index,
        "rows": rows,
        "ingested_at": _utc_iso_now(),
    }) + "\n")
    tmp.replace(marker)


def _utc_iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run_backfill(
    *,
    datasource: str,
    pinot_table: str,
    start_iso: str,
    end_iso: str,
    staging_dir: str | Path,
    pager: DruidSqlPager,
    sink: PinotIngestSink,
    page_rows: int = 50_000,
    time_column: str = "timestamp",
    resume: bool = True,
    progress_callback: "Callable[[BackfillProgress], None] | None" = None,
) -> BackfillResult:
    """
    Page rows out of Druid for the watermark-bounded interval, write each
    page to its own NDJSON file under ``staging_dir``, then push every
    NDJSON file to the Pinot OFFLINE table.

    Each page is processed end-to-end before moving to the next:

      1. Page rows are written to ``page-NNNNNN.json``.
      2. ``sink.ingest_file`` is called for that page.
      3. A ``page-NNNNNN.json.ingested`` marker is written atomically.

    If the run crashes between steps 2 and 3, the marker is missing
    on the next run — we re-ingest that page (idempotent at the
    Druid-source side; Pinot may receive a duplicate segment, which
    operators can deduplicate by table-name suffix or via the
    cutover-report log).

    ``resume`` (default True) scans ``staging_dir`` for existing
    markers on entry: if the fingerprint matches the current run's
    identity (datasource / interval / page size / time column) and
    pages 0..N are all marked ingested, the pager starts from
    OFFSET = (N+1) * page_rows. Pass ``resume=False`` (or delete
    ``staging_dir``) to force a fresh ingest.

    The ``pager`` and ``sink`` arguments are dependency-injection seams —
    tests pass in stubs; real callers use ``DruidHttpSqlPager`` and
    ``PinotIngestFromFileSink``.

    ``time_column`` is the Pinot schema's time column name (default
    ``"timestamp"``, matching dpm's generator). Druid's ``__time`` column
    is renamed to this name on each row, and ISO 8601 strings are
    converted to epoch milliseconds.
    """
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)

    fingerprint = _backfill_fingerprint(
        datasource, start_iso, end_iso, page_rows, time_column,
    )
    fp_path = staging / _RESUME_FINGERPRINT_FILENAME
    pages_resumed = 0
    if resume and fp_path.exists():
        try:
            stored = json.loads(fp_path.read_text())
        except json.JSONDecodeError:
            stored = None
        if stored == fingerprint:
            pages_resumed = _scan_completed_pages(staging)
        # Mismatched fingerprint: silently start over. The fingerprint
        # file itself gets overwritten below so the next run sees a
        # clean state.
    fp_path.write_text(json.dumps(fingerprint, sort_keys=True) + "\n")

    pages_dumped = 0
    rows_dumped = 0
    files_ingested = 0
    page_index = pages_resumed
    start_offset = pages_resumed * page_rows
    # Wall-clock anchor for ``elapsed_s`` / ``rows_per_sec`` reporting.
    # Anchored at the START of this run, not the start of the original
    # backfill — operators monitoring a resumed run want to see the
    # current run's throughput, not a confusingly-low average that
    # includes the prior crashed run's wall clock.
    started_at = time.monotonic()

    for rows in pager.page_rows(
        datasource, start_iso=start_iso, end_iso=end_iso,
        page_rows=page_rows, start_offset=start_offset,
    ):
        page_path = staging / f"page-{page_index:06d}.json"
        with page_path.open("w") as fh:
            for r in rows:
                normalized = _normalize_time_column(r, time_column)
                fh.write(json.dumps(normalized) + "\n")
        sink.ingest_file(page_path, pinot_table)
        _write_marker(page_path, page_index, len(rows))

        pages_dumped += 1
        rows_dumped += len(rows)
        files_ingested += 1
        page_index += 1

        if progress_callback is not None:
            elapsed = time.monotonic() - started_at
            # Floor at ~1ms to avoid /0 on the very first event of an
            # absurdly fast run (e.g. tiny test fixtures).
            rate = rows_dumped / max(elapsed, 0.001)
            try:
                progress_callback(BackfillProgress(
                    page_index=page_index - 1,
                    rows_in_page=len(rows),
                    rows_total_so_far=rows_dumped,
                    pages_done=pages_dumped,
                    pages_resumed=pages_resumed,
                    elapsed_s=elapsed,
                    rows_per_sec=rate,
                ))
            except Exception:  # noqa: BLE001
                # A misbehaving callback must never abort the backfill —
                # the operator's progress display is a nicety, not
                # load-bearing on data correctness. Silent swallow is
                # the right call here.
                pass

    return BackfillResult(
        pages_dumped=pages_dumped,
        rows_dumped=rows_dumped,
        files_ingested=files_ingested,
        staging_dir=staging,
        pages_resumed=pages_resumed,
    )


# Convenience for callers that just want to write NDJSON without pushing
def dump_to_ndjson(
    *,
    datasource: str,
    start_iso: str,
    end_iso: str,
    staging_dir: str | Path,
    pager: DruidSqlPager,
    page_rows: int = 50_000,
    time_column: str = "timestamp",
) -> Iterable[Path]:
    """Page Druid → NDJSON files only (skip Pinot ingest). Yields each path.

    Applies the same ``__time`` → ``time_column`` rename as
    ``run_backfill`` so the dumped files are directly ingestible by Pinot.
    """
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)
    n = 0
    for rows in pager.page_rows(
        datasource, start_iso=start_iso, end_iso=end_iso, page_rows=page_rows
    ):
        page_path = staging / f"page-{n:06d}.json"
        with page_path.open("w") as fh:
            for r in rows:
                normalized = _normalize_time_column(r, time_column)
                fh.write(json.dumps(normalized) + "\n")
        yield page_path
        n += 1
