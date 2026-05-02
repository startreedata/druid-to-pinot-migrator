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
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Protocol


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
    ) -> Iterator[list[dict]]:
        """Yield successive lists of rows (each at most ``page_rows`` long)."""


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
    ) -> Iterator[list[dict]]:
        # Druid SQL TIMESTAMP literals require 'yyyy-MM-dd HH:mm:ss[.SSS]'
        # format — not ISO 8601 with 'T'/'Z'. Convert.
        def _druid_ts(s: str) -> str:
            return s.replace("T", " ").rstrip("Z").rstrip()
        start_druid = _druid_ts(start_iso)
        end_druid = _druid_ts(end_iso)
        offset = 0
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
    """Default sink: HTTPS POST to ``/ingestFromFile`` on the Pinot controller."""

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
) -> BackfillResult:
    """
    Page rows out of Druid for the watermark-bounded interval, write each
    page to its own NDJSON file under ``staging_dir``, then push every
    NDJSON file to the Pinot OFFLINE table.

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

    pages_dumped = 0
    rows_dumped = 0
    paths: list[Path] = []

    for rows in pager.page_rows(
        datasource, start_iso=start_iso, end_iso=end_iso, page_rows=page_rows
    ):
        page_path = staging / f"page-{pages_dumped:06d}.json"
        with page_path.open("w") as fh:
            for r in rows:
                normalized = _normalize_time_column(r, time_column)
                fh.write(json.dumps(normalized) + "\n")
        paths.append(page_path)
        pages_dumped += 1
        rows_dumped += len(rows)

    for p in paths:
        sink.ingest_file(p, pinot_table)

    return BackfillResult(
        pages_dumped=pages_dumped,
        rows_dumped=rows_dumped,
        files_ingested=len(paths),
        staging_dir=staging,
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
