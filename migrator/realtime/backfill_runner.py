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

    def __init__(self, router_url: str, *, timeout: float = 60.0) -> None:
        import requests

        self._url = router_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def page_rows(
        self,
        datasource: str,
        *,
        start_iso: str,
        end_iso: str,
        page_rows: int,
    ) -> Iterator[list[dict]]:
        offset = 0
        while True:
            sql = (
                f'SELECT * FROM "{datasource}" '
                f"WHERE __time >= TIMESTAMP '{start_iso}' "
                f"AND __time <  TIMESTAMP '{end_iso}' "
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

    def __init__(self, controller_url: str, *, timeout: float = 600.0) -> None:
        self._url = controller_url.rstrip("/")
        self._timeout = timeout

    def ingest_file(self, ndjson_path: str | Path, table_name: str) -> None:
        import urllib.parse

        import requests

        batch_cfg = {
            "inputFormat": "json",
            "recordReaderSpec": {
                "dataFormat": "json",
                "className": (
                    "org.apache.pinot.plugin.inputformat.json.JSONRecordReader"
                ),
            },
        }
        url = (
            f"{self._url}/ingestFromFile?tableNameWithType={table_name}_OFFLINE"
            f"&batchConfigMapStr={urllib.parse.quote(json.dumps(batch_cfg))}"
        )
        path = Path(ndjson_path)
        with path.open("rb") as fh:
            resp = requests.post(
                url,
                files={"file": (path.name, fh, "application/octet-stream")},
                timeout=self._timeout,
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Pinot ingestFromFile failed: {resp.status_code} {resp.text[:300]}"
            )


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
) -> BackfillResult:
    """
    Page rows out of Druid for the watermark-bounded interval, write each
    page to its own NDJSON file under ``staging_dir``, then push every
    NDJSON file to the Pinot OFFLINE table.

    The ``pager`` and ``sink`` arguments are dependency-injection seams —
    tests pass in stubs; real callers use ``DruidHttpSqlPager`` and
    ``PinotIngestFromFileSink``.
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
                fh.write(json.dumps(r) + "\n")
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
) -> Iterable[Path]:
    """Page Druid → NDJSON files only (skip Pinot ingest). Yields each path."""
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)
    n = 0
    for rows in pager.page_rows(
        datasource, start_iso=start_iso, end_iso=end_iso, page_rows=page_rows
    ):
        page_path = staging / f"page-{n:06d}.json"
        with page_path.open("w") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
        yield page_path
        n += 1
