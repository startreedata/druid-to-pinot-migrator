"""
Thin Druid Coordinator HTTP client.

Same design pattern as DruidOverlordClient:
- Injectable session for testability (mock without monkey-patching).
- Returns plain dicts / minimal domain objects, not raw response bodies.
- Typed error class so callers can distinguish HTTP / JSON / shape failures.

This client deliberately does NOT depend on `requests` at import time —
the import is lazy in __init__ so the migrator core stays
dependency-light (DruidCoordinatorClient is only loaded when CLI commands
that need it are imported).
"""

from __future__ import annotations

import json
from typing import Any, Protocol


class DruidCoordinatorError(Exception):
    """Raised when the Coordinator returns an error or unexpected payload."""


class _Session(Protocol):
    """Slice of requests.Session this client actually uses."""

    def get(self, url: str, *, timeout: float | None = ...) -> Any: ...
    def post(self, url: str, *, data: bytes | str | None = ...,
             timeout: float | None = ...) -> Any: ...


# ─────────────────────────────────────────────────────────────────────────────
# Domain objects (kept simple; not pydantic to avoid coupling)
# ─────────────────────────────────────────────────────────────────────────────


class SegmentMetadata:
    """
    Aggregated, merged segment-metadata for a Druid datasource.

    Built from Druid's ``segmentMetadata`` query (``merge=true``) plus
    the Coordinator's segment-listing endpoint.
    """

    __slots__ = ("columns", "intervals", "size_bytes", "num_rows")

    def __init__(
        self,
        columns: dict[str, dict[str, Any]],
        intervals: list[str],
        size_bytes: int = 0,
        num_rows: int = 0,
    ) -> None:
        # columns: {col_name: {"type": "STRING", "hasMultipleValues": bool, ...}}
        self.columns = columns
        self.intervals = intervals
        self.size_bytes = size_bytes
        self.num_rows = num_rows

    def __repr__(self) -> str:  # pragma: no cover (debug only)
        return (
            f"SegmentMetadata(cols={list(self.columns)}, "
            f"intervals={self.intervals})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Client
# ─────────────────────────────────────────────────────────────────────────────


class DruidCoordinatorClient:
    """HTTP client for the Druid Coordinator + Broker query API."""

    def __init__(
        self,
        coordinator_url: str,
        broker_url: str | None = None,
        session: _Session | None = None,
        *,
        timeout: float = 15.0,
    ) -> None:
        self._coord = coordinator_url.rstrip("/")
        # Broker is optional; segmentMetadata queries can also go through
        # the Router (which is what some clusters expose externally).
        self._broker = (broker_url or coordinator_url).rstrip("/")
        self._timeout = timeout
        if session is None:
            import requests

            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
        self._session = session

    # ── public API ─────────────────────────────────────────────────────────

    def list_datasources(self) -> list[str]:
        """Return all datasources the Coordinator knows about."""
        return self._json_get(f"{self._coord}/druid/coordinator/v1/datasources")

    def datasource_exists(self, datasource: str) -> bool:
        try:
            return datasource in self.list_datasources()
        except DruidCoordinatorError:
            return False

    def get_datasource_summary(self, datasource: str) -> dict:
        """
        Return ``/datasources/{ds}`` payload — top-level metadata
        (segments count, size, intervals, etc.).
        """
        url = (
            f"{self._coord}/druid/coordinator/v1/datasources/{datasource}"
            "?full"
        )
        return self._json_get(url)

    def get_segment_metadata(self, datasource: str) -> SegmentMetadata:
        """
        Run a ``segmentMetadata`` query (merged across all segments) to
        learn the column set and types Druid sees for this datasource.

        The response is a list (one entry per segment); ``merge=true`` makes
        Druid collapse them into a single merged entry.
        """
        url = f"{self._broker}/druid/v2/"
        # Don't restrict analysisTypes — Druid's default response already
        # includes the columns + intervals + size we need, and an earlier
        # attempt at narrowing to ["interval", "size", "rowSignature"]
        # broke because ROWSIGNATURE isn't a valid AnalysisType enum.
        payload = {
            "queryType": "segmentMetadata",
            "dataSource": datasource,
            "merge": True,
        }
        body = self._json_post(url, payload)
        if not isinstance(body, list) or not body:
            raise DruidCoordinatorError(
                f"segmentMetadata for '{datasource}' returned empty/invalid: {body!r}"
            )
        merged = body[0]
        columns = merged.get("columns") or {}
        if not isinstance(columns, dict):
            raise DruidCoordinatorError(
                f"segmentMetadata.columns for '{datasource}' is not a dict: {columns!r}"
            )
        intervals_obj = merged.get("intervals") or []
        if isinstance(intervals_obj, dict):
            intervals = list(intervals_obj.keys())
        else:
            intervals = list(intervals_obj)
        size_bytes = int(merged.get("size") or 0)
        num_rows = int(merged.get("numRows") or 0)
        return SegmentMetadata(
            columns=columns,
            intervals=intervals,
            size_bytes=size_bytes,
            num_rows=num_rows,
        )

    # ── internals ──────────────────────────────────────────────────────────

    def _json_get(self, url: str) -> Any:
        resp = self._session.get(url, timeout=self._timeout)
        if resp.status_code != 200:
            raise DruidCoordinatorError(
                f"GET {url} returned {resp.status_code}: "
                f"{getattr(resp, 'text', '')[:500]}"
            )
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise DruidCoordinatorError(
                f"GET {url} returned non-JSON body: {exc}"
            ) from exc

    def _json_post(self, url: str, payload: dict) -> Any:
        resp = self._session.post(
            url, data=json.dumps(payload), timeout=self._timeout
        )
        if resp.status_code != 200:
            raise DruidCoordinatorError(
                f"POST {url} returned {resp.status_code}: "
                f"{getattr(resp, 'text', '')[:500]}"
            )
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise DruidCoordinatorError(
                f"POST {url} returned non-JSON body: {exc}"
            ) from exc
