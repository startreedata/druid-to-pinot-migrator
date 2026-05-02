"""Default Druid + Pinot SQL clients backed by ``requests``.

Each client takes an optional ``session`` so callers can plug in an
already-authenticated ``requests.Session`` (the same one
``migrator.auth.session_from_env`` builds).
"""

from __future__ import annotations

import json

import requests


class DruidHttpSqlClient:
    """Thin wrapper around Druid's ``/druid/v2/sql`` endpoint."""

    def __init__(
        self,
        base_url: str,
        *,
        session: requests.Session | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._url = base_url.rstrip("/")
        self._timeout = timeout
        if session is None:
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
        self._session = session

    def query(self, sql: str) -> list[dict]:
        resp = self._session.post(
            f"{self._url}/druid/v2/sql",
            data=json.dumps({"query": sql, "resultFormat": "object"}),
            timeout=self._timeout,
        )
        resp.raise_for_status()
        out = resp.json()
        if isinstance(out, dict) and "error" in out:
            # Druid SQL errors are returned as 200-with-error-body in
            # some configurations — surface them so the parity runner
            # logs the message instead of silently treating them as 0
            # rows.
            raise RuntimeError(f"Druid SQL error: {out.get('errorMessage', out)}")
        return out


class PinotHttpSqlClient:
    """Thin wrapper around Pinot Broker's ``/query/sql`` endpoint."""

    def __init__(
        self,
        base_url: str,
        *,
        session: requests.Session | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._url = base_url.rstrip("/")
        self._timeout = timeout
        if session is None:
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
        self._session = session

    def query(self, sql: str) -> list[list]:
        resp = self._session.post(
            f"{self._url}/query/sql",
            data=json.dumps({"sql": sql}),
            timeout=self._timeout,
        )
        resp.raise_for_status()
        out = resp.json()
        if out.get("exceptions"):
            # Pinot returns query errors inside an `exceptions` array
            # while still 200ing the HTTP layer. Treat the same way as
            # Druid above.
            raise RuntimeError(f"Pinot SQL error: {out['exceptions']}")
        rt = out.get("resultTable")
        if not rt:
            return []
        return rt.get("rows", [])
