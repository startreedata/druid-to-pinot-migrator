"""Unit tests for the default Druid + Pinot SQL clients."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from migrator.parity.clients import DruidHttpSqlClient, PinotHttpSqlClient


class _FakeResp:
    def __init__(self, payload, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _SpySession:
    def __init__(self, payload, status: int = 200) -> None:
        self.posts: list[tuple[str, dict]] = []
        self.headers: dict[str, str] = {}
        self._payload = payload
        self._status = status

    def post(self, url, *, timeout=None, **kwargs):
        self.posts.append((url, kwargs))
        return _FakeResp(self._payload, self._status)


# ─────────────────────────────────────────────────────────────────────────────
# DruidHttpSqlClient
# ─────────────────────────────────────────────────────────────────────────────


class TestDruidHttpSqlClient:
    def test_query_returns_rows(self):
        session = _SpySession([{"a": 1}, {"a": 2}])
        client = DruidHttpSqlClient("http://druid:8888/", session=session)
        rows = client.query("SELECT * FROM x")

        assert rows == [{"a": 1}, {"a": 2}]
        # Trailing slash on base_url is stripped before composition.
        assert session.posts[0][0] == "http://druid:8888/druid/v2/sql"
        # Body carries the Druid resultFormat=object knob.
        body = session.posts[0][1]["data"]
        assert "resultFormat" in body
        assert "SELECT * FROM x" in body

    def test_query_raises_on_druid_error_payload(self):
        # Druid SQL errors come back as 200 + an `error` key in the body.
        session = _SpySession({"error": "PARSE_ERROR", "errorMessage": "bad SQL"})
        client = DruidHttpSqlClient("http://druid:8888", session=session)
        with pytest.raises(RuntimeError, match="bad SQL"):
            client.query("SELECT bogus")

    def test_default_session_when_none_provided(self):
        # No session passed → client builds its own with the JSON
        # Content-Type set on the session headers.
        client = DruidHttpSqlClient("http://druid:8888")
        # Internal session is private but addressable for the smoke
        # check; the contract we care about is "didn't blow up on init".
        assert client._session is not None


# ─────────────────────────────────────────────────────────────────────────────
# PinotHttpSqlClient
# ─────────────────────────────────────────────────────────────────────────────


class TestPinotHttpSqlClient:
    def test_query_returns_rows(self):
        session = _SpySession({
            "resultTable": {
                "dataSchema": {"columnNames": ["c"]},
                "rows": [[1], [2], [3]],
            },
            "exceptions": [],
        })
        client = PinotHttpSqlClient("http://pinot:8099/", session=session)
        rows = client.query("SELECT c FROM t")
        assert rows == [[1], [2], [3]]
        assert session.posts[0][0] == "http://pinot:8099/query/sql"

    def test_empty_result_table_returns_empty(self):
        # Some Pinot queries return success with no `resultTable` key.
        session = _SpySession({"exceptions": []})
        client = PinotHttpSqlClient("http://pinot:8099", session=session)
        assert client.query("SELECT 1") == []

    def test_pinot_exceptions_raise(self):
        session = _SpySession({
            "exceptions": [{"message": "table not found", "errorCode": 404}],
        })
        client = PinotHttpSqlClient("http://pinot:8099", session=session)
        with pytest.raises(RuntimeError, match="Pinot SQL error"):
            client.query("SELECT * FROM nope")

    def test_default_session_when_none_provided(self):
        client = PinotHttpSqlClient("http://pinot:8099")
        assert client._session is not None
