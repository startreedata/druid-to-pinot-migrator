"""
Integration test for the ``dpm extract-spec`` CLI.

Hits the CLI through Typer's CliRunner and verifies the end-to-end flow:
- Coordinator + Overlord HTTP clients are exercised
- The extracted spec round-trips through ``dpm generate`` to produce
  Pinot artifacts (no parser/normalizer regressions)

HTTP is mocked at the requests.Session level via monkeypatch so we don't
need a live Druid cluster.
"""

from __future__ import annotations

import json as _json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from migrator.cli.app import app


# ─── Shared mock-Session ───────────────────────────────────────────────────


class _Resp:
    def __init__(self, status_code: int, body: Any):
        self.status_code = status_code
        self._body = body if isinstance(body, str) else _json.dumps(body)
        self.text = self._body

    def json(self):
        return _json.loads(self._body)


class _RoutedSession:
    """Tiny requests.Session stand-in that routes by exact URL match."""

    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self._get: dict[str, _Resp] = {}
        self._post: dict[str, _Resp] = {}

    def add_get(self, url: str, status: int, body: Any) -> None:
        self._get[url] = _Resp(status, body)

    def add_post(self, url: str, status: int, body: Any) -> None:
        self._post[url] = _Resp(status, body)

    def get(self, url, *, timeout=None):
        if url not in self._get:
            return _Resp(404, "no route")
        return self._get[url]

    def post(self, url, *, data=None, timeout=None):
        if url not in self._post:
            return _Resp(404, "no route")
        return self._post[url]


@pytest.fixture
def mock_session(monkeypatch) -> _RoutedSession:
    """Patch requests.Session so all clients see this stub."""
    sess = _RoutedSession()

    def _factory(*a, **kw):
        return sess

    # Both modules import requests lazily inside __init__ — patch
    # `requests.Session` so the new instance returned is our stub.
    import requests

    monkeypatch.setattr(requests, "Session", _factory)
    return sess


# ─── Stream extraction (supervisor present) ────────────────────────────────


COORD = "http://druid-coord:8081"
OVERLORD = "http://druid-overlord:8081"
DS = "events"


def _wire_stream_routes(sess: _RoutedSession) -> None:
    sess.add_get(
        f"{COORD}/druid/coordinator/v1/datasources",
        200, [DS],
    )
    sess.add_get(
        f"{OVERLORD}/druid/indexer/v1/supervisor",
        200, ["events-supervisor"],
    )
    sess.add_get(
        f"{OVERLORD}/druid/indexer/v1/supervisor/events-supervisor",
        200, {
            "id": "events-supervisor",
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": DS,
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": ["country"]},
                    "metricsSpec": [],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "HOUR",
                        "queryGranularity": "MINUTE",
                        "rollup": False,
                    },
                },
                "ioConfig": {
                    "topic": "events",
                    "consumerProperties": {
                        "bootstrap.servers": "kafka.internal:9092"
                    },
                },
                "tuningConfig": {"type": "kafka"},
            },
        },
    )


class TestExtractSpecCliStream:
    def test_stream_extraction_writes_valid_spec(self, mock_session, tmp_path: Path):
        _wire_stream_routes(mock_session)
        out_path = tmp_path / "spec.json"

        result = CliRunner().invoke(
            app,
            [
                "extract-spec",
                "--datasource", DS,
                "--coordinator-url", COORD,
                "--overlord-url", OVERLORD,
                "--out", str(out_path),
            ],
        )
        assert result.exit_code == 0, result.output

        # The written spec round-trips through `dpm generate`
        assert out_path.exists()
        spec = _json.loads(out_path.read_text())
        assert spec["type"] == "kafka"
        assert spec["spec"]["dataSchema"]["dataSource"] == DS

        # Run dpm generate against the extracted spec end-to-end
        gen_dir = tmp_path / "gen"
        gen_result = CliRunner().invoke(
            app,
            ["generate", str(out_path), "--out", str(gen_dir)],
        )
        assert gen_result.exit_code == 0, gen_result.output
        assert (gen_dir / "schema.json").exists()
        assert (gen_dir / "table-realtime.json").exists()

        schema = _json.loads((gen_dir / "schema.json").read_text())
        assert schema["schemaName"] == DS

    def test_unknown_datasource_exits_with_error(self, mock_session):
        mock_session.add_get(
            f"{COORD}/druid/coordinator/v1/datasources", 200, ["other"]
        )
        result = CliRunner().invoke(
            app,
            [
                "extract-spec",
                "--datasource", "missing",
                "--coordinator-url", COORD,
            ],
        )
        assert result.exit_code != 0
        assert "not found" in result.output.lower()

    def test_invalid_prefer_value_exits_with_error(self, mock_session):
        mock_session.add_get(
            f"{COORD}/druid/coordinator/v1/datasources", 200, [DS]
        )
        result = CliRunner().invoke(
            app,
            [
                "extract-spec",
                "--datasource", DS,
                "--coordinator-url", COORD,
                "--prefer", "neither",
            ],
        )
        assert result.exit_code == 2
        assert "neither" in result.output.lower()


# ─── Batch extraction (no supervisor; warnings present) ────────────────────


class TestExtractSpecCliBatch:
    def test_batch_extraction_emits_warnings_and_writes_spec(
        self, mock_session, tmp_path: Path
    ):
        # Coordinator says the datasource exists
        mock_session.add_get(
            f"{COORD}/druid/coordinator/v1/datasources", 200, [DS]
        )
        # Overlord says no supervisors
        mock_session.add_get(
            f"{OVERLORD}/druid/indexer/v1/supervisor", 200, []
        )
        # segmentMetadata response
        mock_session.add_post(
            f"{COORD}/druid/v2/",
            200,
            [{
                "intervals": ["2024-03-01T00:00:00Z/2024-03-02T00:00:00Z"],
                "columns": {
                    "__time": {"type": "LONG"},
                    "country": {"type": "STRING"},
                    "revenue": {"type": "DOUBLE"},
                },
                "size": 100,
                "numRows": 50,
            }],
        )
        # Datasource summary fallback
        mock_session.add_get(
            f"{COORD}/druid/coordinator/v1/datasources/{DS}?full",
            200, {"name": DS, "segments": {}},
        )

        out_path = tmp_path / "spec.json"
        result = CliRunner().invoke(
            app,
            [
                "extract-spec",
                "--datasource", DS,
                "--coordinator-url", COORD,
                "--overlord-url", OVERLORD,
                "--out", str(out_path),
            ],
        )
        assert result.exit_code == 0, result.output
        # Warnings should be surfaced in the CLI output
        assert "warning" in result.output.lower()
        assert "inputSource" in result.output

        spec = _json.loads(out_path.read_text())
        assert spec["type"] == "index_parallel"
        # Metrics + dims inferred from segment metadata
        ds = spec["spec"]["dataSchema"]
        assert ds["dimensionsSpec"]["dimensions"] == ["country"]
        assert ds["metricsSpec"][0]["name"] == "revenue"

    def test_batch_extraction_skips_overlord_when_omitted(
        self, mock_session, tmp_path: Path
    ):
        """If --overlord-url is not passed, the extractor must use batch
        path even if a supervisor would have matched. Confirm by NOT
        wiring any overlord routes — if the extractor tried to call
        them, the unmocked URL would 404 and the CLI would fail."""
        mock_session.add_get(
            f"{COORD}/druid/coordinator/v1/datasources", 200, [DS]
        )
        mock_session.add_post(
            f"{COORD}/druid/v2/",
            200,
            [{
                "intervals": ["2024-03-01/2024-03-02"],
                "columns": {
                    "__time": {"type": "LONG"},
                    "country": {"type": "STRING"},
                },
            }],
        )
        mock_session.add_get(
            f"{COORD}/druid/coordinator/v1/datasources/{DS}?full",
            200, {"name": DS},
        )

        out_path = tmp_path / "spec.json"
        result = CliRunner().invoke(
            app,
            [
                "extract-spec",
                "--datasource", DS,
                "--coordinator-url", COORD,
                "--out", str(out_path),
            ],
        )
        assert result.exit_code == 0, result.output
        assert _json.loads(out_path.read_text())["type"] == "index_parallel"
