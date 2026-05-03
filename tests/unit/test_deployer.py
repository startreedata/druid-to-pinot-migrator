"""Unit tests for the Pinot deployer with a stub HTTP session."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.pinot.deployer import (
    DeployArtifacts,
    PinotDeployer,
    discover_artifacts,
)


# ─────────────────────────────────────────────────────────────────────────────
# Stub session
# ─────────────────────────────────────────────────────────────────────────────


class _Resp:
    """Minimal duck-typed ``requests.Response`` for tests."""

    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


class StubSession:
    """Records every POST and returns canned responses keyed by URL."""

    def __init__(self, responses: dict[str, _Resp] | None = None,
                 default: _Resp | None = None) -> None:
        self.responses = responses or {}
        self.default = default or _Resp(200)
        self.posts: list[tuple[str, str]] = []  # (url, body)
        self.headers: dict[str, str] = {}

    def post(self, url, *, data=None, headers=None, timeout=None):
        self.posts.append((url, data))
        return self.responses.get(url, self.default)


# ─────────────────────────────────────────────────────────────────────────────
# discover_artifacts
# ─────────────────────────────────────────────────────────────────────────────


class TestDiscoverArtifacts:
    def test_finds_all_three(self, tmp_path: Path):
        (tmp_path / "schema.json").write_text("{}")
        (tmp_path / "table-offline.json").write_text("{}")
        (tmp_path / "table-realtime.json").write_text("{}")
        a = discover_artifacts(tmp_path)
        assert a.schema is not None and a.schema.name == "schema.json"
        assert a.offline_table is not None
        assert a.realtime_table is not None

    def test_missing_files_become_none(self, tmp_path: Path):
        # Only schema present (the OFFLINE-only output of `dpm generate`
        # against a batch spec, sans realtime).
        (tmp_path / "schema.json").write_text("{}")
        (tmp_path / "table-offline.json").write_text("{}")
        a = discover_artifacts(tmp_path)
        assert a.schema is not None
        assert a.offline_table is not None
        assert a.realtime_table is None  # absent

    def test_empty_dir(self, tmp_path: Path):
        a = discover_artifacts(tmp_path)
        assert a.schema is None
        assert a.offline_table is None
        assert a.realtime_table is None


# ─────────────────────────────────────────────────────────────────────────────
# PinotDeployer.deploy
# ─────────────────────────────────────────────────────────────────────────────


def _write(path: Path, body: dict) -> Path:
    path.write_text(json.dumps(body))
    return path


class TestPinotDeployer:
    def test_deploys_in_correct_order(self, tmp_path: Path):
        schema = _write(tmp_path / "schema.json", {"schemaName": "ds"})
        offline = _write(tmp_path / "off.json", {"tableName": "ds_OFFLINE"})
        realtime = _write(tmp_path / "rt.json", {"tableName": "ds_REALTIME"})

        sess = StubSession(default=_Resp(200))
        deployer = PinotDeployer("http://pinot:9000", session=sess)
        report = deployer.deploy(DeployArtifacts(schema, offline, realtime))

        # Order matters: schema → offline → realtime.
        urls = [u for u, _ in sess.posts]
        assert urls == [
            "http://pinot:9000/schemas",
            "http://pinot:9000/tables",
            "http://pinot:9000/tables",
        ]
        assert report.all_ok
        assert report.created == 3
        assert report.already_exists == 0
        assert report.errored == 0
        assert [r.status for r in report.results] == [
            "created", "created", "created"
        ]

    def test_409_treated_as_already_exists(self, tmp_path: Path):
        schema = _write(tmp_path / "schema.json", {"schemaName": "ds"})
        offline = _write(tmp_path / "off.json", {"tableName": "ds_OFFLINE"})
        # Schema already deployed (409); table is fresh (200).
        sess = StubSession(responses={
            "http://pinot:9000/schemas": _Resp(409, "schema already exists"),
            "http://pinot:9000/tables":  _Resp(200),
        })
        deployer = PinotDeployer("http://pinot:9000", session=sess)
        report = deployer.deploy(DeployArtifacts(schema, offline))

        assert report.all_ok  # 409 is not an error
        assert report.created == 1
        assert report.already_exists == 1
        statuses = [r.status for r in report.results]
        assert statuses == ["already_exists", "created"]

    def test_5xx_is_error(self, tmp_path: Path):
        schema = _write(tmp_path / "schema.json", {"schemaName": "ds"})
        sess = StubSession(default=_Resp(500, "internal server error"))
        deployer = PinotDeployer("http://pinot:9000", session=sess)
        report = deployer.deploy(DeployArtifacts(schema=schema))

        assert not report.all_ok
        assert report.errored == 1
        assert "HTTP 500" in report.results[0].detail

    def test_skips_none_artifacts(self, tmp_path: Path):
        # Realtime-only deploy.
        realtime = _write(tmp_path / "rt.json", {"tableName": "ds_REALTIME"})
        sess = StubSession(default=_Resp(200))
        deployer = PinotDeployer("http://pinot:9000", session=sess)
        report = deployer.deploy(DeployArtifacts(realtime_table=realtime))

        assert len(report.results) == 1
        assert sess.posts == [
            ("http://pinot:9000/tables", json.dumps({"tableName": "ds_REALTIME"}))
        ]

    def test_uses_injected_session(self, tmp_path: Path):
        # The deployer must use the session you pass in (so auth headers
        # configured by migrator.auth flow through).
        schema = _write(tmp_path / "schema.json", {"schemaName": "ds"})
        sess = StubSession(default=_Resp(200))
        sess.headers["Authorization"] = "Basic admin:admin"  # marker
        deployer = PinotDeployer("http://pinot:9000", session=sess)
        deployer.deploy(DeployArtifacts(schema=schema))

        # The session we passed in is the one that received the POST.
        assert len(sess.posts) == 1

    def test_name_extracted_from_body(self, tmp_path: Path):
        schema = _write(tmp_path / "schema.json", {"schemaName": "my_ds"})
        offline = _write(tmp_path / "off.json", {"tableName": "my_ds_OFFLINE"})
        sess = StubSession(default=_Resp(200))
        deployer = PinotDeployer("http://pinot:9000", session=sess)
        report = deployer.deploy(DeployArtifacts(schema, offline))

        assert report.results[0].name == "my_ds"
        assert report.results[1].name == "my_ds_OFFLINE"

    def test_name_falls_back_to_filename_on_bad_json(self, tmp_path: Path):
        # Defensive: if the file isn't valid JSON we still want a useful
        # name in the report for the error message.
        bad = tmp_path / "broken-schema.json"
        bad.write_text("not valid json {{")
        sess = StubSession(default=_Resp(400, "bad json"))
        deployer = PinotDeployer("http://pinot:9000", session=sess)
        report = deployer.deploy(DeployArtifacts(schema=bad))

        assert report.results[0].name == "broken-schema"
        assert report.errored == 1


# ─────────────────────────────────────────────────────────────────────────────
# DeployReport summary properties
# ─────────────────────────────────────────────────────────────────────────────


class TestDeployReport:
    def test_all_ok_with_only_creates(self, tmp_path: Path):
        schema = _write(tmp_path / "schema.json", {"schemaName": "ds"})
        sess = StubSession(default=_Resp(200))
        report = PinotDeployer("http://x", session=sess).deploy(
            DeployArtifacts(schema=schema)
        )
        assert report.all_ok

    def test_all_ok_with_mixed_creates_and_existing(self, tmp_path: Path):
        schema = _write(tmp_path / "schema.json", {"schemaName": "ds"})
        offline = _write(tmp_path / "off.json", {"tableName": "ds_OFFLINE"})
        sess = StubSession(responses={
            "http://x/schemas": _Resp(200),
            "http://x/tables":  _Resp(409, "exists"),
        })
        report = PinotDeployer("http://x", session=sess).deploy(
            DeployArtifacts(schema, offline)
        )
        assert report.all_ok  # 409 is fine

    def test_not_ok_when_any_error(self, tmp_path: Path):
        schema = _write(tmp_path / "schema.json", {"schemaName": "ds"})
        offline = _write(tmp_path / "off.json", {"tableName": "ds_OFFLINE"})
        sess = StubSession(responses={
            "http://x/schemas": _Resp(200),
            "http://x/tables":  _Resp(500, "boom"),
        })
        report = PinotDeployer("http://x", session=sess).deploy(
            DeployArtifacts(schema, offline)
        )
        assert not report.all_ok
