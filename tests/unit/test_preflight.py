"""Unit tests for migrator.preflight — Druid + Pinot probes."""

from __future__ import annotations

import pytest

from migrator.preflight import (
    PreflightCheck,
    probe_druid_datasource,
    probe_druid_router,
    probe_pinot_broker,
    probe_pinot_controller,
    probe_pinot_tenant,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fakes
# ─────────────────────────────────────────────────────────────────────────────


class _FakeResp:
    def __init__(self, status: int, body=None, text: str = "") -> None:
        self.status_code = status
        self._body = body
        self.text = text

    def json(self):
        if self._body is None:
            raise ValueError("no JSON body")
        return self._body


class _FakeSession:
    """Returns canned responses keyed by URL substring.

    Pass either a mapping of substring → _FakeResp, or a single
    response that's used for every GET. The first matching prefix
    wins, so callers can shadow more specific URLs.
    """

    def __init__(self, responses) -> None:
        self.calls: list[str] = []
        self._responses = responses

    def get(self, url, *, timeout=None, **kwargs):
        self.calls.append(url)
        if isinstance(self._responses, dict):
            for needle, resp in self._responses.items():
                if needle in url:
                    return resp
            raise AssertionError(f"unexpected GET {url}")
        return self._responses


class _ExplodingSession:
    """Always raises — simulates a network-level failure."""

    def get(self, url, *, timeout=None, **kwargs):
        raise ConnectionError("nope")


# ─────────────────────────────────────────────────────────────────────────────
# Druid probes
# ─────────────────────────────────────────────────────────────────────────────


class TestProbeDruidRouter:
    def test_reachable_with_version(self):
        session = _FakeSession(_FakeResp(200, {"version": "31.0.0"}))
        c = probe_druid_router(session, "http://druid:8888/")
        assert c.ok is True
        assert "31.0.0" in c.detail
        assert c.data == {"version": "31.0.0"}

    def test_reachable_without_version_payload(self):
        # Some clusters return /status without a version string.
        session = _FakeSession(_FakeResp(200, {}))
        c = probe_druid_router(session, "http://druid:8888")
        assert c.ok is True
        assert c.detail == "reachable"

    def test_non_200_is_failure(self):
        session = _FakeSession(_FakeResp(503, text="overload"))
        c = probe_druid_router(session, "http://druid:8888")
        assert c.ok is False
        assert "503" in c.detail

    def test_network_error_is_failure(self):
        c = probe_druid_router(_ExplodingSession(), "http://druid:8888")
        assert c.ok is False
        assert "nope" in c.detail

    def test_strips_trailing_slash_from_url(self):
        session = _FakeSession(_FakeResp(200, {"version": "31"}))
        probe_druid_router(session, "http://druid:8888/")
        # Single slash, not double — the URL composer rstrips.
        assert session.calls == ["http://druid:8888/status"]


class TestProbeDruidDatasource:
    def test_existing_datasource_passes(self):
        session = _FakeSession(_FakeResp(200, ["pageviews", "events"]))
        c = probe_druid_datasource(session, "http://druid:8081", "pageviews")
        assert c.ok is True

    def test_missing_datasource_fails(self):
        session = _FakeSession(_FakeResp(200, ["other"]))
        c = probe_druid_datasource(session, "http://druid:8081", "pageviews")
        assert c.ok is False
        assert "not found" in c.detail
        # Tells the operator how many datasources DO exist (for ruling
        # out wrong-coordinator-URL bugs).
        assert "1" in c.detail

    def test_coordinator_unreachable_fails(self):
        c = probe_druid_datasource(
            _ExplodingSession(), "http://druid:8081", "pageviews",
        )
        assert c.ok is False
        assert "unreachable" in c.detail

    def test_non_200_fails(self):
        session = _FakeSession(_FakeResp(401))
        c = probe_druid_datasource(session, "http://druid:8081", "pageviews")
        assert c.ok is False
        assert "401" in c.detail


# ─────────────────────────────────────────────────────────────────────────────
# Pinot probes
# ─────────────────────────────────────────────────────────────────────────────


class TestProbePinotController:
    def test_reachable_with_version(self):
        session = _FakeSession(_FakeResp(200, {"pinot-controller": "1.5.0"}))
        c = probe_pinot_controller(session, "http://pinot:9000")
        assert c.ok is True
        assert "1.5.0" in c.detail
        assert c.data == {"version": "1.5.0"}

    def test_picks_first_string_value_when_no_controller_key(self):
        # /version sometimes returns just {"git":"<sha>","build":"..."}
        # without a controller-flavoured key. Fall back to any string.
        session = _FakeSession(_FakeResp(200, {"git": "abc", "build": "1.0"}))
        c = probe_pinot_controller(session, "http://pinot:9000")
        assert c.ok is True
        # Either git or build is acceptable — both are stringy values.
        assert c.data is not None
        assert c.data["version"] in {"abc", "1.0"}

    def test_non_200_fails(self):
        session = _FakeSession(_FakeResp(500))
        c = probe_pinot_controller(session, "http://pinot:9000")
        assert c.ok is False

    def test_network_error_fails(self):
        c = probe_pinot_controller(_ExplodingSession(), "http://pinot:9000")
        assert c.ok is False


class TestProbePinotBroker:
    def test_health_ok(self):
        session = _FakeSession(_FakeResp(200, text="OK"))
        c = probe_pinot_broker(session, "http://pinot:8099/")
        assert c.ok is True
        assert session.calls == ["http://pinot:8099/health"]

    def test_non_200_fails(self):
        session = _FakeSession(_FakeResp(503))
        c = probe_pinot_broker(session, "http://pinot:8099")
        assert c.ok is False

    def test_unreachable_fails(self):
        c = probe_pinot_broker(_ExplodingSession(), "http://pinot:8099")
        assert c.ok is False
        assert "unreachable" in c.detail


class TestProbePinotTenant:
    def test_existing_tenant_passes_server_side(self):
        session = _FakeSession(_FakeResp(200, {
            "SERVER_TENANTS": ["DefaultTenant"],
            "BROKER_TENANTS": ["DefaultTenant"],
        }))
        c = probe_pinot_tenant(session, "http://pinot:9000", "DefaultTenant")
        assert c.ok is True
        # Detail tells the operator which tier(s) matched — useful when
        # debugging a tenant-naming mismatch between brokers and servers.
        assert "server" in c.detail
        assert "broker" in c.detail

    def test_missing_tenant_fails(self):
        session = _FakeSession(_FakeResp(200, {
            "SERVER_TENANTS": ["other"],
            "BROKER_TENANTS": ["other"],
        }))
        c = probe_pinot_tenant(session, "http://pinot:9000", "missing")
        assert c.ok is False
        # The detail surfaces the actual tenant set so operators can spot typos.
        assert "other" in c.detail

    def test_unreachable_fails(self):
        c = probe_pinot_tenant(
            _ExplodingSession(), "http://pinot:9000", "DefaultTenant",
        )
        assert c.ok is False


# ─────────────────────────────────────────────────────────────────────────────
# PreflightCheck
# ─────────────────────────────────────────────────────────────────────────────


class TestPreflightCheckSerialisation:
    def test_to_dict_drops_none_data(self):
        c = PreflightCheck(name="x", target="t", ok=True, detail="ok")
        assert "data" not in c.to_dict()

    def test_to_dict_keeps_data_when_set(self):
        c = PreflightCheck(
            name="x", target="t", ok=True, detail="ok",
            data={"version": "1.0"},
        )
        d = c.to_dict()
        assert d["data"] == {"version": "1.0"}
