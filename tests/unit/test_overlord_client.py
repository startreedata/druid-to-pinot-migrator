"""Unit tests for the Druid Overlord client (mocked HTTP)."""

from __future__ import annotations

import pytest

from migrator.druid.overlord_client import (
    DruidOverlordClient,
    DruidOverlordError,
)
from migrator.realtime.models import StreamPlatform


# ─────────────────────────────────────────────────────────────────────────────
# Minimal mock session — meets the `_Session` Protocol shape.
# Hand-rolled so the test suite stays free of `requests-mock` dependency.
# ─────────────────────────────────────────────────────────────────────────────


class _Resp:
    def __init__(self, status_code: int, body: dict | str):
        self.status_code = status_code
        if isinstance(body, dict):
            import json as _j
            self._body = _j.dumps(body)
        else:
            self._body = body
        self.text = self._body

    def json(self):
        import json as _j
        return _j.loads(self._body)


class _MockSession:
    def __init__(self, routes: dict[str, _Resp]):
        self._routes = routes
        self.calls: list[str] = []

    def get(self, url: str, *, timeout=None):
        self.calls.append(url)
        if url not in self._routes:
            return _Resp(404, "no route")
        return self._routes[url]


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def overlord_url() -> str:
    return "http://druid-overlord:8081"


class TestGetSupervisorOffsets:
    def test_happy_path_returns_offset_map(self, overlord_url):
        status_payload = {
            "payload": {
                "topic": "events",
                "dataSource": "events_ds",
                "latestOffsets": {"0": 100, "1": 250},
                "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
            }
        }
        session = _MockSession({
            f"{overlord_url}/druid/indexer/v1/supervisor/sup1/status": _Resp(200, status_payload),
        })
        client = DruidOverlordClient(overlord_url, session=session)

        m = client.get_supervisor_offsets("sup1")
        assert m.platform == StreamPlatform.KAFKA
        assert m.topic == "events"
        assert m.datasource == "events_ds"
        assert m.supervisor_id == "sup1"
        assert m.offset_dict == {0: 100, 1: 250}
        assert m.watermark_iso.startswith("2024-03-01")
        assert m.watermark_ms == 1709251200000

    def test_falls_back_to_currentOffsets(self, overlord_url):
        # Some Druid versions only emit `currentOffsets`
        status_payload = {
            "payload": {
                "topic": "events",
                "dataSource": "events_ds",
                "currentOffsets": {"0": 5},
                "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
            }
        }
        session = _MockSession({
            f"{overlord_url}/druid/indexer/v1/supervisor/s/status": _Resp(200, status_payload),
        })
        client = DruidOverlordClient(overlord_url, session=session)

        m = client.get_supervisor_offsets("s")
        assert m.offset_dict == {0: 5}

    def test_falls_back_to_spec_for_topic(self, overlord_url):
        # Overlord status omits the topic; client should fetch the spec next
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/s/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/s"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "dataSource": "ds",
                    "latestOffsets": {"0": 10},
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
            spec_url: _Resp(200, {"spec": {"ioConfig": {"topic": "from-spec"}}}),
        })
        client = DruidOverlordClient(overlord_url, session=session)

        m = client.get_supervisor_offsets("s")
        assert m.topic == "from-spec"

    def test_raises_when_topic_unresolvable(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/s/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/s"
        session = _MockSession({
            status_url: _Resp(200, {"payload": {"latestOffsets": {"0": 1}}}),
            spec_url: _Resp(200, {"spec": {}}),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        with pytest.raises(DruidOverlordError, match="topic"):
            client.get_supervisor_offsets("s")

    def test_raises_on_http_error(self, overlord_url):
        session = _MockSession({
            f"{overlord_url}/druid/indexer/v1/supervisor/s/status":
                _Resp(500, "internal error"),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        with pytest.raises(DruidOverlordError, match="500"):
            client.get_supervisor_offsets("s")

    def test_raises_on_non_json_body(self, overlord_url):
        session = _MockSession({
            f"{overlord_url}/druid/indexer/v1/supervisor/s/status":
                _Resp(200, "not json"),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        with pytest.raises(DruidOverlordError, match="non-JSON"):
            client.get_supervisor_offsets("s")

    def test_raises_when_offsets_not_a_dict(self, overlord_url):
        session = _MockSession({
            f"{overlord_url}/druid/indexer/v1/supervisor/s/status":
                _Resp(200, {
                    "payload": {
                        "topic": "t",
                        "dataSource": "d",
                        "latestOffsets": "broken",
                        "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                    }
                }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        with pytest.raises(DruidOverlordError, match="latestOffsets"):
            client.get_supervisor_offsets("s")

    def test_watermark_falls_back_to_now_when_missing(self, overlord_url):
        session = _MockSession({
            f"{overlord_url}/druid/indexer/v1/supervisor/s/status":
                _Resp(200, {
                    "payload": {
                        "topic": "t",
                        "dataSource": "d",
                        "latestOffsets": {"0": 1},
                    }
                }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("s")
        # No timestamp in payload, fallback to "now"
        assert m.watermark_ms > 0
        assert "T" in m.watermark_iso
