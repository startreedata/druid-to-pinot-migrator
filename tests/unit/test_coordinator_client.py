"""Unit tests for the Druid Coordinator client (mocked HTTP)."""

from __future__ import annotations

import json as _json

import pytest

from migrator.druid.coordinator_client import (
    DruidCoordinatorClient,
    DruidCoordinatorError,
    SegmentMetadata,
)


# ─── Test scaffolding ──────────────────────────────────────────────────────


class _Resp:
    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self._body = body if isinstance(body, str) else _json.dumps(body)
        self.text = self._body

    def json(self):
        return _json.loads(self._body)


class _Session:
    def __init__(
        self,
        get_routes: dict | None = None,
        post_routes: dict | None = None,
    ) -> None:
        self.get_routes = get_routes or {}
        self.post_routes = post_routes or {}
        self.gets: list[str] = []
        self.posts: list[tuple[str, dict]] = []

    def get(self, url, *, timeout=None):
        self.gets.append(url)
        return self.get_routes.get(url, _Resp(404, "no route"))

    def post(self, url, *, data=None, timeout=None):
        payload = _json.loads(data) if isinstance(data, str) else data
        self.posts.append((url, payload))
        return self.post_routes.get(url, _Resp(404, "no route"))


COORD = "http://druid-coordinator:8081"


# ─── list_datasources ──────────────────────────────────────────────────────


class TestListDatasources:
    def test_returns_list(self):
        s = _Session(get_routes={
            f"{COORD}/druid/coordinator/v1/datasources": _Resp(200, ["a", "b"]),
        })
        c = DruidCoordinatorClient(COORD, session=s)
        assert c.list_datasources() == ["a", "b"]

    def test_raises_on_500(self):
        s = _Session(get_routes={
            f"{COORD}/druid/coordinator/v1/datasources": _Resp(500, "boom"),
        })
        c = DruidCoordinatorClient(COORD, session=s)
        with pytest.raises(DruidCoordinatorError, match="500"):
            c.list_datasources()

    def test_raises_on_non_json(self):
        s = _Session(get_routes={
            f"{COORD}/druid/coordinator/v1/datasources": _Resp(200, "<html>"),
        })
        c = DruidCoordinatorClient(COORD, session=s)
        with pytest.raises(DruidCoordinatorError, match="non-JSON"):
            c.list_datasources()


class TestDatasourceExists:
    def test_true_when_present(self):
        s = _Session(get_routes={
            f"{COORD}/druid/coordinator/v1/datasources": _Resp(200, ["events"]),
        })
        c = DruidCoordinatorClient(COORD, session=s)
        assert c.datasource_exists("events")

    def test_false_when_absent(self):
        s = _Session(get_routes={
            f"{COORD}/druid/coordinator/v1/datasources": _Resp(200, ["other"]),
        })
        c = DruidCoordinatorClient(COORD, session=s)
        assert not c.datasource_exists("events")

    def test_false_when_coordinator_unreachable(self):
        s = _Session()  # 404 default
        c = DruidCoordinatorClient(COORD, session=s)
        assert not c.datasource_exists("events")


# ─── get_segment_metadata ──────────────────────────────────────────────────


class TestGetSegmentMetadata:
    def test_parses_merged_response(self):
        url = f"{COORD}/druid/v2/"
        s = _Session(post_routes={
            url: _Resp(200, [{
                "id": "merged",
                "intervals": ["2024-03-01T00:00:00.000Z/2024-03-02T00:00:00.000Z"],
                "columns": {
                    "__time": {"type": "LONG"},
                    "country": {"type": "STRING"},
                    "revenue": {"type": "DOUBLE"},
                    "tags": {"type": "STRING", "hasMultipleValues": True},
                },
                "size": 1234,
                "numRows": 100,
            }]),
        })
        c = DruidCoordinatorClient(COORD, session=s)
        meta = c.get_segment_metadata("events")

        assert isinstance(meta, SegmentMetadata)
        assert set(meta.columns) == {"__time", "country", "revenue", "tags"}
        assert meta.intervals == [
            "2024-03-01T00:00:00.000Z/2024-03-02T00:00:00.000Z"
        ]
        assert meta.size_bytes == 1234
        assert meta.num_rows == 100

        # Confirm we sent the merge request
        sent_url, sent_body = s.posts[0]
        assert sent_url == url
        assert sent_body["queryType"] == "segmentMetadata"
        assert sent_body["dataSource"] == "events"
        assert sent_body["merge"] is True

    def test_parses_intervals_dict_form(self):
        # Some Druid versions return intervals as {iv: count}
        url = f"{COORD}/druid/v2/"
        s = _Session(post_routes={
            url: _Resp(200, [{
                "intervals": {"2024-03-01/2024-03-02": 1},
                "columns": {"__time": {"type": "LONG"}},
            }]),
        })
        c = DruidCoordinatorClient(COORD, session=s)
        meta = c.get_segment_metadata("events")
        assert meta.intervals == ["2024-03-01/2024-03-02"]

    def test_raises_on_empty_response(self):
        url = f"{COORD}/druid/v2/"
        s = _Session(post_routes={url: _Resp(200, [])})
        c = DruidCoordinatorClient(COORD, session=s)
        with pytest.raises(DruidCoordinatorError, match="empty"):
            c.get_segment_metadata("events")

    def test_raises_when_columns_not_dict(self):
        url = f"{COORD}/druid/v2/"
        s = _Session(post_routes={
            url: _Resp(200, [{"columns": "broken"}]),
        })
        c = DruidCoordinatorClient(COORD, session=s)
        with pytest.raises(DruidCoordinatorError, match="not a dict"):
            c.get_segment_metadata("events")


class TestGetDatasourceSummary:
    def test_round_trips_payload(self):
        url = f"{COORD}/druid/coordinator/v1/datasources/events?full"
        payload = {
            "name": "events",
            "segments": {
                "minTime": "2024-03-01T00:00:00.000Z",
                "maxTime": "2024-03-30T00:00:00.000Z",
                "count": 30,
            },
        }
        s = _Session(get_routes={url: _Resp(200, payload)})
        c = DruidCoordinatorClient(COORD, session=s)
        assert c.get_datasource_summary("events") == payload
