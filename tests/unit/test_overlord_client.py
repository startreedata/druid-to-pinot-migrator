"""Unit tests for the Druid Overlord client (mocked HTTP)."""

from __future__ import annotations

import pytest

from migrator.druid.overlord_client import (
    DruidOverlordClient,
    DruidOverlordError,
    _detect_platform,
    _positions_from_tasks,
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

    def test_kafka_payload_with_stream_field_keeps_offsets(self, overlord_url):
        # Regression for the live-matrix break: a real Druid Kafka
        # supervisor status carries BOTH a ``stream`` field (the topic
        # name, in Druid's unified report) AND ``latestOffsets``. The
        # client must detect Kafka and preserve the offsets — not
        # misroute to Kinesis and drop them.
        status_payload = {
            "payload": {
                "stream": "events",
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
        assert m.offset_dict == {0: 100, 1: 250}
        assert m.shard_sequences == []

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

    def test_watermark_iso_uses_pinot_compatible_format(self, overlord_url):
        # Pinot's TIMESTAMP offset criterion uses Java Instant.parse, which
        # requires `…Z` (not `+00:00`) and 3-digit millisecond precision.
        # Catch regressions here before they cost a CI matrix run.
        import re

        session = _MockSession({
            f"{overlord_url}/druid/indexer/v1/supervisor/s/status":
                _Resp(200, {
                    "payload": {
                        "topic": "t",
                        "dataSource": "d",
                        "latestOffsets": {"0": 1},
                        "lastIngestedTimestamp": "2024-03-01T00:00:00.123Z",
                    }
                }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("s")
        # Must end in Z, must have exactly 3-digit fractional seconds
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", m.watermark_iso
        ), f"watermark_iso must be Pinot-compatible: got {m.watermark_iso!r}"

    def test_watermark_iso_format_for_epoch_millis_payload(self, overlord_url):
        import re

        session = _MockSession({
            f"{overlord_url}/druid/indexer/v1/supervisor/s/status":
                _Resp(200, {
                    "payload": {
                        "topic": "t",
                        "dataSource": "d",
                        "latestOffsets": {"0": 1},
                        "lastIngestedTimestamp": 1709251200456,
                    }
                }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("s")
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", m.watermark_iso
        ), m.watermark_iso

    def test_kafka_works_without_spec_endpoint(self, overlord_url):
        # A Kafka supervisor whose spec endpoint is unavailable still
        # works: the spec fetch degrades to {}, and integer-valued
        # latestOffsets make the value-type heuristic / default land on
        # Kafka. (extract-offsets must not hard-depend on the spec.)
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/s/status"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "stream": "t", "dataSource": "d",
                    "latestOffsets": {"0": 1},
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("s")
        assert m.platform == StreamPlatform.KAFKA
        assert m.offset_dict == {0: 1}


# ─────────────────────────────────────────────────────────────────────────────
# Kinesis — exercised against the REAL Druid supervisor status shape.
#
# Druid's SeekableStreamSupervisorReportPayload is shared by Kafka and
# Kinesis: BOTH report positions under ``latestOffsets`` and the stream
# id under ``stream``. For Kinesis the latestOffsets VALUES are opaque
# sequence-number strings (~56 chars); there is no ``latestSequenceNumbers``
# field. Platform comes from the spec's ``type``. These fixtures mirror
# that exact shape — the gap that a fictional ``latestSequenceNumbers``
# mock previously hid.
# ─────────────────────────────────────────────────────────────────────────────


# Representative Kinesis sequence numbers (Kinesis uses 56-digit strings).
_SEQ_0 = "49590338765432109876543210987654321098765432109876543210"
_SEQ_1 = "49590338000000000000000000000000000000000000000000000001"


class TestGetSupervisorOffsetsKinesis:
    def _kinesis_session(self, overlord_url, supervisor="k", **payload_extra):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/{supervisor}/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/{supervisor}"
        payload = {
            "stream": "payment-events",
            "dataSource": "payments",
            "latestOffsets": {
                "shardId-000000000001": _SEQ_1,
                "shardId-000000000000": _SEQ_0,
            },
            "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
        }
        payload.update(payload_extra)
        return _MockSession({
            status_url: _Resp(200, {"payload": payload}),
            spec_url: _Resp(200, {
                "type": "kinesis",
                "spec": {"ioConfig": {
                    "stream": "payment-events",
                    "endpoint": "kinesis.us-east-1.amazonaws.com",
                }},
            }),
        })

    def test_kinesis_happy_path_real_shape(self, overlord_url):
        session = self._kinesis_session(overlord_url)
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert m.platform == StreamPlatform.KINESIS
        assert m.topic == "payment-events"
        assert m.stream_name == "payment-events"
        assert m.datasource == "payments"
        # Kafka offsets list stays empty; shard sequences populated from
        # latestOffsets, sorted by shard id.
        assert m.offsets == []
        assert [s.shard_id for s in m.shard_sequences] == [
            "shardId-000000000000", "shardId-000000000001",
        ]
        assert m.sequence_for("shardId-000000000000") == _SEQ_0
        assert m.watermark_iso.startswith("2024-03-01")

    def test_kinesis_sequence_strings_not_parsed_as_int(self, overlord_url):
        # The crux: a 56-digit Kinesis sequence number under latestOffsets
        # must NOT be coerced through int() / a Kafka partition — it stays
        # an opaque string on the shard. (The old code misdetected Kinesis
        # as Kafka and crashed on int('shardId-…').)
        session = self._kinesis_session(overlord_url)
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert all(isinstance(s.sequence_number, str) for s in m.shard_sequences)
        assert m.sequence_for("shardId-000000000001") == _SEQ_1

    def test_kinesis_detected_even_when_spec_endpoint_down(self, overlord_url):
        # Spec unavailable → value-type heuristic: long opaque string
        # values under latestOffsets imply Kinesis, so we don't misparse
        # them as Kafka offsets.
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        session = _MockSession({
            status_url: _Resp(200, {"payload": {
                "stream": "payment-events",
                "latestOffsets": {"shardId-000000000000": _SEQ_0},
                "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
            }}),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert m.platform == StreamPlatform.KINESIS
        assert m.sequence_for("shardId-000000000000") == _SEQ_0

    def test_kinesis_stream_from_spec_when_absent_in_payload(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/k"
        session = _MockSession({
            status_url: _Resp(200, {"payload": {
                "latestOffsets": {"shardId-0": _SEQ_0},
                "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
            }}),
            spec_url: _Resp(200, {
                "type": "kinesis",
                "spec": {"ioConfig": {"stream": "from-spec-stream"}},
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert m.platform == StreamPlatform.KINESIS
        assert m.topic == "from-spec-stream"

    def test_kinesis_raises_when_stream_unresolvable(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/k"
        session = _MockSession({
            status_url: _Resp(200, {"payload": {
                "latestOffsets": {"shardId-0": _SEQ_0},
                "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
            }}),
            spec_url: _Resp(200, {"type": "kinesis", "spec": {}}),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        with pytest.raises(DruidOverlordError, match="Kinesis stream"):
            client.get_supervisor_offsets("k")

    def test_kinesis_skips_empty_sequence_values(self, overlord_url):
        session = self._kinesis_session(overlord_url, latestOffsets={
            "shardId-0": _SEQ_0, "shardId-1": None, "shardId-2": "",
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert [s.shard_id for s in m.shard_sequences] == ["shardId-0"]


class TestDetectPlatform:
    """``_detect_platform`` — spec ``type`` is authoritative; ioConfig and
    a value-type heuristic are fallbacks for when the spec is missing."""

    def test_spec_type_wins(self):
        assert _detect_platform({"type": "kinesis"}, {}) == StreamPlatform.KINESIS
        assert _detect_platform({"type": "kafka"}, {}) == StreamPlatform.KAFKA

    def test_ioconfig_shape_fallback(self):
        assert _detect_platform(
            {"spec": {"ioConfig": {"stream": "s"}}}, {}
        ) == StreamPlatform.KINESIS
        assert _detect_platform(
            {"spec": {"ioConfig": {"topic": "t"}}}, {}
        ) == StreamPlatform.KAFKA

    def test_payload_type_fallback(self):
        assert _detect_platform({}, {"type": "kinesis"}) == StreamPlatform.KINESIS
        assert _detect_platform({}, {"type": "kafka"}) == StreamPlatform.KAFKA

    def test_value_type_heuristic_long_string_is_kinesis(self):
        # No spec, no payload type: a long opaque string position value
        # implies Kinesis.
        assert _detect_platform(
            {}, {}, {"shardId-0": _SEQ_0}
        ) == StreamPlatform.KINESIS

    def test_value_type_heuristic_int_is_kafka(self):
        assert _detect_platform({}, {}, {"0": 100}) == StreamPlatform.KAFKA

    def test_defaults_kafka_when_unclassifiable(self):
        assert _detect_platform({}, {}) == StreamPlatform.KAFKA
        assert _detect_platform({}, {}, {}) == StreamPlatform.KAFKA


# Representative Kinesis sequence numbers (Kinesis uses 56-digit strings).
_TASK_SEQ_0 = "49590338765432109876543210987654321098765432109876543210"
_TASK_SEQ_1 = "49590338000000000000000000000000000000000000000000000099"


class TestPositionsFromTasks:
    """``_positions_from_tasks`` — the fallback that reads consumed
    positions from activeTasks/publishingTasks[].currentOffsets when the
    supervisor-level latestOffsets is absent (the real Kinesis case)."""

    def test_merges_active_and_publishing_tasks(self):
        payload = {
            "activeTasks": [
                {"id": "t1", "currentOffsets": {"shardId-000000000000": _TASK_SEQ_0}},
            ],
            "publishingTasks": [
                {"id": "t0", "currentOffsets": {"shardId-000000000001": _TASK_SEQ_1}},
            ],
        }
        merged = _positions_from_tasks(payload)
        assert merged == {
            "shardId-000000000000": _TASK_SEQ_0,
            "shardId-000000000001": _TASK_SEQ_1,
        }

    def test_handoff_keeps_furthest_kafka_offset_numerically(self):
        # Same partition reported by a publishing task (older) and an
        # active task (newer) during handoff — keep the larger. Must be a
        # NUMERIC compare: lexicographically "100" < "99".
        payload = {
            "activeTasks": [{"currentOffsets": {"0": 100}}],
            "publishingTasks": [{"currentOffsets": {"0": 99}}],
        }
        assert _positions_from_tasks(payload) == {"0": 100}

    def test_handoff_keeps_furthest_kinesis_sequence(self):
        payload = {
            "activeTasks": [{"currentOffsets": {"shardId-0": _TASK_SEQ_0}}],
            "publishingTasks": [{"currentOffsets": {"shardId-0": _TASK_SEQ_1}}],
        }
        # _TASK_SEQ_0 > _TASK_SEQ_1 numerically.
        assert _positions_from_tasks(payload) == {"shardId-0": _TASK_SEQ_0}

    def test_skips_empty_and_null_values(self):
        payload = {
            "activeTasks": [
                {"currentOffsets": {"a": _TASK_SEQ_0, "b": None, "c": ""}},
            ],
        }
        assert _positions_from_tasks(payload) == {"a": _TASK_SEQ_0}

    def test_empty_when_no_tasks(self):
        assert _positions_from_tasks({}) == {}
        assert _positions_from_tasks({"activeTasks": [], "publishingTasks": []}) == {}

    def test_tolerates_malformed_task_entries(self):
        payload = {"activeTasks": ["not-a-dict", {"currentOffsets": "nope"}, {}]}
        assert _positions_from_tasks(payload) == {}


class TestGetSupervisorOffsetsFallsBackToTasks:
    """get_supervisor_offsets must use activeTasks positions when the
    supervisor-level latestOffsets is absent — the real Druid Kinesis
    shape that the live test surfaced."""

    def test_kinesis_positions_from_active_tasks(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/k"
        session = _MockSession({
            status_url: _Resp(200, {"payload": {
                "stream": "payment-events",
                "dataSource": "payments",
                "state": "RUNNING",
                # No latestOffsets/currentOffsets at the supervisor level.
                "activeTasks": [
                    {"id": "t1", "currentOffsets": {
                        "shardId-000000000000": _TASK_SEQ_0,
                        "shardId-000000000001": _TASK_SEQ_1,
                    }},
                ],
                "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
            }}),
            spec_url: _Resp(200, {
                "type": "kinesis",
                "spec": {"ioConfig": {"stream": "payment-events"}},
            }),
        })
        m = DruidOverlordClient(overlord_url, session=session).get_supervisor_offsets("k")
        assert m.platform == StreamPlatform.KINESIS
        assert [s.shard_id for s in m.shard_sequences] == [
            "shardId-000000000000", "shardId-000000000001",
        ]
        assert m.sequence_for("shardId-000000000000") == _TASK_SEQ_0

    def test_supervisor_level_offsets_still_win_when_present(self, overlord_url):
        # If the supervisor DID compute latestOffsets, use that (don't
        # override with task positions).
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/k"
        session = _MockSession({
            status_url: _Resp(200, {"payload": {
                "stream": "s",
                "latestOffsets": {"shardId-0": _TASK_SEQ_0},
                "activeTasks": [{"currentOffsets": {"shardId-9": _TASK_SEQ_1}}],
                "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
            }}),
            spec_url: _Resp(200, {"type": "kinesis", "spec": {}}),
        })
        m = DruidOverlordClient(overlord_url, session=session).get_supervisor_offsets("k")
        assert [s.shard_id for s in m.shard_sequences] == ["shardId-0"]

    def test_kafka_offsets_from_active_tasks(self, overlord_url):
        # The fallback is platform-agnostic — Kafka benefits too.
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/s/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/s"
        session = _MockSession({
            status_url: _Resp(200, {"payload": {
                "topic": "events",
                "state": "RUNNING",
                "activeTasks": [{"currentOffsets": {"0": 100, "1": 250}}],
                "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
            }}),
            spec_url: _Resp(200, {"type": "kafka", "spec": {}}),
        })
        m = DruidOverlordClient(overlord_url, session=session).get_supervisor_offsets("s")
        assert m.platform == StreamPlatform.KAFKA
        assert m.offset_dict == {0: 100, 1: 250}
