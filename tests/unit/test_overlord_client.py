"""Unit tests for the Druid Overlord client (mocked HTTP)."""

from __future__ import annotations

import pytest

from migrator.druid.overlord_client import (
    DruidOverlordClient,
    DruidOverlordError,
    _detect_platform,
    _detect_platform_from_payload,
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

    def test_kafka_happy_path_does_not_fetch_spec(self, overlord_url):
        # When topic + offsets are in the status payload, the platform is
        # detected without a spec call — only the status URL is hit.
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/s/status"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "topic": "t", "dataSource": "d",
                    "latestOffsets": {"0": 1},
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("s")
        assert m.platform == StreamPlatform.KAFKA
        # The spec endpoint was never called.
        assert all(not url.endswith("/supervisor/s") for url in session.calls)


class TestGetSupervisorOffsetsKinesis:
    def test_kinesis_happy_path(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "stream": "payment-events",
                    "dataSource": "payments",
                    "latestSequenceNumbers": {
                        "shardId-000000000001": "49590200",
                        "shardId-000000000000": "49590100",
                    },
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert m.platform == StreamPlatform.KINESIS
        assert m.topic == "payment-events"
        assert m.stream_name == "payment-events"
        assert m.datasource == "payments"
        assert m.offsets == []
        # Sorted by shard id.
        assert [s.shard_id for s in m.shard_sequences] == [
            "shardId-000000000000", "shardId-000000000001",
        ]
        assert m.sequence_for("shardId-000000000000") == "49590100"
        assert m.watermark_iso.startswith("2024-03-01")
        # Detected from payload shape — no spec call.
        assert all(not url.endswith("/supervisor/k") for url in session.calls)

    def test_kinesis_falls_back_to_currentSequenceNumbers(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "stream": "evts",
                    "currentSequenceNumbers": {"shardId-0": "5"},
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert m.platform == StreamPlatform.KINESIS
        assert m.sequence_for("shardId-0") == "5"

    def test_kinesis_stream_from_spec_when_absent_in_payload(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/k"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "latestSequenceNumbers": {"shardId-0": "5"},
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
            spec_url: _Resp(200, {
                "type": "kinesis",
                "spec": {"ioConfig": {"stream": "from-spec-stream"}},
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert m.topic == "from-spec-stream"

    def test_kinesis_raises_when_stream_unresolvable(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/k"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "latestSequenceNumbers": {"shardId-0": "5"},
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
            spec_url: _Resp(200, {"type": "kinesis", "spec": {}}),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        with pytest.raises(DruidOverlordError, match="Kinesis stream"):
            client.get_supervisor_offsets("k")

    def test_kinesis_raises_when_sequence_numbers_not_a_dict(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "stream": "evts",
                    "latestSequenceNumbers": "broken",
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        with pytest.raises(DruidOverlordError, match="latestSequenceNumbers"):
            client.get_supervisor_offsets("k")

    def test_kinesis_skips_empty_sequence_numbers(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "stream": "evts",
                    "latestSequenceNumbers": {
                        "shardId-0": "5", "shardId-1": None, "shardId-2": "",
                    },
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        # Only the shard with a real sequence number is kept.
        assert [s.shard_id for s in m.shard_sequences] == ["shardId-0"]


class TestPlatformDetectionFallback:
    """Exercises the spec-based detection path that fires only when the
    status payload carries no discriminating signal."""

    def test_ambiguous_payload_uses_spec_type_kinesis(self, overlord_url):
        # Payload has no offsets/sequences/topic/stream → ambiguous, so
        # the client consults the supervisor spec (type=kinesis) and
        # captures a watermark-only Kinesis map.
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/k/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/k"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "dataSource": "payments",
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
            spec_url: _Resp(200, {
                "type": "kinesis",
                "spec": {"ioConfig": {"stream": "payment-events"}},
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("k")
        assert m.platform == StreamPlatform.KINESIS
        assert m.topic == "payment-events"
        assert m.shard_sequences == []  # watermark-only

    def test_ambiguous_payload_uses_spec_type_kafka(self, overlord_url):
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/s/status"
        spec_url = f"{overlord_url}/druid/indexer/v1/supervisor/s"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "dataSource": "d",
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
            spec_url: _Resp(200, {
                "type": "kafka",
                "spec": {"ioConfig": {"topic": "from-spec"}},
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        m = client.get_supervisor_offsets("s")
        assert m.platform == StreamPlatform.KAFKA
        assert m.topic == "from-spec"

    def test_ambiguous_payload_spec_fetch_fails_defaults_kafka(self, overlord_url):
        # Spec endpoint not mocked → _try_get_supervisor_spec swallows
        # the error and returns {}; detection defaults to Kafka, and the
        # topic is then unresolvable → a clear error (not a crash).
        status_url = f"{overlord_url}/druid/indexer/v1/supervisor/s/status"
        session = _MockSession({
            status_url: _Resp(200, {
                "payload": {
                    "dataSource": "d",
                    "lastIngestedTimestamp": "2024-03-01T00:00:00.000Z",
                }
            }),
        })
        client = DruidOverlordClient(overlord_url, session=session)
        with pytest.raises(DruidOverlordError, match="topic"):
            client.get_supervisor_offsets("s")


class TestDetectPlatformHelpers:
    def test_from_payload_kinesis_signals(self):
        assert _detect_platform_from_payload(
            {"latestSequenceNumbers": {}}
        ) == StreamPlatform.KINESIS
        assert _detect_platform_from_payload(
            {"stream": "s"}
        ) == StreamPlatform.KINESIS

    def test_from_payload_kafka_signals(self):
        assert _detect_platform_from_payload(
            {"latestOffsets": {}}
        ) == StreamPlatform.KAFKA
        assert _detect_platform_from_payload(
            {"topic": "t"}
        ) == StreamPlatform.KAFKA

    def test_from_payload_ambiguous_returns_none(self):
        assert _detect_platform_from_payload({"dataSource": "d"}) is None

    def test_detect_platform_spec_type_wins(self):
        assert _detect_platform({"type": "kinesis"}, {}) == StreamPlatform.KINESIS
        assert _detect_platform({"type": "kafka"}, {}) == StreamPlatform.KAFKA

    def test_detect_platform_ioconfig_shape(self):
        assert _detect_platform(
            {"spec": {"ioConfig": {"stream": "s"}}}, {}
        ) == StreamPlatform.KINESIS
        assert _detect_platform(
            {"spec": {"ioConfig": {"topic": "t"}}}, {}
        ) == StreamPlatform.KAFKA

    def test_detect_platform_payload_type_last_resort(self):
        assert _detect_platform({}, {"type": "kinesis"}) == StreamPlatform.KINESIS

    def test_detect_platform_defaults_kafka(self):
        assert _detect_platform({}, {}) == StreamPlatform.KAFKA
