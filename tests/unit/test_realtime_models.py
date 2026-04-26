"""Unit tests for migrator.realtime.models."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from migrator.realtime.models import (
    KafkaOffsetMap,
    KafkaPartitionOffset,
    StreamPlatform,
)


class TestKafkaPartitionOffset:
    def test_holds_partition_and_offset(self):
        po = KafkaPartitionOffset(partition=3, offset=42)
        assert po.partition == 3
        assert po.offset == 42

    def test_is_immutable(self):
        po = KafkaPartitionOffset(partition=0, offset=1)
        with pytest.raises(ValidationError):
            po.partition = 7  # type: ignore[misc]

    def test_rejects_negative_partition(self):
        with pytest.raises(ValidationError):
            KafkaPartitionOffset(partition=-1, offset=0)

    def test_rejects_negative_offset(self):
        with pytest.raises(ValidationError):
            KafkaPartitionOffset(partition=0, offset=-1)


class TestKafkaOffsetMap:
    def _make(self, **overrides):
        defaults = dict(
            topic="events",
            supervisor_id="events-supervisor",
            datasource="events",
            watermark_iso="2024-03-01T00:00:00.000+00:00",
            watermark_ms=1709251200000,
            offsets=[
                KafkaPartitionOffset(partition=0, offset=100),
                KafkaPartitionOffset(partition=1, offset=200),
            ],
        )
        defaults.update(overrides)
        return KafkaOffsetMap(**defaults)

    def test_constructed_with_kafka_default(self):
        m = self._make()
        assert m.platform == StreamPlatform.KAFKA

    def test_kinesis_currently_blocked(self):
        with pytest.raises(ValidationError) as exc:
            self._make(platform=StreamPlatform.KINESIS)
        assert "Kinesis" in str(exc.value)

    def test_offset_for_returns_value(self):
        m = self._make()
        assert m.offset_for(0) == 100
        assert m.offset_for(1) == 200

    def test_offset_for_missing_returns_none(self):
        m = self._make()
        assert m.offset_for(99) is None

    def test_offset_dict_round_trip(self):
        m = self._make()
        assert m.offset_dict == {0: 100, 1: 200}

    def test_round_trips_via_json(self):
        m1 = self._make()
        roundtripped = KafkaOffsetMap.model_validate_json(m1.model_dump_json())
        assert roundtripped == m1

    def test_captured_at_defaults_to_utc_iso(self):
        m = self._make()
        # Just make sure it's set and ISO-shaped
        assert m.captured_at_iso
        assert "T" in m.captured_at_iso
