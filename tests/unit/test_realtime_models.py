"""Unit tests for migrator.realtime.models."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from migrator.realtime.models import (
    KafkaOffsetMap,
    KafkaPartitionOffset,
    KinesisShardSequence,
    StreamOffsetMap,
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

    def test_kinesis_now_accepted(self):
        # v0.14-dev: Kinesis is wired end-to-end through the cutover
        # path, so the platform validator no longer rejects it.
        m = self._make(platform=StreamPlatform.KINESIS, offsets=[])
        assert m.platform == StreamPlatform.KINESIS

    def test_unknown_platform_rejected(self):
        with pytest.raises(ValidationError):
            self._make(platform="pulsar")  # type: ignore[arg-type]

    def test_kafka_offset_map_is_stream_offset_map_alias(self):
        # Back-compat: the old name still points at the generalised model.
        assert KafkaOffsetMap is StreamOffsetMap

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


class TestKinesisShardSequence:
    def test_holds_shard_and_sequence(self):
        ss = KinesisShardSequence(
            shard_id="shardId-000000000001", sequence_number="49590338..."
        )
        assert ss.shard_id == "shardId-000000000001"
        assert ss.sequence_number == "49590338..."

    def test_is_immutable(self):
        ss = KinesisShardSequence(shard_id="s", sequence_number="1")
        with pytest.raises(ValidationError):
            ss.sequence_number = "2"  # type: ignore[misc]

    def test_rejects_empty_shard_or_sequence(self):
        with pytest.raises(ValidationError):
            KinesisShardSequence(shard_id="", sequence_number="1")
        with pytest.raises(ValidationError):
            KinesisShardSequence(shard_id="s", sequence_number="")


class TestStreamOffsetMapKinesis:
    def _make_kinesis(self, **overrides):
        defaults = dict(
            platform=StreamPlatform.KINESIS,
            topic="payment-events",
            supervisor_id="payments-sup",
            datasource="payments",
            watermark_iso="2024-03-01T00:00:00.000+00:00",
            watermark_ms=1709251200000,
            shard_sequences=[
                KinesisShardSequence(
                    shard_id="shardId-000000000000", sequence_number="100"
                ),
                KinesisShardSequence(
                    shard_id="shardId-000000000001", sequence_number="200"
                ),
            ],
        )
        defaults.update(overrides)
        return StreamOffsetMap(**defaults)

    def test_stream_name_aliases_topic(self):
        m = self._make_kinesis()
        assert m.stream_name == "payment-events"

    def test_sequence_for_returns_value(self):
        m = self._make_kinesis()
        assert m.sequence_for("shardId-000000000000") == "100"
        assert m.sequence_for("shardId-000000000001") == "200"

    def test_sequence_for_missing_returns_none(self):
        m = self._make_kinesis()
        assert m.sequence_for("shardId-999") is None

    def test_offsets_empty_for_kinesis(self):
        m = self._make_kinesis()
        assert m.offsets == []
        assert m.offset_dict == {}

    def test_round_trips_via_json(self):
        m1 = self._make_kinesis()
        roundtripped = StreamOffsetMap.model_validate_json(m1.model_dump_json())
        assert roundtripped == m1
        assert roundtripped.platform == StreamPlatform.KINESIS
