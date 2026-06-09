"""
Domain models for hybrid (REALTIME + OFFLINE) Druid → Pinot migrations.

These types are deliberately platform-neutral where possible so future
streaming sources (Kinesis, Pulsar) can extend them without an awkward
refactor. Per-platform concerns (Kafka topic name, partition number) live
on the concrete subclasses; cross-platform concerns (watermark timestamp,
offset payload) live on the base.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ─────────────────────────────────────────────────────────────────────────────
# Platform discriminator
# ─────────────────────────────────────────────────────────────────────────────


class StreamPlatform(str, Enum):
    """Supported streaming platforms."""

    KAFKA = "kafka"
    KINESIS = "kinesis"


# ─────────────────────────────────────────────────────────────────────────────
# Per-partition / per-shard positions
# ─────────────────────────────────────────────────────────────────────────────


class KafkaPartitionOffset(BaseModel):
    """Offset captured for a single Kafka partition at a moment in time."""

    partition: int = Field(ge=0)
    offset: int = Field(
        ge=0,
        description="Next offset Pinot should read (i.e. one past Druid's last "
                    "committed offset). Equivalent to Kafka's 'committed' offset.",
    )

    model_config = ConfigDict(frozen=True)


class KinesisShardSequence(BaseModel):
    """Sequence number captured for a single Kinesis shard at a moment in time.

    Kinesis positions are per-shard *sequence numbers* — opaque,
    monotonically-increasing strings, NOT integer offsets like Kafka.
    They are captured for the runbook (documentation); the active
    cutover boundary is the watermark timestamp, which Pinot's Kinesis
    consumer honours via ``auto.offset.reset`` exactly as the Kafka
    consumer does.
    """

    shard_id: str = Field(
        min_length=1,
        description="Kinesis shard identifier, e.g. 'shardId-000000000001'.",
    )
    sequence_number: str = Field(
        min_length=1,
        description="Last sequence number Druid committed for this shard.",
    )

    model_config = ConfigDict(frozen=True)


# ─────────────────────────────────────────────────────────────────────────────
# Offset / watermark snapshot
# ─────────────────────────────────────────────────────────────────────────────


class StreamOffsetMap(BaseModel):
    """
    Snapshot of a Druid streaming supervisor's committed position.

    Platform-neutral: works for both Kafka and Kinesis supervisors.
    Two pieces of information are captured together so consumers don't
    have to recompute either one:

    - **Per-shard/partition positions** — informational, for the
      runbook. Kafka populates ``offsets`` (integer offsets per
      partition); Kinesis populates ``shard_sequences`` (opaque
      sequence-number strings per shard). At most one list is non-empty.
    - ``watermark_iso`` — ISO-8601 timestamp at which Druid had
      committed everything ≤ this point. This is the ACTIVE part of the
      seed: Pinot consumes from it via
      ``stream.<kafka|kinesis>.consumer.prop.auto.offset.reset =
      <watermark_iso>``. Pinot's Kinesis consumer honours a timestamp
      offset criterion exactly as the Kafka consumer does, which is why
      the same hybrid-cutover mechanism works unchanged for Kinesis —
      the per-shard sequence numbers never need to be replayed.
    """

    platform: StreamPlatform = StreamPlatform.KAFKA
    topic: str = Field(
        description="Kafka topic name, or Kinesis stream name — the stream "
                    "identifier the REALTIME table consumes from.",
    )
    supervisor_id: str = ""
    datasource: str = ""
    captured_at_iso: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="UTC time at which this snapshot was taken.",
    )
    watermark_iso: str = Field(
        ...,
        description=(
            "ISO-8601 UTC timestamp marking the boundary between OFFLINE and "
            "REALTIME data. Pinot starts consumption from this timestamp."
        ),
    )
    watermark_ms: int = Field(
        ge=0,
        description="Same as watermark_iso, expressed as epoch milliseconds.",
    )
    watermark_estimated: bool = Field(
        default=False,
        description=(
            "True when the watermark could not be read from a precise "
            "timestamp in the supervisor status (e.g. Kinesis, whose "
            "report carries no absolute timestamp) and fell back to "
            "capture-time now(). A now()-watermark risks data loss at "
            "cutover if the supervisor is lagging — Pinot would start "
            "consuming AFTER events Druid hadn't yet ingested. Refine it "
            "to MAX(__time) of the datasource via "
            "``migrator.realtime.watermark.refine_watermark`` (the cutover "
            "orchestrator does this automatically when a Druid SQL client "
            "is available)."
        ),
    )
    offsets: list[KafkaPartitionOffset] = Field(
        default_factory=list,
        description="Kafka per-partition offsets (empty for Kinesis sources).",
    )
    shard_sequences: list[KinesisShardSequence] = Field(
        default_factory=list,
        description="Kinesis per-shard sequence numbers (empty for Kafka sources).",
    )

    model_config = ConfigDict()

    @field_validator("platform")
    @classmethod
    def _supported_platform(cls, v: StreamPlatform) -> StreamPlatform:
        if v not in (StreamPlatform.KAFKA, StreamPlatform.KINESIS):
            raise ValueError(
                f"Unsupported stream platform '{v.value}'. "
                "Supported: kafka, kinesis."
            )
        return v

    @property
    def stream_name(self) -> str:
        """Alias for ``topic`` that reads naturally for Kinesis sources."""
        return self.topic

    def offset_for(self, partition: int) -> int | None:
        """Return the Kafka offset for a partition, or None if not present."""
        for po in self.offsets:
            if po.partition == partition:
                return po.offset
        return None

    @property
    def offset_dict(self) -> dict[int, int]:
        """Per-partition Kafka offset map for runbook / template rendering."""
        return {po.partition: po.offset for po in self.offsets}

    def sequence_for(self, shard_id: str) -> str | None:
        """Return the Kinesis sequence number for a shard, or None."""
        for ss in self.shard_sequences:
            if ss.shard_id == shard_id:
                return ss.sequence_number
        return None


# Backward-compatibility alias. ``KafkaOffsetMap`` was the original name
# from when only Kafka was supported; it now points at the platform-
# neutral model so existing imports / serialised artifacts keep working.
KafkaOffsetMap = StreamOffsetMap


# ─────────────────────────────────────────────────────────────────────────────
# Hybrid migration plan
# ─────────────────────────────────────────────────────────────────────────────


class BackfillRange(BaseModel):
    """A time interval to be backfilled OFFLINE from Druid into Pinot."""

    start_iso: str = Field(
        ...,
        description="Inclusive start of the backfill interval (ISO-8601 UTC).",
    )
    end_iso: str = Field(
        ...,
        description="Exclusive end of the backfill interval — equals the watermark.",
    )
    page_rows: int = Field(
        default=50_000,
        ge=1,
        description="Druid SQL paging size for the dump phase.",
    )


class HybridMigrationPlan(BaseModel):
    """
    The full set of artifacts the planner produces for a hybrid migration.

    Conceptually:

      [ Druid datasource           ]   ──watermark──▶
      ↓
      [ Pinot OFFLINE table  (... < watermark) ]
      [ Pinot REALTIME table (>= watermark)    ]

    The schema is shared between OFFLINE and REALTIME (Pinot's hybrid-table
    requirement). The REALTIME stream config has the watermark embedded as
    its starting offset criteria.
    """

    datasource_name: str
    schema_: dict = Field(alias="schema")
    offline_table: dict
    realtime_table: dict
    backfill_range: BackfillRange
    backfill_job: dict = Field(
        default_factory=dict,
        description="Pinot batch-ingestion job spec for the backfill.",
    )
    watermark: StreamOffsetMap

    model_config = ConfigDict(populate_by_name=True)

    def to_dict(self) -> dict[str, Any]:
        """Plain-dict form suitable for JSON serialisation."""
        return self.model_dump(by_alias=True)
