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
    KINESIS = "kinesis"  # placeholder; not yet wired into planner / client


# ─────────────────────────────────────────────────────────────────────────────
# Per-partition offsets
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


# ─────────────────────────────────────────────────────────────────────────────
# Offset / watermark snapshot
# ─────────────────────────────────────────────────────────────────────────────


class KafkaOffsetMap(BaseModel):
    """
    Snapshot of a Druid Kafka supervisor's committed position.

    Two pieces of information are captured together so consumers don't have
    to recompute either one:

    - ``offsets`` — per-partition Kafka offsets (informational; used for
      runbook + manual reset via ``kafka-consumer-groups.sh`` if desired)
    - ``watermark_iso`` — ISO-8601 timestamp at which Druid had committed
      everything ≤ this point. This is what Pinot consumes via
      ``stream.kafka.consumer.prop.auto.offset.reset = <watermark_iso>``.

    The watermark is the ACTIVE part of the seed; offsets are documentation.
    """

    platform: StreamPlatform = StreamPlatform.KAFKA
    topic: str
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
    offsets: list[KafkaPartitionOffset] = Field(default_factory=list)

    model_config = ConfigDict()

    @field_validator("platform")
    @classmethod
    def _kafka_only_for_now(cls, v: StreamPlatform) -> StreamPlatform:
        if v != StreamPlatform.KAFKA:
            raise ValueError(
                f"Only StreamPlatform.KAFKA is currently supported (got {v.value}). "
                "Kinesis is on the roadmap; track via the Kinesis follow-up issue."
            )
        return v

    def offset_for(self, partition: int) -> int | None:
        """Return the offset for a given partition, or None if not present."""
        for po in self.offsets:
            if po.partition == partition:
                return po.offset
        return None

    @property
    def offset_dict(self) -> dict[int, int]:
        """Per-partition map convenient for runbook / template rendering."""
        return {po.partition: po.offset for po in self.offsets}


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
    watermark: KafkaOffsetMap

    model_config = ConfigDict(populate_by_name=True)

    def to_dict(self) -> dict[str, Any]:
        """Plain-dict form suitable for JSON serialisation."""
        return self.model_dump(by_alias=True)
