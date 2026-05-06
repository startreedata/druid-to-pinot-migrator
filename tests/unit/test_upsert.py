"""Unit tests for Pinot upsert table generation.

Druid has no row-level upsert, so this is operator-driven via CLI
flags / ``UpsertConfig``. The tests cover three layers:

  - Schema generator: ``primaryKeyColumns`` appears (only) when
    ``canonical.upsert.enabled``.
  - Table generator: ``upsertConfig`` + ``routing.instanceSelectorType=
    strictReplicaGroup`` appear (only) when upsert is on.
  - Pipeline: ``generate_bundle(upsert_config=...)`` validates source
    kind + PK existence + comparison-column existence and surfaces
    clear errors before any artifact is emitted.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    MetricField,
    TimeField,
    UpsertConfig,
)
from migrator.pinot.schema_generator import PinotSchemaGenerator
from migrator.pinot.table_generator import PinotTableGenerator
from migrator.translators.pipeline import generate_bundle


FIXTURES = Path(__file__).parent.parent / "fixtures"


def _stream_canonical(**overrides) -> CanonicalMigrationModel:
    base = dict(
        datasource_name="events",
        source_kind="stream",
        classification="raw_event",
        time_field=TimeField(column_name="event_time", format="millis"),
        dimensions=[
            DimensionField(name="user_id", druid_type="string", pinot_type="STRING"),
            DimensionField(name="region",  druid_type="string", pinot_type="STRING"),
        ],
        metrics=[
            MetricField(name="events", druid_type="count",
                        pinot_type="LONG", aggregation="SUM"),
        ],
        raw_io_config={
            "type": "kafka",
            "topic": "events",
            "consumerProperties": {"bootstrap.servers": "k:9092"},
        },
    )
    base.update(overrides)
    return CanonicalMigrationModel(**base)


# ─────────────────────────────────────────────────────────────────────────────
# Schema generator
# ─────────────────────────────────────────────────────────────────────────────


class TestSchemaPrimaryKeyColumns:
    def test_primaryKeyColumns_emitted_when_upsert_enabled(self):
        c = _stream_canonical(
            upsert=UpsertConfig(enabled=True, primary_key=["user_id"]),
        )
        schema = PinotSchemaGenerator().generate(c)
        assert schema["primaryKeyColumns"] == ["user_id"]

    def test_compound_primary_key_preserved(self):
        c = _stream_canonical(
            upsert=UpsertConfig(
                enabled=True, primary_key=["user_id", "tenant_id"],
            ),
        )
        schema = PinotSchemaGenerator().generate(c)
        # Order matters — Pinot's upsert hash uses tuple order.
        assert schema["primaryKeyColumns"] == ["user_id", "tenant_id"]

    def test_no_primaryKeyColumns_when_upsert_disabled(self):
        c = _stream_canonical()  # default UpsertConfig: enabled=False
        schema = PinotSchemaGenerator().generate(c)
        assert "primaryKeyColumns" not in schema

    def test_no_primaryKeyColumns_when_enabled_but_no_pk(self):
        # Defensive: a partial config (enabled=True but primary_key=[])
        # should not declare an empty key list, which Pinot rejects
        # at table-create time.
        c = _stream_canonical(
            upsert=UpsertConfig(enabled=True, primary_key=[]),
        )
        schema = PinotSchemaGenerator().generate(c)
        assert "primaryKeyColumns" not in schema


# ─────────────────────────────────────────────────────────────────────────────
# Table generator
# ─────────────────────────────────────────────────────────────────────────────


class TestRealtimeTableUpsertBlock:
    def test_upsertConfig_FULL_with_default_comparison(self):
        # Comparison column defaults to the time field — the most common
        # operator intent ("latest event wins for this PK").
        c = _stream_canonical(
            upsert=UpsertConfig(enabled=True, primary_key=["user_id"]),
        )
        table = PinotTableGenerator().generate_realtime(c)
        assert table["upsertConfig"]["mode"] == "FULL"
        assert table["upsertConfig"]["comparisonColumns"] == ["event_time"]
        # Routing is mandatory for upsert — broken dedup without it.
        assert table["routing"]["instanceSelectorType"] == "strictReplicaGroup"

    def test_explicit_comparison_column_overrides_time_field(self):
        c = _stream_canonical(
            metrics=[
                MetricField(name="version", druid_type="longSum",
                            pinot_type="LONG", aggregation="SUM"),
            ],
            upsert=UpsertConfig(
                enabled=True, primary_key=["user_id"],
                comparison_column="version",
            ),
        )
        table = PinotTableGenerator().generate_realtime(c)
        assert table["upsertConfig"]["comparisonColumns"] == ["version"]

    def test_partial_mode_carries_strategies(self):
        c = _stream_canonical(
            upsert=UpsertConfig(
                enabled=True, primary_key=["user_id"],
                mode="PARTIAL",
                partial_columns={"region": "OVERWRITE", "events": "INCREMENT"},
            ),
        )
        table = PinotTableGenerator().generate_realtime(c)
        assert table["upsertConfig"]["mode"] == "PARTIAL"
        assert (
            table["upsertConfig"]["partialUpsertStrategies"]
            == {"region": "OVERWRITE", "events": "INCREMENT"}
        )

    def test_upsert_disabled_does_not_emit_block(self):
        c = _stream_canonical()
        table = PinotTableGenerator().generate_realtime(c)
        assert "upsertConfig" not in table
        assert "routing" not in table or (
            "instanceSelectorType" not in table.get("routing", {})
        )

    def test_existing_streamConfigs_unchanged_by_upsert(self):
        # The upsert block is additive — the Kafka streamConfigs the
        # operator depends on for ingestion stay byte-identical to the
        # non-upsert case.
        c_no_upsert = _stream_canonical()
        c_upsert = _stream_canonical(
            upsert=UpsertConfig(enabled=True, primary_key=["user_id"]),
        )
        gen = PinotTableGenerator()
        plain = gen.generate_realtime(c_no_upsert)
        upsert_table = gen.generate_realtime(c_upsert)
        assert (
            plain["tableIndexConfig"]["streamConfigs"]
            == upsert_table["tableIndexConfig"]["streamConfigs"]
        )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline validation: errors surface before any artifact is written
# ─────────────────────────────────────────────────────────────────────────────


class TestUpsertPipelineValidation:
    def test_batch_source_rejected(self, tmp_path: Path):
        spec = FIXTURES / "raw_batch" / "spec.json"
        result = generate_bundle(
            str(spec), out_dir=str(tmp_path),
            upsert_config=UpsertConfig(
                enabled=True, primary_key=["user"],
            ),
        )
        assert not result.success
        assert any("streaming source" in e for e in result.errors)
        # Failed before writing any artifact.
        assert not (tmp_path / "schema.json").exists()

    def test_unknown_primary_key_rejected_with_known_list(self, tmp_path: Path):
        spec = FIXTURES / "raw_stream" / "spec.json"
        result = generate_bundle(
            str(spec), out_dir=str(tmp_path),
            upsert_config=UpsertConfig(
                enabled=True, primary_key=["definitely_not_a_column"],
            ),
        )
        assert not result.success
        msg = " ".join(result.errors)
        # Error names the bad PK and lists what IS available — the
        # operator should be able to fix the typo without re-reading
        # the spec.
        assert "definitely_not_a_column" in msg
        assert "Known columns" in msg

    def test_unknown_comparison_column_rejected(self, tmp_path: Path):
        spec = FIXTURES / "raw_stream" / "spec.json"
        result = generate_bundle(
            str(spec), out_dir=str(tmp_path),
            upsert_config=UpsertConfig(
                enabled=True,
                primary_key=["user_id"],
                comparison_column="not_a_real_column",
            ),
        )
        assert not result.success
        assert any(
            "comparison column" in e and "not_a_real_column" in e
            for e in result.errors
        )

    def test_happy_path_generates_upsert_artifacts(self, tmp_path: Path):
        spec = FIXTURES / "raw_stream" / "spec.json"
        result = generate_bundle(
            str(spec), out_dir=str(tmp_path),
            upsert_config=UpsertConfig(
                enabled=True, primary_key=["user_id"],
            ),
        )
        assert result.success
        schema = json.loads((tmp_path / "schema.json").read_text())
        table = json.loads((tmp_path / "table-realtime.json").read_text())
        assert schema["primaryKeyColumns"] == ["user_id"]
        assert table["upsertConfig"]["mode"] == "FULL"
        assert table["routing"]["instanceSelectorType"] == "strictReplicaGroup"
        # No OFFLINE table was emitted — Pinot upsert is REALTIME-only,
        # so dpm shouldn't suggest an OFFLINE half that wouldn't work.
        # (raw_stream is a stream spec so this is implicit, but worth
        # locking in.)
        assert not (tmp_path / "table-offline.json").exists()

    def test_upsert_config_none_is_default_no_op(self, tmp_path: Path):
        # generate_bundle without upsert_config should produce the
        # same artifacts as v0.10.x — backward compat seal.
        spec = FIXTURES / "raw_stream" / "spec.json"
        result = generate_bundle(str(spec), out_dir=str(tmp_path))
        assert result.success
        schema = json.loads((tmp_path / "schema.json").read_text())
        table = json.loads((tmp_path / "table-realtime.json").read_text())
        assert "primaryKeyColumns" not in schema
        assert "upsertConfig" not in table
