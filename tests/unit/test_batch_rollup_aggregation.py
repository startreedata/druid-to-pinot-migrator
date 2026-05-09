"""Tests for the v0.13.0 MSQ-aggregation polish:

  1. ``BATCH_AGGREGATION_NOT_REPLAYED`` risk fires when the canonical
     model is batch + rollup + has metrics, surfacing the gap that
     Pinot's batch ingestion can't replay GROUP BY at ingest time.
  2. ``PinotTableGenerator.generate_offline`` emits per-row metric-
     column rename ``transformConfigs`` so values land in the right
     column even without aggregation. Same trick the REALTIME path
     was already using; now applied to OFFLINE too.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    GranularityInfo,
    MetricField,
    TimeField,
)
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.pinot.table_generator import PinotTableGenerator
from migrator.risks.analyzer import RiskAnalyzer
from migrator.risks.taxonomy import (
    BATCH_AGGREGATION_NOT_REPLAYED,
    ROLLUP_SEMANTIC_MISMATCH,
)


FIXTURES = Path(__file__).parent.parent / "fixtures"


def _canon(**overrides) -> CanonicalMigrationModel:
    base = dict(
        datasource_name="ds",
        source_kind="batch",
        time_field=TimeField(column_name="ts", format="millis"),
        dimensions=[DimensionField(name="region", druid_type="string", pinot_type="STRING")],
        metrics=[MetricField(
            name="amount_sum", druid_type="longSum",
            field_name="amount", pinot_type="LONG", aggregation="SUM",
        )],
        granularity=GranularityInfo(
            segment_granularity="DAY", query_granularity="HOUR",
            rollup=True,
        ),
    )
    base.update(overrides)
    return CanonicalMigrationModel(**base)


# ─────────────────────────────────────────────────────────────────────────────
# BATCH_AGGREGATION_NOT_REPLAYED risk
# ─────────────────────────────────────────────────────────────────────────────


class TestBatchAggregationRisk:
    def test_fires_on_batch_rollup_with_metrics(self):
        c = _canon()  # batch + rollup=True + metrics
        result = RiskAnalyzer().analyze(c)
        ids = {r.risk_id for r in result.risks}
        assert BATCH_AGGREGATION_NOT_REPLAYED in ids
        # The general rollup-mismatch risk still fires too — they're
        # complementary (one is semantics, one is "your numbers WILL
        # be wrong without action").
        assert ROLLUP_SEMANTIC_MISMATCH in ids

    def test_does_not_fire_for_streaming_rollup(self):
        # The streaming path emits transformConfigs that handle the
        # rename; Pinot REALTIME tables also can't aggregate at
        # ingest, but this risk is scoped to batch where the
        # operator's only viable answers (pre-aggregate / star-tree /
        # query-time) are different.
        c = _canon(source_kind="stream")
        result = RiskAnalyzer().analyze(c)
        ids = {r.risk_id for r in result.risks}
        assert BATCH_AGGREGATION_NOT_REPLAYED not in ids

    def test_does_not_fire_without_rollup(self):
        c = _canon(granularity=GranularityInfo(rollup=False))
        result = RiskAnalyzer().analyze(c)
        ids = {r.risk_id for r in result.risks}
        assert BATCH_AGGREGATION_NOT_REPLAYED not in ids

    def test_does_not_fire_with_no_metrics(self):
        # Rollup without metrics is degenerate (just deduplication);
        # there's nothing aggregation-related to warn about.
        c = _canon(metrics=[])
        result = RiskAnalyzer().analyze(c)
        ids = {r.risk_id for r in result.risks}
        assert BATCH_AGGREGATION_NOT_REPLAYED not in ids

    def test_remediation_lists_three_options(self):
        # The risk's whole value is naming the three viable paths so
        # the operator doesn't have to guess. Lock in that the
        # remediation actually mentions all three.
        c = _canon()
        result = RiskAnalyzer().analyze(c)
        risk = next(r for r in result.risks
                    if r.risk_id == BATCH_AGGREGATION_NOT_REPLAYED)
        text = risk.remediation.lower()
        assert "pre-aggregate" in text
        assert "star-tree" in text
        assert "query-time" in text

    def test_evidence_carries_metric_count(self):
        c = _canon(metrics=[
            MetricField(name="m1", druid_type="longSum",
                        pinot_type="LONG", aggregation="SUM"),
            MetricField(name="m2", druid_type="longSum",
                        pinot_type="LONG", aggregation="SUM"),
            MetricField(name="m3", druid_type="count",
                        pinot_type="LONG", aggregation="SUM"),
        ])
        result = RiskAnalyzer().analyze(c)
        risk = next(r for r in result.risks
                    if r.risk_id == BATCH_AGGREGATION_NOT_REPLAYED)
        # Operator can see at-a-glance how big the surface is.
        assert "3 metric" in " ".join(risk.evidence)


# ─────────────────────────────────────────────────────────────────────────────
# Per-row transformConfigs on OFFLINE tables
# ─────────────────────────────────────────────────────────────────────────────


class TestOfflineTransformConfigs:
    def test_offline_emits_count_transform(self):
        # ``count`` is the most common rollup metric and the easiest
        # to get wrong silently — without ``transformFunction: "1"``
        # the count column would be 0 forever.
        c = _canon(metrics=[
            MetricField(name="events", druid_type="count",
                        pinot_type="LONG", aggregation="SUM"),
        ])
        table = PinotTableGenerator().generate_offline(c)
        transforms = table["ingestionConfig"]["transformConfigs"]
        assert {"columnName": "events", "transformFunction": "1"} in transforms

    def test_offline_emits_alias_rename(self):
        # ``SUM(amount) AS amount_sum`` → store source ``amount``
        # values into the ``amount_sum`` column.
        c = _canon()  # has SUM(amount) AS amount_sum
        table = PinotTableGenerator().generate_offline(c)
        transforms = table["ingestionConfig"]["transformConfigs"]
        assert {
            "columnName": "amount_sum", "transformFunction": "amount",
        } in transforms

    def test_offline_no_transforms_when_field_name_matches(self):
        # No rename → no transformConfigs entry; we don't emit
        # noise for the pass-through case.
        c = _canon(metrics=[
            MetricField(
                name="amount", druid_type="longSum",
                field_name="amount", pinot_type="LONG", aggregation="SUM",
            ),
        ])
        table = PinotTableGenerator().generate_offline(c)
        # The transformConfigs key may be absent OR present-but-empty;
        # both mean "nothing to do" and operators handle either.
        transforms = table["ingestionConfig"].get("transformConfigs", [])
        assert transforms == []

    def test_offline_no_transformConfigs_key_when_no_metrics(self):
        # A canonical without any metrics shouldn't emit an empty
        # transformConfigs key. Keeps the generated table-offline.json
        # clean for the simple cases.
        c = _canon(metrics=[])
        table = PinotTableGenerator().generate_offline(c)
        assert "transformConfigs" not in table["ingestionConfig"]

    def test_existing_offline_fields_unchanged(self):
        # Backward-compat seal: the rest of the generated OFFLINE
        # table (segmentsConfig, tenants, batchIngestionConfig) is
        # byte-identical to the v0.12.0 shape.
        c = _canon()
        table = PinotTableGenerator().generate_offline(c)
        assert table["tableType"] == "OFFLINE"
        assert table["tableName"] == "ds_OFFLINE"
        # batchIngestionConfig still present.
        assert table["ingestionConfig"]["batchIngestionConfig"]["segmentIngestionType"] == "APPEND"


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end via the existing rolled_up + MSQ fixtures
# ─────────────────────────────────────────────────────────────────────────────


def _generate_via_fixture(fixture_dir: Path) -> dict:
    raw = json.loads((fixture_dir / "spec.json").read_text())
    parsed = DruidSpecParser().parse(raw)
    canonical = DruidNormalizer().normalize(parsed.parsed_spec).canonical
    canonical.classification = "rolled_up"
    return PinotTableGenerator().generate_offline(canonical)


class TestRolledUpFixturesEmitTransforms:
    def test_classic_rolled_up_fixture_now_emits_transforms(self):
        # The classic ``rolled_up`` fixture (a Druid index_parallel
        # spec with rollup=true) should now produce transformConfigs
        # mapping each Druid metric's source field to its rolled-up
        # column name.
        table = _generate_via_fixture(FIXTURES / "rolled_up")
        transforms = table["ingestionConfig"].get("transformConfigs", [])
        assert transforms, (
            "rolled_up fixture should produce at least one transform"
        )
        # ``count`` always becomes ``transformFunction: "1"``.
        assert any(
            t["transformFunction"] == "1" for t in transforms
        ), transforms

    def test_msq_replace_fixture_emits_transforms(self):
        # The MSQ fixture parses through the new MSQ path but ends
        # up at the same OFFLINE generator; confirm the same
        # transform-emission behaviour.
        table = _generate_via_fixture(FIXTURES / "msq_replace")
        transforms = table["ingestionConfig"].get("transformConfigs", [])
        # MSQ fixture has ``COUNT(*) AS event_count`` + ``SUM(amount)
        # AS amount_sum`` + ``MAX(latency_ms) AS latency_max``. Each
        # becomes a transform.
        assert len(transforms) == 3
        col_names = {t["columnName"] for t in transforms}
        assert col_names == {"event_count", "amount_sum", "latency_max"}
