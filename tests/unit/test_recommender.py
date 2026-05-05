"""Unit tests for migrator.recommendations.recommender."""

from __future__ import annotations

import pytest

from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    GranularityInfo,
    MetricField,
    TimeField,
)
from migrator.recommendations.recommender import (
    Recommendation,
    _is_id_like,
    recommend,
)


def _canon(**overrides) -> CanonicalMigrationModel:
    base = dict(
        datasource_name="ds",
        source_kind="batch",
        classification="raw_event",
        time_field=TimeField(column_name="timestamp", format="millis"),
        dimensions=[],
        metrics=[],
        granularity=GranularityInfo(),
    )
    base.update(overrides)
    return CanonicalMigrationModel(**base)


def _kinds(recs: list[Recommendation]) -> set[str]:
    return {r.kind for r in recs}


# ─────────────────────────────────────────────────────────────────────────────
# Star-tree
# ─────────────────────────────────────────────────────────────────────────────


class TestStarTree:
    def test_recommended_when_dims_and_metrics_exist(self):
        c = _canon(
            dimensions=[
                DimensionField(name="region", druid_type="string", pinot_type="STRING"),
                DimensionField(name="device", druid_type="string", pinot_type="STRING"),
            ],
            metrics=[
                MetricField(name="events", druid_type="count",
                            pinot_type="LONG", aggregation="SUM"),
            ],
        )
        recs = recommend(c)
        st = next(r for r in recs if r.kind == "star_tree")
        assert st.severity == "high"
        # The hint contains the dim list and the function-pair
        # encoding Pinot expects.
        cfg = st.config_hint["tableIndexConfig"]["starTreeIndexConfigs"][0]
        assert cfg["dimensionsSplitOrder"] == ["region", "device"]
        assert "SUM__events" in cfg["functionColumnPairs"]

    def test_skipped_when_no_metrics(self):
        c = _canon(
            dimensions=[DimensionField(
                name="region", druid_type="string", pinot_type="STRING",
            )],
        )
        assert "star_tree" not in _kinds(recommend(c))

    def test_skipped_when_no_dimensions(self):
        c = _canon(
            metrics=[MetricField(
                name="events", druid_type="count",
                pinot_type="LONG", aggregation="SUM",
            )],
        )
        assert "star_tree" not in _kinds(recommend(c))


# ─────────────────────────────────────────────────────────────────────────────
# Sketch aggregator suggestions
# ─────────────────────────────────────────────────────────────────────────────


class TestSketchAggregators:
    def test_hyperUnique_maps_to_DistinctCountHLL(self):
        c = _canon(metrics=[
            MetricField(
                name="users", druid_type="hyperUnique",
                field_name="user_id", pinot_type="BYTES", aggregation="HLL",
            ),
        ])
        recs = recommend(c)
        agg = next(r for r in recs if r.kind == "aggregator")
        assert agg.target == "users"
        assert "DistinctCountHLL" in agg.rationale
        # Hint produces a transformConfig that callers can paste into
        # their table config wholesale.
        tc = agg.config_hint["ingestionConfig"]["transformConfigs"][0]
        assert tc["columnName"] == "users"
        assert "DistinctCountHLL(user_id)" == tc["transformFunction"]

    def test_thetaSketch_maps_to_DistinctCountThetaSketch(self):
        c = _canon(metrics=[
            MetricField(
                name="dau", druid_type="thetaSketch",
                field_name="user_id", pinot_type="BYTES", aggregation="THETA",
            ),
        ])
        recs = recommend(c)
        agg = next(r for r in recs if r.kind == "aggregator")
        assert "DistinctCountThetaSketch" in agg.rationale

    def test_quantiles_sketch_maps_to_PercentileTDigest(self):
        c = _canon(metrics=[
            MetricField(
                name="latency_p99", druid_type="quantilesDoublesSketch",
                field_name="latency", pinot_type="DOUBLE", aggregation="PERCENTILE",
            ),
        ])
        recs = recommend(c)
        agg = next(r for r in recs if r.kind == "aggregator")
        assert "PercentileTDigest" in agg.rationale

    def test_no_recommendation_for_plain_sum(self):
        c = _canon(metrics=[
            MetricField(name="amount", druid_type="doubleSum",
                        pinot_type="DOUBLE", aggregation="SUM"),
        ])
        recs = recommend(c)
        assert all(r.kind != "aggregator" for r in recs)


# ─────────────────────────────────────────────────────────────────────────────
# Sorted column
# ─────────────────────────────────────────────────────────────────────────────


class TestSortedColumn:
    def test_recommends_time_column(self):
        c = _canon()
        rec = next(r for r in recommend(c) if r.kind == "sorted_column")
        assert rec.target == "timestamp"
        assert rec.severity == "medium"
        assert (
            rec.config_hint["tableIndexConfig"]["sortedColumn"] == ["timestamp"]
        )

    def test_skipped_when_no_time_field(self):
        c = _canon(time_field=None)
        assert "sorted_column" not in _kinds(recommend(c))


# ─────────────────────────────────────────────────────────────────────────────
# Range index
# ─────────────────────────────────────────────────────────────────────────────


class TestRangeIndex:
    def test_recommended_for_numeric_metrics(self):
        c = _canon(
            metrics=[
                MetricField(name="amount", druid_type="doubleSum",
                            pinot_type="DOUBLE", aggregation="SUM"),
                MetricField(name="count", druid_type="count",
                            pinot_type="LONG", aggregation="SUM"),
            ],
        )
        rec = next(r for r in recommend(c) if r.kind == "range_index")
        # Both metric names appear in the target string.
        assert "amount" in rec.target
        assert "count" in rec.target

    def test_excludes_time_column_already_sorted(self):
        # A metric named the same as the time column (silly but legal)
        # should be skipped — already sorted means range queries are
        # already fast.
        c = _canon(
            time_field=TimeField(column_name="ts", format="millis"),
            metrics=[
                MetricField(name="ts", druid_type="longSum",
                            pinot_type="LONG", aggregation="SUM"),
                MetricField(name="amount", druid_type="doubleSum",
                            pinot_type="DOUBLE", aggregation="SUM"),
            ],
        )
        rec = next(r for r in recommend(c) if r.kind == "range_index")
        # Time column dropped; only ``amount`` remains.
        assert "amount" in rec.target
        assert "ts" not in rec.target.split(", ")

    def test_skipped_when_only_string_metrics(self):
        c = _canon(metrics=[
            MetricField(name="hll", druid_type="hyperUnique",
                        pinot_type="BYTES", aggregation="HLL"),
        ])
        assert "range_index" not in _kinds(recommend(c))


# ─────────────────────────────────────────────────────────────────────────────
# Inverted index + bloom filter — id-like heuristic
# ─────────────────────────────────────────────────────────────────────────────


class TestIdLikeHeuristic:
    @pytest.mark.parametrize(
        "name, expected",
        [
            ("user_id", True),
            ("USER_ID", True),
            ("uuid",    True),
            ("session_uuid", True),
            ("api_key", True),
            ("auth_token", True),
            # negatives
            ("region", False),
            ("event_count", False),
            ("idempotent", False),  # 'id' substring but not the pattern
            ("price", False),
        ],
    )
    def test_id_like_pattern(self, name: str, expected: bool):
        assert _is_id_like(name) is expected

    def test_inverted_and_bloom_recommended_for_id_dim(self):
        c = _canon(dimensions=[
            DimensionField(name="user_id", druid_type="string", pinot_type="STRING"),
            DimensionField(name="region", druid_type="string", pinot_type="STRING"),
        ])
        recs = recommend(c)
        kinds = _kinds(recs)
        assert "inverted_index" in kinds
        assert "bloom_filter" in kinds
        inverted = next(r for r in recs if r.kind == "inverted_index")
        # Only the id-like dim is in the target.
        assert "user_id" in inverted.target
        assert "region" not in inverted.target

    def test_no_recommendation_when_no_id_like_dim(self):
        c = _canon(dimensions=[
            DimensionField(name="region", druid_type="string", pinot_type="STRING"),
        ])
        kinds = _kinds(recommend(c))
        assert "inverted_index" not in kinds
        assert "bloom_filter" not in kinds


# ─────────────────────────────────────────────────────────────────────────────
# Severity ordering
# ─────────────────────────────────────────────────────────────────────────────


class TestOrdering:
    def test_high_severity_recommendations_come_first(self):
        # Build a canonical with EVERY kind of recommendation in play
        # so we can sanity-check the order.
        c = _canon(
            dimensions=[
                DimensionField(name="user_id", druid_type="string", pinot_type="STRING"),
                DimensionField(name="region", druid_type="string", pinot_type="STRING"),
            ],
            metrics=[
                MetricField(name="events", druid_type="count",
                            pinot_type="LONG", aggregation="SUM"),
                MetricField(name="users", druid_type="hyperUnique",
                            field_name="user_id", pinot_type="BYTES",
                            aggregation="HLL"),
            ],
        )
        recs = recommend(c)
        # First recommendation is the star-tree (highest impact).
        assert recs[0].kind == "star_tree"
        # No high-severity rec lands after a low-severity one.
        severities = [r.severity for r in recs]
        # Map to ordinals for comparison.
        order = {"high": 0, "medium": 1, "low": 2}
        ords = [order[s] for s in severities]
        assert ords == sorted(ords), (
            f"Severities not non-decreasing: {severities}"
        )
