"""Unit tests for ``derive_queries_from_canonical``."""

from __future__ import annotations

from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    GranularityInfo,
    MetricField,
)
from migrator.parity.query_builder import derive_queries_from_canonical


def _dim(name: str, multi: bool = False) -> DimensionField:
    return DimensionField(name=name, multi_value=multi)


def _metric(name: str, druid_type: str, field_name: str = "") -> MetricField:
    return MetricField(
        name=name,
        druid_type=druid_type,
        field_name=field_name or name,
        pinot_type="LONG",
        aggregation="SUM",
    )


def _model(
    *,
    metrics: list[MetricField] | None = None,
    dimensions: list[DimensionField] | None = None,
    rollup: bool = False,
) -> CanonicalMigrationModel:
    return CanonicalMigrationModel(
        datasource_name="ds",
        source_kind="batch",
        metrics=metrics or [],
        dimensions=dimensions or [],
        granularity=GranularityInfo(rollup=rollup),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Total event-count query
# ─────────────────────────────────────────────────────────────────────────────


class TestTotalCountQuery:
    def test_no_rollup_uses_count_star(self):
        # Raw events, no rollup → COUNT(*) on both sides.
        c = _model()
        qs = derive_queries_from_canonical(c)
        first = qs[0]
        assert first.label == "Total event count"
        assert "COUNT(*)" in first.druid
        assert "COUNT(*)" in first.pinot

    def test_rollup_with_count_metric_uses_sum_count_metric_on_druid(self):
        # Rolled-up Druid datasource with a `count` metric → Druid uses
        # SUM(<count metric>) (preserves original event count); Pinot
        # still uses COUNT(*) (no rollup on the Pinot side).
        c = _model(
            metrics=[_metric("events", "count")],
            rollup=True,
        )
        qs = derive_queries_from_canonical(c)
        first = qs[0]
        assert "SUM(\"events\")" in first.druid
        assert "COUNT(*)" in first.pinot

    def test_rollup_without_count_metric_falls_back_to_count_star(self):
        # Rolled-up but no count metric (rare) → COUNT(*) is the best
        # approximation we have.
        c = _model(
            metrics=[_metric("session_ms_sum", "longSum", "session_ms")],
            rollup=True,
        )
        qs = derive_queries_from_canonical(c)
        first = qs[0]
        assert "COUNT(*)" in first.druid
        assert "COUNT(*)" in first.pinot


# ─────────────────────────────────────────────────────────────────────────────
# Per-metric aggregate queries
# ─────────────────────────────────────────────────────────────────────────────


class TestMetricQueries:
    def test_count_metric_skipped_to_avoid_duplicate(self):
        # The total-count query already covers `count` metrics; don't
        # emit a second SUM(count) entry.
        c = _model(metrics=[_metric("events", "count")], rollup=True)
        qs = derive_queries_from_canonical(c)
        labels = [q.label for q in qs]
        # Only one entry mentions events: the total-count query.
        assert sum("events" in lbl.lower() for lbl in labels) == 0
        assert qs[0].label == "Total event count"

    def test_long_sum_emits_sum(self):
        c = _model(metrics=[_metric("session_ms_sum", "longSum", "session_ms")])
        qs = derive_queries_from_canonical(c)
        sum_q = next(q for q in qs if q.label == "SUM(session_ms_sum)")
        assert "SUM(\"session_ms_sum\")" in sum_q.druid
        assert "SUM(\"session_ms_sum\")" in sum_q.pinot

    def test_long_min_emits_min(self):
        c = _model(metrics=[_metric("bytes_sent_min", "longMin", "bytes_sent")])
        qs = derive_queries_from_canonical(c)
        labels = [q.label for q in qs]
        assert "MIN(bytes_sent_min)" in labels

    def test_long_max_emits_max(self):
        c = _model(metrics=[_metric("session_ms_max", "longMax", "session_ms")])
        qs = derive_queries_from_canonical(c)
        labels = [q.label for q in qs]
        assert "MAX(session_ms_max)" in labels

    def test_double_sum_also_uses_sum(self):
        c = _model(metrics=[_metric("revenue", "doubleSum", "revenue_usd")])
        qs = derive_queries_from_canonical(c)
        labels = [q.label for q in qs]
        assert "SUM(revenue)" in labels

    def test_unknown_metric_type_silently_skipped(self):
        # Sketch / approximate metrics don't have a SQL-equivalent
        # aggregator we can compare; skip rather than emit a
        # divergent query.
        c = _model(metrics=[_metric("unique_users", "thetaSketch")])
        qs = derive_queries_from_canonical(c)
        labels = [q.label for q in qs]
        assert "SUM(unique_users)" not in labels
        assert "MIN(unique_users)" not in labels
        assert "MAX(unique_users)" not in labels


# ─────────────────────────────────────────────────────────────────────────────
# Per-dimension groupby queries
# ─────────────────────────────────────────────────────────────────────────────


class TestGroupByQueries:
    def test_emits_one_per_single_value_dim(self):
        c = _model(dimensions=[_dim("region"), _dim("platform")])
        qs = derive_queries_from_canonical(c)
        groupbys = [q for q in qs if q.type == "groupby"]
        assert {q.label for q in groupbys} == {
            "events by region", "events by platform"
        }

    def test_groupby_uses_count_star_when_no_rollup(self):
        c = _model(dimensions=[_dim("region")])
        gby = next(q for q in derive_queries_from_canonical(c) if q.type == "groupby")
        assert "COUNT(*)" in gby.druid
        assert "COUNT(*)" in gby.pinot

    def test_groupby_uses_sum_count_metric_on_druid_under_rollup(self):
        c = _model(
            dimensions=[_dim("region")],
            metrics=[_metric("events", "count")],
            rollup=True,
        )
        gby = next(q for q in derive_queries_from_canonical(c) if q.type == "groupby")
        assert "SUM(\"events\")" in gby.druid
        assert "COUNT(*)" in gby.pinot

    def test_groupby_quotes_identifier(self):
        # GROUP BY needs the dim quoted on both sides — Druid SQL
        # requires it for case-sensitivity, and quoting on Pinot is
        # always safe.
        c = _model(dimensions=[_dim("region")])
        gby = next(q for q in derive_queries_from_canonical(c) if q.type == "groupby")
        assert 'GROUP BY "region"' in gby.druid
        assert 'GROUP BY "region"' in gby.pinot
        assert 'ORDER BY "region"' in gby.druid

    def test_multivalue_dim_skipped(self):
        # MV dims diverge between engines (each MV value contributes a
        # row in Pinot's GROUP BY) — the auto path opts them out.
        c = _model(dimensions=[_dim("region"), _dim("tags", multi=True)])
        groupbys = [q for q in derive_queries_from_canonical(c) if q.type == "groupby"]
        labels = {q.label for q in groupbys}
        assert "events by region" in labels
        assert "events by tags" not in labels

    def test_groupby_emits_explicit_large_limit_on_both_sides(self):
        # Pinot's broker defaults to LIMIT 10 on GROUP BY queries —
        # silently truncates past the 10th group. The auto-derived
        # query needs an explicit large LIMIT on BOTH sides so a
        # high-cardinality dimension doesn't produce a false-positive
        # divergence (Druid 30 groups vs Pinot 10 groups). Live
        # coverage in tests/docker/test_deploy_and_parity_live.py
        # surfaced this on a 30-distinct-user_id dataset.
        c = _model(dimensions=[_dim("user_id")])
        gby = next(q for q in derive_queries_from_canonical(c) if q.type == "groupby")
        assert "LIMIT 1000000" in gby.druid
        assert "LIMIT 1000000" in gby.pinot


# ─────────────────────────────────────────────────────────────────────────────
# pinot_table override
# ─────────────────────────────────────────────────────────────────────────────


class TestTableNameOverride:
    def test_default_uses_canonical_datasource_name(self):
        c = _model()
        qs = derive_queries_from_canonical(c)
        assert "\"ds\"" in qs[0].druid
        assert "\"ds\"" in qs[0].pinot

    def test_pinot_table_override_only_affects_pinot(self):
        c = _model()
        qs = derive_queries_from_canonical(c, pinot_table="ds_renamed")
        assert "\"ds\"" in qs[0].druid
        assert "\"ds_renamed\"" in qs[0].pinot

    def test_druid_table_override(self):
        c = _model()
        qs = derive_queries_from_canonical(
            c, druid_table="ds_v2", pinot_table="ds_v2",
        )
        assert "\"ds_v2\"" in qs[0].druid
        assert "\"ds_v2\"" in qs[0].pinot


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end shape: classic pageviews migration
# ─────────────────────────────────────────────────────────────────────────────


class TestPageviewsShape:
    def test_full_query_set_for_classic_rolled_up_pageviews(self):
        c = _model(
            dimensions=[_dim("region"), _dim("platform"), _dim("page")],
            metrics=[
                _metric("events", "count"),
                _metric("session_ms_sum", "longSum", "session_ms"),
                _metric("bytes_sent_sum", "longSum", "bytes_sent"),
                _metric("session_ms_max", "longMax", "session_ms"),
                _metric("bytes_sent_min", "longMin", "bytes_sent"),
            ],
            rollup=True,
        )
        qs = derive_queries_from_canonical(c)
        labels = [q.label for q in qs]
        # 1 total + 4 metrics (count is folded in) + 3 group-bys
        assert labels == [
            "Total event count",
            "SUM(session_ms_sum)",
            "SUM(bytes_sent_sum)",
            "MAX(session_ms_max)",
            "MIN(bytes_sent_min)",
            "events by region",
            "events by platform",
            "events by page",
        ]
