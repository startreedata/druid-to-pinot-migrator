"""Unit tests for the Druid SQL query classifier."""

from __future__ import annotations

import pytest

from migrator.queries.classifier import (
    SEV_INCOMPATIBLE,
    SEV_RISKY,
    VERDICT_COMPATIBLE,
    VERDICT_INCOMPATIBLE,
    VERDICT_RISKY,
    classify_query,
)


# ─────────────────────────────────────────────────────────────────────────────
# Compatible-path queries
# ─────────────────────────────────────────────────────────────────────────────


class TestCompatibleQueries:

    def test_simple_select_compatible(self):
        c = classify_query("SELECT a, b FROM events WHERE c = 1")
        assert c.verdict == VERDICT_COMPATIBLE
        assert c.issues == []

    def test_aggregation_compatible(self):
        c = classify_query(
            "SELECT region, COUNT(*), SUM(amount) FROM events GROUP BY region"
        )
        assert c.verdict == VERDICT_COMPATIBLE

    def test_order_by_limit_compatible(self):
        c = classify_query(
            "SELECT a FROM events ORDER BY ts DESC LIMIT 100"
        )
        assert c.verdict == VERDICT_COMPATIBLE


# ─────────────────────────────────────────────────────────────────────────────
# Incompatible-path queries — Druid-only or wire-incompatible
# ─────────────────────────────────────────────────────────────────────────────


class TestIncompatibleFunctions:

    def test_approx_count_distinct_ds_hll(self):
        c = classify_query(
            "SELECT APPROX_COUNT_DISTINCT_DS_HLL(user) FROM events"
        )
        assert c.verdict == VERDICT_INCOMPATIBLE
        assert any(
            i.pattern == "APPROX_COUNT_DISTINCT_DS_HLL"
            and i.severity == SEV_INCOMPATIBLE
            for i in c.issues
        )

    def test_approx_count_distinct_ds_theta(self):
        c = classify_query(
            "SELECT APPROX_COUNT_DISTINCT_DS_THETA(user) FROM events"
        )
        assert c.verdict == VERDICT_INCOMPATIBLE
        assert any(i.pattern == "APPROX_COUNT_DISTINCT_DS_THETA" for i in c.issues)

    def test_lookup_function(self):
        c = classify_query("SELECT LOOKUP(country, 'name_lookup') FROM events")
        assert c.verdict == VERDICT_INCOMPATIBLE
        assert any(i.pattern == "LOOKUP" for i in c.issues)

    def test_mv_concat(self):
        c = classify_query("SELECT MV_CONCAT(tags, ', ') FROM events")
        assert c.verdict == VERDICT_INCOMPATIBLE
        assert any(i.pattern == "MV_CONCAT" for i in c.issues)

    def test_mv_overlap(self):
        c = classify_query(
            "SELECT * FROM events WHERE MV_OVERLAP(tags, ARRAY['a','b'])"
        )
        assert c.verdict == VERDICT_INCOMPATIBLE
        assert any(i.pattern == "MV_OVERLAP" for i in c.issues)

    def test_hll_sketch_estimate(self):
        c = classify_query("SELECT HLL_SKETCH_ESTIMATE(sketch_col) FROM events")
        assert c.verdict == VERDICT_INCOMPATIBLE


# ─────────────────────────────────────────────────────────────────────────────
# Risky-path queries — different name or limited Pinot support
# ─────────────────────────────────────────────────────────────────────────────


class TestRiskyFunctions:

    def test_time_floor_risky(self):
        c = classify_query(
            "SELECT TIME_FLOOR(__time, 'PT1H') AS h, COUNT(*) FROM events GROUP BY 1"
        )
        assert c.verdict == VERDICT_RISKY
        assert any(
            i.pattern == "TIME_FLOOR" and i.severity == SEV_RISKY
            for i in c.issues
        )

    def test_time_shift_risky(self):
        c = classify_query("SELECT TIME_SHIFT(__time, 'PT1H', -1) FROM events")
        assert c.verdict == VERDICT_RISKY
        assert any(i.pattern == "TIME_SHIFT" for i in c.issues)

    def test_timestampadd_risky(self):
        c = classify_query(
            "SELECT TIMESTAMPADD(MINUTE, 5, __time) FROM events"
        )
        assert c.verdict == VERDICT_RISKY
        assert any(i.pattern == "TIMESTAMPADD" for i in c.issues)


class TestRiskyStructures:

    def test_join_risky(self):
        c = classify_query(
            "SELECT a.x FROM t1 a JOIN t2 b ON a.id = b.id"
        )
        assert c.verdict == VERDICT_RISKY
        assert any(i.pattern == "JOIN" for i in c.issues)

    def test_window_function_risky(self):
        c = classify_query(
            "SELECT x, ROW_NUMBER() OVER (PARTITION BY y ORDER BY z) "
            "FROM events"
        )
        assert c.verdict == VERDICT_RISKY
        assert any(i.pattern == "WINDOW_FUNCTION" for i in c.issues)

    def test_subquery_risky(self):
        c = classify_query(
            "SELECT (SELECT MAX(x) FROM s) AS m FROM events"
        )
        assert c.verdict == VERDICT_RISKY
        assert any(i.pattern == "SUBQUERY" for i in c.issues)

    def test_json_path_risky(self):
        c = classify_query("SELECT data->>'name' FROM events")
        assert c.verdict == VERDICT_RISKY
        assert any(i.pattern == "JSON_PATH" for i in c.issues)


# ─────────────────────────────────────────────────────────────────────────────
# Mixed-severity queries — worst-severity wins
# ─────────────────────────────────────────────────────────────────────────────


class TestMultipleIssues:

    def test_incompatible_dominates_risky(self):
        # LOOKUP (incompatible) AND TIME_FLOOR (risky) → INCOMPATIBLE.
        c = classify_query(
            "SELECT LOOKUP(country, 'cc'), TIME_FLOOR(__time, 'PT1H') "
            "FROM events"
        )
        assert c.verdict == VERDICT_INCOMPATIBLE
        patterns = {i.pattern for i in c.issues}
        assert "LOOKUP" in patterns
        assert "TIME_FLOOR" in patterns

    def test_multiple_risky_stays_risky(self):
        c = classify_query(
            "SELECT TIME_FLOOR(__time, 'PT1H'), TIMESTAMPADD(MINUTE, 5, __time) "
            "FROM events"
        )
        assert c.verdict == VERDICT_RISKY
        patterns = {i.pattern for i in c.issues}
        assert "TIME_FLOOR" in patterns
        assert "TIMESTAMPADD" in patterns

    def test_duplicate_function_emits_duplicate_issue(self):
        # The classifier doesn't dedupe — that's the aggregator's job.
        c = classify_query(
            "SELECT LOOKUP(a, 'x'), LOOKUP(b, 'x') FROM events"
        )
        lookups = [i for i in c.issues if i.pattern == "LOOKUP"]
        assert len(lookups) == 2


# ─────────────────────────────────────────────────────────────────────────────
# Parse failures + edge cases
# ─────────────────────────────────────────────────────────────────────────────


class TestErrorPaths:

    def test_malformed_sql_classified_incompatible(self):
        c = classify_query("SELECT FROM WHERE")
        assert c.verdict == VERDICT_INCOMPATIBLE
        assert any(i.pattern == "PARSE_ERROR" for i in c.issues)

    def test_empty_string_classified_incompatible(self):
        c = classify_query("")
        assert c.verdict == VERDICT_INCOMPATIBLE
        # Either EMPTY_QUERY or PARSE_ERROR is acceptable; both signal
        # "nothing to migrate".
        assert any(
            i.pattern in {"EMPTY_QUERY", "PARSE_ERROR"} for i in c.issues
        )

    def test_query_id_propagates(self):
        c = classify_query("SELECT 1", query_id="dashboard_42.sql")
        assert c.query_id == "dashboard_42.sql"

    def test_classification_to_dict_has_expected_shape(self):
        c = classify_query("SELECT LOOKUP(c, 'x') FROM t", query_id="q1")
        d = c.to_dict()
        assert d["query_id"] == "q1"
        assert d["verdict"] == VERDICT_INCOMPATIBLE
        assert isinstance(d["issues"], list)
        assert d["issues"][0]["pattern"] == "LOOKUP"
        assert "pinot_equivalent" in d["issues"][0]
