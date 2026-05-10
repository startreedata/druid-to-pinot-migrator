"""Unit tests for the cluster wave planner."""

from __future__ import annotations

import pytest

from migrator.cluster.inspector import (
    COMPAT_ERROR,
    COMPAT_GREEN,
    COMPAT_RED,
    COMPAT_YELLOW,
    ClusterReport,
    DatasourceReport,
)
from migrator.cluster.wave_planner import (
    WAVE_1,
    WAVE_2,
    WAVE_3,
    WAVE_ORDER,
    WAVE_QUARANTINE,
    WAVE_TRIAGE,
    WavePlan,
    plan_waves,
    render_wave_plan_markdown,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — build minimal DatasourceReport / ClusterReport without going
# through the inspector pipeline. The planner's contract is over the report
# data shape, so direct construction is the cleaner test surface.
# ─────────────────────────────────────────────────────────────────────────────


def _ds(
    name: str,
    *,
    compat: str,
    risks: list[dict] | None = None,
    unsupported: list[dict] | None = None,
) -> DatasourceReport:
    return DatasourceReport(
        datasource=name,
        compat=compat,
        risks=risks or [],
        unsupported_features=unsupported or [],
    )


def _report(*datasources: DatasourceReport) -> ClusterReport:
    r = ClusterReport(
        coordinator_url="http://coord:8081",
        overlord_url=None,
        started_at="2026-05-09T00:00:00Z",
    )
    r.datasources = list(datasources)
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Bucket assignment
# ─────────────────────────────────────────────────────────────────────────────


class TestWaveAssignment:

    def test_green_lands_in_wave_1(self):
        plan = plan_waves(_report(_ds("clean", compat=COMPAT_GREEN)))
        assert plan.get(WAVE_1).datasources == ["clean"]
        assert plan.get(WAVE_2).count == 0
        assert plan.get(WAVE_3).count == 0
        assert plan.get(WAVE_QUARANTINE).count == 0
        assert plan.get(WAVE_TRIAGE).count == 0

    def test_yellow_lands_in_wave_2(self):
        plan = plan_waves(_report(_ds(
            "transforms",
            compat=COMPAT_YELLOW,
            risks=[{"risk_id": "TRANSFORM_PORTABILITY_RISK", "severity": "medium"}],
        )))
        assert plan.get(WAVE_2).datasources == ["transforms"]
        assert plan.get(WAVE_1).count == 0

    def test_red_with_only_high_lands_in_wave_3(self):
        plan = plan_waves(_report(_ds(
            "rolled_up",
            compat=COMPAT_RED,
            risks=[{"risk_id": "ROLLUP_SEMANTIC_MISMATCH", "severity": "high"}],
        )))
        assert plan.get(WAVE_3).datasources == ["rolled_up"]
        assert plan.get(WAVE_QUARANTINE).count == 0

    def test_blocking_risk_overrides_compat_to_quarantine(self):
        plan = plan_waves(_report(_ds(
            "sketches",
            compat=COMPAT_RED,
            risks=[{"risk_id": "APPROX_AGGREGATOR_MISMATCH", "severity": "blocking"}],
        )))
        assert plan.get(WAVE_QUARANTINE).datasources == ["sketches"]
        assert plan.get(WAVE_3).count == 0

    def test_blocking_unsupported_feature_also_quarantines(self):
        # Defence-in-depth: BLOCKING from the unsupported_features
        # side path (not the risks list) still routes to quarantine.
        plan = plan_waves(_report(_ds(
            "exotic",
            compat=COMPAT_YELLOW,  # inspector might mis-classify
            unsupported=[{"feature": "weird_thing", "severity": "blocking"}],
        )))
        assert plan.get(WAVE_QUARANTINE).datasources == ["exotic"]

    def test_error_lands_in_triage(self):
        plan = plan_waves(_report(_ds(
            "broken", compat=COMPAT_ERROR,
        )))
        assert plan.get(WAVE_TRIAGE).datasources == ["broken"]

    def test_unknown_compat_falls_back_to_triage(self):
        # Belt-and-braces: an unknown compat string mustn't crash the
        # planner; route to triage so the operator notices.
        plan = plan_waves(_report(_ds("strange", compat="purple")))
        assert plan.get(WAVE_TRIAGE).datasources == ["strange"]


# ─────────────────────────────────────────────────────────────────────────────
# Mixed clusters & ordering
# ─────────────────────────────────────────────────────────────────────────────


class TestPlanShape:

    def test_empty_cluster_has_all_buckets_empty(self):
        plan = plan_waves(_report())
        assert [b.wave for b in plan.buckets] == list(WAVE_ORDER)
        assert all(b.count == 0 for b in plan.buckets)

    def test_buckets_are_alphabetically_sorted_within_wave(self):
        plan = plan_waves(_report(
            _ds("zebra", compat=COMPAT_GREEN),
            _ds("apple", compat=COMPAT_GREEN),
            _ds("mango", compat=COMPAT_GREEN),
        ))
        assert plan.get(WAVE_1).datasources == ["apple", "mango", "zebra"]

    def test_full_mix(self):
        plan = plan_waves(_report(
            _ds("a_clean", compat=COMPAT_GREEN),
            _ds("b_warn", compat=COMPAT_YELLOW,
                risks=[{"risk_id": "X", "severity": "low"}]),
            _ds("c_high", compat=COMPAT_RED,
                risks=[{"risk_id": "ROLLUP_SEMANTIC_MISMATCH", "severity": "high"}]),
            _ds("d_block", compat=COMPAT_RED,
                risks=[{"risk_id": "APPROX_AGGREGATOR_MISMATCH", "severity": "blocking"}]),
            _ds("e_err", compat=COMPAT_ERROR),
        ))
        assert plan.get(WAVE_1).datasources == ["a_clean"]
        assert plan.get(WAVE_2).datasources == ["b_warn"]
        assert plan.get(WAVE_3).datasources == ["c_high"]
        assert plan.get(WAVE_QUARANTINE).datasources == ["d_block"]
        assert plan.get(WAVE_TRIAGE).datasources == ["e_err"]

    def test_get_unknown_wave_raises(self):
        plan = plan_waves(_report())
        with pytest.raises(KeyError):
            plan.get("does_not_exist")

    def test_to_dict_round_trip_shape(self):
        plan = plan_waves(_report(
            _ds("only", compat=COMPAT_GREEN),
        ))
        d = plan.to_dict()
        assert "buckets" in d
        assert len(d["buckets"]) == len(WAVE_ORDER)
        wave1 = next(b for b in d["buckets"] if b["wave"] == WAVE_1)
        assert wave1["datasources"] == ["only"]
        assert wave1["count"] == 1
        assert "rationale" in wave1
        assert "title" in wave1


# ─────────────────────────────────────────────────────────────────────────────
# Markdown rendering
# ─────────────────────────────────────────────────────────────────────────────


class TestRenderMarkdown:

    def test_includes_section_header(self):
        md = render_wave_plan_markdown(plan_waves(_report()))
        assert "## Proposed migration waves" in md

    def test_lists_every_wave_even_when_empty(self):
        md = render_wave_plan_markdown(plan_waves(_report()))
        assert "Wave 1 — Quick wins" in md
        assert "Wave 2 — Review-and-go" in md
        assert "Wave 3 — Engineering needed" in md
        assert "Quarantine" in md
        assert "Triage" in md
        # Empty buckets show a placeholder so operators don't think
        # the bucket was just dropped from the output.
        assert "_No datasources in this wave._" in md

    def test_renders_each_datasource_as_code(self):
        md = render_wave_plan_markdown(plan_waves(_report(
            _ds("payments_v2", compat=COMPAT_GREEN),
        )))
        assert "`payments_v2`" in md

    def test_summary_table_has_count_per_wave(self):
        md = render_wave_plan_markdown(plan_waves(_report(
            _ds("a", compat=COMPAT_GREEN),
            _ds("b", compat=COMPAT_GREEN),
            _ds("c", compat=COMPAT_YELLOW,
                risks=[{"risk_id": "X", "severity": "low"}]),
        )))
        # Wave 1 should show count 2, Wave 2 should show 1
        # (rendered in the | Wave | Count | row).
        assert "| 2 |" in md
        assert "| 1 |" in md
