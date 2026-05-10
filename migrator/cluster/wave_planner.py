"""
Cluster-wide migration wave planner.

Operators running a 50- or 500-datasource Druid cluster don't migrate
everything in one pass. They want a proposed order: which datasources
should go first (lowest risk, highest confidence), which need
engineering work, which can't be migrated as-is.

This module turns a ``ClusterReport`` into a five-bucket plan:

  - **Wave 1 — Quick wins.** ``compat=GREEN`` datasources. No risks
    detected, no unsupported features. Pure copy-paste deploys. Run
    these first to build operator confidence in the tooling and free
    Druid resources before tackling the harder cases.
  - **Wave 2 — Review-and-go.** ``compat=YELLOW`` datasources. Only
    LOW / MEDIUM / INFO risks (transforms, custom timestamps,
    multi-value ambiguity, etc.). Operators read the per-DS warnings,
    fix anything genuinely wrong, then deploy. Most of the cluster
    typically lands here.
  - **Wave 3 — Engineering needed.** ``compat=RED`` datasources where
    every risk is HIGH severity but **none is BLOCKING**. The path
    forward is well-known per risk type — e.g. ROLLUP_SEMANTIC_MISMATCH
    means rewrite COUNT(*) queries; BATCH_AGGREGATION_NOT_REPLAYED
    means add a star-tree; FLATTEN_SPEC_NOT_PORTABLE means upstream
    JSON flattening. Each datasource takes hours to days, not weeks.
  - **Quarantine — Manual redesign.** Any datasource carrying a
    BLOCKING risk (today: APPROX_AGGREGATOR_MISMATCH — Druid sketch
    aggregators that don't have a wire-compatible Pinot equivalent).
    These cannot be migrated as-is; the schema or the upstream
    pipeline has to change. Doesn't fit a wave; needs a redesign
    project of its own.
  - **Triage — Failed to classify.** ``compat=ERROR`` datasources.
    The inspector couldn't extract the spec (auth issues, missing
    segments, exotic supervisor types). These need manual
    investigation before they can be assigned to any wave.

The buckets are *suggestions*, not gates. Operators routinely override
— promoting one Wave-3 datasource ahead of Wave-2 because it's
business-critical, or punting a Wave-1 datasource because nobody owns
it anymore. The planner's job is to give a starting order, not to
dictate one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from migrator.cluster.inspector import (
    COMPAT_ERROR,
    COMPAT_GREEN,
    COMPAT_RED,
    COMPAT_YELLOW,
    ClusterReport,
    DatasourceReport,
)


# ─────────────────────────────────────────────────────────────────────────────
# Wave identifiers
# ─────────────────────────────────────────────────────────────────────────────


WAVE_1 = "wave_1"           # GREEN — quick wins
WAVE_2 = "wave_2"           # YELLOW — review-and-go
WAVE_3 = "wave_3"           # RED with addressable HIGH risks — engineering
WAVE_QUARANTINE = "quarantine"  # BLOCKING risks — needs redesign
WAVE_TRIAGE = "triage"      # ERROR — couldn't classify

WAVE_ORDER = (WAVE_1, WAVE_2, WAVE_3, WAVE_QUARANTINE, WAVE_TRIAGE)

WAVE_TITLES: dict[str, str] = {
    WAVE_1: "Wave 1 — Quick wins",
    WAVE_2: "Wave 2 — Review-and-go",
    WAVE_3: "Wave 3 — Engineering needed",
    WAVE_QUARANTINE: "Quarantine — Manual redesign required",
    WAVE_TRIAGE: "Triage — Inspector failed",
}

WAVE_RATIONALES: dict[str, str] = {
    WAVE_1: (
        "No risks detected; spec maps cleanly to Pinot. Deploy these "
        "first to build operator confidence and free Druid resources."
    ),
    WAVE_2: (
        "Only LOW / MEDIUM / INFO risks. Read the per-datasource "
        "warnings (transform portability, custom timestamps, multi-"
        "value ambiguity, etc.), fix what's genuinely broken, then "
        "deploy."
    ),
    WAVE_3: (
        "At least one HIGH-severity risk that has a known engineering "
        "remediation path (rollup query rewrite, star-tree config, "
        "upstream JSON flattening, etc.). Plan a few hours to a few "
        "days per datasource."
    ),
    WAVE_QUARANTINE: (
        "BLOCKING risk present. Druid sketch aggregators "
        "(thetaSketch / HLLSketch / hyperUnique) are stored as "
        "wire-incompatible BYTES — Pinot cannot consume them as-is. "
        "These datasources need a redesign project of their own: "
        "re-ingest raw events into Pinot and rebuild sketches with "
        "DISTINCTCOUNTHLL / DISTINCTCOUNTTHETASKETCH."
    ),
    WAVE_TRIAGE: (
        "Inspector failed to extract or normalize the spec. Common "
        "causes: auth issue against the coordinator, missing "
        "segments, supervisor type the parser doesn't yet handle. "
        "Investigate per-datasource before assigning to a wave."
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class WaveBucket:
    """One wave's contents — the datasources, plus a count for the
    summary header. Datasources land in stable alphabetical order so
    the report is reproducible run-to-run."""
    wave: str
    title: str
    rationale: str
    datasources: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.datasources)

    def to_dict(self) -> dict[str, Any]:
        return {
            "wave": self.wave,
            "title": self.title,
            "rationale": self.rationale,
            "count": self.count,
            "datasources": list(self.datasources),
        }


@dataclass
class WavePlan:
    """The full migration-order proposal. Always contains all five
    buckets, even if a bucket is empty — operators reading the report
    benefit from seeing "Quarantine: 0" explicitly rather than having
    to infer absence."""
    buckets: list[WaveBucket]

    def get(self, wave: str) -> WaveBucket:
        for b in self.buckets:
            if b.wave == wave:
                return b
        raise KeyError(wave)

    def to_dict(self) -> dict[str, Any]:
        return {"buckets": [b.to_dict() for b in self.buckets]}


# ─────────────────────────────────────────────────────────────────────────────
# Planner
# ─────────────────────────────────────────────────────────────────────────────


def _has_blocking_risk(d: DatasourceReport) -> bool:
    """A datasource is quarantined the moment any single risk OR
    unsupported feature is BLOCKING. This is the strictest check in
    the planner — BLOCKING means "the migration mathematically cannot
    work as-is", not "the operator should think harder"."""
    for r in d.risks:
        if str(r.get("severity", "")).upper() == "BLOCKING":
            return True
    for u in d.unsupported_features:
        if str(u.get("severity", "")).upper() == "BLOCKING":
            return True
    return False


def _assign_wave(d: DatasourceReport) -> str:
    """Map one DatasourceReport to its wave bucket.

    Order matters: BLOCKING is checked before compat-status because a
    YELLOW report with a single BLOCKING unsupported-feature must
    still go to quarantine. (In practice the inspector classifies
    BLOCKING as RED, so this is defence-in-depth — the planner stays
    correct even if the inspector's bucketing changes.)
    """
    if d.compat == COMPAT_ERROR:
        return WAVE_TRIAGE
    if _has_blocking_risk(d):
        return WAVE_QUARANTINE
    if d.compat == COMPAT_GREEN:
        return WAVE_1
    if d.compat == COMPAT_YELLOW:
        return WAVE_2
    if d.compat == COMPAT_RED:
        return WAVE_3
    # Unknown compat string. Send to triage rather than crash — the
    # report is still useful with a single mis-bucketed row.
    return WAVE_TRIAGE


def plan_waves(report: ClusterReport) -> WavePlan:
    """Slice a ``ClusterReport`` into the five-wave migration plan.

    Datasources within a wave are returned in alphabetical order for
    reproducibility — the same input cluster always produces the same
    plan, which matters for reviewing the plan against a saved copy
    from a prior run.
    """
    buckets = {
        wave: WaveBucket(
            wave=wave,
            title=WAVE_TITLES[wave],
            rationale=WAVE_RATIONALES[wave],
        )
        for wave in WAVE_ORDER
    }
    for d in report.datasources:
        buckets[_assign_wave(d)].datasources.append(d.datasource)
    for b in buckets.values():
        b.datasources.sort()
    return WavePlan(buckets=[buckets[w] for w in WAVE_ORDER])


# ─────────────────────────────────────────────────────────────────────────────
# Markdown renderer
# ─────────────────────────────────────────────────────────────────────────────


def render_wave_plan_markdown(plan: WavePlan) -> str:
    """Pretty markdown rendering for the cluster-report. Buckets in
    fixed wave order so the operator reads top-to-bottom in
    migration-priority order."""
    lines: list[str] = []
    lines.append("## Proposed migration waves")
    lines.append("")
    lines.append(
        "Suggested order for migrating the cluster. The planner "
        "groups datasources by risk profile so operators can run "
        "low-risk waves first to build confidence before tackling "
        "datasources that need real engineering work."
    )
    lines.append("")
    lines.append("| Wave | Count | Description |")
    lines.append("|---|---|---|")
    for b in plan.buckets:
        lines.append(f"| **{b.title}** | {b.count} | {b.rationale} |")
    lines.append("")

    for b in plan.buckets:
        lines.append(f"### {b.title}")
        lines.append("")
        if not b.datasources:
            lines.append("_No datasources in this wave._")
            lines.append("")
            continue
        for ds in b.datasources:
            lines.append(f"- `{ds}`")
        lines.append("")
    return "\n".join(lines)
