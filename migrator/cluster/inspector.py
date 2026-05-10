"""
Cluster-wide compatibility inspection.

Walks every datasource on a running Druid cluster, runs the spec
extractor + parser + normalizer + risk analyzer on each, and
aggregates the per-datasource verdicts into a top-level migration-
readiness report.

Key design decisions:

  - **Resilient by default.** A single datasource that fails to
    extract (auth issue, missing segments, exotic supervisor type)
    must NOT abort the whole report — it gets logged with status
    ``error`` and the run continues. Operators usually want the
    "what's the lay of the land" view; per-DS gotchas are
    investigated afterwards.
  - **Three-bucket compat status** based on risk severity:
      GREEN  — no risks at any severity, no unsupported features.
      YELLOW — only LOW/MEDIUM risks. Safe to migrate with manual
               review; operator can usually leave a TODO.
      RED    — at least one HIGH/BLOCKING risk OR unsupported
               feature. Should not be migrated as-is.
  - **No state mutation.** Read-only against the Druid cluster.
    Doesn't write anywhere except the operator's --out directory.
"""

from __future__ import annotations

import datetime
import json
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from migrator.core.models import CanonicalMigrationModel
from migrator.druid.classifiers import classify_datasource
from migrator.druid.coordinator_client import (
    DruidCoordinatorClient,
    DruidCoordinatorError,
)
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.overlord_client import DruidOverlordClient
from migrator.druid.parser import DruidSpecParser
from migrator.druid.spec_extractor import extract_spec
from migrator.risks.analyzer import RiskAnalyzer


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────


COMPAT_GREEN = "green"
COMPAT_YELLOW = "yellow"
COMPAT_RED = "red"
COMPAT_ERROR = "error"   # extraction failed; can't classify


@dataclass
class DatasourceReport:
    """One datasource's compatibility verdict."""
    datasource: str
    compat: str
    source_kind: str = "unknown"           # batch / stream / unknown
    classification: str = "unknown"        # raw_event / rolled_up / etc.
    supervisor_id: str | None = None       # set when source_kind=stream
    extraction_warnings: list[str] = field(default_factory=list)
    risks: list[dict] = field(default_factory=list)         # severity / risk_id / description
    unsupported_features: list[dict] = field(default_factory=list)
    error: str | None = None               # set when compat=error
    elapsed_s: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "datasource": self.datasource,
            "compat": self.compat,
            "source_kind": self.source_kind,
            "classification": self.classification,
            "supervisor_id": self.supervisor_id,
            "extraction_warnings": list(self.extraction_warnings),
            "risks": list(self.risks),
            "unsupported_features": list(self.unsupported_features),
            "error": self.error,
            "elapsed_s": round(self.elapsed_s, 3),
        }


@dataclass
class ClusterReport:
    """Top-level cluster-wide compatibility report."""
    coordinator_url: str
    overlord_url: str | None
    started_at: str
    finished_at: str | None = None
    datasources: list[DatasourceReport] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.datasources)

    @property
    def by_status(self) -> dict[str, int]:
        return Counter(d.compat for d in self.datasources)

    @property
    def by_classification(self) -> dict[str, int]:
        # Skip ``error`` rows from this breakdown — they're not
        # meaningful to classify.
        return Counter(
            d.classification for d in self.datasources
            if d.compat != COMPAT_ERROR
        )

    def top_blocking_issues(self, n: int = 10) -> list[tuple[str, int]]:
        """The N most-frequently-occurring blocking issues across the
        whole cluster. Useful for prioritising fixes — if 27 of 50
        datasources hit ``ROLLUP_SEMANTIC_MISMATCH``, that's the
        first thing to investigate."""
        c: Counter = Counter()
        for d in self.datasources:
            for r in d.risks:
                # Only count HIGH / BLOCKING — that's what's
                # actually blocking migration.
                if r.get("severity", "").upper() in {"HIGH", "BLOCKING"}:
                    c[r.get("risk_id", "unknown")] += 1
            for u in d.unsupported_features:
                if u.get("severity", "").upper() in {"HIGH", "BLOCKING"}:
                    c[u.get("feature", "unknown")] += 1
        return c.most_common(n)

    def to_dict(self) -> dict[str, Any]:
        # Local import: wave_planner imports from this module, so the
        # top-level import would be circular.
        from migrator.cluster.wave_planner import plan_waves
        return {
            "coordinator_url": self.coordinator_url,
            "overlord_url": self.overlord_url,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "total": self.total,
            "by_status": dict(self.by_status),
            "by_classification": dict(self.by_classification),
            "top_blocking_issues": [
                {"issue": k, "count": v} for k, v in self.top_blocking_issues()
            ],
            "wave_plan": plan_waves(self).to_dict(),
            "datasources": [d.to_dict() for d in self.datasources],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Inspector
# ─────────────────────────────────────────────────────────────────────────────


def _classify_compat(
    risks: list, unsupported: list,
) -> str:
    """Reduce risk + unsupported lists into one of GREEN / YELLOW / RED.

    The bar is deliberately conservative: a single HIGH or BLOCKING
    item flips the whole datasource to RED, even if everything else
    is clean. Operators reading this report want the worst-case
    indicator; nuance lives in the per-datasource detail.
    """
    has_blocker = any(
        getattr(r, "severity", "").upper() in {"HIGH", "BLOCKING"}
        for r in risks
    ) or any(
        getattr(u, "severity", "").upper() in {"HIGH", "BLOCKING"}
        for u in unsupported
    )
    if has_blocker:
        return COMPAT_RED
    has_minor = bool(risks) or bool(unsupported)
    return COMPAT_YELLOW if has_minor else COMPAT_GREEN


def inspect_one(
    datasource: str,
    *,
    coordinator: DruidCoordinatorClient,
    overlord: DruidOverlordClient | None,
) -> DatasourceReport:
    """Inspect a single datasource. Catches every failure mode so
    callers can drive the function in a loop without per-DS
    try/except boilerplate."""
    t0 = time.time()
    try:
        extracted = extract_spec(
            datasource, coordinator=coordinator, overlord=overlord,
        )
    except (DruidCoordinatorError, ValueError) as exc:
        return DatasourceReport(
            datasource=datasource,
            compat=COMPAT_ERROR,
            error=f"extract failed: {exc}",
            elapsed_s=time.time() - t0,
        )
    except Exception as exc:  # noqa: BLE001 — never abort the loop
        return DatasourceReport(
            datasource=datasource,
            compat=COMPAT_ERROR,
            error=f"unexpected: {type(exc).__name__}: {exc}",
            elapsed_s=time.time() - t0,
        )

    parsed = DruidSpecParser().parse(extracted.spec)
    if not parsed.success or parsed.parsed_spec is None:
        return DatasourceReport(
            datasource=datasource,
            compat=COMPAT_ERROR,
            source_kind=extracted.source_kind,
            supervisor_id=extracted.supervisor_id,
            extraction_warnings=extracted.warnings,
            error=f"parse failed: {parsed.errors}",
            elapsed_s=time.time() - t0,
        )

    norm = DruidNormalizer().normalize(parsed.parsed_spec)
    if not norm.success or norm.canonical is None:
        return DatasourceReport(
            datasource=datasource,
            compat=COMPAT_ERROR,
            source_kind=extracted.source_kind,
            supervisor_id=extracted.supervisor_id,
            extraction_warnings=extracted.warnings,
            error=f"normalize failed: {norm.errors}",
            elapsed_s=time.time() - t0,
        )

    canonical: CanonicalMigrationModel = norm.canonical
    canonical.classification = classify_datasource(canonical).value

    analyze = RiskAnalyzer().analyze(canonical)
    compat = _classify_compat(analyze.risks, canonical.unsupported_features)

    return DatasourceReport(
        datasource=datasource,
        compat=compat,
        source_kind=canonical.source_kind,
        classification=canonical.classification,
        supervisor_id=extracted.supervisor_id,
        extraction_warnings=extracted.warnings,
        risks=[
            {
                "risk_id": r.risk_id,
                "severity": r.severity,
                "confidence": r.confidence,
                "description": r.description,
            }
            for r in analyze.risks
        ],
        unsupported_features=[
            {
                "feature": u.feature,
                "reason": u.reason,
                "severity": u.severity,
            }
            for u in canonical.unsupported_features
        ],
        elapsed_s=time.time() - t0,
    )


def inspect_cluster(
    *,
    coordinator: DruidCoordinatorClient,
    overlord: DruidOverlordClient | None = None,
    coordinator_url: str = "",
    overlord_url: str | None = None,
    datasources: list[str] | None = None,
    progress_callback=None,
) -> ClusterReport:
    """Walk every datasource (or the explicit ``datasources`` filter)
    and produce a ``ClusterReport``.

    ``progress_callback(idx, total, datasource_name)`` fires after
    each datasource finishes — lets the CLI render a progress line
    on what's typically a multi-second-per-DS operation. Optional;
    None means no callback.
    """
    started = _utc_iso_now()
    if datasources is None:
        try:
            datasources = sorted(coordinator.list_datasources())
        except DruidCoordinatorError as exc:
            # Couldn't even list. Surface a single empty report with
            # the failure cause; the caller's exit code drives off
            # this.
            report = ClusterReport(
                coordinator_url=coordinator_url,
                overlord_url=overlord_url,
                started_at=started,
                finished_at=_utc_iso_now(),
            )
            report.datasources.append(DatasourceReport(
                datasource="<list_datasources>",
                compat=COMPAT_ERROR,
                error=f"coordinator list failed: {exc}",
            ))
            return report

    report = ClusterReport(
        coordinator_url=coordinator_url,
        overlord_url=overlord_url,
        started_at=started,
    )
    for i, ds in enumerate(datasources, start=1):
        ds_report = inspect_one(
            ds, coordinator=coordinator, overlord=overlord,
        )
        report.datasources.append(ds_report)
        if progress_callback is not None:
            try:
                progress_callback(i, len(datasources), ds)
            except Exception:  # noqa: BLE001
                # Same contract as backfill_runner: a misbehaving
                # callback must never abort the run.
                pass
    report.finished_at = _utc_iso_now()
    return report


# ─────────────────────────────────────────────────────────────────────────────
# Markdown renderer
# ─────────────────────────────────────────────────────────────────────────────


def render_markdown(report: ClusterReport) -> str:
    """Pretty markdown summary suitable for an internal
    migration-readiness review document."""
    # Local import: wave_planner imports from this module.
    from migrator.cluster.wave_planner import plan_waves, render_wave_plan_markdown
    lines: list[str] = []
    lines.append("# Druid → Pinot Cluster Compatibility Report")
    lines.append("")
    lines.append(f"- **Coordinator:** `{report.coordinator_url}`")
    if report.overlord_url:
        lines.append(f"- **Overlord:** `{report.overlord_url}`")
    lines.append(f"- **Generated:** {report.started_at}")
    lines.append(f"- **Total datasources:** {report.total}")
    lines.append("")

    lines.append("## Compatibility breakdown")
    lines.append("")
    lines.append("| Status | Count |")
    lines.append("|---|---|")
    by_status = report.by_status
    for status in (COMPAT_GREEN, COMPAT_YELLOW, COMPAT_RED, COMPAT_ERROR):
        emoji = {
            COMPAT_GREEN: ":white_check_mark:",
            COMPAT_YELLOW: ":warning:",
            COMPAT_RED: ":x:",
            COMPAT_ERROR: ":grey_question:",
        }[status]
        lines.append(f"| {emoji} {status.upper()} | {by_status.get(status, 0)} |")
    lines.append("")

    if report.by_classification:
        lines.append("## Classification breakdown")
        lines.append("")
        lines.append("| Classification | Count |")
        lines.append("|---|---|")
        for cls, n in sorted(
            report.by_classification.items(), key=lambda x: -x[1],
        ):
            lines.append(f"| `{cls}` | {n} |")
        lines.append("")

    # Wave plan goes ABOVE the top-blocking-issues table because the
    # waves are the actionable artifact (what to migrate next) — the
    # top-issues table is supporting context for *why* certain waves
    # are heavy. Operators have told us they read the report from the
    # top.
    if report.datasources:
        lines.append(render_wave_plan_markdown(plan_waves(report)))

    top = report.top_blocking_issues(20)
    if top:
        lines.append("## Top blocking issues across the cluster")
        lines.append("")
        lines.append(
            "These are the issues most likely to cost migration time. "
            "Fixing the top one or two may unblock a large chunk of the "
            "cluster at once."
        )
        lines.append("")
        lines.append("| Issue | Datasources affected |")
        lines.append("|---|---|")
        for issue, count in top:
            lines.append(f"| `{issue}` | {count} |")
        lines.append("")

    lines.append("## Per-datasource detail")
    lines.append("")
    lines.append(
        "| Status | Datasource | Source kind | Classification | "
        "Top issue |"
    )
    lines.append("|---|---|---|---|---|")
    # Sort: RED first (most attention), then YELLOW, then GREEN, then ERROR.
    order = {COMPAT_RED: 0, COMPAT_YELLOW: 1, COMPAT_ERROR: 2, COMPAT_GREEN: 3}
    for d in sorted(
        report.datasources,
        key=lambda x: (order.get(x.compat, 99), x.datasource),
    ):
        emoji = {
            COMPAT_GREEN: ":white_check_mark:",
            COMPAT_YELLOW: ":warning:",
            COMPAT_RED: ":x:",
            COMPAT_ERROR: ":grey_question:",
        }[d.compat]
        top_issue = "—"
        if d.error:
            top_issue = f"_{d.error[:80]}_"
        elif d.risks:
            top_issue = (
                f"`{d.risks[0]['risk_id']}` ({d.risks[0]['severity']})"
            )
        elif d.unsupported_features:
            top_issue = (
                f"`{d.unsupported_features[0]['feature']}` "
                f"({d.unsupported_features[0]['severity']})"
            )
        lines.append(
            f"| {emoji} {d.compat} | `{d.datasource}` | "
            f"{d.source_kind} | {d.classification} | {top_issue} |"
        )
    lines.append("")

    return "\n".join(lines)


def _utc_iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Disk writer
# ─────────────────────────────────────────────────────────────────────────────


def write_report(report: ClusterReport, out_dir: Path) -> dict[str, Path]:
    """Write the report's three artifacts: ``summary.json`` (the
    full structured report), ``cluster-report.md`` (pretty
    markdown), and a ``datasources/<name>.json`` per datasource for
    operators who want to grep through the detail.

    Returns the paths so callers can list them or surface them to the
    operator."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ds_dir = out_dir / "datasources"
    ds_dir.mkdir(exist_ok=True)

    paths: dict[str, Path] = {}

    summary_path = out_dir / "summary.json"
    summary_path.write_text(
        json.dumps(report.to_dict(), indent=2, default=str) + "\n",
    )
    paths["summary"] = summary_path

    md_path = out_dir / "cluster-report.md"
    md_path.write_text(render_markdown(report) + "\n")
    paths["markdown"] = md_path

    for d in report.datasources:
        # Sanitize name for filesystem safety — Druid datasource
        # names can contain anything; we keep alphanumerics and a
        # narrow whitelist.
        safe = "".join(
            c if c.isalnum() or c in "._-" else "_" for c in d.datasource
        )[:120]
        ds_path = ds_dir / f"{safe}.json"
        ds_path.write_text(
            json.dumps(d.to_dict(), indent=2, default=str) + "\n",
        )

    return paths
