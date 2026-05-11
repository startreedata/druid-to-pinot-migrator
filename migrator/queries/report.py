"""
Aggregate per-query classifications into a cluster-wide report.

Mirrors the cluster-report module's shape so operators get a familiar
artifact: ``summary.json`` (structured) + ``query-report.md`` (pretty).
The aggregator's job is to surface what's blocking *most* queries —
if 800 of 1000 dashboard queries hit the same ``LOOKUP()`` call,
fixing that one pattern unblocks the bulk of the migration.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from migrator.queries.classifier import (
    VERDICT_COMPATIBLE,
    VERDICT_INCOMPATIBLE,
    VERDICT_RISKY,
    QueryClassification,
)


@dataclass
class QueryReport:
    """Top-level query-compatibility report. Holds the per-query
    classifications and computes counts / top-pattern lists on
    demand — the inputs are small enough that recomputing is cheaper
    than keeping the aggregates in sync."""
    queries: list[QueryClassification] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.queries)

    @property
    def by_verdict(self) -> dict[str, int]:
        return Counter(q.verdict for q in self.queries)

    def top_patterns(self, n: int = 20) -> list[tuple[str, int]]:
        """The N most-frequently-occurring blocking / risky patterns
        across the whole input. Counts a pattern once per query so a
        query that uses ``LOOKUP`` four times still counts as 1 — what
        operators want is "how many queries are affected"."""
        c: Counter = Counter()
        for q in self.queries:
            seen: set[str] = set()
            for issue in q.issues:
                if issue.pattern in seen:
                    continue
                c[issue.pattern] += 1
                seen.add(issue.pattern)
        return c.most_common(n)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "by_verdict": dict(self.by_verdict),
            "top_patterns": [
                {"pattern": p, "queries_affected": n}
                for p, n in self.top_patterns()
            ],
            "queries": [q.to_dict() for q in self.queries],
        }


def render_markdown(report: QueryReport) -> str:
    """Pretty markdown rendering. Sections, top-down:
    1. Headline counts (one number per verdict).
    2. Top patterns table — what's blocking the most queries.
    3. Per-query detail — sorted INCOMPATIBLE → RISKY → COMPATIBLE
       so the worst offenders are at the top.
    """
    lines: list[str] = []
    lines.append("# Druid → Pinot Query Compatibility Report")
    lines.append("")
    lines.append(f"- **Total queries:** {report.total}")
    lines.append("")

    lines.append("## Verdict breakdown")
    lines.append("")
    lines.append("| Verdict | Count |")
    lines.append("|---|---|")
    by = report.by_verdict
    for v, emoji in (
        (VERDICT_COMPATIBLE, ":white_check_mark:"),
        (VERDICT_RISKY, ":warning:"),
        (VERDICT_INCOMPATIBLE, ":x:"),
    ):
        lines.append(f"| {emoji} {v.upper()} | {by.get(v, 0)} |")
    lines.append("")

    top = report.top_patterns(20)
    if top:
        lines.append("## Top compatibility patterns")
        lines.append("")
        lines.append(
            "Queries-affected counts each pattern once per query. "
            "Fix the most common patterns first — a single rewrite "
            "can unblock dozens of dashboards at once."
        )
        lines.append("")
        lines.append("| Pattern | Queries affected |")
        lines.append("|---|---|")
        for pattern, n in top:
            lines.append(f"| `{pattern}` | {n} |")
        lines.append("")

    lines.append("## Per-query detail")
    lines.append("")
    order = {VERDICT_INCOMPATIBLE: 0, VERDICT_RISKY: 1, VERDICT_COMPATIBLE: 2}
    for q in sorted(
        report.queries,
        key=lambda x: (order.get(x.verdict, 99), x.query_id),
    ):
        emoji = {
            VERDICT_COMPATIBLE: ":white_check_mark:",
            VERDICT_RISKY: ":warning:",
            VERDICT_INCOMPATIBLE: ":x:",
        }[q.verdict]
        lines.append(f"### {emoji} `{q.query_id}` — {q.verdict}")
        lines.append("")
        if not q.issues:
            lines.append("_No issues detected — should run on Pinot unchanged._")
            lines.append("")
            continue
        for issue in q.issues:
            lines.append(
                f"- **{issue.pattern}** ({issue.severity}): {issue.detail}"
            )
            if issue.pinot_equivalent:
                lines.append(f"  - Pinot equivalent: {issue.pinot_equivalent}")
        lines.append("")

    return "\n".join(lines)


def write_report(report: QueryReport, out_dir: Path) -> dict[str, Path]:
    """Write the report's two artifacts: ``summary.json`` (the full
    structured report) and ``query-report.md`` (pretty markdown).
    Returns the paths so the CLI can echo them."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}

    summary_path = out_dir / "summary.json"
    summary_path.write_text(
        json.dumps(report.to_dict(), indent=2, default=str) + "\n",
    )
    paths["summary"] = summary_path

    md_path = out_dir / "query-report.md"
    md_path.write_text(render_markdown(report) + "\n")
    paths["markdown"] = md_path

    return paths
