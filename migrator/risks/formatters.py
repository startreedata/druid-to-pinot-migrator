from __future__ import annotations

from migrator.core.models import RiskAnnotation


def format_risk_table_text(risks: list[RiskAnnotation]) -> str:
    """Return a plain-text table of risks."""
    if not risks:
        return "No risks identified.\n"
    lines = [
        f"{'Risk ID':<35} {'Severity':<10} {'Confidence':<10}",
        "-" * 60,
    ]
    for r in risks:
        lines.append(f"{r.risk_id:<35} {r.severity:<10} {r.confidence:<10}")
    return "\n".join(lines) + "\n"


def format_risk_markdown(risks: list[RiskAnnotation]) -> str:
    """Return a Markdown-formatted risk list."""
    if not risks:
        return "_No risks identified._\n"
    lines: list[str] = []
    for r in risks:
        lines.append(f"### {r.risk_id}")
        lines.append(f"- **Severity**: {r.severity}")
        lines.append(f"- **Confidence**: {r.confidence}")
        lines.append(f"- **Description**: {r.description}")
        if r.evidence:
            lines.append(f"- **Evidence**: {'; '.join(r.evidence)}")
        if r.remediation:
            lines.append(f"- **Remediation**: {r.remediation}")
        lines.append("")
    return "\n".join(lines)
