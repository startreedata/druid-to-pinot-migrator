from __future__ import annotations

from migrator.core.models import RiskAnnotation


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
