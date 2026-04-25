from __future__ import annotations

from migrator.core.enums import RiskSeverity
from migrator.core.models import RiskAnnotation

_SEVERITY_PENALTIES: dict[str, float] = {
    RiskSeverity.BLOCKING.value: 0.30,
    RiskSeverity.HIGH.value: 0.15,
    RiskSeverity.MEDIUM.value: 0.05,
    RiskSeverity.LOW.value: 0.01,
    RiskSeverity.INFO.value: 0.00,
}


def compute_confidence_score(risks: list[RiskAnnotation]) -> float:
    """Compute a migration confidence score in [0.0, 1.0].

    Starts at 1.0 and deducts:
    - 0.30 per BLOCKING risk
    - 0.15 per HIGH risk
    - 0.05 per MEDIUM risk
    - 0.01 per LOW risk
    """
    score = 1.0
    for risk in risks:
        penalty = _SEVERITY_PENALTIES.get(risk.severity, 0.0)
        score -= penalty
    return max(0.0, min(1.0, score))
