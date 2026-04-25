from __future__ import annotations

from migrator.core.models import CanonicalMigrationModel, ValidationCheck, ValidationReport
from migrator.core.enums import ValidationStatus
from migrator.core.models import RiskAnnotation
from migrator.validation.artifact_checks import ArtifactValidator
from migrator.validation.scoring import compute_confidence_score
from migrator.validation.static_checks import StaticSpecValidator


def build_validation_report(
    canonical: CanonicalMigrationModel,
    risks: list[RiskAnnotation],
    schema: dict | None = None,
    table: dict | None = None,
) -> ValidationReport:
    """Run all validators and produce a ValidationReport."""
    all_checks: list[ValidationCheck] = []

    # Static spec checks
    static_validator = StaticSpecValidator()
    all_checks.extend(static_validator.validate(canonical))

    # Artifact checks (if artifacts provided)
    if schema is not None and table is not None:
        artifact_validator = ArtifactValidator()
        all_checks.extend(artifact_validator.validate({"schema": schema, "table": table}))

    # Compute overall status
    statuses = {c.status for c in all_checks}
    if ValidationStatus.FAIL.value in statuses:
        overall_status = ValidationStatus.FAIL.value
    elif ValidationStatus.WARN.value in statuses:
        overall_status = ValidationStatus.WARN.value
    else:
        overall_status = ValidationStatus.PASS.value

    confidence_score = compute_confidence_score(risks)

    return ValidationReport(
        datasource_name=canonical.datasource_name,
        checks=all_checks,
        confidence_score=confidence_score,
        overall_status=overall_status,
    )
