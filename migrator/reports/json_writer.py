from __future__ import annotations

import json
from pathlib import Path

from migrator.core.models import CanonicalMigrationModel, RiskAnnotation, ValidationReport
from migrator.utils.io import ensure_dir, write_json


class ReportWriter:
    """Write migration reports as JSON files."""

    def write_migration_report(
        self,
        canonical: CanonicalMigrationModel,
        risks: list[RiskAnnotation],
        validation_report: ValidationReport,
        output_dir: str | Path,
    ) -> Path:
        """Write a combined migration-report.json."""
        output_dir = Path(output_dir)
        ensure_dir(output_dir)

        report: dict = {
            "datasource_name": canonical.datasource_name,
            "source_kind": canonical.source_kind,
            "classification": canonical.classification,
            "confidence_score": validation_report.confidence_score,
            "overall_status": validation_report.overall_status,
            "risks": [r.model_dump() for r in risks],
            "validation_checks": [c.model_dump() for c in validation_report.checks],
            "unsupported_features": [u.model_dump() for u in canonical.unsupported_features],
            "notes": canonical.notes,
        }
        path = output_dir / "migration-report.json"
        write_json(path, report)
        return path

    def write_risks(
        self,
        risks: list[RiskAnnotation],
        output_dir: str | Path,
    ) -> Path:
        """Write risks.json."""
        output_dir = Path(output_dir)
        ensure_dir(output_dir)
        data = {"risks": [r.model_dump() for r in risks]}
        path = output_dir / "risks.json"
        write_json(path, data)
        return path

    def write_warnings(
        self,
        warnings: list[str],
        output_dir: str | Path,
    ) -> Path:
        """Write warnings.json."""
        output_dir = Path(output_dir)
        ensure_dir(output_dir)
        data = {"warnings": warnings}
        path = output_dir / "warnings.json"
        write_json(path, data)
        return path
