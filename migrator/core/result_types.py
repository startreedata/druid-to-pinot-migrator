from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from migrator.core.models import CanonicalMigrationModel, RiskAnnotation, ValidationReport


@dataclass
class ParseResult:
    success: bool
    parsed_spec: Any | None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class NormalizeResult:
    success: bool
    canonical: CanonicalMigrationModel | None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class GenerateResult:
    success: bool
    output_dir: str
    files_written: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class AnalyzeResult:
    risks: list[RiskAnnotation] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class ValidateResult:
    report: ValidationReport
    success: bool
