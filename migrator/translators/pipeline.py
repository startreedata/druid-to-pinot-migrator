from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from migrator.core.errors import ParseError
from migrator.core.models import CanonicalMigrationModel, UpsertConfig
from migrator.core.result_types import (
    AnalyzeResult,
    GenerateResult,
    NormalizeResult,
    ValidateResult,
)
from migrator.druid.classifiers import classify_datasource
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.pinot.ingestion_generator import PinotIngestionGenerator
from migrator.pinot.schema_generator import PinotSchemaGenerator
from migrator.pinot.table_generator import PinotTableGenerator
from migrator.reports.json_writer import ReportWriter
from migrator.reports.markdown_writer import MarkdownReportWriter
from migrator.risks.analyzer import RiskAnalyzer
from migrator.utils.io import ensure_dir, read_json_or_yaml, write_json
from migrator.validation.reconciliation import build_validation_report
from migrator.validation.static_checks import StaticSpecValidator


def inspect_spec(path: str) -> dict:
    """Parse and summarise a Druid spec without full normalisation.

    Returns a dict with: datasource_name, source_kind, classification,
    risk_count (0 at this stage), warnings.
    """
    raw = read_json_or_yaml(path)
    parser = DruidSpecParser()
    parse_result = parser.parse(raw)

    if not parse_result.success or parse_result.parsed_spec is None:
        return {
            "datasource_name": "",
            "source_kind": "unknown",
            "classification": "unknown",
            "risk_count": 0,
            "warnings": parse_result.errors + parse_result.warnings,
            "error": "Parse failed",
        }

    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)

    if not norm_result.success or norm_result.canonical is None:
        return {
            "datasource_name": parse_result.parsed_spec.datasource_name,
            "source_kind": "unknown",
            "classification": "unknown",
            "risk_count": 0,
            "warnings": norm_result.errors + norm_result.warnings,
            "error": "Normalization failed",
        }

    canonical = norm_result.canonical
    classification = classify_datasource(canonical)
    canonical.classification = classification.value

    analyzer = RiskAnalyzer()
    analyze_result = analyzer.analyze(canonical)

    return {
        "datasource_name": canonical.datasource_name,
        "source_kind": canonical.source_kind,
        "classification": canonical.classification,
        "risk_count": len(analyze_result.risks),
        "warnings": norm_result.warnings + analyze_result.warnings,
        "dimensions": len(canonical.dimensions),
        "metrics": len(canonical.metrics),
        "transforms": len(canonical.transforms),
        "rollup": canonical.granularity.rollup,
    }


def normalize_spec(path: str) -> NormalizeResult:
    """Full parse + normalize a Druid spec file."""
    raw = read_json_or_yaml(path)
    parser = DruidSpecParser()
    parse_result = parser.parse(raw)

    if not parse_result.success or parse_result.parsed_spec is None:
        return NormalizeResult(
            success=False,
            canonical=None,
            errors=parse_result.errors,
            warnings=parse_result.warnings,
        )

    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)

    if norm_result.success and norm_result.canonical is not None:
        classification = classify_datasource(norm_result.canonical)
        norm_result.canonical.classification = classification.value

    return norm_result


def generate_bundle(
    path: str,
    out_dir: str,
    dry_run: bool = False,
    upsert_config: "UpsertConfig | None" = None,
) -> GenerateResult:
    """Full pipeline: parse -> normalize -> classify -> generate -> risk-analyze -> validate -> report.

    ``upsert_config`` (optional) is operator-supplied — Druid has no
    row-level upsert, so dpm can't derive this from the source spec.
    When set, the generator emits an upsert REALTIME table; the schema
    gets ``primaryKeyColumns`` declared, and the table config gets
    ``upsertConfig`` + ``routing.instanceSelectorType=strictReplicaGroup``.
    Validation (source_kind=stream, primary key columns exist) happens
    after normalization.
    """
    files_written: list[str] = []
    warnings: list[str] = []
    errors: list[str] = []

    # ------------------------------------------------------------------ #
    # Parse
    # ------------------------------------------------------------------ #
    try:
        raw = read_json_or_yaml(path)
    except Exception as exc:
        return GenerateResult(
            success=False,
            output_dir=out_dir,
            errors=[f"Failed to read spec file: {exc}"],
        )

    parser = DruidSpecParser()
    try:
        parse_result = parser.parse(raw)
    except ParseError as exc:
        return GenerateResult(
            success=False,
            output_dir=out_dir,
            errors=[str(exc)],
        )

    if not parse_result.success or parse_result.parsed_spec is None:
        return GenerateResult(
            success=False,
            output_dir=out_dir,
            errors=parse_result.errors,
            warnings=parse_result.warnings,
        )
    warnings.extend(parse_result.warnings)

    # ------------------------------------------------------------------ #
    # Normalize
    # ------------------------------------------------------------------ #
    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)
    if not norm_result.success or norm_result.canonical is None:
        return GenerateResult(
            success=False,
            output_dir=out_dir,
            errors=norm_result.errors,
            warnings=norm_result.warnings,
        )
    warnings.extend(norm_result.warnings)
    canonical: CanonicalMigrationModel = norm_result.canonical

    # ------------------------------------------------------------------ #
    # Classify
    # ------------------------------------------------------------------ #
    classification = classify_datasource(canonical)
    canonical.classification = classification.value

    # ------------------------------------------------------------------ #
    # Upsert config (operator-supplied)
    # ------------------------------------------------------------------ #
    # Druid has no row-level upsert, so this can't be derived from the
    # source spec. When the operator passes ``--upsert-primary-key``
    # (etc.) on the CLI, we apply the config to canonical and validate.
    if upsert_config is not None and upsert_config.enabled:
        # Pinot upsert is REALTIME-only; the OFFLINE table can't be
        # upsert-shaped because historical segments are immutable.
        if canonical.source_kind != "stream":
            return GenerateResult(
                success=False,
                output_dir=out_dir,
                errors=[
                    "--upsert-primary-key requires a streaming source "
                    f"(source_kind=stream); spec has source_kind="
                    f"{canonical.source_kind}. Pinot OFFLINE tables "
                    "cannot be upsert-shaped — historical segments are "
                    "immutable."
                ],
                warnings=warnings,
            )
        # Validate every primary-key column actually exists. The user
        # gets a clear list of valid candidates rather than an
        # opaque Pinot deploy-time failure.
        known_columns = {d.name for d in canonical.dimensions}
        if canonical.time_field is not None:
            known_columns.add(canonical.time_field.column_name)
        unknown_pks = [
            pk for pk in upsert_config.primary_key
            if pk not in known_columns
        ]
        if unknown_pks:
            return GenerateResult(
                success=False,
                output_dir=out_dir,
                errors=[
                    f"upsert primary key(s) {unknown_pks} not found in "
                    f"canonical dimensions / time field. "
                    f"Known columns: {sorted(known_columns)}."
                ],
                warnings=warnings,
            )
        # Validate comparison column too (when explicitly set).
        comparison = upsert_config.comparison_column
        if comparison and comparison not in known_columns and comparison not in {
            m.name for m in canonical.metrics
        }:
            return GenerateResult(
                success=False,
                output_dir=out_dir,
                errors=[
                    f"upsert comparison column '{comparison}' not found "
                    f"in canonical dimensions / metrics / time field."
                ],
                warnings=warnings,
            )
        canonical.upsert = upsert_config

    # ------------------------------------------------------------------ #
    # Generate schema
    # ------------------------------------------------------------------ #
    schema_gen = PinotSchemaGenerator()
    schema, schema_warnings = schema_gen.generate_with_warnings(canonical)
    warnings.extend(schema_warnings)

    # ------------------------------------------------------------------ #
    # Generate table config
    # ------------------------------------------------------------------ #
    table_gen = PinotTableGenerator()
    if canonical.source_kind == "stream":
        table = table_gen.generate_realtime(canonical)
        table_filename = "table-realtime.json"
    else:
        table = table_gen.generate_offline(canonical)
        table_filename = "table-offline.json"

    # ------------------------------------------------------------------ #
    # Generate ingestion spec
    # ------------------------------------------------------------------ #
    ingestion_gen = PinotIngestionGenerator()
    if canonical.source_kind == "stream":
        ingestion_spec = ingestion_gen.generate_stream_config(canonical)
        ingestion_filename = "stream-config.json"
    else:
        ingestion_spec = ingestion_gen.generate_batch_job(canonical)
        ingestion_filename = "batch-job.json"

    # ------------------------------------------------------------------ #
    # Risk analysis
    # ------------------------------------------------------------------ #
    analyzer = RiskAnalyzer()
    analyze_result: AnalyzeResult = analyzer.analyze(canonical)
    warnings.extend(analyze_result.warnings)
    canonical.risk_annotations = analyze_result.risks

    # ------------------------------------------------------------------ #
    # Validation
    # ------------------------------------------------------------------ #
    validation_report = build_validation_report(
        canonical=canonical,
        risks=analyze_result.risks,
        schema=schema,
        table=table,
    )

    # ------------------------------------------------------------------ #
    # Write outputs
    # ------------------------------------------------------------------ #
    if not dry_run:
        out_path = Path(out_dir)
        ensure_dir(out_path)
        reports_dir = out_path / "reports"
        ensure_dir(reports_dir)

        # schema.json
        schema_path = out_path / "schema.json"
        write_json(schema_path, schema)
        files_written.append(str(schema_path))

        # table config
        table_path = out_path / table_filename
        write_json(table_path, table)
        files_written.append(str(table_path))

        # ingestion spec
        ingestion_path = out_path / ingestion_filename
        write_json(ingestion_path, ingestion_spec)
        files_written.append(str(ingestion_path))

        # canonical model
        canonical_path = out_path / "canonical.json"
        write_json(canonical_path, canonical.model_dump())
        files_written.append(str(canonical_path))

        # Reports
        report_writer = ReportWriter()
        rp = report_writer.write_migration_report(
            canonical, analyze_result.risks, validation_report, reports_dir
        )
        files_written.append(str(rp))

        rk = report_writer.write_risks(analyze_result.risks, reports_dir)
        files_written.append(str(rk))

        w_path = report_writer.write_warnings(warnings, reports_dir)
        files_written.append(str(w_path))

        md_writer = MarkdownReportWriter()
        md_path = md_writer.write_summary(
            canonical, analyze_result.risks, validation_report, reports_dir
        )
        files_written.append(str(md_path))

    return GenerateResult(
        success=True,
        output_dir=out_dir,
        files_written=files_written,
        errors=errors,
        warnings=warnings,
    )


def validate_spec(
    path: str,
    generated_dir: str | None = None,
) -> ValidateResult:
    """Validate a Druid spec and optionally validate generated artifacts."""
    # Normalize first
    norm_result = normalize_spec(path)
    if not norm_result.success or norm_result.canonical is None:
        from migrator.core.models import ValidationCheck, ValidationReport

        dummy_report = ValidationReport(
            datasource_name="",
            checks=[
                ValidationCheck(
                    check_id="pipeline.parse_normalize",
                    status="fail",
                    message=f"Failed to parse/normalize: {'; '.join(norm_result.errors)}",
                )
            ],
            confidence_score=0.0,
            overall_status="fail",
        )
        return ValidateResult(report=dummy_report, success=False)

    canonical = norm_result.canonical

    # Load generated artifacts if provided
    schema: dict | None = None
    table: dict | None = None
    if generated_dir is not None:
        gen_path = Path(generated_dir)
        schema_path = gen_path / "schema.json"
        if schema_path.exists():
            import json as _json
            schema = _json.loads(schema_path.read_text())

        # Try both offline and realtime
        for fname in ("table-offline.json", "table-realtime.json"):
            tp = gen_path / fname
            if tp.exists():
                import json as _json
                table = _json.loads(tp.read_text())
                break

    # Risk analysis
    analyzer = RiskAnalyzer()
    analyze_result = analyzer.analyze(canonical)

    # Build report
    validation_report = build_validation_report(
        canonical=canonical,
        risks=analyze_result.risks,
        schema=schema,
        table=table,
    )

    success = validation_report.overall_status != "fail"
    return ValidateResult(report=validation_report, success=success)
