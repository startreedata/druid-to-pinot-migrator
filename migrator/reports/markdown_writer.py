from __future__ import annotations

from pathlib import Path

from migrator.core.models import CanonicalMigrationModel, RiskAnnotation, ValidationReport
from migrator.risks.formatters import format_risk_markdown
from migrator.utils.io import ensure_dir


class MarkdownReportWriter:
    """Write migration summary reports as Markdown files."""

    def write_summary(
        self,
        canonical: CanonicalMigrationModel,
        risks: list[RiskAnnotation],
        validation_report: ValidationReport,
        output_dir: str | Path,
    ) -> Path:
        """Write migration-summary.md."""
        output_dir = Path(output_dir)
        ensure_dir(output_dir)

        lines: list[str] = []

        lines.append(f"# Migration Summary: {canonical.datasource_name}")
        lines.append("")

        # ------------------------------------------------------------------ #
        # Source Summary
        # ------------------------------------------------------------------ #
        lines.append("## Source Summary")
        lines.append("")
        lines.append(f"- **Datasource**: `{canonical.datasource_name}`")
        lines.append(f"- **Source Kind**: {canonical.source_kind}")
        time_col = canonical.time_field.column_name if canonical.time_field else "_none_"
        time_fmt = canonical.time_field.format if canonical.time_field else "_none_"
        lines.append(f"- **Time Column**: `{time_col}` (format: `{time_fmt}`)")
        lines.append(f"- **Dimensions**: {len(canonical.dimensions)}")
        lines.append(f"- **Metrics**: {len(canonical.metrics)}")
        lines.append(f"- **Transforms**: {len(canonical.transforms)}")
        lines.append(
            f"- **Rollup**: {'Yes' if canonical.granularity.rollup else 'No'} "
            f"(segmentGranularity: {canonical.granularity.segment_granularity}, "
            f"queryGranularity: {canonical.granularity.query_granularity})"
        )
        lines.append("")

        # ------------------------------------------------------------------ #
        # Classification
        # ------------------------------------------------------------------ #
        lines.append("## Classification")
        lines.append("")
        lines.append(f"**{canonical.classification}**")
        lines.append("")

        # ------------------------------------------------------------------ #
        # Generated Artifacts
        # ------------------------------------------------------------------ #
        lines.append("## Generated Artifacts")
        lines.append("")
        lines.append("| File | Description |")
        lines.append("|------|-------------|")
        lines.append(f"| `schema.json` | Pinot schema for `{canonical.datasource_name}` |")
        if canonical.source_kind == "stream":
            lines.append(f"| `table-realtime.json` | Pinot REALTIME table config |")
        else:
            lines.append(f"| `table-offline.json` | Pinot OFFLINE table config |")
        lines.append(f"| `reports/migration-report.json` | Full migration report |")
        lines.append(f"| `reports/migration-summary.md` | This file |")
        lines.append("")

        # ------------------------------------------------------------------ #
        # Validation
        # ------------------------------------------------------------------ #
        lines.append("## Validation")
        lines.append("")
        lines.append(f"- **Overall Status**: {validation_report.overall_status.upper()}")
        lines.append(f"- **Confidence Score**: {validation_report.confidence_score:.2f}")
        lines.append("")

        # ------------------------------------------------------------------ #
        # Warnings
        # ------------------------------------------------------------------ #
        if canonical.notes:
            lines.append("## Warnings")
            lines.append("")
            for w in canonical.notes:
                lines.append(f"- {w}")
            lines.append("")

        # ------------------------------------------------------------------ #
        # Risks
        # ------------------------------------------------------------------ #
        lines.append("## Risks")
        lines.append("")
        if risks:
            lines.append(format_risk_markdown(risks))
        else:
            lines.append("_No risks identified._")
            lines.append("")

        # ------------------------------------------------------------------ #
        # Next Steps
        # ------------------------------------------------------------------ #
        lines.append("## Next Steps")
        lines.append("")
        lines.append(
            "1. Review the generated `schema.json` and table config for correctness."
        )
        lines.append(
            "2. Address any BLOCKING risks before attempting migration."
        )
        lines.append(
            "3. Deploy the schema and table config to your Pinot cluster."
        )
        if canonical.source_kind == "stream":
            lines.append(
                "4. Update `streamConfigs` in the table config with your actual Kafka broker "
                "addresses and topic names."
            )
        else:
            lines.append(
                "4. Run the batch ingestion job to populate the Pinot table."
            )
        lines.append(
            "5. Validate query results against the source Druid datasource."
        )
        lines.append("")

        content = "\n".join(lines)
        path = output_dir / "migration-summary.md"
        path.write_text(content, encoding="utf-8")
        return path
