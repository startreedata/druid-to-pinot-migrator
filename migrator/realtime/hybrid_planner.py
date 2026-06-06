"""
Pure planner that builds a hybrid migration plan.

Inputs are already-parsed domain objects (CanonicalMigrationModel,
StreamOffsetMap). No file I/O, no network — that lives in the CLI command
that wraps this module. Keeping the planner pure makes it trivially
unit-testable and re-usable from other tools (e.g. a CI dashboard or a
batch UI).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from migrator.core.enums import SourceKind
from migrator.core.errors import GenerationError
from migrator.core.models import CanonicalMigrationModel
from migrator.pinot.ingestion_generator import PinotIngestionGenerator
from migrator.pinot.schema_generator import PinotSchemaGenerator
from migrator.pinot.table_generator import PinotTableGenerator
from migrator.realtime.models import (
    BackfillRange,
    HybridMigrationPlan,
    StreamOffsetMap,
)


def plan_hybrid_migration(
    canonical: CanonicalMigrationModel,
    watermark: StreamOffsetMap,
    *,
    backfill_start_iso: str | None = None,
    backfill_page_rows: int = 50_000,
) -> HybridMigrationPlan:
    """
    Produce a complete hybrid plan from a normalised Druid model + watermark.

    The function is **pure**: same inputs give the same outputs, no I/O.

    ``backfill_start_iso`` defaults to the lower bound of the canonical
    granularity intervals (or, if absent, "1970-01-01T00:00:00.000Z" as a
    safe sentinel). Callers that know their actual data start time should
    pass it in for an accurate runbook.
    """
    if canonical.source_kind != SourceKind.STREAM.value:
        raise GenerationError(
            f"plan_hybrid_migration requires a stream source, got "
            f"source_kind='{canonical.source_kind}'"
        )

    schema = PinotSchemaGenerator().generate(canonical)
    offline = PinotTableGenerator().generate_offline(canonical)
    realtime = PinotTableGenerator().generate_realtime(
        canonical, watermark_iso=watermark.watermark_iso
    )
    backfill_job = PinotIngestionGenerator().generate_batch_job(canonical)
    # Point the backfill job at the OFFLINE table by default
    backfill_job["tableSpec"]["tableName"] = canonical.datasource_name
    backfill_job["jobType"] = "SegmentCreationAndTarPush"

    backfill_range = BackfillRange(
        start_iso=backfill_start_iso or _infer_start_iso(canonical),
        end_iso=watermark.watermark_iso,
        page_rows=backfill_page_rows,
    )

    return HybridMigrationPlan(
        datasource_name=canonical.datasource_name,
        schema=schema,
        offline_table=offline,
        realtime_table=realtime,
        backfill_range=backfill_range,
        backfill_job=backfill_job,
        watermark=watermark,
    )


def write_hybrid_plan(plan: HybridMigrationPlan, out_dir: str | Path) -> dict[str, Path]:
    """
    Write the plan's individual artifacts to disk.

    Returns a mapping ``{logical_name: path}`` so callers (including the CLI)
    can report what was produced without re-deriving paths. Side-effecting,
    deliberately separated from ``plan_hybrid_migration``.
    """
    import json

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}

    paths["schema"] = out / "schema.json"
    paths["schema"].write_text(json.dumps(plan.schema_, indent=2) + "\n")

    paths["offline_table"] = out / "table-offline.json"
    paths["offline_table"].write_text(json.dumps(plan.offline_table, indent=2) + "\n")

    paths["realtime_table"] = out / "table-realtime.json"
    paths["realtime_table"].write_text(json.dumps(plan.realtime_table, indent=2) + "\n")

    paths["backfill_job"] = out / "backfill-job.json"
    paths["backfill_job"].write_text(json.dumps(plan.backfill_job, indent=2) + "\n")

    paths["plan"] = out / "hybrid-plan.json"
    paths["plan"].write_text(json.dumps(plan.to_dict(), indent=2) + "\n")

    paths["watermark"] = out / "watermark.json"
    paths["watermark"].write_text(
        json.dumps(plan.watermark.model_dump(mode="json"), indent=2) + "\n"
    )

    # Runbook (markdown) — imported here to keep top-of-file imports lean
    from migrator.realtime.runbook_writer import write_runbook
    paths["runbook"] = write_runbook(plan, out)

    return paths


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _infer_start_iso(canonical: CanonicalMigrationModel) -> str:
    """Best-effort start-of-data ISO timestamp from the canonical model."""
    intervals = canonical.granularity.intervals or []
    for iv in intervals:
        if "/" in iv:
            start = iv.split("/", 1)[0]
            try:
                # Validate it parses
                datetime.fromisoformat(start.replace("Z", "+00:00"))
                return start
            except ValueError:
                continue
    return "1970-01-01T00:00:00.000Z"
