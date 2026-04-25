"""
Markdown runbook generation for hybrid Druid → Pinot migrations.

A runbook is a documentation artifact: each step is a numbered command
the operator can run. It is meant to be readable on its own, so reviewers
who don't run the tooling can still understand and audit the migration.
"""

from __future__ import annotations

from pathlib import Path

from migrator.realtime.models import HybridMigrationPlan


def write_runbook(plan: HybridMigrationPlan, out_dir: str | Path) -> Path:
    """Write `runbook.md` for the given hybrid plan; return its path."""
    out_path = Path(out_dir) / "runbook.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_render(plan))
    return out_path


def _render(plan: HybridMigrationPlan) -> str:
    wm = plan.watermark
    parts: list[str] = []
    parts.append(f"# Migration Runbook — {plan.datasource_name}\n")
    parts.append(
        "This runbook walks through migrating the Druid datasource "
        f"`{plan.datasource_name}` to a Pinot **hybrid** (OFFLINE + REALTIME) "
        "table without data loss or duplication.\n"
    )
    parts.append("---\n")

    parts.append("## Watermark\n")
    parts.append(
        f"- Captured at: `{wm.captured_at_iso}`\n"
        f"- Druid supervisor: `{wm.supervisor_id or '(none)'}`\n"
        f"- Kafka topic: `{wm.topic}`\n"
        f"- Boundary timestamp: `{wm.watermark_iso}` ({wm.watermark_ms} ms)\n"
    )
    if wm.offsets:
        parts.append("\n**Per-partition offsets** (informational; Pinot uses the timestamp):\n")
        parts.append("| Partition | Offset |\n|---|---|\n")
        for po in sorted(wm.offsets, key=lambda x: x.partition):
            parts.append(f"| {po.partition} | {po.offset} |\n")
    parts.append("\n---\n")

    parts.append("## Step 1 — Stop Druid Kafka supervisor\n")
    if wm.supervisor_id:
        parts.append(
            "```bash\n"
            f"curl -X POST http://<druid-overlord>/druid/indexer/v1/supervisor/{wm.supervisor_id}/terminate\n"
            "```\n"
        )
    else:
        parts.append("> Supervisor ID was not captured; identify it manually before terminating.\n")
    parts.append(
        "After this point, no further events will be ingested into Druid.\n"
    )

    parts.append("\n## Step 2 — Deploy Pinot schema and tables\n")
    parts.append(
        "Deploy the schema, then both table configs:\n\n"
        "```bash\n"
        "PINOT=http://<pinot-controller>:9000\n\n"
        "curl -X POST -H 'Content-Type: application/json' \\\n"
        "  --data @schema.json \"$PINOT/schemas\"\n\n"
        "curl -X POST -H 'Content-Type: application/json' \\\n"
        "  --data @table-offline.json \"$PINOT/tables\"\n\n"
        "curl -X POST -H 'Content-Type: application/json' \\\n"
        "  --data @table-realtime.json \"$PINOT/tables\"\n"
        "```\n\n"
        "Order matters: the OFFLINE table holds data BEFORE the watermark; the "
        "REALTIME table is configured with `stream.kafka.consumer.prop.auto.offset.reset"
        f" = \"{wm.watermark_iso}\"` so Pinot consumes only events from the watermark "
        "onward — there is no overlap with the OFFLINE half.\n"
    )

    br = plan.backfill_range
    parts.append("\n## Step 3 — Backfill historical data into the OFFLINE table\n")
    parts.append(
        f"Range: `{br.start_iso}` → `{br.end_iso}` (exclusive).\n\n"
        "Two options:\n\n"
        "**A. Tooling path** — let the migrator drive the dump:\n\n"
        "```bash\n"
        f"dpm backfill-batch \\\n"
        f"  --druid-router http://<druid-router>:8888 \\\n"
        f"  --datasource {plan.datasource_name} \\\n"
        f"  --pinot-controller http://<pinot-controller>:9000 \\\n"
        f"  --start-iso '{br.start_iso}' \\\n"
        f"  --end-iso '{br.end_iso}' \\\n"
        f"  --staging-dir /tmp/{plan.datasource_name}-backfill\n"
        "```\n\n"
        "**B. Manual path** — run the dump and ingest yourself with your own ETL. "
        "Druid SQL example for paged extraction:\n\n"
        "```sql\n"
        f'SELECT * FROM "{plan.datasource_name}"\n'
        f"WHERE __time >= TIMESTAMP '{br.start_iso}'\n"
        f"  AND __time <  TIMESTAMP '{br.end_iso}'\n"
        f"ORDER BY __time\n"
        f"OFFSET 0 ROWS FETCH NEXT {br.page_rows} ROWS ONLY\n"
        "```\n\n"
        "Then pass the resulting NDJSON / Parquet to a Pinot `LaunchDataIngestionJob` "
        "using the included `backfill-job.json`.\n"
    )

    parts.append("\n## Step 4 — Verify hybrid query routing\n")
    parts.append(
        "Pinot brokers automatically route queries against `"
        f"{plan.datasource_name}` to the OFFLINE half for time < watermark and to "
        "the REALTIME half for time ≥ watermark. Verify with a parity query against "
        "Druid (or the original source) for a window straddling the watermark:\n\n"
        "```sql\n"
        f"SELECT COUNT(*) FROM {plan.datasource_name}\n"
        f"WHERE timestamp >= '{br.start_iso}'\n"
        f"  AND timestamp <  <after watermark>\n"
        "```\n\n"
        "Counts should match between Druid and Pinot within the tolerance "
        "documented in [Tutorial 18 — Production Checklist](18-production-checklist.md).\n"
    )

    parts.append("\n## Step 5 — Decommission Druid datasource\n")
    parts.append(
        "Once Pinot has caught up and a parity check has passed, the Druid "
        "datasource can be deleted via the Coordinator. This step is irreversible — "
        "keep a backup of the segment metadata.\n"
    )
    return "".join(parts)
