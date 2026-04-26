"""``dpm plan-hybrid`` — pure planner for hybrid Druid → Pinot migrations."""

from __future__ import annotations

from pathlib import Path

import typer

from migrator.druid.classifiers import classify_datasource
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.realtime.hybrid_planner import (
    plan_hybrid_migration,
    write_hybrid_plan,
)
from migrator.realtime.offset_io import load_offset_map
from migrator.utils.io import read_json_or_yaml


def command(
    spec: Path = typer.Argument(..., help="Path to Druid Kafka spec (JSON or YAML)."),
    offset_map: Path = typer.Option(
        ...,
        "--offset-map",
        help="Offset-map JSON produced by `dpm extract-offsets`.",
    ),
    out: Path = typer.Option(
        Path("./hybrid-output"),
        "--out",
        help="Output directory for the generated artifacts.",
    ),
    backfill_start_iso: str | None = typer.Option(
        None,
        "--backfill-start-iso",
        help="Start of the backfill range (defaults to the spec's interval start).",
    ),
    page_rows: int = typer.Option(
        50_000,
        "--page-rows",
        help="Druid SQL paging size for the backfill plan.",
        min=1,
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print the resulting plan as JSON to stdout."
    ),
) -> None:
    """Generate OFFLINE + REALTIME table configs, backfill plan, and runbook."""
    raw = read_json_or_yaml(str(spec))
    parsed = DruidSpecParser().parse(raw)
    if not parsed.success or parsed.parsed_spec is None:
        typer.echo(f"Parse failed: {parsed.errors}", err=True)
        raise typer.Exit(code=2)

    normalised = DruidNormalizer().normalize(parsed.parsed_spec)
    if not normalised.success or normalised.canonical is None:
        typer.echo(f"Normalisation failed: {normalised.errors}", err=True)
        raise typer.Exit(code=2)

    canonical = normalised.canonical
    canonical.classification = classify_datasource(canonical).value

    watermark = load_offset_map(offset_map)
    plan = plan_hybrid_migration(
        canonical,
        watermark,
        backfill_start_iso=backfill_start_iso,
        backfill_page_rows=page_rows,
    )
    paths = write_hybrid_plan(plan, out)

    if json_output:
        import json as _json

        typer.echo(_json.dumps(plan.to_dict(), indent=2))
        return

    typer.echo(f"Wrote {len(paths)} files to {out}/")
    for label, path in sorted(paths.items()):
        typer.echo(f"  {label:18} {path.relative_to(out)}")
