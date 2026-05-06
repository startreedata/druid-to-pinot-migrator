from __future__ import annotations

import json

import typer
from rich.console import Console

from migrator.core.models import UpsertConfig
from migrator.translators.pipeline import generate_bundle

_console = Console()


def command(
    spec: str = typer.Argument(..., help="Path to Druid ingestion spec"),
    out: str = typer.Option("./output", "--out", help="Output directory for generated artifacts"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate generation without writing files"),
    json_output: bool = typer.Option(False, "--json", help="Output result summary as JSON"),
    upsert_primary_key: list[str] = typer.Option(
        None, "--upsert-primary-key",
        help=(
            "Generate a Pinot upsert REALTIME table keyed on this column. "
            "Repeat for compound keys: ``--upsert-primary-key user_id "
            "--upsert-primary-key tenant_id``. Druid has no row-level "
            "upsert, so this is operator-driven — dpm does not infer it "
            "from the spec. Source must be streaming (Kafka / Kinesis); "
            "Pinot OFFLINE tables cannot be upsert-shaped."
        ),
    ),
    upsert_comparison_column: str | None = typer.Option(
        None, "--upsert-comparison-column",
        help=(
            "Column Pinot uses to break ties when two rows share the "
            "same primary key (the ``later'' wins). Defaults to the "
            "Druid time column, which is the right choice >95% of the "
            "time. Override when you have an explicit version / sequence "
            "column distinct from event time."
        ),
    ),
    upsert_mode: str = typer.Option(
        "FULL", "--upsert-mode",
        help=(
            "FULL replaces the whole row on PK collision; PARTIAL applies "
            "per-column merge strategies (currently passes through any "
            "operator-supplied partial config from the spec, but the most "
            "common case is FULL)."
        ),
    ),
) -> None:
    """Generate Pinot migration artifacts from a Druid ingestion spec."""
    # Build the upsert config when --upsert-primary-key is set.
    upsert_config = None
    if upsert_primary_key:
        upsert_config = UpsertConfig(
            enabled=True,
            primary_key=list(upsert_primary_key),
            comparison_column=upsert_comparison_column,
            mode=upsert_mode.upper(),
        )

    try:
        result = generate_bundle(
            spec, out_dir=out, dry_run=dry_run,
            upsert_config=upsert_config,
        )
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    if json_output:
        summary = {
            "success": result.success,
            "output_dir": result.output_dir,
            "files_written": result.files_written,
            "errors": result.errors,
            "warnings": result.warnings,
        }
        typer.echo(json.dumps(summary, indent=2, sort_keys=True))
        if not result.success:
            raise typer.Exit(1)
        return

    if not result.success:
        typer.echo("Generation failed:", err=True)
        for e in result.errors:
            typer.echo(f"  ERROR: {e}", err=True)
        raise typer.Exit(1)

    if dry_run:
        _console.print("[yellow]DRY RUN — no files written.[/yellow]")
        return

    _console.print(f"[green]Generated {len(result.files_written)} file(s) in:[/green] {out}")
    for f in result.files_written:
        _console.print(f"  {f}")
    for w in result.warnings:
        _console.print(f"[yellow]WARNING:[/yellow] {w}")
