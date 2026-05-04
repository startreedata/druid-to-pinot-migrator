from __future__ import annotations

import json

import typer
from rich.console import Console

from migrator.translators.pipeline import generate_bundle

_console = Console()


def command(
    spec: str = typer.Argument(..., help="Path to Druid ingestion spec"),
    out: str = typer.Option("./output", "--out", help="Output directory for generated artifacts"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate generation without writing files"),
    json_output: bool = typer.Option(False, "--json", help="Output result summary as JSON"),
) -> None:
    """Generate Pinot migration artifacts from a Druid ingestion spec."""
    try:
        result = generate_bundle(spec, out_dir=out, dry_run=dry_run)
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
