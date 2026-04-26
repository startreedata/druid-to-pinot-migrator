from __future__ import annotations

import json
from pathlib import Path

import typer

try:
    from rich.console import Console

    _RICH = True
except ImportError:
    _RICH = False

from migrator.translators.pipeline import normalize_spec
from migrator.utils.io import write_json

_console = Console() if _RICH else None


def command(
    spec: str = typer.Argument(..., help="Path to Druid ingestion spec"),
    output: str | None = typer.Option(None, "--out", help="Output file path for canonical JSON"),
    json_output: bool = typer.Option(False, "--json", help="Print canonical model as JSON"),
) -> None:
    """Normalize a Druid ingestion spec into a canonical migration model."""
    try:
        result = normalize_spec(spec)
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    if not result.success or result.canonical is None:
        typer.echo("Normalization failed:", err=True)
        for e in result.errors:
            typer.echo(f"  ERROR: {e}", err=True)
        raise typer.Exit(1)

    canonical = result.canonical
    canonical_dict = canonical.model_dump()

    if output:
        write_json(output, canonical_dict)
        typer.echo(f"Canonical model written to: {output}")

    if json_output or not output:
        typer.echo(json.dumps(canonical_dict, indent=2, sort_keys=True))
        return

    if _RICH and _console:
        _console.print(f"[bold]Datasource:[/bold] {canonical.datasource_name}")
        _console.print(f"[bold]Source Kind:[/bold] {canonical.source_kind}")
        _console.print(f"[bold]Classification:[/bold] {canonical.classification}")
        for w in result.warnings:
            _console.print(f"[yellow]WARNING:[/yellow] {w}")
    else:
        typer.echo(f"Datasource:     {canonical.datasource_name}")
        typer.echo(f"Source Kind:    {canonical.source_kind}")
        typer.echo(f"Classification: {canonical.classification}")
        for w in result.warnings:
            typer.echo(f"WARNING: {w}")
