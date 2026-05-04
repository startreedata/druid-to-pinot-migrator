from __future__ import annotations

import json

import typer
from rich.console import Console

from migrator.translators.pipeline import inspect_spec

_console = Console(stderr=False)


def command(
    spec: str = typer.Argument(..., help="Path to Druid ingestion spec"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Inspect a Druid ingestion spec and print a summary."""
    try:
        summary = inspect_spec(spec)
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    if json_output:
        typer.echo(json.dumps(summary, indent=2, sort_keys=True))
        return

    _console.print(f"[bold]Datasource:[/bold] {summary.get('datasource_name', '')}")
    _console.print(f"[bold]Source Kind:[/bold] {summary.get('source_kind', '')}")
    _console.print(f"[bold]Classification:[/bold] {summary.get('classification', '')}")
    _console.print(f"[bold]Dimensions:[/bold] {summary.get('dimensions', 0)}")
    _console.print(f"[bold]Metrics:[/bold] {summary.get('metrics', 0)}")
    _console.print(f"[bold]Transforms:[/bold] {summary.get('transforms', 0)}")
    _console.print(f"[bold]Rollup:[/bold] {summary.get('rollup', False)}")
    _console.print(f"[bold]Risk Count:[/bold] {summary.get('risk_count', 0)}")
    warnings = summary.get("warnings", [])
    if warnings:
        _console.print(f"\n[yellow]Warnings ({len(warnings)}):[/yellow]")
        for w in warnings:
            _console.print(f"  - {w}")
