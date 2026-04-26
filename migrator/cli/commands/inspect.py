from __future__ import annotations

import json
import sys

import typer

try:
    from rich.console import Console
    from rich.table import Table

    _RICH = True
except ImportError:
    _RICH = False

from migrator.translators.pipeline import inspect_spec

_console = Console(stderr=False) if _RICH else None


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

    if _RICH and _console:
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
    else:
        typer.echo(f"Datasource:     {summary.get('datasource_name', '')}")
        typer.echo(f"Source Kind:    {summary.get('source_kind', '')}")
        typer.echo(f"Classification: {summary.get('classification', '')}")
        typer.echo(f"Dimensions:     {summary.get('dimensions', 0)}")
        typer.echo(f"Metrics:        {summary.get('metrics', 0)}")
        typer.echo(f"Transforms:     {summary.get('transforms', 0)}")
        typer.echo(f"Rollup:         {summary.get('rollup', False)}")
        typer.echo(f"Risk Count:     {summary.get('risk_count', 0)}")
        for w in summary.get("warnings", []):
            typer.echo(f"  WARNING: {w}")
