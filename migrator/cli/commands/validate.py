from __future__ import annotations

import json

import typer

try:
    from rich.console import Console

    _RICH = True
except ImportError:
    _RICH = False

from migrator.translators.pipeline import validate_spec

_console = Console() if _RICH else None


def command(
    spec: str = typer.Argument(..., help="Path to Druid ingestion spec"),
    generated_dir: str | None = typer.Option(
        None, "--generated-dir", help="Directory with generated Pinot artifacts to validate"
    ),
    json_output: bool = typer.Option(False, "--json", help="Output validation report as JSON"),
) -> None:
    """Validate a Druid spec and optionally validate generated Pinot artifacts."""
    try:
        result = validate_spec(spec, generated_dir=generated_dir)
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    report = result.report

    if json_output:
        report_dict = {
            "datasource_name": report.datasource_name,
            "overall_status": report.overall_status,
            "confidence_score": report.confidence_score,
            "checks": [c.model_dump() for c in report.checks],
        }
        typer.echo(json.dumps(report_dict, indent=2, sort_keys=True))
        if not result.success:
            raise typer.Exit(1)
        return

    if _RICH and _console:
        status_color = {"pass": "green", "warn": "yellow", "fail": "red"}.get(
            report.overall_status, "white"
        )
        _console.print(
            f"[bold]Validation Status:[/bold] [{status_color}]{report.overall_status.upper()}[/{status_color}]"
        )
        _console.print(f"[bold]Confidence Score:[/bold] {report.confidence_score:.2f}")
        _console.print(f"\n[bold]Checks ({len(report.checks)}):[/bold]")
        for check in report.checks:
            color = {"pass": "green", "warn": "yellow", "fail": "red"}.get(check.status, "white")
            _console.print(f"  [{color}]{check.status.upper()}[/{color}] {check.check_id}: {check.message}")
    else:
        typer.echo(f"Validation Status: {report.overall_status.upper()}")
        typer.echo(f"Confidence Score: {report.confidence_score:.2f}")
        typer.echo(f"\nChecks ({len(report.checks)}):")
        for check in report.checks:
            typer.echo(f"  {check.status.upper()} {check.check_id}: {check.message}")

    if not result.success:
        raise typer.Exit(1)
