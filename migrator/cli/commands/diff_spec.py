"""``dpm diff-spec`` — what changed between two Druid specs?

Operator-facing wrapper over ``migrator.diff.spec_diff.diff_spec_files``.
Both inputs are Druid ingestion specs (JSON); the diff is computed
over the canonical model so semantic equivalents (key-order changes,
formatting) collapse to "no change". The pinot_implications list is
the operator's TODO for keeping the Pinot side aligned with the
edited spec.

Exit codes:
  0  no semantic change (or change with implications listed; default)
  3  semantic change detected (use --exit-on-change to opt into this)
"""

from __future__ import annotations

import json as _json
from pathlib import Path

import typer
from rich.console import Console

from migrator.diff.spec_diff import diff_spec_files


_console = Console()


def command(
    old: Path = typer.Argument(
        ..., help="Path to the previous Druid spec JSON.",
    ),
    new: Path = typer.Argument(
        ..., help="Path to the updated Druid spec JSON.",
    ),
    json_output: bool = typer.Option(
        False, "--json",
        help="Emit a structured JSON diff instead of pretty text.",
    ),
    exit_on_change: bool = typer.Option(
        False, "--exit-on-change",
        help=(
            "Exit non-zero (3) when any semantic change is detected. "
            "Useful in CI to fail a 'spec must not change unexpectedly' "
            "guard step."
        ),
    ),
) -> None:
    """Compute the canonical-model diff between two Druid specs.

    Highlights what materially changed and surfaces the Pinot-side
    follow-up actions an operator has to take.
    """
    try:
        diff = diff_spec_files(old, new)
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"diff failed: {exc}", err=True)
        raise typer.Exit(code=2)

    if json_output:
        typer.echo(_json.dumps(diff.to_dict(), indent=2, default=str))
    else:
        _console.print(f"[bold]dpm diff-spec[/bold]   {old}  →  {new}")
        _console.print("─" * 60)
        if diff.is_empty:
            _console.print("[green]No semantic change.[/green]")
        else:
            _print_pretty(diff)

    if exit_on_change and not diff.is_empty:
        raise typer.Exit(code=3)


def _print_pretty(diff) -> None:
    if diff.datasource_name_changed:
        _console.print(
            f"[red]datasource_name:[/red] "
            f"{diff.datasource_name_changed.old} → "
            f"{diff.datasource_name_changed.new}"
        )
    if diff.source_kind_changed:
        _console.print(
            f"[red]source_kind:[/red] "
            f"{diff.source_kind_changed.old} → "
            f"{diff.source_kind_changed.new}"
        )
    if diff.classification_changed:
        _console.print(
            f"[yellow]classification:[/yellow] "
            f"{diff.classification_changed.old} → "
            f"{diff.classification_changed.new}"
        )
    if diff.time_field_changes:
        _console.print("[yellow]time_field:[/yellow]")
        for c in diff.time_field_changes:
            _console.print(f"  {c.name}: {c.old} → {c.new}")
    if diff.granularity_changes:
        _console.print("[yellow]granularity:[/yellow]")
        for c in diff.granularity_changes:
            _console.print(f"  {c.name}: {c.old} → {c.new}")
    if not diff.dimensions.is_empty:
        _console.print("[bold]dimensions:[/bold]")
        for d in diff.dimensions.added:
            _console.print(f"  [green]+ {d.name}[/green] ({d.druid_type})")
        for d in diff.dimensions.removed:
            _console.print(f"  [red]- {d.name}[/red] ({d.druid_type})")
        for c in diff.dimensions.type_changed:
            _console.print(f"  [yellow]~ {c.name}[/yellow] type {c.old} → {c.new}")
        for c in diff.dimensions.multi_value_changed:
            _console.print(
                f"  [yellow]~ {c.name}[/yellow] multi-value {c.old} → {c.new}"
            )
    if not diff.metrics.is_empty:
        _console.print("[bold]metrics:[/bold]")
        for m in diff.metrics.added:
            _console.print(f"  [green]+ {m.name}[/green] ({m.aggregation})")
        for m in diff.metrics.removed:
            _console.print(f"  [red]- {m.name}[/red] ({m.aggregation})")
        for c in diff.metrics.aggregation_changed:
            _console.print(f"  [yellow]~ {c.name}[/yellow] agg {c.old} → {c.new}")
        for c in diff.metrics.type_changed:
            _console.print(f"  [yellow]~ {c.name}[/yellow] type {c.old} → {c.new}")
    if diff.pinot_implications:
        _console.print("\n[bold]Pinot implications:[/bold]")
        for line in diff.pinot_implications:
            _console.print(f"  • {line}")
