"""``dpm recommend`` — Pinot indexing + aggregator suggestions.

Companion to ``dpm inspect`` / ``dpm generate``. Inspect tells the
operator what's IN the spec; generate produces the working artifact.
This command answers the next question: "what would I tweak in the
generated table config to make queries actually fast?"

The recommendations are derived from the canonical model alone — no
query log needed for v0.10.0. A future iteration can promote the
heuristic ones to data-driven once we have a query-log ingester.
"""

from __future__ import annotations

import json as _json
from pathlib import Path

import typer
from rich.console import Console

from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.recommendations.recommender import recommend


_console = Console()


def command(
    spec: Path = typer.Argument(
        ..., help="Path to the Druid ingestion spec JSON.",
    ),
    json_output: bool = typer.Option(
        False, "--json",
        help="Emit the recommendations as a JSON array instead of pretty text.",
    ),
) -> None:
    """Suggest Pinot indexing + aggregator tweaks for a Druid spec."""
    try:
        raw = _json.loads(Path(spec).read_text())
        parsed = DruidSpecParser().parse(raw)
        if not parsed.success or parsed.parsed_spec is None:
            raise ValueError(f"parse failed: {parsed.errors}")
        norm = DruidNormalizer().normalize(parsed.parsed_spec)
        if not norm.success or norm.canonical is None:
            raise ValueError(f"normalize failed: {norm.errors}")
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"failed to load spec: {exc}", err=True)
        raise typer.Exit(code=2)

    recs = recommend(norm.canonical)

    if json_output:
        typer.echo(_json.dumps([r.to_dict() for r in recs], indent=2, default=str))
        return

    _console.print(f"[bold]Pinot recommendations for {norm.canonical.datasource_name}[/bold]")
    _console.print("─" * 60)
    if not recs:
        _console.print("No recommendations — the canonical model gives no signal.")
        return
    for r in recs:
        color = {
            "high": "red", "medium": "yellow", "low": "white",
        }.get(r.severity, "white")
        _console.print(
            f"  [{color}]{r.severity.upper():<6s}[/{color}] "
            f"[bold]{r.kind:<16s}[/bold] {r.target}"
        )
        _console.print(f"    {r.rationale}")
    _console.print("")
    _console.print(
        "Each recommendation comes with a config_hint snippet — "
        "use `--json` to see them and copy into your table config."
    )
