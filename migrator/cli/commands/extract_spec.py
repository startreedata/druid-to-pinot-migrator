"""``dpm extract-spec`` — pull a Druid ingestion spec from a running cluster."""

from __future__ import annotations

import json as _json
from pathlib import Path

import typer

from migrator.druid.coordinator_client import (
    DruidCoordinatorClient,
    DruidCoordinatorError,
)
from migrator.druid.overlord_client import (
    DruidOverlordClient,
    DruidOverlordError,
)
from migrator.druid.spec_extractor import extract_spec


def command(
    datasource: str = typer.Option(..., help="Druid datasource name."),
    coordinator_url: str = typer.Option(
        "http://localhost:8081",
        "--coordinator-url",
        help="Druid Coordinator base URL.",
    ),
    broker_url: str | None = typer.Option(
        None,
        "--broker-url",
        help=(
            "Druid Broker base URL (defaults to coordinator URL). Used for "
            "the segmentMetadata query."
        ),
    ),
    overlord_url: str | None = typer.Option(
        None,
        "--overlord-url",
        help=(
            "Druid Overlord base URL. Required to discover Kafka/Kinesis "
            "supervisors. Skip to force the batch extraction path."
        ),
    ),
    prefer: str = typer.Option(
        "auto",
        "--prefer",
        help="Extraction path: auto | stream | batch.",
    ),
    out: Path = typer.Option(
        Path("druid-spec.json"),
        "--out",
        help="Where to write the extracted spec.",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print the extracted spec to stdout as JSON."
    ),
) -> None:
    """Reconstruct a Druid ingestion spec from a live Druid cluster."""
    coord = DruidCoordinatorClient(
        coordinator_url=coordinator_url,
        broker_url=broker_url,
    )
    overlord: DruidOverlordClient | None = None
    if overlord_url is not None:
        overlord = DruidOverlordClient(overlord_url)

    prefer_arg: str | None
    if prefer in ("auto", ""):
        prefer_arg = None
    elif prefer in ("stream", "batch"):
        prefer_arg = prefer
    else:
        typer.echo(f"Invalid --prefer value: {prefer!r}", err=True)
        raise typer.Exit(code=2)

    try:
        result = extract_spec(
            datasource,
            coordinator=coord,
            overlord=overlord,
            prefer=prefer_arg,
        )
    except (DruidCoordinatorError, DruidOverlordError, ValueError) as exc:
        typer.echo(f"Spec extraction failed: {exc}", err=True)
        raise typer.Exit(code=1)

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_json.dumps(result.spec, indent=2) + "\n")

    if json_output:
        typer.echo(_json.dumps(result.spec, indent=2))
        return

    typer.echo(
        f"Extracted {result.source_kind} spec for datasource "
        f"'{datasource}' → {out}"
    )
    if result.supervisor_id:
        typer.echo(f"  source supervisor: {result.supervisor_id}")
    if result.warnings:
        typer.echo(
            f"  {len(result.warnings)} warning(s) — review before deploy:"
        )
        for w in result.warnings:
            typer.echo(f"    • {w}")
