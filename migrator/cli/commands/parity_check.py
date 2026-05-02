"""``dpm parity-check`` — assert query parity across Druid and Pinot."""

from __future__ import annotations

import json as _json
from pathlib import Path

import typer

from migrator.auth import AuthConfigError, session_from_env
from migrator.parity.clients import DruidHttpSqlClient, PinotHttpSqlClient
from migrator.parity.loader import load_queries
from migrator.parity.models import ParityResult
from migrator.parity.runner import run_parity


def _print_pretty(results: list[ParityResult]) -> None:
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        typer.echo(f"  {status}  {r.label:<40s} {r.detail}")


def _print_json(results: list[ParityResult]) -> None:
    payload = {
        "results": [r.model_dump() for r in results],
        "passed": sum(1 for r in results if r.passed),
        "failed": sum(1 for r in results if not r.passed),
        "total": len(results),
    }
    typer.echo(_json.dumps(payload, indent=2, default=str))


def command(
    queries: Path = typer.Option(
        ...,
        "--queries",
        help=(
            "Path to a YAML or JSON file describing the parity queries. "
            "Each entry has a label, druid SQL, pinot SQL, and an optional "
            "type (scalar | groupby) and tolerance. See the docs for the "
            "schema."
        ),
    ),
    druid_url: str = typer.Option(
        "http://localhost:8888",
        "--druid-url",
        help="Druid Router (or Broker) base URL.",
    ),
    pinot_broker: str = typer.Option(
        "http://localhost:8099",
        "--pinot-broker",
        help="Pinot Broker base URL.",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print the results as JSON instead of pretty text."
    ),
    druid_auth: str | None = typer.Option(
        None,
        "--druid-auth",
        help=(
            "Druid auth: 'basic:user:pass', 'bearer:<token>', "
            "'header:K=V', or 'none'. Falls back to env DPM_DRUID_AUTH."
        ),
    ),
    druid_ca: str | None = typer.Option(
        None,
        "--druid-ca",
        help="Path to a CA bundle for Druid TLS. Falls back to env DPM_DRUID_CA.",
    ),
    druid_insecure: bool = typer.Option(
        False,
        "--druid-insecure",
        help="Skip TLS verification when talking to Druid.",
    ),
    pinot_auth: str | None = typer.Option(
        None,
        "--pinot-auth",
        help=(
            "Pinot broker auth: 'basic:user:pass', 'bearer:<token>', "
            "'header:K=V', or 'none'. Falls back to env DPM_PINOT_AUTH."
        ),
    ),
    pinot_ca: str | None = typer.Option(
        None,
        "--pinot-ca",
        help="Path to a CA bundle for Pinot TLS. Falls back to env DPM_PINOT_CA.",
    ),
    pinot_insecure: bool = typer.Option(
        False,
        "--pinot-insecure",
        help="Skip TLS verification when talking to Pinot.",
    ),
) -> None:
    """Run parity queries against Druid and Pinot, exit non-zero on divergence.

    Aimed at the post-migration validation step: codifies the
    "run the same SQL on both sides and assert equality" pattern that
    every operator writes by hand otherwise.
    """
    try:
        druid_session = session_from_env(
            "DRUID",
            auth_value=druid_auth,
            ca_bundle=druid_ca,
            insecure=druid_insecure or None,
        )
        pinot_session = session_from_env(
            "PINOT",
            auth_value=pinot_auth,
            ca_bundle=pinot_ca,
            insecure=pinot_insecure or None,
        )
    except AuthConfigError as exc:
        typer.echo(f"Invalid auth config: {exc}", err=True)
        raise typer.Exit(code=2)

    try:
        spec = load_queries(queries)
    except Exception as exc:
        typer.echo(f"Failed to load queries file: {exc}", err=True)
        raise typer.Exit(code=2)

    druid_client = DruidHttpSqlClient(druid_url, session=druid_session)
    pinot_client = PinotHttpSqlClient(pinot_broker, session=pinot_session)

    results = run_parity(spec.queries, druid=druid_client, pinot=pinot_client)

    if json_output:
        _print_json(results)
    else:
        typer.echo("Parity check")
        typer.echo("─" * 60)
        _print_pretty(results)
        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed
        typer.echo("")
        typer.echo(
            f"Result: {passed} passed, {failed} failed (out of {len(results)})"
        )

    if any(not r.passed for r in results):
        raise typer.Exit(code=1)
