"""``dpm extract-spec`` — pull a Druid ingestion spec from a running cluster."""

from __future__ import annotations

import json as _json
from pathlib import Path

import typer

from migrator.auth import AuthConfigError, session_from_env
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
    druid_auth: str | None = typer.Option(
        None,
        "--druid-auth",
        help=(
            "Druid auth: 'basic:user:pass', 'bearer:<token>', "
            "'header:K=V[;header:K2=V2]', or 'none'. "
            "Falls back to env DPM_DRUID_AUTH."
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
        help="Skip TLS verification when talking to Druid (use only for testing).",
    ),
    druid_cert: str | None = typer.Option(
        None,
        "--druid-cert",
        help=(
            "Client certificate for mTLS. Combined PEM (cert+key), or "
            "the cert half when --druid-key is also given. "
            "Falls back to env DPM_DRUID_CERT."
        ),
    ),
    druid_key: str | None = typer.Option(
        None,
        "--druid-key",
        help=(
            "Client key for mTLS (use with --druid-cert when cert+key "
            "are in separate PEM files). Falls back to env DPM_DRUID_KEY."
        ),
    ),
) -> None:
    """Reconstruct a Druid ingestion spec from a live Druid cluster."""
    try:
        druid_session = session_from_env(
            "DRUID",
            auth_value=druid_auth,
            ca_bundle=druid_ca,
            insecure=druid_insecure or None,
            cert=druid_cert,
            key=druid_key,
        )
    except AuthConfigError as exc:
        typer.echo(f"Invalid --druid-auth: {exc}", err=True)
        raise typer.Exit(code=2)
    coord = DruidCoordinatorClient(
        coordinator_url=coordinator_url,
        broker_url=broker_url,
        session=druid_session,
    )
    overlord: DruidOverlordClient | None = None
    if overlord_url is not None:
        overlord = DruidOverlordClient(overlord_url, session=druid_session)

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
