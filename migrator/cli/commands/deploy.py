"""``dpm deploy`` — push schema + table configs to a Pinot controller."""

from __future__ import annotations

from pathlib import Path

import typer

from migrator.auth import AuthConfigError, session_from_env
from migrator.pinot.deployer import (
    DeployArtifacts,
    PinotDeployer,
    discover_artifacts,
)


def command(
    artifacts_dir: Path | None = typer.Option(
        None,
        "--artifacts-dir",
        help=(
            "Directory containing dpm-generated artifacts "
            "(schema.json, table-offline.json, table-realtime.json). "
            "Whichever files exist will be deployed. Mutually exclusive "
            "with the --schema / --*-table flags."
        ),
    ),
    schema: Path | None = typer.Option(
        None,
        "--schema",
        help="Schema JSON file. Overrides the schema.json in --artifacts-dir.",
    ),
    offline_table: Path | None = typer.Option(
        None,
        "--offline-table",
        help="OFFLINE table config JSON. Overrides table-offline.json.",
    ),
    realtime_table: Path | None = typer.Option(
        None,
        "--realtime-table",
        help="REALTIME table config JSON. Overrides table-realtime.json.",
    ),
    pinot_controller: str = typer.Option(
        "http://localhost:9000",
        "--pinot-controller",
        help="Pinot Controller base URL.",
    ),
    pinot_auth: str | None = typer.Option(
        None,
        "--pinot-auth",
        help=(
            "Pinot controller auth: 'basic:user:pass', 'bearer:<token>', "
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
        help="Skip TLS verification when talking to Pinot (use only for testing).",
    ),
    pinot_cert: str | None = typer.Option(
        None,
        "--pinot-cert",
        help=(
            "Client certificate for Pinot mTLS. Combined PEM, or the cert "
            "half when --pinot-key is also given. Env DPM_PINOT_CERT."
        ),
    ),
    pinot_key: str | None = typer.Option(
        None,
        "--pinot-key",
        help="Client key for Pinot mTLS. Env DPM_PINOT_KEY.",
    ),
) -> None:
    """Deploy Pinot schema + table configs to a controller via REST.

    The command is idempotent: a 409 Conflict from the controller
    (\"already exists\") is treated as a soft success so re-runs after
    a partial failure don't get stuck. Schema deploys first, then
    OFFLINE, then REALTIME — which is the order Pinot's controller
    requires.
    """
    # Build artifacts: start from --artifacts-dir if given, then let
    # individual --schema / --*-table flags override.
    if artifacts_dir is not None:
        artifacts = discover_artifacts(artifacts_dir)
    else:
        artifacts = DeployArtifacts()
    if schema is not None:
        artifacts.schema = schema
    if offline_table is not None:
        artifacts.offline_table = offline_table
    if realtime_table is not None:
        artifacts.realtime_table = realtime_table

    if (artifacts.schema is None
            and artifacts.offline_table is None
            and artifacts.realtime_table is None):
        typer.echo(
            "Nothing to deploy — pass --artifacts-dir or one of "
            "--schema / --offline-table / --realtime-table.",
            err=True,
        )
        raise typer.Exit(code=2)

    try:
        session = session_from_env(
            "PINOT",
            auth_value=pinot_auth,
            ca_bundle=pinot_ca,
            insecure=pinot_insecure or None,
            cert=pinot_cert,
            key=pinot_key,
        )
    except AuthConfigError as exc:
        typer.echo(f"Invalid --pinot-auth: {exc}", err=True)
        raise typer.Exit(code=2)

    deployer = PinotDeployer(pinot_controller, session=session)
    report = deployer.deploy(artifacts)

    for r in report.results:
        marker = {
            "created": "✓",
            "already_exists": "·",
            "error": "✗",
        }.get(r.status, "?")
        line = f"  {marker} {r.artifact:<14s} {r.name:<40s} {r.status}"
        if r.detail and r.status != "created":
            line += f"  ({r.detail})"
        typer.echo(line)

    typer.echo("")
    typer.echo(
        f"Deploy summary: {report.created} created, "
        f"{report.already_exists} already existed, {report.errored} failed"
    )

    if not report.all_ok:
        raise typer.Exit(code=1)
