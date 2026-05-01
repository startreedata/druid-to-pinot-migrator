"""``dpm extract-offsets`` — capture a Druid Kafka supervisor's offsets."""

from __future__ import annotations

from pathlib import Path

import typer

from migrator.auth import AuthConfigError, session_from_env
from migrator.druid.overlord_client import DruidOverlordClient
from migrator.realtime.offset_io import save_offset_map


def command(
    supervisor_id: str = typer.Option(..., help="Druid Kafka supervisor ID."),
    overlord_url: str = typer.Option(
        "http://localhost:8081",
        help="Druid Overlord (or Router) base URL.",
    ),
    out: Path = typer.Option(
        Path("offsets.json"),
        "--out",
        help="Where to write the resulting offset-map JSON.",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print the captured offset map to stdout as JSON."
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
        help="Skip TLS verification when talking to Druid (use only for testing).",
    ),
) -> None:
    """Snapshot Druid's per-partition Kafka offsets and watermark timestamp."""
    try:
        druid_session = session_from_env(
            "DRUID",
            auth_value=druid_auth,
            ca_bundle=druid_ca,
            insecure=druid_insecure or None,
        )
    except AuthConfigError as exc:
        typer.echo(f"Invalid --druid-auth: {exc}", err=True)
        raise typer.Exit(code=2)
    client = DruidOverlordClient(overlord_url, session=druid_session)
    offset_map = client.get_supervisor_offsets(supervisor_id)
    save_offset_map(offset_map, out)
    if json_output:
        import json as _json

        typer.echo(_json.dumps(offset_map.model_dump(mode="json"), indent=2))
    else:
        typer.echo(
            f"Wrote offset map for supervisor '{supervisor_id}' to {out} "
            f"(watermark={offset_map.watermark_iso}, "
            f"partitions={len(offset_map.offsets)})"
        )
