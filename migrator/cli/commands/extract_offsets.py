"""``dpm extract-offsets`` — capture a Druid Kafka supervisor's offsets."""

from __future__ import annotations

from pathlib import Path

import typer

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
) -> None:
    """Snapshot Druid's per-partition Kafka offsets and watermark timestamp."""
    client = DruidOverlordClient(overlord_url)
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
