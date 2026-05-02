"""``dpm backfill-batch`` — page Druid → NDJSON → Pinot OFFLINE table."""

from __future__ import annotations

from pathlib import Path

import typer

from migrator.auth import AuthConfigError, session_from_env
from migrator.realtime.backfill_runner import (
    DruidHttpSqlPager,
    PinotIngestFromFileSink,
    run_backfill,
)


def command(
    datasource: str = typer.Option(..., help="Druid datasource name."),
    pinot_table: str = typer.Option(
        ..., help="Pinot table name (without _OFFLINE suffix)."
    ),
    start_iso: str = typer.Option(
        ...,
        "--start-iso",
        help="Inclusive start of the backfill window (ISO-8601 UTC).",
    ),
    end_iso: str = typer.Option(
        ...,
        "--end-iso",
        help="Exclusive end of the backfill window — typically the watermark.",
    ),
    druid_router: str = typer.Option(
        "http://localhost:8888",
        "--druid-router",
        help="Druid Router (or Broker) base URL.",
    ),
    pinot_controller: str = typer.Option(
        "http://localhost:9000",
        "--pinot-controller",
        help="Pinot Controller base URL.",
    ),
    staging_dir: Path = typer.Option(
        Path("./backfill-staging"),
        "--staging-dir",
        help="Local directory for paged NDJSON files.",
    ),
    page_rows: int = typer.Option(
        50_000,
        "--page-rows",
        help="Druid SQL paging size.",
        min=1,
    ),
    time_column: str = typer.Option(
        "timestamp",
        "--time-column",
        help=(
            "Name of the Pinot schema's time column. Druid SQL exports the "
            "time column as `__time` (ISO 8601 string); dpm renames it to "
            "this name and converts it to epoch milliseconds before "
            "ingest. The default 'timestamp' matches dpm's generated schema."
        ),
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
) -> None:
    """Move historical Druid data into a Pinot OFFLINE table."""
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
    pager = DruidHttpSqlPager(druid_router, session=druid_session)
    sink = PinotIngestFromFileSink(pinot_controller, session=pinot_session)
    result = run_backfill(
        datasource=datasource,
        pinot_table=pinot_table,
        start_iso=start_iso,
        end_iso=end_iso,
        staging_dir=staging_dir,
        pager=pager,
        sink=sink,
        page_rows=page_rows,
        time_column=time_column,
    )
    typer.echo(
        f"Backfill complete: {result.rows_dumped} rows in {result.pages_dumped} "
        f"pages → Pinot table {pinot_table}_OFFLINE"
    )
    typer.echo(f"  staging directory: {result.staging_dir}")
