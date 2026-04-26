"""``dpm backfill-batch`` — page Druid → NDJSON → Pinot OFFLINE table."""

from __future__ import annotations

from pathlib import Path

import typer

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
) -> None:
    """Move historical Druid data into a Pinot OFFLINE table."""
    pager = DruidHttpSqlPager(druid_router)
    sink = PinotIngestFromFileSink(pinot_controller)
    result = run_backfill(
        datasource=datasource,
        pinot_table=pinot_table,
        start_iso=start_iso,
        end_iso=end_iso,
        staging_dir=staging_dir,
        pager=pager,
        sink=sink,
        page_rows=page_rows,
    )
    typer.echo(
        f"Backfill complete: {result.rows_dumped} rows in {result.pages_dumped} "
        f"pages → Pinot table {pinot_table}_OFFLINE"
    )
    typer.echo(f"  staging directory: {result.staging_dir}")
