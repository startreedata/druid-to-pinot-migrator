"""``dpm backfill-batch`` — page Druid → NDJSON → Pinot OFFLINE table."""

from __future__ import annotations

from pathlib import Path

import typer

from migrator.auth import AuthConfigError, session_from_env
from migrator.realtime.backfill_runner import (
    DruidHttpSqlPager,
    PinotIngestFromFileSink,
    PinotIngestFromUriSink,
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
    druid_cert: str | None = typer.Option(
        None,
        "--druid-cert",
        help=(
            "Client certificate for Druid mTLS. Combined PEM, or the cert "
            "half when --druid-key is also given. Env DPM_DRUID_CERT."
        ),
    ),
    druid_key: str | None = typer.Option(
        None,
        "--druid-key",
        help="Client key for Druid mTLS. Env DPM_DRUID_KEY.",
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
    mode: str = typer.Option(
        "ingest-from-file",
        "--mode",
        help=(
            "Pinot ingestion mode:\n"
            "  ingest-from-file (default): upload each NDJSON file body via\n"
            "    multipart POST to /ingestFromFile. Simple but the file body\n"
            "    travels through the controller — fine up to ~1M rows/page.\n"
            "  ingest-from-uri: control-plane-only POST to /ingestFromURI;\n"
            "    the Pinot controller pulls each file directly from a\n"
            "    file:// URI. Requires the staging directory to be on a\n"
            "    filesystem the controller can see (typically a shared\n"
            "    Kubernetes volume). Scales much further than mode=ingest-from-file."
        ),
    ),
    uri_prefix: str | None = typer.Option(
        None,
        "--uri-prefix",
        help=(
            "When --mode=ingest-from-uri, optionally override the URI scheme. "
            "If unset, the sink emits file:// URIs against the staging dir's "
            "absolute path. If set (e.g. 's3://my-bucket/staging/'), the "
            "operator is responsible for uploading the staging files under "
            "this prefix BEFORE running the ingest."
        ),
    ),
) -> None:
    """Move historical Druid data into a Pinot OFFLINE table."""
    try:
        druid_session = session_from_env(
            "DRUID",
            auth_value=druid_auth,
            ca_bundle=druid_ca,
            insecure=druid_insecure or None,
            cert=druid_cert,
            key=druid_key,
        )
        pinot_session = session_from_env(
            "PINOT",
            auth_value=pinot_auth,
            ca_bundle=pinot_ca,
            insecure=pinot_insecure or None,
            cert=pinot_cert,
            key=pinot_key,
        )
    except AuthConfigError as exc:
        typer.echo(f"Invalid auth config: {exc}", err=True)
        raise typer.Exit(code=2)
    pager = DruidHttpSqlPager(druid_router, session=druid_session)
    if mode == "ingest-from-file":
        sink = PinotIngestFromFileSink(pinot_controller, session=pinot_session)
    elif mode == "ingest-from-uri":
        sink = PinotIngestFromUriSink(
            pinot_controller, session=pinot_session, uri_prefix=uri_prefix,
        )
    else:
        typer.echo(
            f"Invalid --mode {mode!r} (expected 'ingest-from-file' or "
            f"'ingest-from-uri')",
            err=True,
        )
        raise typer.Exit(code=2)
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
