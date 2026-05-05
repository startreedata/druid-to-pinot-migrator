"""``dpm cutover`` — orchestrate the whole hybrid migration in one command."""

from __future__ import annotations

from pathlib import Path

import typer

from migrator.auth import AuthConfigError, session_from_env
from migrator.druid.overlord_client import DruidOverlordClient
from migrator.parity.clients import DruidHttpSqlClient, PinotHttpSqlClient
from migrator.pinot.deployer import PinotDeployer
from migrator.realtime.backfill_runner import (
    DruidHttpSqlPager,
    PinotIngestFromFileSink,
)
from migrator.realtime.cutover import CutoverConfig, run_cutover


def command(
    supervisor_id: str = typer.Option(
        ...,
        "--supervisor-id",
        help="Druid Kafka supervisor ID (also used to derive the offset map).",
    ),
    datasource: str = typer.Option(
        ...,
        "--datasource",
        help="Druid datasource name (used for the SQL backfill pager).",
    ),
    pinot_table: str = typer.Option(
        ...,
        "--pinot-table",
        help="Pinot table base name (without _OFFLINE / _REALTIME suffix).",
    ),
    spec: Path = typer.Option(
        ...,
        "--spec",
        help=(
            "Druid Kafka supervisor JSON. Used as input to plan-hybrid "
            "and to auto-derive parity queries via the canonical model."
        ),
    ),
    out_dir: Path = typer.Option(
        Path("./cutover-out"),
        "--out",
        help=(
            "Directory to collect every cutover artifact under: offsets, "
            "schema, table configs, deploy/parity reports, the top-level "
            "cutover-report.json."
        ),
    ),
    staging_dir: Path = typer.Option(
        Path("./cutover-staging"),
        "--staging-dir",
        help="Local directory for paged backfill NDJSON files.",
    ),
    backfill_start_iso: str = typer.Option(
        "1970-01-01T00:00:00.000Z",
        "--backfill-start-iso",
        help="Inclusive start of the backfill window.",
    ),
    backfill_end_iso: str | None = typer.Option(
        None,
        "--backfill-end-iso",
        help=(
            "Exclusive end of the backfill window. Defaults to the captured "
            "watermark (recommended for hybrid migrations)."
        ),
    ),
    backfill_page_rows: int = typer.Option(
        50_000,
        "--backfill-page-rows",
        help="Druid SQL paging size for the backfill.",
        min=1,
    ),
    backfill_time_column: str = typer.Option(
        "timestamp",
        "--backfill-time-column",
        help=(
            "Pinot schema's time column name. Druid SQL exports __time; "
            "dpm renames it to this name and converts ISO → ms before "
            "ingest. Default 'timestamp' matches dpm's generator."
        ),
    ),
    druid_overlord: str = typer.Option(
        "http://localhost:8081",
        "--druid-overlord",
        help="Druid Overlord (or Coordinator/Router) URL for offset capture.",
    ),
    druid_router: str = typer.Option(
        "http://localhost:8888",
        "--druid-router",
        help="Druid Router (or Broker) URL for backfill SQL paging.",
    ),
    pinot_controller: str = typer.Option(
        "http://localhost:9000",
        "--pinot-controller",
        help="Pinot Controller URL for deploy + ingestFromFile.",
    ),
    pinot_broker: str = typer.Option(
        "http://localhost:8099",
        "--pinot-broker",
        help="Pinot Broker URL for parity-check SQL queries.",
    ),
    skip_deploy: bool = typer.Option(
        False, "--skip-deploy",
        help="Skip the schema/table deploy phase.",
    ),
    skip_backfill: bool = typer.Option(
        False, "--skip-backfill",
        help="Skip the OFFLINE backfill phase.",
    ),
    skip_parity: bool = typer.Option(
        False, "--skip-parity",
        help="Skip the post-deploy parity-check phase.",
    ),
    continue_on_error: bool = typer.Option(
        False, "--continue-on-error",
        help=(
            "Don't abort after the first error — run every remaining "
            "phase anyway (useful for diagnostic dry-runs)."
        ),
    ),
    no_resume: bool = typer.Option(
        False, "--no-resume",
        help=(
            "Ignore any existing cutover-checkpoint.json under --out and "
            "run every phase from scratch. By default a re-run picks up "
            "where the previous run left off (skipping phases marked ok)."
        ),
    ),
    restart_from: str | None = typer.Option(
        None, "--restart-from",
        help=(
            "Re-run from this phase onward, keeping earlier phases' ok "
            "status from the checkpoint. Phases: extract_offsets, "
            "plan_hybrid, deploy, backfill, parity. Useful when only a "
            "later phase needs another attempt — e.g. parity flapped on "
            "a transient broker error."
        ),
    ),
    druid_auth: str | None = typer.Option(
        None, "--druid-auth",
        help=(
            "Druid auth: 'basic:user:pass', 'bearer:<token>', "
            "'header:K=V', or 'none'. Falls back to env DPM_DRUID_AUTH."
        ),
    ),
    druid_ca: str | None = typer.Option(
        None, "--druid-ca",
        help="Path to a CA bundle for Druid TLS. Falls back to env DPM_DRUID_CA.",
    ),
    druid_insecure: bool = typer.Option(
        False, "--druid-insecure",
        help="Skip TLS verification when talking to Druid.",
    ),
    druid_cert: str | None = typer.Option(
        None, "--druid-cert",
        help=(
            "Client certificate for Druid mTLS. Combined PEM, or the cert "
            "half when --druid-key is also given. Env DPM_DRUID_CERT."
        ),
    ),
    druid_key: str | None = typer.Option(
        None, "--druid-key",
        help="Client key for Druid mTLS. Env DPM_DRUID_KEY.",
    ),
    pinot_auth: str | None = typer.Option(
        None, "--pinot-auth",
        help=(
            "Pinot auth (controller + broker share the flag): "
            "'basic:user:pass', 'bearer:<token>', 'header:K=V', or 'none'. "
            "Falls back to env DPM_PINOT_AUTH."
        ),
    ),
    pinot_ca: str | None = typer.Option(
        None, "--pinot-ca",
        help="Path to a CA bundle for Pinot TLS. Falls back to env DPM_PINOT_CA.",
    ),
    pinot_insecure: bool = typer.Option(
        False, "--pinot-insecure",
        help="Skip TLS verification when talking to Pinot.",
    ),
    pinot_cert: str | None = typer.Option(
        None, "--pinot-cert",
        help=(
            "Client certificate for Pinot mTLS. Combined PEM, or the cert "
            "half when --pinot-key is also given. Env DPM_PINOT_CERT."
        ),
    ),
    pinot_key: str | None = typer.Option(
        None, "--pinot-key",
        help="Client key for Pinot mTLS. Env DPM_PINOT_KEY.",
    ),
) -> None:
    """Run a Druid → Pinot hybrid cutover end-to-end.

    Composes:

      extract-offsets → plan-hybrid → deploy → backfill-batch → parity-check

    Each phase emits one line; the top-level ``cutover-report.json`` in
    ``--out`` records the structured outcome (incl. parity per-query
    results). Exit code is 0 when every non-skipped step succeeds; 1
    when any step errored. ``--continue-on-error`` keeps the run going
    after a failure (still exits 1 at the end).
    """
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

    cfg = CutoverConfig(
        supervisor_id=supervisor_id,
        datasource=datasource,
        pinot_table=pinot_table,
        spec_path=spec,
        out_dir=out_dir,
        staging_dir=staging_dir,
        backfill_start_iso=backfill_start_iso,
        backfill_end_iso=backfill_end_iso,
        backfill_page_rows=backfill_page_rows,
        backfill_time_column=backfill_time_column,
        skip_deploy=skip_deploy,
        skip_backfill=skip_backfill,
        skip_parity=skip_parity,
        abort_on_error=not continue_on_error,
        resume=not no_resume,
        restart_from=restart_from,
    )

    overlord = DruidOverlordClient(druid_overlord, session=druid_session)
    deployer = PinotDeployer(pinot_controller, session=pinot_session)
    pager = DruidHttpSqlPager(druid_router, session=druid_session)
    sink = PinotIngestFromFileSink(pinot_controller, session=pinot_session)
    druid_sql = DruidHttpSqlClient(druid_router, session=druid_session)
    pinot_sql = PinotHttpSqlClient(pinot_broker, session=pinot_session)

    report = run_cutover(
        cfg,
        overlord=overlord,
        deployer=deployer,
        pager=pager,
        pinot_ingest_sink=sink,
        druid_sql_client=druid_sql,
        pinot_sql_client=pinot_sql,
    )

    typer.echo("Cutover")
    typer.echo("─" * 60)
    for s in report.steps:
        marker = {"ok": "✓", "skipped": "·", "error": "✗"}.get(s.status, "?")
        line = f"  {marker} {s.step:<18s} {s.status:<8s} {s.detail}"
        if s.artifact:
            line += f"  [{s.artifact}]"
        typer.echo(line)
    if report.parity:
        typer.echo("")
        typer.echo("Parity:")
        for r in report.parity:
            status = "PASS" if r.passed else "FAIL"
            typer.echo(f"  {status}  {r.label:<40s} {r.detail}")

    typer.echo("")
    typer.echo(f"Report: {report.out_dir}/cutover-report.json")

    if not report.all_ok:
        raise typer.Exit(code=1)
