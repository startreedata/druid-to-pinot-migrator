"""``dpm cutover-many`` — run the cutover orchestrator across N datasources.

Reads a YAML/JSON manifest listing datasources and per-entry cutover
config, then runs ``run_cutover`` for each. Each datasource gets its
own ``<out>/<datasource>/`` subdirectory (with its own checkpoint, so
resume works per-entry). The aggregate result lands in
``<out>/batch-report.json``.

Use this for the typical "migrate 30 datasources from one Druid
cluster" case where shell-looping ``dpm cutover`` would lose the
top-level "X of Y succeeded" view.
"""

from __future__ import annotations

import json as _json
from pathlib import Path

import typer
from rich.console import Console

from migrator.auth import AuthConfigError, session_from_env
from migrator.druid.overlord_client import DruidOverlordClient
from migrator.parity.clients import DruidHttpSqlClient, PinotHttpSqlClient
from migrator.pinot.deployer import PinotDeployer
from migrator.realtime.backfill_runner import (
    DruidHttpSqlPager,
    PinotIngestFromFileSink,
)
from migrator.realtime.batch_cutover import (
    BatchCutoverDefaults,
    BatchCutoverEntry,
    BatchCutoverManifest,
    run_batch_cutover,
)


_console = Console()


def command(
    manifest: Path = typer.Option(
        ...,
        "--manifest",
        help="YAML or JSON manifest listing the datasources to cut over.",
    ),
    out: Path = typer.Option(
        Path("./cutover-batch-out"),
        "--out",
        help=(
            "Top-level output directory. Each datasource gets a "
            "subdirectory under it; a batch-report.json is written here."
        ),
    ),
    staging_dir: Path = typer.Option(
        Path("./cutover-batch-staging"),
        "--staging-dir",
        help="Top-level staging directory (one subdir per datasource).",
    ),
    abort_on_first_failure: bool = typer.Option(
        False, "--abort-on-first-failure",
        help=(
            "Stop the batch as soon as one datasource ends with all_ok=False. "
            "Default: keep going so a flaky parity on one datasource doesn't "
            "block the rest of the batch."
        ),
    ),
    no_resume: bool = typer.Option(
        False, "--no-resume",
        help=(
            "Pass --no-resume to every per-datasource cutover (ignore each "
            "entry's existing checkpoint). Default: each datasource resumes "
            "from where its individual checkpoint left off."
        ),
    ),
    restart_from: str | None = typer.Option(
        None, "--restart-from",
        help=(
            "Pass --restart-from <phase> to every per-datasource cutover. "
            "Useful for 're-run parity across all datasources' patterns."
        ),
    ),
    druid_auth: str | None = typer.Option(
        None, "--druid-auth",
        help=(
            "Druid auth: 'basic:user:pass', 'bearer:<token>', "
            "'header:K=V', or 'none'. Falls back to env DPM_DRUID_AUTH. "
            "Applied to every datasource in the batch."
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
        help="Client certificate for Druid mTLS. Env DPM_DRUID_CERT.",
    ),
    druid_key: str | None = typer.Option(
        None, "--druid-key",
        help="Client key for Druid mTLS. Env DPM_DRUID_KEY.",
    ),
    pinot_auth: str | None = typer.Option(
        None, "--pinot-auth",
        help=(
            "Pinot auth (controller + broker share the flag): "
            "'basic:user:pass', 'bearer:<token>', 'header:K=V', or 'none'."
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
        help="Client certificate for Pinot mTLS. Env DPM_PINOT_CERT.",
    ),
    pinot_key: str | None = typer.Option(
        None, "--pinot-key",
        help="Client key for Pinot mTLS. Env DPM_PINOT_KEY.",
    ),
) -> None:
    """Run a multi-datasource cutover from a manifest file."""
    try:
        druid_session = session_from_env(
            "DRUID",
            auth_value=druid_auth, ca_bundle=druid_ca,
            insecure=druid_insecure or None,
            cert=druid_cert, key=druid_key,
        )
        pinot_session = session_from_env(
            "PINOT",
            auth_value=pinot_auth, ca_bundle=pinot_ca,
            insecure=pinot_insecure or None,
            cert=pinot_cert, key=pinot_key,
        )
    except AuthConfigError as exc:
        typer.echo(f"Invalid auth config: {exc}", err=True)
        raise typer.Exit(code=2)

    try:
        m = BatchCutoverManifest.from_path(manifest)
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Failed to load manifest {manifest}: {exc}", err=True)
        raise typer.Exit(code=2)

    if not m.datasources:
        typer.echo("Manifest has no datasources.", err=True)
        raise typer.Exit(code=2)

    def _client_factory(
        defaults: BatchCutoverDefaults, entry: BatchCutoverEntry,
    ):
        # Defaults have only the cluster URLs we care about — auth
        # comes from the CLI flags above and applies uniformly.
        druid_overlord = defaults.druid_overlord or "http://localhost:8081"
        druid_router = defaults.druid_router or "http://localhost:8888"
        pinot_controller = defaults.pinot_controller or "http://localhost:9000"
        pinot_broker = defaults.pinot_broker or "http://localhost:8099"
        return {
            "overlord": DruidOverlordClient(druid_overlord, session=druid_session),
            "deployer": PinotDeployer(pinot_controller, session=pinot_session),
            "pager": DruidHttpSqlPager(druid_router, session=druid_session),
            "pinot_ingest_sink": PinotIngestFromFileSink(
                pinot_controller, session=pinot_session,
            ),
            "druid_sql_client": DruidHttpSqlClient(druid_router, session=druid_session),
            "pinot_sql_client": PinotHttpSqlClient(pinot_broker, session=pinot_session),
        }

    report = run_batch_cutover(
        m,
        out_root=out,
        staging_root=staging_dir,
        client_factory=_client_factory,
        abort_on_first_failure=abort_on_first_failure,
        resume=not no_resume,
        restart_from=restart_from,
    )

    _console.print(f"[bold]Batch cutover[/bold] — {report.total} datasources")
    _console.print("─" * 60)
    for e in report.entries:
        marker = "[green]✓[/green]" if e.all_ok else "[red]✗[/red]"
        line = f"  {marker} {e.datasource:<24s} {e.pinot_table:<24s} {e.elapsed_s:>6.1f}s"
        if e.error:
            line += f"  [{e.error}]"
        _console.print(line)
    _console.print("")
    _console.print(
        f"Result: {report.succeeded} succeeded, {report.failed} failed "
        f"(of {report.total})"
    )
    _console.print(f"Report: {out}/batch-report.json")

    if not report.all_ok:
        raise typer.Exit(code=1)
