"""``dpm cluster-report`` — Druid → Pinot compatibility report for an
entire running Druid cluster.

For each datasource the cluster knows about:
  1. Resolve its supervisor (streaming) or reconstruct a synthetic
     batch spec from segment metadata (``dpm extract-spec`` logic).
  2. Run the parser → normalizer → risk analyzer.
  3. Bucket into GREEN / YELLOW / RED / ERROR.

Aggregate verdicts roll up into a top-level summary plus the N most
frequent blocking issues — useful for prioritising which Druid
features to fix first when you have 50+ datasources to migrate.

Output: ``<out>/summary.json`` (structured), ``<out>/cluster-report.md``
(human-readable), ``<out>/datasources/<name>.json`` per datasource.
"""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from migrator.auth import AuthConfigError, session_from_env
from migrator.cluster.inspector import (
    COMPAT_ERROR,
    COMPAT_GREEN,
    COMPAT_RED,
    COMPAT_YELLOW,
    inspect_cluster,
    write_report,
)
from migrator.druid.coordinator_client import DruidCoordinatorClient
from migrator.druid.overlord_client import DruidOverlordClient


_console = Console()


def command(
    druid_coordinator: str = typer.Option(
        "http://localhost:8081",
        "--druid-coordinator",
        help="Druid Coordinator base URL.",
    ),
    druid_overlord: str | None = typer.Option(
        None,
        "--druid-overlord",
        help=(
            "Druid Overlord base URL. Optional — when set, dpm uses "
            "the high-fidelity supervisor-based path for streaming "
            "datasources. When unset, every datasource falls through "
            "to segment-metadata reconstruction."
        ),
    ),
    out: Path = typer.Option(
        Path("./cluster-report"),
        "--out",
        help="Output directory for the report files.",
    ),
    datasources: list[str] = typer.Option(
        None,
        "--datasource",
        help=(
            "Only inspect these datasources (repeatable). When unset, "
            "every datasource the Coordinator knows about is inspected."
        ),
    ),
    fail_on_red: bool = typer.Option(
        False, "--fail-on-red",
        help=(
            "Exit non-zero (3) when any datasource is bucketed RED. "
            "Useful in CI to gate \"do not migrate this cluster yet\" "
            "on a green-status assertion."
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
        help="Client certificate for Druid mTLS. Env DPM_DRUID_CERT.",
    ),
    druid_key: str | None = typer.Option(
        None, "--druid-key",
        help="Client key for Druid mTLS. Env DPM_DRUID_KEY.",
    ),
) -> None:
    """Walk a Druid cluster and report Druid → Pinot migration readiness."""
    try:
        druid_session = session_from_env(
            "DRUID",
            auth_value=druid_auth, ca_bundle=druid_ca,
            insecure=druid_insecure or None,
            cert=druid_cert, key=druid_key,
        )
    except AuthConfigError as exc:
        typer.echo(f"Invalid auth config: {exc}", err=True)
        raise typer.Exit(code=2)

    coordinator = DruidCoordinatorClient(
        druid_coordinator, session=druid_session,
    )
    overlord = (
        DruidOverlordClient(druid_overlord, session=druid_session)
        if druid_overlord else None
    )

    # Live progress so the operator sees something on a 50-datasource
    # cluster — each DS can take seconds for segment-metadata fetch.
    def _progress(idx: int, total: int, ds: str) -> None:
        _console.print(
            f"[dim]\\[{idx:>3d}/{total:>3d}][/dim] {ds}",
            highlight=False,
        )

    _console.print(
        f"[bold]Inspecting Druid cluster[/bold] @ {druid_coordinator}"
    )
    _console.print("─" * 60)

    report = inspect_cluster(
        coordinator=coordinator,
        overlord=overlord,
        coordinator_url=druid_coordinator,
        overlord_url=druid_overlord,
        datasources=list(datasources) if datasources else None,
        progress_callback=_progress,
    )

    paths = write_report(report, out)

    # Summary table for the terminal — the markdown file has the
    # full per-datasource detail.
    _console.print("")
    _console.print(f"[bold]{report.total} datasource(s) inspected[/bold]")
    by_status = report.by_status
    color = {
        COMPAT_GREEN: "green",
        COMPAT_YELLOW: "yellow",
        COMPAT_RED: "red",
        COMPAT_ERROR: "white",
    }
    for status in (COMPAT_GREEN, COMPAT_YELLOW, COMPAT_RED, COMPAT_ERROR):
        n = by_status.get(status, 0)
        _console.print(
            f"  [{color[status]}]{status.upper():<8s}[/{color[status]}] {n}"
        )

    top = report.top_blocking_issues(5)
    if top:
        _console.print("")
        _console.print("[bold]Top blocking issues[/bold]")
        for issue, count in top:
            _console.print(f"  [red]{count:>3d}×[/red] {issue}")

    _console.print("")
    _console.print(f"Report: {paths['markdown']}")
    _console.print(f"        {paths['summary']}")

    if fail_on_red and by_status.get(COMPAT_RED, 0) > 0:
        # Exit code 3 distinguishes "RED datasources detected" from
        # exit 2 (config error) and exit 1 (run error). CI guard
        # scripts can branch on it.
        raise typer.Exit(code=3)
