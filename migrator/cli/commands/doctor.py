"""``dpm doctor`` — preflight checks against Druid + Pinot.

Run this *before* a long ``dpm cutover`` to catch the obvious problems
fast: an unreachable controller, a wrong URL, a missing datasource, a
typoed tenant name. Each probe is HTTP-level only (no
data-mutating calls) and the whole pass takes a few seconds.

Exit code is 0 when every selected probe passes; 1 if any fails.
"""

from __future__ import annotations

import json as _json

import typer
from rich.console import Console

from migrator.auth import AuthConfigError, session_from_env
from migrator.preflight import (
    PreflightCheck,
    probe_druid_datasource,
    probe_druid_router,
    probe_pinot_broker,
    probe_pinot_controller,
    probe_pinot_tenant,
)


_console = Console()


def command(
    druid_router: str = typer.Option(
        "http://localhost:8888",
        "--druid-router",
        help="Druid Router (or Broker) base URL.",
    ),
    druid_coordinator: str | None = typer.Option(
        None,
        "--druid-coordinator",
        help=(
            "Druid Coordinator base URL. Defaults to --druid-router. "
            "Pass an explicit URL when --datasource is set and the "
            "Coordinator lives on a different host."
        ),
    ),
    pinot_controller: str = typer.Option(
        "http://localhost:9000",
        "--pinot-controller",
        help="Pinot Controller base URL.",
    ),
    pinot_broker: str | None = typer.Option(
        None,
        "--pinot-broker",
        help=(
            "Pinot Broker base URL. If unset, broker reachability is "
            "skipped — useful when only the controller is exposed."
        ),
    ),
    datasource: str | None = typer.Option(
        None,
        "--datasource",
        help="Optional Druid datasource to verify exists.",
    ),
    pinot_tenant: str | None = typer.Option(
        None,
        "--pinot-tenant",
        help=(
            "Optional Pinot tenant to verify exists "
            "(checked against SERVER_TENANTS and BROKER_TENANTS)."
        ),
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Emit a structured JSON report instead of pretty text.",
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
        help="Client certificate for Pinot mTLS. Env DPM_PINOT_CERT.",
    ),
    pinot_key: str | None = typer.Option(
        None, "--pinot-key",
        help="Client key for Pinot mTLS. Env DPM_PINOT_KEY.",
    ),
) -> None:
    """Probe Druid + Pinot for connectivity, version, and optional config presence."""
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

    coordinator_url = druid_coordinator or druid_router

    checks: list[PreflightCheck] = []
    checks.append(probe_druid_router(druid_session, druid_router))
    checks.append(probe_pinot_controller(pinot_session, pinot_controller))
    if pinot_broker:
        checks.append(probe_pinot_broker(pinot_session, pinot_broker))
    if datasource:
        checks.append(probe_druid_datasource(
            druid_session, coordinator_url, datasource,
        ))
    if pinot_tenant:
        checks.append(probe_pinot_tenant(
            pinot_session, pinot_controller, pinot_tenant,
        ))

    if json_output:
        payload = {
            "ok": all(c.ok for c in checks),
            "checks": [c.to_dict() for c in checks],
        }
        typer.echo(_json.dumps(payload, indent=2))
    else:
        _console.print("[bold]Doctor[/bold]")
        _console.print("─" * 60)
        for c in checks:
            marker = "[green]✓[/green]" if c.ok else "[red]✗[/red]"
            _console.print(
                f"  {marker} {c.name:<22s} {c.target:<32s} {c.detail}"
            )
        passed = sum(1 for c in checks if c.ok)
        failed = len(checks) - passed
        _console.print("")
        _console.print(f"Result: {passed} ok, {failed} failed")

    if any(not c.ok for c in checks):
        raise typer.Exit(code=1)
