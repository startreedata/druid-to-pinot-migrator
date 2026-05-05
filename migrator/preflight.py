"""
Preflight probes for ``dpm doctor``.

Goal: surface connectivity / version / config problems *before* a long
``dpm cutover`` would fail mid-stream. Each probe returns a
``PreflightCheck`` so the CLI can render a green/red list and exit 1
on any failure.

The functions here take an injectable ``session`` (the same shape as
``requests.Session``) so the doctor command can plumb in an
already-authenticated session built by ``migrator.auth.session_from_env``,
and so unit tests can inject fakes without monkey-patching ``requests``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Protocol


@dataclass
class PreflightCheck:
    """Result of a single preflight probe.

    ``ok`` is the binary verdict; ``detail`` is a one-line human-
    readable summary that's good enough to render in the CLI without
    further formatting; ``data`` is structured payload (e.g. version
    string, list of datasources) for the ``--json`` output.
    """
    name: str
    target: str
    ok: bool
    detail: str = ""
    data: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # Drop None payloads to keep --json output tight.
        if d.get("data") is None:
            d.pop("data")
        return d


class _Session(Protocol):
    """Slice of ``requests.Session`` the probes actually use."""

    def get(self, url: str, *, timeout: float | None = ...) -> Any: ...


def _safe_get(
    session: _Session, url: str, *, timeout: float = 5.0,
) -> tuple[int | None, Any, str]:
    """Issue a GET, never raise. Return (status, body, error-string).

    Catches every exception so a single unreachable endpoint doesn't
    abort the whole preflight pass.
    """
    try:
        resp = session.get(url, timeout=timeout)
    except Exception as exc:  # noqa: BLE001 — preflight must keep going
        return None, None, str(exc)
    status = resp.status_code
    body: Any = None
    try:
        body = resp.json()
    except Exception:  # noqa: BLE001
        body = getattr(resp, "text", "")
    return status, body, ""


# ─────────────────────────────────────────────────────────────────────────────
# Druid probes
# ─────────────────────────────────────────────────────────────────────────────


def probe_druid_router(session: _Session, router_url: str) -> PreflightCheck:
    """Check the Druid Router/Broker is reachable and report its version.

    Druid's ``/status`` endpoint returns ``{"version": "...", ...}`` —
    cheap, doesn't depend on any datasource or supervisor existing.
    """
    url = f"{router_url.rstrip('/')}/status"
    status, body, err = _safe_get(session, url)
    if err:
        return PreflightCheck(
            "druid-router", router_url, ok=False,
            detail=f"unreachable: {err}",
        )
    if status != 200:
        return PreflightCheck(
            "druid-router", router_url, ok=False,
            detail=f"HTTP {status}",
        )
    version = (body or {}).get("version") if isinstance(body, dict) else None
    return PreflightCheck(
        "druid-router", router_url, ok=True,
        detail=f"version {version}" if version else "reachable",
        data={"version": version} if version else None,
    )


def probe_druid_datasource(
    session: _Session, coordinator_url: str, datasource: str,
) -> PreflightCheck:
    """Check a specific datasource exists on the Coordinator.

    Uses the lightweight ``/datasources`` listing rather than fetching
    full metadata — we only need to know the name is present.
    """
    url = f"{coordinator_url.rstrip('/')}/druid/coordinator/v1/datasources"
    status, body, err = _safe_get(session, url)
    if err:
        return PreflightCheck(
            "druid-datasource", datasource, ok=False,
            detail=f"coordinator unreachable: {err}",
        )
    if status != 200:
        return PreflightCheck(
            "druid-datasource", datasource, ok=False,
            detail=f"HTTP {status} from coordinator",
        )
    names = body if isinstance(body, list) else []
    if datasource not in names:
        return PreflightCheck(
            "druid-datasource", datasource, ok=False,
            detail=f"not found (cluster has {len(names)} datasources)",
        )
    return PreflightCheck(
        "druid-datasource", datasource, ok=True, detail="exists",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pinot probes
# ─────────────────────────────────────────────────────────────────────────────


def probe_pinot_controller(
    session: _Session, controller_url: str,
) -> PreflightCheck:
    """Check the Pinot Controller is reachable and report its version.

    Pinot 1.x exposes ``/version`` which returns a JSON object keyed by
    component (``{"pinot-controller": "1.5.0", ...}``); ``/health``
    returns 200 OK plus ``"OK"`` text. We hit ``/version`` because the
    output is structured.
    """
    url = f"{controller_url.rstrip('/')}/version"
    status, body, err = _safe_get(session, url)
    if err:
        return PreflightCheck(
            "pinot-controller", controller_url, ok=False,
            detail=f"unreachable: {err}",
        )
    if status != 200:
        return PreflightCheck(
            "pinot-controller", controller_url, ok=False,
            detail=f"HTTP {status}",
        )
    # /version returns a dict; pull whichever controller-flavoured key
    # is present (different distros use different keys).
    version: str | None = None
    if isinstance(body, dict):
        for k, v in body.items():
            if "controller" in k.lower() and isinstance(v, str):
                version = v
                break
        if version is None and body:
            # Fall back to the first stringy value — better than nothing.
            for v in body.values():
                if isinstance(v, str):
                    version = v
                    break
    return PreflightCheck(
        "pinot-controller", controller_url, ok=True,
        detail=f"version {version}" if version else "reachable",
        data={"version": version} if version else None,
    )


def probe_pinot_broker(
    session: _Session, broker_url: str,
) -> PreflightCheck:
    """Check the Pinot Broker's ``/health`` endpoint."""
    url = f"{broker_url.rstrip('/')}/health"
    status, body, err = _safe_get(session, url)
    if err:
        return PreflightCheck(
            "pinot-broker", broker_url, ok=False,
            detail=f"unreachable: {err}",
        )
    if status != 200:
        return PreflightCheck(
            "pinot-broker", broker_url, ok=False,
            detail=f"HTTP {status}",
        )
    return PreflightCheck(
        "pinot-broker", broker_url, ok=True, detail="reachable",
    )


def probe_pinot_tenant(
    session: _Session, controller_url: str, tenant: str,
) -> PreflightCheck:
    """Check a Pinot tenant exists.

    Pinot's ``/tenants`` endpoint returns
    ``{"SERVER_TENANTS": [...], "BROKER_TENANTS": [...]}``. We accept a
    match on either side — operators sometimes pass just a server-tier
    name, sometimes a broker-tier name.
    """
    url = f"{controller_url.rstrip('/')}/tenants"
    status, body, err = _safe_get(session, url)
    if err:
        return PreflightCheck(
            "pinot-tenant", tenant, ok=False,
            detail=f"controller unreachable: {err}",
        )
    if status != 200:
        return PreflightCheck(
            "pinot-tenant", tenant, ok=False,
            detail=f"HTTP {status}",
        )
    server = (body or {}).get("SERVER_TENANTS", []) if isinstance(body, dict) else []
    broker = (body or {}).get("BROKER_TENANTS", []) if isinstance(body, dict) else []
    if tenant in server or tenant in broker:
        kind = []
        if tenant in server:
            kind.append("server")
        if tenant in broker:
            kind.append("broker")
        return PreflightCheck(
            "pinot-tenant", tenant, ok=True,
            detail=f"exists ({'+'.join(kind)})",
        )
    return PreflightCheck(
        "pinot-tenant", tenant, ok=False,
        detail=(
            f"not found (server tenants: {server or '[]'}, "
            f"broker tenants: {broker or '[]'})"
        ),
    )
