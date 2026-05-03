"""``dpm parity-check`` — assert query parity across Druid and Pinot."""

from __future__ import annotations

import json as _json
from pathlib import Path

import typer

from migrator.auth import AuthConfigError, session_from_env
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.parity.clients import DruidHttpSqlClient, PinotHttpSqlClient
from migrator.parity.loader import load_queries
from migrator.parity.models import ParityQuery, ParityResult
from migrator.parity.query_builder import derive_queries_from_canonical
from migrator.parity.runner import run_parity


def _load_canonical(path: Path):
    """Load a canonical migration model from a Druid spec or canonical JSON.

    Two file shapes are accepted, distinguished structurally:

    - A Druid ingestion spec (top-level ``type`` and ``spec.dataSchema``
      or top-level ``dataSchema``): parsed via ``DruidSpecParser`` →
      ``DruidNormalizer``. This is the most common input — it lets
      ``parity-check --from-canonical`` work directly off the same
      spec the operator passed to ``dpm generate`` / ``dpm plan-hybrid``.

    - A canonical model JSON (top-level ``datasource_name``,
      ``dimensions``, ``metrics``, etc.): loaded directly via
      ``CanonicalMigrationModel.model_validate``. This is what
      ``dpm generate`` writes to ``canonical.json``.
    """
    import json as _json

    from migrator.core.models import CanonicalMigrationModel

    raw = _json.loads(path.read_text())
    if "datasource_name" in raw and "dimensions" in raw:
        return CanonicalMigrationModel.model_validate(raw)
    parsed = DruidSpecParser().parse(raw)
    norm = DruidNormalizer().normalize(parsed.parsed_spec)
    return norm.canonical


def _print_pretty(results: list[ParityResult]) -> None:
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        typer.echo(f"  {status}  {r.label:<40s} {r.detail}")


def _print_json(results: list[ParityResult]) -> None:
    payload = {
        "results": [r.model_dump() for r in results],
        "passed": sum(1 for r in results if r.passed),
        "failed": sum(1 for r in results if not r.passed),
        "total": len(results),
    }
    typer.echo(_json.dumps(payload, indent=2, default=str))


def command(
    queries: Path | None = typer.Option(
        None,
        "--queries",
        help=(
            "Path to a YAML or JSON file describing the parity queries. "
            "Each entry has a label, druid SQL, pinot SQL, and an optional "
            "type (scalar | groupby) and tolerance. Mutually exclusive with "
            "--from-canonical."
        ),
    ),
    from_canonical: Path | None = typer.Option(
        None,
        "--from-canonical",
        help=(
            "Auto-derive a default parity-query set from a Druid spec or a "
            "canonical migration model JSON. Generates: total event count, "
            "SUM/MIN/MAX per metric, COUNT grouped by each single-value "
            "dimension. Mutually exclusive with --queries."
        ),
    ),
    pinot_table: str | None = typer.Option(
        None,
        "--pinot-table",
        help=(
            "Override the Pinot table name used by --from-canonical "
            "queries (defaults to the canonical datasource name)."
        ),
    ),
    druid_url: str = typer.Option(
        "http://localhost:8888",
        "--druid-url",
        help="Druid Router (or Broker) base URL.",
    ),
    pinot_broker: str = typer.Option(
        "http://localhost:8099",
        "--pinot-broker",
        help="Pinot Broker base URL.",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print the results as JSON instead of pretty text."
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
        help="Skip TLS verification when talking to Druid.",
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
            "Pinot broker auth: 'basic:user:pass', 'bearer:<token>', "
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
        help="Skip TLS verification when talking to Pinot.",
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
) -> None:
    """Run parity queries against Druid and Pinot, exit non-zero on divergence.

    Aimed at the post-migration validation step: codifies the
    "run the same SQL on both sides and assert equality" pattern that
    every operator writes by hand otherwise.
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

    if (queries is None) == (from_canonical is None):
        typer.echo(
            "Pass exactly one of --queries (manual list) or "
            "--from-canonical (auto-derive from a Druid spec).",
            err=True,
        )
        raise typer.Exit(code=2)

    parity_queries: list[ParityQuery]
    if queries is not None:
        try:
            spec = load_queries(queries)
        except Exception as exc:
            typer.echo(f"Failed to load queries file: {exc}", err=True)
            raise typer.Exit(code=2)
        parity_queries = spec.queries
    else:
        try:
            canonical = _load_canonical(from_canonical)
        except Exception as exc:
            typer.echo(
                f"Failed to load canonical from {from_canonical}: {exc}",
                err=True,
            )
            raise typer.Exit(code=2)
        parity_queries = derive_queries_from_canonical(
            canonical, pinot_table=pinot_table,
        )

    druid_client = DruidHttpSqlClient(druid_url, session=druid_session)
    pinot_client = PinotHttpSqlClient(pinot_broker, session=pinot_session)

    results = run_parity(parity_queries, druid=druid_client, pinot=pinot_client)

    if json_output:
        _print_json(results)
    else:
        typer.echo("Parity check")
        typer.echo("─" * 60)
        _print_pretty(results)
        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed
        typer.echo("")
        typer.echo(
            f"Result: {passed} passed, {failed} failed (out of {len(results)})"
        )

    if any(not r.passed for r in results):
        raise typer.Exit(code=1)
