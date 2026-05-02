"""
Parity check orchestrator.

The runner is structured around two thin protocols — a Druid SQL client
and a Pinot SQL client — so unit tests can swap in stubs and avoid
needing live clusters. Real callers wire up clients backed by an
authenticated ``requests.Session`` (see ``migrator.parity.clients``).
"""

from __future__ import annotations

from typing import Protocol

from migrator.parity.models import ParityQuery, ParityResult


# ─────────────────────────────────────────────────────────────────────────────
# Client protocols
# ─────────────────────────────────────────────────────────────────────────────


class DruidSqlClient(Protocol):
    def query(self, sql: str) -> list[dict]:
        """Execute a Druid SQL query, return rows as a list of dicts.

        Druid's ``/druid/v2/sql`` endpoint with ``resultFormat: object``
        returns this shape natively.
        """


class PinotSqlClient(Protocol):
    def query(self, sql: str) -> list[list]:
        """Execute a Pinot SQL query, return rows as ordered list-of-lists.

        Pinot's ``/query/sql`` endpoint returns
        ``resultTable.rows`` in this shape.
        """


# ─────────────────────────────────────────────────────────────────────────────
# Comparison helpers
# ─────────────────────────────────────────────────────────────────────────────


def _values_equal(d, p, tolerance: float) -> bool:
    """True if Druid value ``d`` and Pinot value ``p`` agree within tolerance.

    The two engines have minor type differences worth normalising:

    - Druid emits integer counts as ``int``; Pinot emits SUMs of integer
      columns as ``float``. ``1500 == 1500.0`` is already True under
      Python equality, so for exact-match comparisons we lean on that.
    - For floating-point sums (e.g. SUM of doubles), engines may
      disagree at the last representable bit. The ``tolerance`` knob
      exists exactly for that case — relative error.
    """
    if d is None and p is None:
        return True
    if d is None or p is None:
        return False
    if isinstance(d, (int, float)) and isinstance(p, (int, float)):
        if d == p:
            return True
        if tolerance <= 0:
            return False
        denom = max(abs(d), abs(p), 1.0)
        return abs(d - p) / denom <= tolerance
    return d == p


def _scalar(client_d, client_p, q: ParityQuery) -> ParityResult:
    drows = client_d.query(q.druid)
    prows = client_p.query(q.pinot)
    dv = list(drows[0].values())[0] if drows else None
    pv = prows[0][0] if prows else None
    ok = _values_equal(dv, pv, q.tolerance)
    detail = f"druid={dv}  pinot={pv}"
    if not ok and q.tolerance > 0:
        detail += f"  (tolerance={q.tolerance})"
    return ParityResult(
        label=q.label,
        passed=ok,
        detail=detail,
        druid_value=dv,
        pinot_value=pv,
    )


def _groupby(client_d, client_p, q: ParityQuery) -> ParityResult:
    drows = client_d.query(q.druid)
    prows = client_p.query(q.pinot)

    # Druid returns dicts → take values in declaration order.
    # Pinot returns ordered list-of-lists already.
    d_pairs = [tuple(r.values()) for r in drows]
    p_pairs = [tuple(r) for r in prows]

    d_sorted = sorted(d_pairs)
    p_sorted = sorted(p_pairs)

    if d_sorted == p_sorted:
        return ParityResult(
            label=q.label,
            passed=True,
            detail=f"({len(d_sorted)} groups)",
        )

    # Locate the first divergence so the failure message is useful.
    diff_msg: str
    if len(d_sorted) != len(p_sorted):
        diff_msg = (
            f"druid groups={len(d_sorted)}  pinot groups={len(p_sorted)}"
        )
    else:
        for i, (d, p) in enumerate(zip(d_sorted, p_sorted, strict=True)):
            if d != p:
                diff_msg = f"first diff @ row {i}: druid={d}  pinot={p}"
                break
        else:  # pragma: no cover — equality already returned above
            diff_msg = "diverge but no row-level diff found"
    return ParityResult(label=q.label, passed=False, detail=diff_msg)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────


def run_parity(
    queries: list[ParityQuery],
    *,
    druid: DruidSqlClient,
    pinot: PinotSqlClient,
) -> list[ParityResult]:
    """Run every query against both engines, return a result per query.

    Failures inside a single query (network blips, SQL parse errors)
    don't abort the run — each is captured as ``passed=False`` with the
    exception text in ``detail``. That matches how an operator would
    want this in CI: see *all* failures, not just the first.
    """
    results: list[ParityResult] = []
    for q in queries:
        try:
            if q.type == "groupby":
                results.append(_groupby(druid, pinot, q))
            else:
                results.append(_scalar(druid, pinot, q))
        except Exception as exc:  # noqa: BLE001
            results.append(ParityResult(
                label=q.label,
                passed=False,
                detail=f"ERROR: {exc}",
            ))
    return results
