"""
Column-presence parity — detect columns that exist in Druid but are
missing or mostly-null in the migrated Pinot table.

Different intent from the existing aggregate-parity (``run_parity``):
that one validates ``SUM(x)`` matches between sides; this one
validates that ``x`` made it to Pinot at all and isn't being silently
dropped by a type mismatch / encoding bug / filter.

Per the operator's scope decision: dimension VALUE comparisons and
metric VALUE comparisons are explicitly out of scope. Custom
aggregation logic varies enough across migrations that locking it in
generates noise, not signal. We focus on the two failure modes that
are unambiguously "something went wrong":

  - ``MISSING_FROM_PINOT`` — column exists in the canonical model
    (i.e. dpm thinks it should be in the migrated table) but
    Pinot's ``SELECT <col>`` returns 0 rows / errors. Either the
    schema didn't deploy or the column name changed mid-migration.
  - ``NULL_RATE_DIVERGENCE`` — column is present in both but Pinot's
    null rate is materially higher than Druid's. Surfaces type-
    mapping accidents (STRING → LONG conversion that turns text
    into NULL), broken transforms (the ``amount → amount_sum``
    rename misfired), and time-zone bugs that bucket data outside
    the parity window.

Threshold knobs let operators tune signal/noise — the default
``null_rate_tolerance=0.10`` (10 percentage-point margin) is
deliberately loose since Druid and Pinot don't agree on null
semantics for empty strings vs missing fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from migrator.core.models import CanonicalMigrationModel
from migrator.parity.models import ParityResult


@dataclass
class ColumnPresenceCheck:
    """One column's presence verdict. The aggregate ``run_column_presence``
    returns a list of these alongside ``ParityResult``-shaped objects so
    the existing report renderer reuses them."""
    column: str
    kind: str               # ``MISSING_FROM_PINOT`` | ``NULL_RATE_DIVERGENCE`` | ``OK``
    druid_null_rate: float | None
    pinot_null_rate: float | None
    detail: str


def _safe_float(v: Any) -> float | None:
    """Coerce a SQL result cell to a float, tolerating None / strings.
    Returns None when the cell isn't a finite number — callers treat
    that as "no signal" and the check skips."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def _druid_null_rate(
    client: Any, datasource: str, column: str,
) -> float | None:
    """Return the fraction of rows where ``column`` is NULL in Druid.

    ``COUNT(*) - COUNT(<col>)`` is the canonical SQL trick that
    works on every engine that follows the standard COUNT(non-null)
    semantics — Druid included. Returns None on any query error so
    the caller can skip the check rather than report a false
    NULL_RATE_DIVERGENCE.
    """
    sql = (
        f'SELECT '
        f'CAST(COUNT(*) AS DOUBLE) AS total, '
        f'CAST(COUNT(*) - COUNT("{column}") AS DOUBLE) AS nulls '
        f'FROM "{datasource}"'
    )
    try:
        rows = client.query(sql)
    except Exception:  # noqa: BLE001 — surface as "no signal", not failure
        return None
    if not rows:
        return None
    row = rows[0]
    total = _safe_float(row.get("total") if isinstance(row, dict) else row[0])
    nulls = _safe_float(row.get("nulls") if isinstance(row, dict) else row[1])
    if not total or total <= 0:
        return None
    if nulls is None:
        return None
    return max(0.0, min(1.0, nulls / total))


def _pinot_null_rate(
    client: Any, table: str, column: str,
) -> tuple[float | None, bool]:
    """Return ``(null_rate, column_exists)`` for ``column`` in Pinot.

    ``column_exists=False`` is the load-bearing signal for the
    ``MISSING_FROM_PINOT`` verdict — when Pinot's SQL layer rejects
    the query because the column isn't in the schema, we don't want
    to report "100% null"; we want "this column never landed."
    """
    sql = (
        f'SELECT '
        f'CAST(COUNT(*) AS DOUBLE) AS total, '
        f'CAST(COUNT(*) - COUNT("{column}") AS DOUBLE) AS nulls '
        f'FROM {table}'
    )
    try:
        rows = client.query(sql)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).lower()
        # Pinot's broker returns column-not-found in the
        # ``exceptions`` array of the response body; the
        # PinotHttpSqlClient wraps that in a RuntimeError whose
        # message contains the upstream text. Heuristic substring
        # match — different Pinot versions phrase it differently.
        if any(s in msg for s in (
            "cannot find", "unknown column", "no field with name",
            "could not be resolved", "unknown identifier",
        )):
            return None, False
        return None, True   # Some other failure; treat as "exists but no signal"
    if not rows:
        return None, True
    row = rows[0]
    if isinstance(row, dict):
        total = _safe_float(row.get("total"))
        nulls = _safe_float(row.get("nulls"))
    else:
        total = _safe_float(row[0])
        nulls = _safe_float(row[1])
    if not total or total <= 0:
        return None, True
    if nulls is None:
        return None, True
    return max(0.0, min(1.0, nulls / total)), True


def run_column_presence(
    canonical: CanonicalMigrationModel,
    *,
    druid_client: Any,
    pinot_client: Any,
    pinot_table: str,
    null_rate_tolerance: float = 0.10,
) -> list[ParityResult]:
    """Walk every dimension + metric in the canonical model and emit
    one ``ParityResult`` per column.

    Three outcomes per column:

      - **passed=True** — column present in Pinot AND null-rate
        within tolerance of Druid's (or both sides return no
        signal, in which case we don't fail; a missing baseline
        from Druid means we can't draw conclusions).
      - **passed=False, ``MISSING_FROM_PINOT``** — Pinot rejected
        the query as unknown column. Almost always a real problem.
      - **passed=False, ``NULL_RATE_DIVERGENCE``** — Pinot's null
        rate is more than ``null_rate_tolerance`` higher than
        Druid's. Operator should investigate the type mapping or
        ingestion transform for that column.

    Time field is included (it's the most common type-mapping
    failure point — ISO ↔ epoch-ms confusion).
    """
    results: list[ParityResult] = []
    columns: list[str] = []
    if canonical.time_field:
        columns.append(canonical.time_field.column_name)
    columns.extend(d.name for d in canonical.dimensions)
    columns.extend(m.name for m in canonical.metrics)

    # Druid datasource name = canonical.datasource_name (Druid
    # tables don't have an OFFLINE/REALTIME suffix). The Pinot
    # table name is provided by the caller — usually identical to
    # the datasource but operators sometimes rename mid-migration.
    druid_ds = canonical.datasource_name

    for col in columns:
        druid_rate = _druid_null_rate(druid_client, druid_ds, col)
        pinot_rate, exists = _pinot_null_rate(pinot_client, pinot_table, col)

        if not exists:
            results.append(ParityResult(
                label=f"column presence: {col}",
                passed=False,
                detail=(
                    f"column '{col}' exists in canonical model but "
                    f"Pinot rejects the query as unknown column — "
                    f"check the deployed schema."
                ),
                druid_value=druid_rate,
                pinot_value=None,
            ))
            continue

        if druid_rate is None or pinot_rate is None:
            # No signal from one side — neither pass nor fail.
            # The verdict is "couldn't measure"; we still emit a
            # result so the report enumerates every column.
            results.append(ParityResult(
                label=f"column presence: {col}",
                passed=True,
                detail=(
                    f"column '{col}': no signal "
                    f"(druid_null_rate={druid_rate}, pinot_null_rate={pinot_rate})"
                ),
                druid_value=druid_rate,
                pinot_value=pinot_rate,
            ))
            continue

        delta = pinot_rate - druid_rate
        if delta > null_rate_tolerance:
            results.append(ParityResult(
                label=f"column presence: {col}",
                passed=False,
                detail=(
                    f"column '{col}': null-rate divergence "
                    f"druid={druid_rate:.1%} pinot={pinot_rate:.1%} "
                    f"(Δ={delta:+.1%}, tolerance={null_rate_tolerance:.0%}) — "
                    f"check ingestion transform / type mapping."
                ),
                druid_value=druid_rate,
                pinot_value=pinot_rate,
            ))
        else:
            results.append(ParityResult(
                label=f"column presence: {col}",
                passed=True,
                detail=(
                    f"column '{col}': null-rates match "
                    f"druid={druid_rate:.1%} pinot={pinot_rate:.1%}"
                ),
                druid_value=druid_rate,
                pinot_value=pinot_rate,
            ))
    return results
