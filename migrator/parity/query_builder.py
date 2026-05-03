"""
Derive a default parity-query set from a canonical migration model.

The intent is to give operators a "useful out of the box" parity gate
without forcing them to hand-write a queries YAML. The auto-derived
set covers the high-leverage checks that almost every migration cares
about:

- Total row / event count
- SUM / MIN / MAX of every numeric metric (per the metric's aggregator)
- COUNT (or SUM-of-count under rollup) per dimension, grouped

The hard part is rollup semantics: Druid's `COUNT(*)` returns
post-rollup row count, while Pinot stores raw events (in a non-rollup
Pinot table). We resolve that the same way the docs always recommend:
when the canonical model declares rollup + a `count`-type metric,
``COUNT(*)`` on Druid is replaced with ``SUM(<count_metric>)`` so the
two engines agree on the original event count.
"""

from __future__ import annotations

from migrator.core.models import CanonicalMigrationModel, MetricField
from migrator.parity.models import ParityQuery


# Druid SQL aggregator → SQL function used in the parity query.
# For ``count`` we generate SUM(<name>) on Druid (pre-aggregated count
# metric) and COUNT(*) on Pinot, which is the rollup-mismatch trick.
_AGG_TO_SQL: dict[str, str] = {
    "longsum": "SUM",
    "doublesum": "SUM",
    "floatsum": "SUM",
    "longmin": "MIN",
    "doublemin": "MIN",
    "floatmin": "MIN",
    "longmax": "MAX",
    "doublemax": "MAX",
    "floatmax": "MAX",
}


def _q_druid(ident: str) -> str:
    """Quote an identifier for Druid SQL (double quotes)."""
    return '"' + ident.replace('"', '""') + '"'


def _q_pinot(ident: str) -> str:
    """Quote an identifier for Pinot SQL (double quotes)."""
    return '"' + ident.replace('"', '""') + '"'


def _count_metric(canonical: CanonicalMigrationModel) -> MetricField | None:
    """Return the first ``count``-type metric in the canonical model."""
    for m in canonical.metrics:
        if m.druid_type.lower() == "count":
            return m
    return None


def _aggregate_query(
    canonical: CanonicalMigrationModel,
    metric: MetricField,
    *,
    druid_table: str,
    pinot_table: str,
) -> ParityQuery | None:
    """One ``SUM/MIN/MAX(metric)`` parity query, or ``None`` if the
    metric has no SQL-equivalent aggregator (e.g. sketch-typed metrics
    we don't support comparing yet)."""
    sql_agg = _AGG_TO_SQL.get(metric.druid_type.lower())
    if sql_agg is None:
        return None
    name = metric.name
    return ParityQuery(
        label=f"{sql_agg}({name})",
        druid=(
            f"SELECT {sql_agg}({_q_druid(name)}) AS v "
            f"FROM {_q_druid(druid_table)}"
        ),
        pinot=(
            f"SELECT {sql_agg}({_q_pinot(name)}) "
            f"FROM {_q_pinot(pinot_table)}"
        ),
    )


def _total_count_query(
    canonical: CanonicalMigrationModel,
    *,
    druid_table: str,
    pinot_table: str,
) -> ParityQuery:
    """Total event-count parity query.

    Under rollup, Druid's ``COUNT(*)`` returns post-rollup row count.
    The original event count is preserved in the ``count`` metric, so
    we use ``SUM(<count_metric>)`` on Druid and ``COUNT(*)`` on Pinot
    (which has no rollup). When there's no ``count`` metric, fall
    back to ``COUNT(*)`` on both — the right thing for raw events.
    """
    count_metric = _count_metric(canonical)
    if count_metric is not None and canonical.granularity.rollup:
        druid_sql = (
            f"SELECT SUM({_q_druid(count_metric.name)}) AS v "
            f"FROM {_q_druid(druid_table)}"
        )
    else:
        druid_sql = f"SELECT COUNT(*) AS v FROM {_q_druid(druid_table)}"
    pinot_sql = f"SELECT COUNT(*) FROM {_q_pinot(pinot_table)}"
    return ParityQuery(
        label="Total event count",
        druid=druid_sql,
        pinot=pinot_sql,
    )


def _groupby_count_query(
    canonical: CanonicalMigrationModel,
    dim_name: str,
    *,
    druid_table: str,
    pinot_table: str,
) -> ParityQuery:
    """``COUNT`` (or ``SUM(count_metric)``) grouped by a single dimension."""
    count_metric = _count_metric(canonical)
    if count_metric is not None and canonical.granularity.rollup:
        d_select = f"SUM({_q_druid(count_metric.name)})"
        p_select = "COUNT(*)"
    else:
        d_select = "COUNT(*)"
        p_select = "COUNT(*)"
    return ParityQuery(
        label=f"events by {dim_name}",
        druid=(
            f"SELECT {_q_druid(dim_name)}, {d_select} "
            f"FROM {_q_druid(druid_table)} "
            f"GROUP BY {_q_druid(dim_name)} "
            f"ORDER BY {_q_druid(dim_name)}"
        ),
        pinot=(
            f"SELECT {_q_pinot(dim_name)}, {p_select} "
            f"FROM {_q_pinot(pinot_table)} "
            f"GROUP BY {_q_pinot(dim_name)} "
            f"ORDER BY {_q_pinot(dim_name)}"
        ),
        type="groupby",
    )


def derive_queries_from_canonical(
    canonical: CanonicalMigrationModel,
    *,
    druid_table: str | None = None,
    pinot_table: str | None = None,
) -> list[ParityQuery]:
    """Auto-generate a sensible default parity-query set.

    ``druid_table`` and ``pinot_table`` default to
    ``canonical.datasource_name`` — pass them explicitly only if your
    Pinot table name differs (e.g. you renamed it on cutover).

    The order is intentional: the cheapest, highest-signal check
    (total count) runs first; per-metric scalars next; per-dimension
    groupbys last. Operators reading the report top-down see the
    "did this migration land at all" signal before the long tail of
    fine-grained checks.
    """
    druid_table = druid_table or canonical.datasource_name
    pinot_table = pinot_table or canonical.datasource_name

    queries: list[ParityQuery] = []
    queries.append(_total_count_query(
        canonical, druid_table=druid_table, pinot_table=pinot_table,
    ))

    for m in canonical.metrics:
        # The total-count query already covers the count metric — skip it
        # here so the report doesn't list it twice.
        if m.druid_type.lower() == "count":
            continue
        q = _aggregate_query(
            canonical, m,
            druid_table=druid_table, pinot_table=pinot_table,
        )
        if q is not None:
            queries.append(q)

    for dim in canonical.dimensions:
        # Only single-value dimensions get a GROUP BY query — multi-
        # value semantics differ between engines (each MV value
        # contributes a row in Pinot's GROUP BY) and would diverge
        # without the operator opting in. Worth surfacing as an
        # opt-in once we have a flag for it.
        if getattr(dim, "multi_value", False):
            continue
        queries.append(_groupby_count_query(
            canonical, dim.name,
            druid_table=druid_table, pinot_table=pinot_table,
        ))

    return queries
