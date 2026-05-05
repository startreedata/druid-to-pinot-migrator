"""
Pinot indexing + aggregator recommendations from a Druid canonical model.

The risk analyzer surfaces *problems* (unsupported types, partitionsSpec
needs manual mapping). This module surfaces *opportunities* — Pinot
features that would meaningfully improve the migrated table's
ergonomics or query latency, derived from what the canonical model
already tells us:

  - **Star-tree** when the table has dimensions + metrics. Star-tree
    pre-aggregates GROUP BY combinations, which is the single biggest
    wall-clock win on dashboard-style queries that did fine in Druid
    only because of its segment-level rollup.
  - **DistinctCountHLL / DistinctCountThetaSketch** when the Druid
    spec uses ``hyperUnique`` / ``thetaSketch`` aggregators. Pinot has
    direct equivalents; without recommending them an operator typically
    leaves them as plain DISTINCT(*) which is orders of magnitude slower.
  - **Sorted column** on the time column. Pinot can only have one
    sorted column per segment; the time column is right >95% of the
    time for hybrid migrations.
  - **Inverted / bloom-filter index** on high-cardinality string
    dimensions used as equality filters. Without query-log access this
    is a heuristic — names ending in ``_id``, ``_uuid``, or matching
    common identifier patterns get the recommendation.
  - **Range index** on numeric metrics. Range-filterable, and Pinot's
    range index hugely accelerates ``BETWEEN`` / ``>= AND <=`` queries.

What's deliberately NOT recommended without more signal:
  - Specific star-tree dimension orderings — needs cardinality data.
  - Per-column compression / encoding — Pinot's default is fine and
    overrides require deployment-specific benchmarking.

Future work: a query-log ingester that promotes some heuristics to
data-driven recommendations (e.g. "5% of queries filter on
``user_country``; bloom-filter it") and downgrades others.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    MetricField,
)


# ─────────────────────────────────────────────────────────────────────────────
# Recommendation type
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Recommendation:
    """One Pinot-side optimisation suggestion.

    ``config_hint`` is a partial JSON snippet operators can drop into
    their generated table config to apply the recommendation. Hints
    are conservative — pre-canonical rather than fully merged — so
    callers can review before pasting in.
    """
    kind: str          # star_tree | aggregator | sorted_column | inverted_index | bloom_filter | range_index
    target: str        # column name or "<table>"
    severity: str      # high | medium | low
    rationale: str
    config_hint: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "target": self.target,
            "severity": self.severity,
            "rationale": self.rationale,
            "config_hint": self.config_hint,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Heuristics
# ─────────────────────────────────────────────────────────────────────────────


# Druid sketch / cardinality aggregations and their Pinot counterparts.
# Listed by Druid metric ``druid_type`` so the canonical-model loop
# can map directly without re-parsing the source spec.
_SKETCH_TO_PINOT_AGG = {
    "hyperUnique": "DistinctCountHLL",
    "HLLSketch":   "DistinctCountHLL",
    "thetaSketch": "DistinctCountThetaSketch",
    "cardinality": "DistinctCountHLL",
    "quantilesDoublesSketch": "PercentileTDigest",
    "fixedBucketsHistogram":  "PercentileTDigest",
    "momentSketch": "PercentileTDigest",
}


# Heuristic regex for "this is probably a high-cardinality identifier".
# Matches names ending in or containing these patterns. Conservative
# on false positives because a wrong recommendation here is just
# noise; missing a real id-column means we don't propose an index.
_ID_LIKE_PATTERN = re.compile(
    r"(_(id|uuid|guid|key|token)$|^(id|uuid|guid|key)$)",
    re.IGNORECASE,
)


def _is_id_like(name: str) -> bool:
    return bool(_ID_LIKE_PATTERN.search(name))


# ─────────────────────────────────────────────────────────────────────────────
# Recommendation pipeline
# ─────────────────────────────────────────────────────────────────────────────


def recommend(canonical: CanonicalMigrationModel) -> list[Recommendation]:
    """Compute the full set of Pinot recommendations for a canonical model.

    Order of returned items reflects severity: high-impact first
    (star-tree, sketch aggregator swaps), then medium (sorted column,
    range index), then low (heuristic bloom / inverted on id-like
    dims). Callers can re-sort if they prefer.
    """
    out: list[Recommendation] = []
    out.extend(_recommend_star_tree(canonical))
    out.extend(_recommend_sketch_aggregators(canonical))
    out.extend(_recommend_sorted_column(canonical))
    out.extend(_recommend_range_index_on_metrics(canonical))
    out.extend(_recommend_inverted_index_on_id_dims(canonical))
    out.extend(_recommend_bloom_filter_on_id_dims(canonical))
    return out


def _recommend_star_tree(c: CanonicalMigrationModel) -> list[Recommendation]:
    """Star-tree fits when there are dims AND metrics — the typical
    rolled-up-event shape. Without cardinality data we can't pick the
    optimal split column, so we emit a *template* config the operator
    fills in — better than nothing, and clearly flagged."""
    if not (c.dimensions and c.metrics):
        return []
    dim_names = [d.name for d in c.dimensions]
    # Aggregations to pre-compute: every metric with a SUM-flavoured
    # rollup translates 1:1 to Pinot's star-tree functionSpec.
    function_specs: list[dict[str, Any]] = []
    for m in c.metrics:
        agg = m.aggregation.upper() if m.aggregation else ""
        if agg in {"SUM", "MIN", "MAX", "COUNT"}:
            function_specs.append({
                "functionType": agg,
                "column": m.field_name or m.name,
            })
    config_hint = {
        "tableIndexConfig": {
            "starTreeIndexConfigs": [{
                "dimensionsSplitOrder": dim_names,
                # ``skipStarNodeCreationForDimensions`` is left empty —
                # operator should populate after looking at cardinality.
                "skipStarNodeCreationForDimensions": [],
                "functionColumnPairs": [
                    f"{spec['functionType']}__{spec['column']}"
                    for spec in function_specs
                ],
                "maxLeafRecords": 10000,
            }],
        },
    }
    return [Recommendation(
        kind="star_tree",
        target=c.datasource_name or "<table>",
        severity="high",
        rationale=(
            f"{len(c.dimensions)} dims × {len(c.metrics)} metrics — "
            "star-tree pre-aggregates GROUP BY combos and is the "
            "single biggest latency win for dashboard-style queries. "
            "Set ``skipStarNodeCreationForDimensions`` based on actual "
            "cardinality (skip the highest-cardinality dim)."
        ),
        config_hint=config_hint,
    )]


def _recommend_sketch_aggregators(
    c: CanonicalMigrationModel,
) -> list[Recommendation]:
    """Druid sketch metrics map to Pinot equivalents that Pinot's
    auto-mapping pipeline doesn't pick up by default."""
    out: list[Recommendation] = []
    for m in c.metrics:
        replacement = _SKETCH_TO_PINOT_AGG.get(m.druid_type)
        if not replacement:
            continue
        out.append(Recommendation(
            kind="aggregator",
            target=m.name,
            severity="high",
            rationale=(
                f"Druid metric '{m.name}' uses {m.druid_type}; the "
                f"Pinot equivalent {replacement} is much faster than "
                "the dpm-default DISTINCT(*) fallback."
            ),
            config_hint={
                "ingestionConfig": {
                    "transformConfigs": [{
                        "columnName": m.name,
                        "transformFunction": f"{replacement}({m.field_name or m.name})",
                    }],
                },
                "tableIndexConfig": {
                    "aggregateMetrics": True,
                },
            },
        ))
    return out


def _recommend_sorted_column(
    c: CanonicalMigrationModel,
) -> list[Recommendation]:
    """The time column is the best sorted-column choice >95% of the
    time. Pinot allows only one sorted column per segment, so we
    don't try to be clever — recommend time and let the operator
    override if they have a better idea."""
    if c.time_field is None or not c.time_field.column_name:
        return []
    return [Recommendation(
        kind="sorted_column",
        target=c.time_field.column_name,
        severity="medium",
        rationale=(
            "Sort segments by the time column to accelerate "
            "time-range filters and time-series GROUP BY. Pinot allows "
            "exactly one sorted column per segment; time is the right "
            "default for hybrid (REALTIME + OFFLINE) tables."
        ),
        config_hint={
            "tableIndexConfig": {
                "sortedColumn": [c.time_field.column_name],
            },
        },
    )]


def _recommend_range_index_on_metrics(
    c: CanonicalMigrationModel,
) -> list[Recommendation]:
    """Numeric metrics are commonly range-filtered (``amount BETWEEN
    100 AND 200``); Pinot's range index speeds those up significantly.
    Skip recommendation for the time column (which is sorted and
    therefore already range-fast)."""
    out: list[Recommendation] = []
    skip = (
        {c.time_field.column_name} if c.time_field else set()
    )
    numeric_metric_names = [
        m.name for m in c.metrics
        if m.pinot_type in {"INT", "LONG", "FLOAT", "DOUBLE"}
        and m.name not in skip
    ]
    if not numeric_metric_names:
        return out
    out.append(Recommendation(
        kind="range_index",
        target=", ".join(numeric_metric_names),
        severity="medium",
        rationale=(
            f"{len(numeric_metric_names)} numeric metric(s); range-"
            "indexed columns serve BETWEEN / `>=` / `<=` filters via "
            "skip-list lookup instead of a full scan. Cheap to add at "
            "table-create time."
        ),
        config_hint={
            "tableIndexConfig": {
                "rangeIndexColumns": list(numeric_metric_names),
            },
        },
    ))
    return out


def _recommend_inverted_index_on_id_dims(
    c: CanonicalMigrationModel,
) -> list[Recommendation]:
    out: list[Recommendation] = []
    targets = [d.name for d in c.dimensions if _is_id_like(d.name)]
    if not targets:
        return out
    out.append(Recommendation(
        kind="inverted_index",
        target=", ".join(targets),
        severity="low",
        rationale=(
            f"{len(targets)} id-like dim(s) ({', '.join(targets)}); "
            "inverted index turns equality filters on these into a "
            "constant-time bitmap lookup. Heuristic — confirm against "
            "actual query patterns before committing."
        ),
        config_hint={
            "tableIndexConfig": {
                "invertedIndexColumns": list(targets),
            },
        },
    ))
    return out


def _recommend_bloom_filter_on_id_dims(
    c: CanonicalMigrationModel,
) -> list[Recommendation]:
    """Bloom filter shines on high-cardinality columns where a query
    typically matches few segments. Same heuristic as inverted-index
    but the recommendation is severity=low because bloom is a
    space-time trade-off — not always worth it."""
    out: list[Recommendation] = []
    targets = [d.name for d in c.dimensions if _is_id_like(d.name)]
    if not targets:
        return out
    out.append(Recommendation(
        kind="bloom_filter",
        target=", ".join(targets),
        severity="low",
        rationale=(
            "id-like dims tend to be high-cardinality; bloom filters "
            "let Pinot prune irrelevant segments before scanning. "
            "Trades segment-build time + memory for filter latency — "
            "benchmark before turning on for huge tables."
        ),
        config_hint={
            "tableIndexConfig": {
                "bloomFilterColumns": list(targets),
            },
        },
    ))
    return out
