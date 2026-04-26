from __future__ import annotations

from migrator.core.enums import DatasourceClassification
from migrator.core.models import CanonicalMigrationModel

_SIMPLE_ADDITIVE_TYPES = frozenset(
    [
        "count",
        "longSum",
        "doubleSum",
        "floatSum",
        "floatMin",
        "floatMax",
        "longMin",
        "longMax",
        "doubleMin",
        "doubleMax",
    ]
)

_COMPLEX_TYPES = frozenset(
    [
        "thetaSketch",
        "HLLSketchBuild",
        "HLLSketchMerge",
        "hyperUnique",
        "quantilesDoublesSketch",
        "momentSketch",
        "fixedBucketsHistogram",
    ]
)


def classify_datasource(canonical: CanonicalMigrationModel) -> DatasourceClassification:
    """Classify a datasource based on its canonical model."""
    rollup = canonical.granularity.rollup
    metrics = canonical.metrics

    # Any sketch/complex/unsupported aggregator -> COMPLEX_AGGREGATED
    for m in metrics:
        if m.druid_type in _COMPLEX_TYPES:
            return DatasourceClassification.COMPLEX_AGGREGATED
        # BYTES pinot_type is also a signal for unsupported complex field
        if m.pinot_type == "BYTES":
            return DatasourceClassification.COMPLEX_AGGREGATED

    if rollup:
        # Rollup with only simple additive aggregators
        metric_types = {m.druid_type for m in metrics}
        if metric_types.issubset(_SIMPLE_ADDITIVE_TYPES):
            return DatasourceClassification.ROLLED_UP_ADDITIVE
        # Rollup with non-additive aggregators is still complex
        return DatasourceClassification.COMPLEX_AGGREGATED

    # No rollup
    # No metrics or only simple metrics => raw event
    if not metrics:
        return DatasourceClassification.RAW_EVENT
    metric_types = {m.druid_type for m in metrics}
    simple_agg_types = frozenset(["count", "longSum", "doubleSum", "floatSum",
                                   "longMin", "longMax", "doubleMin", "doubleMax",
                                   "floatMin", "floatMax"])
    if metric_types.issubset(simple_agg_types):
        return DatasourceClassification.RAW_EVENT

    return DatasourceClassification.UNKNOWN
