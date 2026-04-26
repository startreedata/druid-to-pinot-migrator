from __future__ import annotations

# ---------------------------------------------------------------------------
# Druid dimension/column type -> Pinot type mapping
# ---------------------------------------------------------------------------
DRUID_TO_PINOT_TYPE: dict[str, str] = {
    "string": "STRING",
    "long": "LONG",
    "float": "FLOAT",
    "double": "DOUBLE",
    "complex": "BYTES",
    "hyperUnique": "BYTES",
    "thetaSketch": "BYTES",
    "HLLSketchBuild": "BYTES",
    "HLLSketchMerge": "BYTES",
    "quantilesDoublesSketch": "BYTES",
    "momentSketch": "BYTES",
    "fixedBucketsHistogram": "BYTES",
}

# ---------------------------------------------------------------------------
# Druid aggregation metric type -> (pinot_type, aggregation_function)
# ---------------------------------------------------------------------------
_METRIC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "count": ("LONG", "COUNT"),
    "longSum": ("LONG", "SUM"),
    "longMin": ("LONG", "MIN"),
    "longMax": ("LONG", "MAX"),
    "doubleSum": ("DOUBLE", "SUM"),
    "doubleMin": ("DOUBLE", "MIN"),
    "doubleMax": ("DOUBLE", "MAX"),
    "floatSum": ("DOUBLE", "SUM"),
    "floatMin": ("DOUBLE", "MIN"),
    "floatMax": ("DOUBLE", "MAX"),
    # Approximate / sketch types
    "thetaSketch": ("BYTES", "DISTINCTCOUNTTHETASKETCH"),
    "HLLSketchBuild": ("BYTES", "DISTINCTCOUNTHLL"),
    "HLLSketchMerge": ("BYTES", "DISTINCTCOUNTHLL"),
    "hyperUnique": ("BYTES", "DISTINCTCOUNTHLL"),
    "quantilesDoublesSketch": ("BYTES", "PERCENTILEEST"),
    "momentSketch": ("BYTES", "PERCENTILEEST"),
    "fixedBucketsHistogram": ("BYTES", "HISTOGRAM"),
}


def map_metric_to_pinot(druid_type: str) -> tuple[str, str]:
    """Return (pinot_type, aggregation_function) for a Druid aggregation type.

    Falls back to (DOUBLE, SUM) for unknown types and emits no error here —
    callers should warn appropriately.
    """
    return _METRIC_TYPE_MAP.get(druid_type, ("DOUBLE", "SUM"))
