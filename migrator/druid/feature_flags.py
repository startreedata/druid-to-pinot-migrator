from __future__ import annotations

import re

from migrator.core.models import DimensionField, MetricField, TransformField

_COMPLEX_AGGREGATOR_TYPES = frozenset(
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

# Patterns that suggest non-trivial transform expressions
_COMPLEX_EXPRESSION_PATTERNS = [
    r"\bcase\b",
    r"\bif\b\s*\(",
    r"\bcoalesce\b",
    r"\bconcat\b",
    r"\bnvl\b",
    r"\bregexp\b",
    r"\btimestamp_parse\b",
    r"\bunix_timestamp\b",
    r"->",  # JSON path
    r"\[",  # Array index / JSON
]

_NESTED_PATH_PATTERN = re.compile(r"[\w]+\.[\w]+|[\w]+\[")


def detect_complex_aggregators(metrics: list[MetricField]) -> list[str]:
    """Return the names of metrics that use complex/sketch aggregators."""
    return [m.name for m in metrics if m.druid_type in _COMPLEX_AGGREGATOR_TYPES]


def detect_multivalue_ambiguity(dimensions: list[DimensionField]) -> list[str]:
    """Return dimension names that are or may be multi-value."""
    return [d.name for d in dimensions if d.multi_value]


def detect_transform_portability_risk(transforms: list[TransformField]) -> list[str]:
    """Return transform names whose expressions are non-trivial (risky to port)."""
    risky: list[str] = []
    for t in transforms:
        expr_lower = t.expression.lower()
        for pattern in _COMPLEX_EXPRESSION_PATTERNS:
            if re.search(pattern, expr_lower, re.IGNORECASE):
                risky.append(t.name)
                break
    return risky


def detect_nested_fields(raw_sections: dict) -> list[str]:
    """Look for nested path patterns in raw spec sections (e.g. flattenSpec)."""
    nested_fields: list[str] = []
    text = str(raw_sections)
    matches = _NESTED_PATH_PATTERN.findall(text)
    # De-duplicate and filter obvious non-field matches
    seen: set[str] = set()
    for m in matches:
        if m not in seen:
            seen.add(m)
            nested_fields.append(m)
    return nested_fields
