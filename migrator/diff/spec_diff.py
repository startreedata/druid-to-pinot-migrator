"""
Druid spec diff — what changed between two Druid ingestion specs, and
what the operator has to redo on the Pinot side as a result.

Use case: a long-running migration where the upstream Druid spec
evolves (a new dimension lands, a metric type changes, the time
column gets renamed). The operator wants to know:

  1. *Did the spec actually change in a way that matters* — small
     formatting differences and reordered keys are noise.
  2. *Which Pinot-side artifacts have to be re-deployed* — schema
     changes need a Pinot schema PUT; index-config changes need a
     table reload; only stream-config changes need a REALTIME table
     re-creation.

The diff is computed over the canonical model rather than raw Druid
JSON: that way two semantically equivalent specs that differ only in
key order or comment lines diff to "no change", and the Pinot
implications can be derived from a stable shape.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    MetricField,
    TimeField,
)
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser


# ─────────────────────────────────────────────────────────────────────────────
# Result shapes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class FieldChange:
    """A single field whose value changed between old and new."""
    name: str
    old: Any
    new: Any

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "old": self.old, "new": self.new}


@dataclass
class DimensionsDiff:
    added: list[DimensionField] = field(default_factory=list)
    removed: list[DimensionField] = field(default_factory=list)
    type_changed: list[FieldChange] = field(default_factory=list)
    multi_value_changed: list[FieldChange] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (
            self.added or self.removed
            or self.type_changed or self.multi_value_changed
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "added": [d.model_dump() for d in self.added],
            "removed": [d.model_dump() for d in self.removed],
            "type_changed": [c.to_dict() for c in self.type_changed],
            "multi_value_changed": [c.to_dict() for c in self.multi_value_changed],
        }


@dataclass
class MetricsDiff:
    added: list[MetricField] = field(default_factory=list)
    removed: list[MetricField] = field(default_factory=list)
    aggregation_changed: list[FieldChange] = field(default_factory=list)
    type_changed: list[FieldChange] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (
            self.added or self.removed
            or self.aggregation_changed or self.type_changed
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "added": [m.model_dump() for m in self.added],
            "removed": [m.model_dump() for m in self.removed],
            "aggregation_changed": [c.to_dict() for c in self.aggregation_changed],
            "type_changed": [c.to_dict() for c in self.type_changed],
        }


@dataclass
class SpecDiff:
    """Top-level diff between two canonical models."""
    datasource_name_changed: FieldChange | None = None
    source_kind_changed: FieldChange | None = None
    classification_changed: FieldChange | None = None
    input_format_changed: FieldChange | None = None
    time_field_changes: list[FieldChange] = field(default_factory=list)
    granularity_changes: list[FieldChange] = field(default_factory=list)
    dimensions: DimensionsDiff = field(default_factory=DimensionsDiff)
    metrics: MetricsDiff = field(default_factory=MetricsDiff)
    pinot_implications: list[str] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """True when the diff carries no semantic change.

        Operators use this to short-circuit "nothing to redeploy" —
        a spec edit that only renames a keyword or reorders dimensions
        in a way the parser normalises shouldn't trigger any Pinot work.
        """
        return (
            self.datasource_name_changed is None
            and self.source_kind_changed is None
            and self.classification_changed is None
            and self.input_format_changed is None
            and not self.time_field_changes
            and not self.granularity_changes
            and self.dimensions.is_empty
            and self.metrics.is_empty
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_empty": self.is_empty,
            "datasource_name_changed": (
                self.datasource_name_changed.to_dict()
                if self.datasource_name_changed else None
            ),
            "source_kind_changed": (
                self.source_kind_changed.to_dict()
                if self.source_kind_changed else None
            ),
            "classification_changed": (
                self.classification_changed.to_dict()
                if self.classification_changed else None
            ),
            "input_format_changed": (
                self.input_format_changed.to_dict()
                if self.input_format_changed else None
            ),
            "time_field_changes": [c.to_dict() for c in self.time_field_changes],
            "granularity_changes": [c.to_dict() for c in self.granularity_changes],
            "dimensions": self.dimensions.to_dict(),
            "metrics": self.metrics.to_dict(),
            "pinot_implications": list(self.pinot_implications),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Diff computation
# ─────────────────────────────────────────────────────────────────────────────


def diff_canonical(
    old: CanonicalMigrationModel,
    new: CanonicalMigrationModel,
) -> SpecDiff:
    """Compute the structural diff between two canonical models.

    Field semantics:
      - ``datasource_name``: a rename here is rare but operationally
        huge — it means a different Pinot table.
      - ``source_kind``: batch ↔ stream is a full migration restart.
      - ``time_field``: column rename or format change forces a Pinot
        schema rewrite (the dateTimeFieldSpec ties to the data layout).
      - ``dimensions``: add/remove → schema change; type change → schema
        change AND data may be incompatible with existing segments.
      - ``metrics``: same as dimensions, plus aggregation changes
        require re-rolling-up the data.
      - ``granularity.segment_granularity``: changes how Pinot
        partitions OFFLINE segments — the existing segments remain
        valid but new ones differ.
      - ``granularity.rollup``: flipping rollup on/off is a full
        re-ingest.
    """
    diff = SpecDiff()

    if old.datasource_name != new.datasource_name:
        diff.datasource_name_changed = FieldChange(
            name="datasource_name",
            old=old.datasource_name, new=new.datasource_name,
        )
    if old.source_kind != new.source_kind:
        diff.source_kind_changed = FieldChange(
            name="source_kind",
            old=old.source_kind, new=new.source_kind,
        )
    if old.classification != new.classification:
        diff.classification_changed = FieldChange(
            name="classification",
            old=old.classification, new=new.classification,
        )
    if old.input_format != new.input_format:
        diff.input_format_changed = FieldChange(
            name="input_format",
            old=old.input_format, new=new.input_format,
        )

    diff.time_field_changes = _diff_time_field(old.time_field, new.time_field)
    diff.granularity_changes = _diff_granularity(old.granularity, new.granularity)
    diff.dimensions = _diff_dimensions(old.dimensions, new.dimensions)
    diff.metrics = _diff_metrics(old.metrics, new.metrics)
    diff.pinot_implications = _derive_pinot_implications(diff)

    return diff


def diff_spec_files(old_path: Path, new_path: Path) -> SpecDiff:
    """Diff two Druid spec JSON files end-to-end (parse + normalize + diff)."""
    return diff_canonical(
        _load_canonical(old_path),
        _load_canonical(new_path),
    )


def _load_canonical(path: Path) -> CanonicalMigrationModel:
    raw = json.loads(Path(path).read_text())
    parsed = DruidSpecParser().parse(raw)
    if not parsed.success or parsed.parsed_spec is None:
        raise ValueError(f"failed to parse Druid spec at {path}: {parsed.errors}")
    norm = DruidNormalizer().normalize(parsed.parsed_spec)
    if not norm.success or norm.canonical is None:
        raise ValueError(
            f"failed to normalize Druid spec at {path}: {norm.errors}"
        )
    return norm.canonical


# ─────────────────────────────────────────────────────────────────────────────
# Per-section diff helpers
# ─────────────────────────────────────────────────────────────────────────────


def _diff_time_field(
    old: TimeField | None, new: TimeField | None,
) -> list[FieldChange]:
    if old is None and new is None:
        return []
    if old is None:
        return [FieldChange("time_field", None, new.model_dump())]
    if new is None:
        return [FieldChange("time_field", old.model_dump(), None)]
    out: list[FieldChange] = []
    if old.column_name != new.column_name:
        out.append(FieldChange("time_field.column_name",
                               old.column_name, new.column_name))
    if old.format != new.format:
        out.append(FieldChange("time_field.format", old.format, new.format))
    if old.timezone != new.timezone:
        out.append(FieldChange("time_field.timezone",
                               old.timezone, new.timezone))
    return out


def _diff_granularity(old, new) -> list[FieldChange]:
    out: list[FieldChange] = []
    if old.segment_granularity != new.segment_granularity:
        out.append(FieldChange(
            "granularity.segment_granularity",
            old.segment_granularity, new.segment_granularity,
        ))
    if old.query_granularity != new.query_granularity:
        out.append(FieldChange(
            "granularity.query_granularity",
            old.query_granularity, new.query_granularity,
        ))
    if old.rollup != new.rollup:
        out.append(FieldChange(
            "granularity.rollup", old.rollup, new.rollup,
        ))
    return out


def _diff_dimensions(
    old: list[DimensionField], new: list[DimensionField],
) -> DimensionsDiff:
    old_by_name = {d.name: d for d in old}
    new_by_name = {d.name: d for d in new}
    out = DimensionsDiff()
    for name, new_dim in new_by_name.items():
        if name not in old_by_name:
            out.added.append(new_dim)
            continue
        old_dim = old_by_name[name]
        if old_dim.druid_type != new_dim.druid_type or \
                old_dim.pinot_type != new_dim.pinot_type:
            out.type_changed.append(FieldChange(
                name=name,
                old={"druid_type": old_dim.druid_type,
                     "pinot_type": old_dim.pinot_type},
                new={"druid_type": new_dim.druid_type,
                     "pinot_type": new_dim.pinot_type},
            ))
        if old_dim.multi_value != new_dim.multi_value:
            out.multi_value_changed.append(FieldChange(
                name=name, old=old_dim.multi_value, new=new_dim.multi_value,
            ))
    for name, old_dim in old_by_name.items():
        if name not in new_by_name:
            out.removed.append(old_dim)
    return out


def _diff_metrics(
    old: list[MetricField], new: list[MetricField],
) -> MetricsDiff:
    old_by_name = {m.name: m for m in old}
    new_by_name = {m.name: m for m in new}
    out = MetricsDiff()
    for name, new_metric in new_by_name.items():
        if name not in old_by_name:
            out.added.append(new_metric)
            continue
        old_metric = old_by_name[name]
        if old_metric.aggregation != new_metric.aggregation:
            out.aggregation_changed.append(FieldChange(
                name=name,
                old=old_metric.aggregation, new=new_metric.aggregation,
            ))
        if old_metric.druid_type != new_metric.druid_type or \
                old_metric.pinot_type != new_metric.pinot_type:
            out.type_changed.append(FieldChange(
                name=name,
                old={"druid_type": old_metric.druid_type,
                     "pinot_type": old_metric.pinot_type},
                new={"druid_type": new_metric.druid_type,
                     "pinot_type": new_metric.pinot_type},
            ))
    for name, old_metric in old_by_name.items():
        if name not in new_by_name:
            out.removed.append(old_metric)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Pinot-side implications
# ─────────────────────────────────────────────────────────────────────────────


def _derive_pinot_implications(diff: SpecDiff) -> list[str]:
    """Translate the structural diff into operator-actionable Pinot
    implications. Each line is one thing to do (or to know about).
    """
    out: list[str] = []
    if diff.datasource_name_changed:
        out.append(
            f"datasource renamed "
            f"({diff.datasource_name_changed.old} → "
            f"{diff.datasource_name_changed.new}); "
            "the existing Pinot table CANNOT be reused — create a new "
            "table and re-ingest."
        )
    if diff.source_kind_changed:
        out.append(
            f"source_kind changed "
            f"({diff.source_kind_changed.old} → "
            f"{diff.source_kind_changed.new}); "
            "OFFLINE↔REALTIME table type change requires full re-deploy."
        )
    schema_changes = (
        diff.dimensions.added or diff.dimensions.removed
        or diff.dimensions.type_changed
        or diff.metrics.added or diff.metrics.removed
        or diff.metrics.type_changed
    )
    if schema_changes:
        added = len(diff.dimensions.added) + len(diff.metrics.added)
        removed = len(diff.dimensions.removed) + len(diff.metrics.removed)
        type_changed = (
            len(diff.dimensions.type_changed) + len(diff.metrics.type_changed)
        )
        out.append(
            f"Pinot schema needs PUT: {added} added, "
            f"{removed} removed, {type_changed} type-changed columns. "
            "Existing segments may need to be re-ingested if column types "
            "are now incompatible."
        )
    if diff.dimensions.multi_value_changed:
        out.append(
            f"{len(diff.dimensions.multi_value_changed)} dimension(s) "
            "flipped multi-value flag; this changes the Pinot field type "
            "(SV ↔ MV) and requires segment re-ingest."
        )
    if diff.metrics.aggregation_changed:
        out.append(
            f"{len(diff.metrics.aggregation_changed)} metric(s) changed "
            "aggregation function; the rollup output is different — full "
            "re-ingest required."
        )
    if diff.time_field_changes:
        out.append(
            f"time_field changed ({len(diff.time_field_changes)} fields); "
            "Pinot schema dateTimeFieldSpec must be updated and existing "
            "segments may need a time-column re-encode."
        )
    if diff.input_format_changed:
        old_fmt = diff.input_format_changed.old
        new_fmt = diff.input_format_changed.new
        out.append(
            f"input format changed ({old_fmt} → {new_fmt}); the Pinot "
            "batch-job's RecordReader (and the REALTIME table's Kafka "
            "decoder, for stream specs) must be regenerated. "
            "``dpm generate`` will pick the right Pinot plugin "
            "automatically — but the deployed table config has to be "
            "redeployed to apply the change."
        )
    for change in diff.granularity_changes:
        if change.name == "granularity.rollup":
            out.append(
                "rollup flag flipped — full re-ingest required (rollup "
                "is an ingestion-time operation, not query-time)."
            )
        elif change.name == "granularity.segment_granularity":
            out.append(
                f"segment_granularity changed "
                f"({change.old} → {change.new}); existing segments stay "
                "valid but new segments will be sized differently."
            )
        elif change.name == "granularity.query_granularity":
            out.append(
                f"query_granularity changed "
                f"({change.old} → {change.new}); review whether downstream "
                "queries still produce equivalent results."
            )

    if not out and not diff.is_empty:
        # A semantic change exists but doesn't map to a known Pinot action.
        out.append(
            "Semantic spec change detected with no automatic Pinot mapping; "
            "review the per-section diff manually."
        )
    return out
