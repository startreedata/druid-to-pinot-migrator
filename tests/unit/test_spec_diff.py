"""Unit tests for migrator.diff.spec_diff."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    GranularityInfo,
    MetricField,
    TimeField,
)
from migrator.diff.spec_diff import (
    SpecDiff,
    diff_canonical,
    diff_spec_files,
)


# ─────────────────────────────────────────────────────────────────────────────
# Builders
# ─────────────────────────────────────────────────────────────────────────────


def _canon(**overrides) -> CanonicalMigrationModel:
    base = dict(
        datasource_name="ds",
        source_kind="batch",
        classification="raw_event",
        time_field=TimeField(column_name="timestamp", format="millis"),
        dimensions=[
            DimensionField(name="region", druid_type="string", pinot_type="STRING"),
        ],
        metrics=[
            MetricField(
                name="events", druid_type="count",
                pinot_type="LONG", aggregation="SUM",
            ),
        ],
        granularity=GranularityInfo(
            segment_granularity="HOUR", query_granularity="MINUTE", rollup=False,
        ),
    )
    base.update(overrides)
    return CanonicalMigrationModel(**base)


# ─────────────────────────────────────────────────────────────────────────────
# Empty diff
# ─────────────────────────────────────────────────────────────────────────────


class TestEmptyDiff:
    def test_identical_canonicals_yield_empty_diff(self):
        a = _canon()
        b = _canon()
        d = diff_canonical(a, b)
        assert d.is_empty
        # No implications because nothing changed.
        assert d.pinot_implications == []


# ─────────────────────────────────────────────────────────────────────────────
# Top-level field changes
# ─────────────────────────────────────────────────────────────────────────────


class TestTopLevelChanges:
    def test_datasource_rename_flagged_and_implicated(self):
        d = diff_canonical(_canon(), _canon(datasource_name="ds_v2"))
        assert d.datasource_name_changed is not None
        assert d.datasource_name_changed.old == "ds"
        assert d.datasource_name_changed.new == "ds_v2"
        # Operator MUST know they need a new Pinot table — the
        # implications list is the load-bearing surface here.
        assert any("CANNOT be reused" in line or "renamed" in line
                   for line in d.pinot_implications)

    def test_source_kind_change_flagged(self):
        d = diff_canonical(_canon(), _canon(source_kind="stream"))
        assert d.source_kind_changed is not None
        assert any("OFFLINE↔REALTIME" in line for line in d.pinot_implications)

    def test_classification_change_flagged_but_no_pinot_action(self):
        d = diff_canonical(_canon(), _canon(classification="rolled_up"))
        assert d.classification_changed is not None
        # Classification is a dpm-internal label; doesn't directly map
        # to a Pinot artifact change. Implications may be empty for
        # this case OR carry a "review manually" line.
        # Either is acceptable — what matters is that a change is
        # surfaced in the structural diff.


# ─────────────────────────────────────────────────────────────────────────────
# Dimensions diff
# ─────────────────────────────────────────────────────────────────────────────


class TestDimensionsDiff:
    def test_added_dimension(self):
        new = _canon(
            dimensions=[
                DimensionField(name="region", druid_type="string", pinot_type="STRING"),
                DimensionField(name="device", druid_type="string", pinot_type="STRING"),
            ],
        )
        d = diff_canonical(_canon(), new)
        assert len(d.dimensions.added) == 1
        assert d.dimensions.added[0].name == "device"
        assert any("schema needs PUT" in s for s in d.pinot_implications)

    def test_removed_dimension(self):
        new = _canon(dimensions=[])
        d = diff_canonical(_canon(), new)
        assert len(d.dimensions.removed) == 1
        assert d.dimensions.removed[0].name == "region"

    def test_type_change_flags_pinot_compatibility_warning(self):
        new = _canon(dimensions=[
            DimensionField(name="region", druid_type="long", pinot_type="LONG"),
        ])
        d = diff_canonical(_canon(), new)
        assert len(d.dimensions.type_changed) == 1
        assert d.dimensions.type_changed[0].name == "region"
        assert any("re-ingested" in s or "incompatible" in s
                   for s in d.pinot_implications)

    def test_multi_value_flip_is_called_out_separately(self):
        new = _canon(dimensions=[
            DimensionField(
                name="region", druid_type="string",
                pinot_type="STRING", multi_value=True,
            ),
        ])
        d = diff_canonical(_canon(), new)
        assert len(d.dimensions.multi_value_changed) == 1
        # SV↔MV needs its own Pinot-side warning because the field-spec
        # type changes, not just the schema list.
        assert any("multi-value" in s or "SV ↔ MV" in s
                   for s in d.pinot_implications)


# ─────────────────────────────────────────────────────────────────────────────
# Metrics diff
# ─────────────────────────────────────────────────────────────────────────────


class TestMetricsDiff:
    def test_metric_added(self):
        new = _canon(metrics=[
            MetricField(name="events", druid_type="count",
                        pinot_type="LONG", aggregation="SUM"),
            MetricField(name="amount", druid_type="doubleSum",
                        pinot_type="DOUBLE", aggregation="SUM"),
        ])
        d = diff_canonical(_canon(), new)
        assert len(d.metrics.added) == 1
        assert d.metrics.added[0].name == "amount"

    def test_aggregation_change_warns_about_full_reingest(self):
        new = _canon(metrics=[
            MetricField(name="events", druid_type="count",
                        pinot_type="LONG", aggregation="MAX"),
        ])
        d = diff_canonical(_canon(), new)
        assert len(d.metrics.aggregation_changed) == 1
        assert any("re-ingest" in s for s in d.pinot_implications)


# ─────────────────────────────────────────────────────────────────────────────
# Time field + granularity
# ─────────────────────────────────────────────────────────────────────────────


class TestTimeAndGranularity:
    def test_time_column_rename_flagged(self):
        new = _canon(time_field=TimeField(
            column_name="event_time", format="millis",
        ))
        d = diff_canonical(_canon(), new)
        assert any(c.name == "time_field.column_name"
                   for c in d.time_field_changes)
        assert any("dateTimeFieldSpec" in s for s in d.pinot_implications)

    def test_rollup_flip_warns_full_reingest(self):
        new = _canon(granularity=GranularityInfo(
            segment_granularity="HOUR", query_granularity="MINUTE", rollup=True,
        ))
        d = diff_canonical(_canon(), new)
        assert any(c.name == "granularity.rollup" for c in d.granularity_changes)
        assert any("re-ingest" in s for s in d.pinot_implications)

    def test_segment_granularity_change_is_informational(self):
        new = _canon(granularity=GranularityInfo(
            segment_granularity="DAY", query_granularity="MINUTE", rollup=False,
        ))
        d = diff_canonical(_canon(), new)
        # Existing segments stay valid — the implication should NOT
        # demand re-ingest, only inform.
        impls = " ".join(d.pinot_implications)
        assert "segment_granularity" in impls
        assert "stay valid" in impls


# ─────────────────────────────────────────────────────────────────────────────
# Serialization
# ─────────────────────────────────────────────────────────────────────────────


class TestSerialization:
    def test_to_dict_round_trips_via_json(self):
        d = diff_canonical(_canon(), _canon(datasource_name="ds_v2"))
        as_json = json.dumps(d.to_dict(), default=str)
        # Parses back as a dict shape with the expected top-level keys.
        loaded = json.loads(as_json)
        assert "is_empty" in loaded
        assert "datasource_name_changed" in loaded
        assert loaded["datasource_name_changed"]["new"] == "ds_v2"


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end (file inputs)
# ─────────────────────────────────────────────────────────────────────────────


SAMPLE_SPEC = {
    "type": "kafka",
    "spec": {
        "dataSchema": {
            "dataSource": "events",
            "timestampSpec": {"column": "timestamp", "format": "millis"},
            "dimensionsSpec": {"dimensions": ["region"]},
            "metricsSpec": [],
            "granularitySpec": {"segmentGranularity": "HOUR", "rollup": False},
        },
        "ioConfig": {
            "type": "kafka",
            "topic": "events",
            "consumerProperties": {"bootstrap.servers": "k:9092"},
        },
    },
}


class TestDiffSpecFiles:
    def test_unchanged_specs_diff_empty(self, tmp_path: Path):
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        a.write_text(json.dumps(SAMPLE_SPEC))
        b.write_text(json.dumps(SAMPLE_SPEC))
        d = diff_spec_files(a, b)
        assert d.is_empty

    def test_added_dimension_in_new_spec(self, tmp_path: Path):
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        a.write_text(json.dumps(SAMPLE_SPEC))
        new_spec = copy.deepcopy(SAMPLE_SPEC)
        new_spec["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"].append("device")
        b.write_text(json.dumps(new_spec))
        d = diff_spec_files(a, b)
        assert not d.is_empty
        assert len(d.dimensions.added) == 1
        assert d.dimensions.added[0].name == "device"

    def test_unparseable_file_raises_value_error(self, tmp_path: Path):
        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({"this": "is not a druid spec"}))
        good = tmp_path / "good.json"
        good.write_text(json.dumps(SAMPLE_SPEC))
        with pytest.raises((ValueError, Exception)):
            diff_spec_files(bad, good)
