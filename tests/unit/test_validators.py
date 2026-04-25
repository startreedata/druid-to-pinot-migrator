from __future__ import annotations

import json
from pathlib import Path

from migrator.core.enums import ValidationStatus
from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    GranularityInfo,
    MetricField,
    RiskAnnotation,
    TimeField,
)
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.validation.artifact_checks import ArtifactValidator
from migrator.validation.scoring import compute_confidence_score
from migrator.validation.static_checks import StaticSpecValidator

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _canonical_from_fixture(name: str):
    raw = json.loads((FIXTURES / name / "spec.json").read_text())
    parser = DruidSpecParser()
    parse_result = parser.parse(raw)
    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)
    return norm_result.canonical


def _make_minimal_canonical(datasource_name: str = "test_ds") -> CanonicalMigrationModel:
    return CanonicalMigrationModel(
        datasource_name=datasource_name,
        source_kind="batch",
        classification="raw_event",
        time_field=TimeField(column_name="ts", format="millis"),
        dimensions=[DimensionField(name="dim1")],
        metrics=[],
    )


class TestStaticSpecValidator:
    def setup_method(self):
        self.validator = StaticSpecValidator()

    def test_valid_canonical_passes(self):
        canonical = _canonical_from_fixture("raw_batch")
        canonical.classification = "raw_event"
        checks = self.validator.validate(canonical)
        fail_checks = [c for c in checks if c.status == ValidationStatus.FAIL.value]
        assert fail_checks == []

    def test_empty_datasource_name_fails(self):
        canonical = _make_minimal_canonical(datasource_name="")
        checks = self.validator.validate(canonical)
        name_check = next(
            (c for c in checks if c.check_id == "static.datasource_name_present"), None
        )
        assert name_check is not None
        assert name_check.status == ValidationStatus.FAIL.value

    def test_no_time_field_fails(self):
        canonical = _make_minimal_canonical()
        canonical.time_field = None
        checks = self.validator.validate(canonical)
        time_check = next(
            (c for c in checks if c.check_id == "static.time_field_present"), None
        )
        assert time_check is not None
        assert time_check.status == ValidationStatus.FAIL.value

    def test_duplicate_field_names_fail(self):
        canonical = _make_minimal_canonical()
        canonical.dimensions = [DimensionField(name="dup"), DimensionField(name="dup")]
        checks = self.validator.validate(canonical)
        dup_check = next(
            (c for c in checks if c.check_id == "static.field_names_unique"), None
        )
        assert dup_check is not None
        assert dup_check.status == ValidationStatus.FAIL.value

    def test_unknown_classification_warns(self):
        canonical = _make_minimal_canonical()
        canonical.classification = "unknown"
        checks = self.validator.validate(canonical)
        cls_check = next(
            (c for c in checks if c.check_id == "static.classification_assigned"), None
        )
        assert cls_check is not None
        assert cls_check.status == ValidationStatus.WARN.value

    def test_known_classification_passes(self):
        canonical = _make_minimal_canonical()
        canonical.classification = "raw_event"
        checks = self.validator.validate(canonical)
        cls_check = next(
            (c for c in checks if c.check_id == "static.classification_assigned"), None
        )
        assert cls_check is not None
        assert cls_check.status == ValidationStatus.PASS.value


class TestArtifactValidator:
    def setup_method(self):
        self.validator = ArtifactValidator()

    def _make_valid_schema(self, time_col: str = "ts") -> dict:
        return {
            "schemaName": "test_ds",
            "dimensionFieldSpecs": [{"name": "dim1", "dataType": "STRING"}],
            "metricFieldSpecs": [],
            "dateTimeFieldSpecs": [
                {"name": time_col, "dataType": "LONG", "format": "EPOCH|MILLISECONDS|1", "granularity": "MILLISECONDS"}
            ],
        }

    def _make_valid_offline_table(self, time_col: str = "ts") -> dict:
        return {
            "tableName": "test_ds_OFFLINE",
            "tableType": "OFFLINE",
            "segmentsConfig": {"timeColumnName": time_col},
            "tableIndexConfig": {},
        }

    def test_valid_artifacts_pass(self):
        schema = self._make_valid_schema()
        table = self._make_valid_offline_table()
        checks = self.validator.validate({"schema": schema, "table": table})
        fail_checks = [c for c in checks if c.status == ValidationStatus.FAIL.value]
        assert fail_checks == []

    def test_time_column_mismatch_fails(self):
        schema = self._make_valid_schema(time_col="ts")
        table = self._make_valid_offline_table(time_col="event_time")
        checks = self.validator.validate({"schema": schema, "table": table})
        time_check = next(
            (c for c in checks if c.check_id == "artifact.time_column_match"), None
        )
        assert time_check is not None
        assert time_check.status == ValidationStatus.FAIL.value

    def test_invalid_table_type_fails(self):
        schema = self._make_valid_schema()
        table = {"tableName": "test_ds_INVALID", "tableType": "INVALID", "segmentsConfig": {"timeColumnName": "ts"}, "tableIndexConfig": {}}
        checks = self.validator.validate({"schema": schema, "table": table})
        type_check = next(
            (c for c in checks if c.check_id == "artifact.table_type_valid"), None
        )
        assert type_check is not None
        assert type_check.status == ValidationStatus.FAIL.value

    def test_no_datetime_spec_fails(self):
        schema = {"schemaName": "x", "dimensionFieldSpecs": [], "metricFieldSpecs": [], "dateTimeFieldSpecs": []}
        table = {"tableName": "x_OFFLINE", "tableType": "OFFLINE", "segmentsConfig": {}, "tableIndexConfig": {}}
        checks = self.validator.validate({"schema": schema, "table": table})
        dt_check = next(
            (c for c in checks if c.check_id == "artifact.schema_has_datetime"), None
        )
        assert dt_check is not None
        assert dt_check.status == ValidationStatus.FAIL.value

    def test_realtime_without_stream_configs_fails(self):
        schema = self._make_valid_schema()
        table = {
            "tableName": "test_ds_REALTIME",
            "tableType": "REALTIME",
            "segmentsConfig": {"timeColumnName": "ts"},
            "tableIndexConfig": {},  # no streamConfigs
        }
        checks = self.validator.validate({"schema": schema, "table": table})
        sc_check = next(
            (c for c in checks if c.check_id == "artifact.realtime_has_stream_configs"), None
        )
        assert sc_check is not None
        assert sc_check.status == ValidationStatus.FAIL.value


class TestConfidenceScore:
    def test_no_risks_full_score(self):
        assert compute_confidence_score([]) == 1.0

    def test_blocking_risk_reduces_score(self):
        risks = [
            RiskAnnotation(
                risk_id="R1", severity="blocking", confidence="certain", description="test"
            )
        ]
        score = compute_confidence_score(risks)
        assert score == pytest.approx(0.70, abs=0.001)

    def test_high_risk_reduces_score(self):
        risks = [
            RiskAnnotation(
                risk_id="R1", severity="high", confidence="certain", description="test"
            )
        ]
        score = compute_confidence_score(risks)
        assert score == pytest.approx(0.85, abs=0.001)

    def test_medium_risk_reduces_score(self):
        risks = [
            RiskAnnotation(
                risk_id="R1", severity="medium", confidence="likely", description="test"
            )
        ]
        score = compute_confidence_score(risks)
        assert score == pytest.approx(0.95, abs=0.001)

    def test_multiple_risks_accumulate(self):
        risks = [
            RiskAnnotation(risk_id="R1", severity="blocking", confidence="certain", description="t"),
            RiskAnnotation(risk_id="R2", severity="high", confidence="certain", description="t"),
        ]
        score = compute_confidence_score(risks)
        assert score == pytest.approx(0.55, abs=0.001)

    def test_score_clamped_to_zero(self):
        risks = [
            RiskAnnotation(risk_id=f"R{i}", severity="blocking", confidence="certain", description="t")
            for i in range(10)
        ]
        score = compute_confidence_score(risks)
        assert score == 0.0

    def test_score_never_exceeds_one(self):
        score = compute_confidence_score([])
        assert score <= 1.0


import pytest
