"""
End-to-end integration tests for the migration pipeline.

These tests exercise the full parse -> normalize -> classify -> generate ->
risk-analyze -> validate -> report chain for all five fixtures, and verify
correctness of the emitted artifacts, risks, and validation reports.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.translators.pipeline import (
    generate_bundle,
    inspect_spec,
    normalize_spec,
    validate_spec,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict | list:
    return json.loads(path.read_text())


def bundle(fixture: str, tmp_path: Path):
    """Run generate_bundle and return (result, out_path)."""
    spec = str(FIXTURES / fixture / "spec.json")
    result = generate_bundle(spec, out_dir=str(tmp_path))
    return result, tmp_path


# ---------------------------------------------------------------------------
# 1. Full pipeline succeeds for every fixture
# ---------------------------------------------------------------------------

class TestPipelineCompletesForAllFixtures:
    @pytest.mark.parametrize("fixture", [
        "raw_batch",
        "raw_stream",
        "rolled_up",
        "transforms",
        "unsupported_complex",
    ])
    def test_generate_bundle_succeeds(self, fixture, tmp_path):
        result, _ = bundle(fixture, tmp_path)
        assert result.success, f"[{fixture}] generate_bundle failed: {result.errors}"

    @pytest.mark.parametrize("fixture", [
        "raw_batch",
        "raw_stream",
        "rolled_up",
        "transforms",
        "unsupported_complex",
    ])
    def test_no_errors_in_result(self, fixture, tmp_path):
        result, _ = bundle(fixture, tmp_path)
        assert result.errors == [], f"[{fixture}] unexpected errors: {result.errors}"

    @pytest.mark.parametrize("fixture", [
        "raw_batch",
        "raw_stream",
        "rolled_up",
        "transforms",
        "unsupported_complex",
    ])
    def test_schema_json_written(self, fixture, tmp_path):
        bundle(fixture, tmp_path)
        assert (tmp_path / "schema.json").exists()

    @pytest.mark.parametrize("fixture", [
        "raw_batch",
        "raw_stream",
        "rolled_up",
        "transforms",
        "unsupported_complex",
    ])
    def test_reports_dir_written(self, fixture, tmp_path):
        bundle(fixture, tmp_path)
        assert (tmp_path / "reports").is_dir()
        assert (tmp_path / "reports" / "migration-report.json").exists()
        assert (tmp_path / "reports" / "risks.json").exists()
        assert (tmp_path / "reports" / "warnings.json").exists()
        assert (tmp_path / "reports" / "migration-summary.md").exists()


# ---------------------------------------------------------------------------
# 2. Correct table type emitted per fixture
# ---------------------------------------------------------------------------

class TestTableTypePerFixture:
    def test_raw_batch_emits_offline_table(self, tmp_path):
        bundle("raw_batch", tmp_path)
        assert (tmp_path / "table-offline.json").exists()
        assert not (tmp_path / "table-realtime.json").exists()

    def test_raw_stream_emits_realtime_table(self, tmp_path):
        bundle("raw_stream", tmp_path)
        assert (tmp_path / "table-realtime.json").exists()
        assert not (tmp_path / "table-offline.json").exists()

    def test_rolled_up_emits_offline_table(self, tmp_path):
        bundle("rolled_up", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_offline_table_type_field(self, tmp_path):
        bundle("raw_batch", tmp_path)
        data = load_json(tmp_path / "table-offline.json")
        assert data["tableType"] == "OFFLINE"

    def test_realtime_table_type_field(self, tmp_path):
        bundle("raw_stream", tmp_path)
        data = load_json(tmp_path / "table-realtime.json")
        assert data["tableType"] == "REALTIME"

    def test_realtime_table_has_stream_configs(self, tmp_path):
        bundle("raw_stream", tmp_path)
        data = load_json(tmp_path / "table-realtime.json")
        stream_configs = data["tableIndexConfig"]["streamConfigs"]
        assert "streamType" in stream_configs
        assert "stream.kafka.topic.name" in stream_configs
        assert "stream.kafka.broker.list" in stream_configs
        assert "stream.kafka.consumer.type" in stream_configs
        assert "stream.kafka.decoder.class.name" in stream_configs

    def test_realtime_table_topic_matches_spec(self, tmp_path):
        """Kafka topic from ioConfig should appear in the realtime streamConfigs."""
        bundle("raw_stream", tmp_path)
        data = load_json(tmp_path / "table-realtime.json")
        topic = data["tableIndexConfig"]["streamConfigs"]["stream.kafka.topic.name"]
        assert topic == "clickstream-events"


# ---------------------------------------------------------------------------
# 3. Schema content correctness
# ---------------------------------------------------------------------------

class TestSchemaContent:
    def test_raw_batch_schema_name(self, tmp_path):
        bundle("raw_batch", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "pageviews"

    def test_raw_batch_dimension_fields(self, tmp_path):
        bundle("raw_batch", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        dim_names = {f["name"] for f in schema["dimensionFieldSpecs"]}
        assert dim_names == {"page", "user", "region"}

    def test_raw_batch_no_metrics(self, tmp_path):
        """Batch fixture has empty metricsSpec; metric list should be empty."""
        bundle("raw_batch", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["metricFieldSpecs"] == []

    def test_raw_batch_datetime_field(self, tmp_path):
        bundle("raw_batch", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        dt_specs = schema["dateTimeFieldSpecs"]
        assert len(dt_specs) == 1
        assert dt_specs[0]["name"] == "timestamp"

    def test_rolled_up_metric_fields(self, tmp_path):
        bundle("rolled_up", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        metric_names = {f["name"] for f in schema["metricFieldSpecs"]}
        assert "impressions" in metric_names
        assert "clicks" in metric_names
        assert "revenue" in metric_names

    def test_rolled_up_metric_types(self, tmp_path):
        """count -> LONG, longSum -> LONG, doubleSum -> DOUBLE."""
        bundle("rolled_up", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        by_name = {f["name"]: f["dataType"] for f in schema["metricFieldSpecs"]}
        assert by_name["impressions"] == "LONG"    # count
        assert by_name["clicks"] == "LONG"          # longSum
        assert by_name["revenue"] == "DOUBLE"       # doubleSum

    def test_unsupported_complex_bytes_metrics(self, tmp_path):
        """thetaSketch / HLL / hyperUnique must map to BYTES."""
        bundle("unsupported_complex", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        by_name = {f["name"]: f["dataType"] for f in schema["metricFieldSpecs"]}
        assert by_name["unique_users"] == "BYTES"
        assert by_name["approx_users"] == "BYTES"
        assert by_name["hll_users"] == "BYTES"

    def test_dimensions_sorted_alphabetically(self, tmp_path):
        """Dimensions in schema must be in alphabetical order."""
        bundle("raw_batch", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        names = [f["name"] for f in schema["dimensionFieldSpecs"]]
        assert names == sorted(names)

    def test_metrics_sorted_alphabetically(self, tmp_path):
        bundle("rolled_up", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        names = [f["name"] for f in schema["metricFieldSpecs"]]
        assert names == sorted(names)

    def test_schema_output_is_deterministic(self, tmp_path, tmp_path_factory):
        """Generating twice from the same spec produces identical schemas."""
        out1 = tmp_path_factory.mktemp("out1")
        out2 = tmp_path_factory.mktemp("out2")
        bundle("rolled_up", out1)
        bundle("rolled_up", out2)
        schema1 = (out1 / "schema.json").read_text()
        schema2 = (out2 / "schema.json").read_text()
        assert schema1 == schema2

    def test_stream_time_field_name(self, tmp_path):
        bundle("raw_stream", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["dateTimeFieldSpecs"][0]["name"] == "event_time"

    def test_stream_time_format_is_epoch_millis(self, tmp_path):
        """'millis' time format should map to EPOCH|MILLISECONDS."""
        bundle("raw_stream", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        fmt = schema["dateTimeFieldSpecs"][0]["format"]
        assert "EPOCH" in fmt
        assert "MILLISECONDS" in fmt


# ---------------------------------------------------------------------------
# 4. Classification correctness via normalize_spec
# ---------------------------------------------------------------------------

class TestClassification:
    def test_raw_batch_classified_raw_event(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = normalize_spec(spec)
        assert result.success
        assert result.canonical.classification == "raw_event"

    def test_raw_stream_classified_raw_event(self):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        result = normalize_spec(spec)
        assert result.success
        assert result.canonical.classification == "raw_event"

    def test_rolled_up_classified_rolled_up_additive(self):
        spec = str(FIXTURES / "rolled_up" / "spec.json")
        result = normalize_spec(spec)
        assert result.success
        assert result.canonical.classification == "rolled_up_additive"

    def test_unsupported_complex_classified_complex_aggregated(self):
        spec = str(FIXTURES / "unsupported_complex" / "spec.json")
        result = normalize_spec(spec)
        assert result.success
        assert result.canonical.classification == "complex_aggregated"

    def test_raw_batch_source_kind_is_batch(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.source_kind == "batch"

    def test_raw_stream_source_kind_is_stream(self):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.source_kind == "stream"


# ---------------------------------------------------------------------------
# 5. Risk analysis correctness
# ---------------------------------------------------------------------------

class TestRiskAnalysis:
    def _risks(self, fixture: str, tmp_path: Path) -> list[dict]:
        bundle(fixture, tmp_path)
        data = load_json(tmp_path / "reports" / "risks.json")
        # risks.json has structure {"risks": [...]}
        return data["risks"]

    def test_raw_batch_has_no_blocking_risks(self, tmp_path):
        risks = self._risks("raw_batch", tmp_path)
        blocking = [r for r in risks if r["severity"] == "blocking"]
        assert blocking == []

    def test_rolled_up_has_rollup_risk(self, tmp_path):
        risks = self._risks("rolled_up", tmp_path)
        ids = [r["risk_id"] for r in risks]
        assert "ROLLUP_SEMANTIC_MISMATCH" in ids

    def test_rolled_up_rollup_risk_is_high(self, tmp_path):
        risks = self._risks("rolled_up", tmp_path)
        rollup_risks = [r for r in risks if r["risk_id"] == "ROLLUP_SEMANTIC_MISMATCH"]
        assert rollup_risks[0]["severity"] == "high"

    def test_unsupported_complex_has_blocking_risk(self, tmp_path):
        risks = self._risks("unsupported_complex", tmp_path)
        ids = [r["risk_id"] for r in risks]
        assert "APPROX_AGGREGATOR_MISMATCH" in ids
        blocking = [r for r in risks if r["risk_id"] == "APPROX_AGGREGATOR_MISMATCH"]
        assert blocking[0]["severity"] == "blocking"

    def test_unsupported_complex_has_unsupported_field_risk(self, tmp_path):
        risks = self._risks("unsupported_complex", tmp_path)
        ids = [r["risk_id"] for r in risks]
        assert "UNSUPPORTED_COMPLEX_FIELD" in ids

    def test_transforms_fixture_has_transform_portability_risk(self, tmp_path):
        risks = self._risks("transforms", tmp_path)
        ids = [r["risk_id"] for r in risks]
        assert "TRANSFORM_PORTABILITY_RISK" in ids

    def test_transforms_portability_risk_is_medium(self, tmp_path):
        risks = self._risks("transforms", tmp_path)
        xform_risks = [r for r in risks if r["risk_id"] == "TRANSFORM_PORTABILITY_RISK"]
        assert xform_risks[0]["severity"] == "medium"

    def test_risk_annotations_have_required_fields(self, tmp_path):
        risks = self._risks("rolled_up", tmp_path)
        for r in risks:
            assert "risk_id" in r
            assert "severity" in r
            assert "confidence" in r
            assert "description" in r
            assert "remediation" in r

    def test_risk_evidence_is_non_empty_for_known_risks(self, tmp_path):
        risks = self._risks("rolled_up", tmp_path)
        rollup = next(r for r in risks if r["risk_id"] == "ROLLUP_SEMANTIC_MISMATCH")
        assert len(rollup["evidence"]) > 0


# ---------------------------------------------------------------------------
# 6. Confidence scoring
# ---------------------------------------------------------------------------

class TestConfidenceScoring:
    def _score(self, fixture: str) -> float:
        spec = str(FIXTURES / fixture / "spec.json")
        result = validate_spec(spec)
        return result.report.confidence_score

    def test_raw_batch_has_high_confidence(self):
        assert self._score("raw_batch") >= 0.9

    def test_unsupported_complex_has_lower_confidence_than_raw_batch(self):
        assert self._score("unsupported_complex") < self._score("raw_batch")

    def test_rolled_up_has_lower_confidence_than_raw_batch(self):
        assert self._score("rolled_up") < self._score("raw_batch")

    def test_unsupported_complex_confidence_is_significantly_reduced(self):
        """BLOCKING + HIGH risks should push score well below 0.7.

        unsupported_complex emits one BLOCKING (-0.30) and one HIGH (-0.15)
        risk, leaving confidence at 0.55.  The exact value is tested elsewhere;
        this assertion just confirms a meaningful penalty was applied.
        """
        score = self._score("unsupported_complex")
        assert score < 0.7

    def test_confidence_score_is_between_zero_and_one(self):
        for fixture in ("raw_batch", "raw_stream", "rolled_up", "transforms", "unsupported_complex"):
            score = self._score(fixture)
            assert 0.0 <= score <= 1.0, f"[{fixture}] score out of range: {score}"


# ---------------------------------------------------------------------------
# 7. Validation report content
# ---------------------------------------------------------------------------

class TestValidationReport:
    def test_raw_batch_overall_status_is_pass(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = validate_spec(spec)
        assert result.report.overall_status == "pass"

    def test_raw_batch_all_static_checks_pass(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = validate_spec(spec)
        failed = [c for c in result.report.checks if c.status == "fail"]
        assert failed == []

    def test_validate_with_generated_artifacts(self, tmp_path):
        """validate_spec with generated_dir should also run artifact checks."""
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec, out_dir=str(tmp_path))
        result = validate_spec(spec, generated_dir=str(tmp_path))
        assert result.success
        check_ids = {c.check_id for c in result.report.checks}
        # Artifact checks should be included
        assert any("artifact" in cid or "schema" in cid or "time_column" in cid
                   for cid in check_ids)

    def test_validate_stream_with_generated_artifacts(self, tmp_path):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        generate_bundle(spec, out_dir=str(tmp_path))
        result = validate_spec(spec, generated_dir=str(tmp_path))
        assert result.success

    def test_validation_report_contains_datasource_name(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = validate_spec(spec)
        assert result.report.datasource_name == "pageviews"

    def test_validation_report_has_checks_list(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = validate_spec(spec)
        assert isinstance(result.report.checks, list)
        assert len(result.report.checks) > 0


# ---------------------------------------------------------------------------
# 8. inspect_spec summary correctness
# ---------------------------------------------------------------------------

class TestInspectSpec:
    def test_raw_batch_datasource_name(self):
        info = inspect_spec(str(FIXTURES / "raw_batch" / "spec.json"))
        assert info["datasource_name"] == "pageviews"

    def test_raw_batch_classification(self):
        info = inspect_spec(str(FIXTURES / "raw_batch" / "spec.json"))
        assert info["classification"] == "raw_event"

    def test_raw_batch_source_kind(self):
        info = inspect_spec(str(FIXTURES / "raw_batch" / "spec.json"))
        assert info["source_kind"] == "batch"

    def test_raw_stream_source_kind(self):
        info = inspect_spec(str(FIXTURES / "raw_stream" / "spec.json"))
        assert info["source_kind"] == "stream"

    def test_rolled_up_rollup_flag(self):
        info = inspect_spec(str(FIXTURES / "rolled_up" / "spec.json"))
        assert info["rollup"] is True

    def test_raw_batch_rollup_flag_false(self):
        info = inspect_spec(str(FIXTURES / "raw_batch" / "spec.json"))
        assert info["rollup"] is False

    def test_transforms_fixture_dimension_count(self):
        info = inspect_spec(str(FIXTURES / "transforms" / "spec.json"))
        assert info["dimensions"] == 3

    def test_rolled_up_metric_count(self):
        info = inspect_spec(str(FIXTURES / "rolled_up" / "spec.json"))
        assert info["metrics"] == 3

    def test_unsupported_complex_risk_count_nonzero(self):
        info = inspect_spec(str(FIXTURES / "unsupported_complex" / "spec.json"))
        assert info["risk_count"] > 0

    def test_raw_batch_risk_count_zero(self):
        info = inspect_spec(str(FIXTURES / "raw_batch" / "spec.json"))
        assert info["risk_count"] == 0


# ---------------------------------------------------------------------------
# 9. Migration report content
# ---------------------------------------------------------------------------

class TestMigrationReport:
    def _report(self, fixture: str, tmp_path: Path) -> dict:
        bundle(fixture, tmp_path)
        return load_json(tmp_path / "reports" / "migration-report.json")

    def test_report_contains_datasource_name(self, tmp_path):
        report = self._report("raw_batch", tmp_path)
        assert report["datasource_name"] == "pageviews"

    def test_report_contains_risks_list(self, tmp_path):
        report = self._report("rolled_up", tmp_path)
        assert "risks" in report
        assert isinstance(report["risks"], list)

    def test_report_contains_confidence_score(self, tmp_path):
        report = self._report("raw_batch", tmp_path)
        assert "confidence_score" in report
        assert 0.0 <= report["confidence_score"] <= 1.0

    def test_report_contains_classification(self, tmp_path):
        report = self._report("rolled_up", tmp_path)
        assert report.get("classification") == "rolled_up_additive"

    def test_markdown_summary_contains_sections(self, tmp_path):
        bundle("raw_batch", tmp_path)
        md = (tmp_path / "reports" / "migration-summary.md").read_text()
        assert "pageviews" in md
        # Should have at least a classification section
        assert "raw_event" in md or "Classification" in md

    def test_markdown_summary_is_well_formed(self, tmp_path):
        """Markdown should start with a heading."""
        bundle("raw_batch", tmp_path)
        md = (tmp_path / "reports" / "migration-summary.md").read_text()
        assert md.strip().startswith("#")


# ---------------------------------------------------------------------------
# 10. Dry-run produces no files
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_no_files_for_batch(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = generate_bundle(spec, out_dir=str(tmp_path), dry_run=True)
        assert result.success
        assert result.files_written == []
        assert list(tmp_path.rglob("*")) == []

    def test_dry_run_no_files_for_stream(self, tmp_path):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        result = generate_bundle(spec, out_dir=str(tmp_path), dry_run=True)
        assert result.success
        assert result.files_written == []

    def test_dry_run_still_returns_warnings(self, tmp_path):
        """Dry run should still analyze and surface warnings."""
        spec = str(FIXTURES / "unsupported_complex" / "spec.json")
        result = generate_bundle(spec, out_dir=str(tmp_path), dry_run=True)
        assert result.success
        # Complex aggregator spec should produce warnings even in dry run
        assert len(result.warnings) > 0


# ---------------------------------------------------------------------------
# 11. Table config content correctness
# ---------------------------------------------------------------------------

class TestTableConfig:
    def test_offline_table_name_suffix(self, tmp_path):
        bundle("raw_batch", tmp_path)
        table = load_json(tmp_path / "table-offline.json")
        assert table["tableName"] == "pageviews_OFFLINE"

    def test_realtime_table_name_suffix(self, tmp_path):
        bundle("raw_stream", tmp_path)
        table = load_json(tmp_path / "table-realtime.json")
        assert table["tableName"] == "clickstream_REALTIME"

    def test_offline_segments_config_has_time_column(self, tmp_path):
        bundle("raw_batch", tmp_path)
        table = load_json(tmp_path / "table-offline.json")
        assert table["segmentsConfig"]["timeColumnName"] == "timestamp"

    def test_realtime_segments_config_has_time_column(self, tmp_path):
        bundle("raw_stream", tmp_path)
        table = load_json(tmp_path / "table-realtime.json")
        assert table["segmentsConfig"]["timeColumnName"] == "event_time"

    def test_offline_has_tenants(self, tmp_path):
        bundle("raw_batch", tmp_path)
        table = load_json(tmp_path / "table-offline.json")
        assert "tenants" in table
        assert "broker" in table["tenants"]
        assert "server" in table["tenants"]

    def test_offline_has_retention_config(self, tmp_path):
        bundle("raw_batch", tmp_path)
        table = load_json(tmp_path / "table-offline.json")
        seg_cfg = table["segmentsConfig"]
        assert "retentionTimeUnit" in seg_cfg
        assert "retentionTimeValue" in seg_cfg

    def test_schema_time_column_matches_table_time_column(self, tmp_path):
        """The dateTimeFieldSpec name in schema must match segmentsConfig.timeColumnName."""
        bundle("raw_batch", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        table = load_json(tmp_path / "table-offline.json")
        schema_time = schema["dateTimeFieldSpecs"][0]["name"]
        table_time = table["segmentsConfig"]["timeColumnName"]
        assert schema_time == table_time

    def test_schema_stream_time_column_matches_realtime_table(self, tmp_path):
        bundle("raw_stream", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        table = load_json(tmp_path / "table-realtime.json")
        assert schema["dateTimeFieldSpecs"][0]["name"] == table["segmentsConfig"]["timeColumnName"]


# ---------------------------------------------------------------------------
# 12. Canonical model file correctness
# ---------------------------------------------------------------------------

class TestCanonicalModelFile:
    def test_canonical_json_written(self, tmp_path):
        bundle("raw_batch", tmp_path)
        assert (tmp_path / "canonical.json").exists()

    def test_canonical_json_has_required_fields(self, tmp_path):
        bundle("raw_batch", tmp_path)
        canonical = load_json(tmp_path / "canonical.json")
        for field in ("datasource_name", "source_kind", "classification",
                      "dimensions", "metrics", "granularity"):
            assert field in canonical, f"Missing field: {field}"

    def test_canonical_datasource_name(self, tmp_path):
        bundle("rolled_up", tmp_path)
        canonical = load_json(tmp_path / "canonical.json")
        assert canonical["datasource_name"] == "ad_metrics"

    def test_canonical_transforms_present(self, tmp_path):
        bundle("transforms", tmp_path)
        canonical = load_json(tmp_path / "canonical.json")
        assert len(canonical["transforms"]) == 1
        assert canonical["transforms"][0]["name"] == "event_category"


# ---------------------------------------------------------------------------
# 13. Files written list is complete and accurate
# ---------------------------------------------------------------------------

class TestFilesWrittenList:
    def test_files_written_non_empty(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = generate_bundle(spec, out_dir=str(tmp_path))
        assert len(result.files_written) > 0

    def test_all_listed_files_exist_on_disk(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = generate_bundle(spec, out_dir=str(tmp_path))
        for f in result.files_written:
            assert Path(f).exists(), f"Listed file does not exist: {f}"

    def test_schema_in_files_written(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = generate_bundle(spec, out_dir=str(tmp_path))
        names = [Path(f).name for f in result.files_written]
        assert "schema.json" in names

    def test_migration_report_in_files_written(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = generate_bundle(spec, out_dir=str(tmp_path))
        names = [Path(f).name for f in result.files_written]
        assert "migration-report.json" in names


# ---------------------------------------------------------------------------
# 14. New fixture: hash_partitioned (orders with hashed partitionsSpec)
# ---------------------------------------------------------------------------

class TestHashPartitioned:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("hash_partitioned", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("hash_partitioned", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("hash_partitioned", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "orders"

    def test_dimension_fields_present(self, tmp_path):
        bundle("hash_partitioned", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        dim_names = {f["name"] for f in schema["dimensionFieldSpecs"]}
        assert dim_names == {"order_id", "customer_id", "product_id", "status"}

    def test_metric_fields_present(self, tmp_path):
        bundle("hash_partitioned", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        metric_names = {f["name"] for f in schema["metricFieldSpecs"]}
        assert "order_count" in metric_names
        assert "total_amount" in metric_names

    def test_partitioning_risk_detected(self, tmp_path):
        bundle("hash_partitioned", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "PARTITIONING_CONFIG_REQUIRED" in risk_ids

    def test_partitioning_risk_is_medium(self, tmp_path):
        bundle("hash_partitioned", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        p_risk = next(r for r in risks if r["risk_id"] == "PARTITIONING_CONFIG_REQUIRED")
        assert p_risk["severity"] == "medium"

    def test_partitioning_risk_evidence_mentions_hashed(self, tmp_path):
        bundle("hash_partitioned", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        p_risk = next(r for r in risks if r["risk_id"] == "PARTITIONING_CONFIG_REQUIRED")
        evidence_text = " ".join(p_risk["evidence"])
        assert "hashed" in evidence_text.lower()

    def test_classification_is_raw_event(self):
        spec = str(FIXTURES / "hash_partitioned" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.classification == "raw_event"

    def test_source_kind_is_batch(self):
        spec = str(FIXTURES / "hash_partitioned" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.source_kind == "batch"


# ---------------------------------------------------------------------------
# 15. New fixture: range_partitioned (sensor_readings with range + minmax)
# ---------------------------------------------------------------------------

class TestRangePartitioned:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("range_partitioned", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("range_partitioned", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("range_partitioned", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "sensor_readings"

    def test_typed_double_dimensions(self, tmp_path):
        bundle("range_partitioned", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        by_name = {f["name"]: f["dataType"] for f in schema["dimensionFieldSpecs"]}
        assert by_name["temperature"] == "DOUBLE"
        assert by_name["humidity"] == "DOUBLE"

    def test_minmax_metrics_types(self, tmp_path):
        """doubleMin/Max should map to DOUBLE, count to LONG."""
        bundle("range_partitioned", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        by_name = {f["name"]: f["dataType"] for f in schema["metricFieldSpecs"]}
        assert by_name["reading_count"] == "LONG"
        assert by_name["temp_min"] == "DOUBLE"
        assert by_name["temp_max"] == "DOUBLE"
        assert by_name["temp_sum"] == "DOUBLE"

    def test_partitioning_risk_detected(self, tmp_path):
        bundle("range_partitioned", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "PARTITIONING_CONFIG_REQUIRED" in risk_ids

    def test_partitioning_risk_evidence_mentions_range(self, tmp_path):
        bundle("range_partitioned", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        p_risk = next(r for r in risks if r["risk_id"] == "PARTITIONING_CONFIG_REQUIRED")
        evidence_text = " ".join(p_risk["evidence"])
        assert "range" in evidence_text.lower()

    def test_no_rollup_risk(self, tmp_path):
        bundle("range_partitioned", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "ROLLUP_SEMANTIC_MISMATCH" not in risk_ids


# ---------------------------------------------------------------------------
# 16. New fixture: multivalue_dims (content_tags with MV dimensions)
# ---------------------------------------------------------------------------

class TestMultivalueDims:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("multivalue_dims", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("multivalue_dims", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("multivalue_dims", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "content_tags"

    def test_all_dimensions_present(self, tmp_path):
        bundle("multivalue_dims", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        dim_names = {f["name"] for f in schema["dimensionFieldSpecs"]}
        assert {"content_id", "author", "tags", "categories", "language"} == dim_names

    def test_mv_ambiguity_risk_detected(self, tmp_path):
        bundle("multivalue_dims", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "MULTIVALUE_AMBIGUITY" in risk_ids

    def test_mv_ambiguity_risk_is_medium(self, tmp_path):
        bundle("multivalue_dims", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        mv_risk = next(r for r in risks if r["risk_id"] == "MULTIVALUE_AMBIGUITY")
        assert mv_risk["severity"] == "medium"

    def test_mv_ambiguity_evidence_mentions_mv_dims(self, tmp_path):
        bundle("multivalue_dims", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        mv_risk = next(r for r in risks if r["risk_id"] == "MULTIVALUE_AMBIGUITY")
        evidence_text = " ".join(mv_risk["evidence"])
        # tags and categories are the MV dims
        assert "tags" in evidence_text or "categories" in evidence_text

    def test_classification_is_raw_event(self):
        spec = str(FIXTURES / "multivalue_dims" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.classification == "raw_event"

    def test_two_dimensions_are_multi_value(self):
        spec = str(FIXTURES / "multivalue_dims" / "spec.json")
        result = normalize_spec(spec)
        mv_dims = [d for d in result.canonical.dimensions if d.multi_value]
        assert len(mv_dims) == 2
        mv_names = {d.name for d in mv_dims}
        assert mv_names == {"tags", "categories"}


# ---------------------------------------------------------------------------
# 17. New fixture: minmax_metrics (product_prices with rollup + full minmax)
# ---------------------------------------------------------------------------

class TestMinmaxMetrics:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("minmax_metrics", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("minmax_metrics", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("minmax_metrics", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "product_prices"

    def test_all_ten_metrics_present(self, tmp_path):
        bundle("minmax_metrics", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        metric_names = {f["name"] for f in schema["metricFieldSpecs"]}
        expected = {
            "price_updates", "price_min", "price_max", "price_sum",
            "qty_min", "qty_max", "qty_total",
            "score_sum", "score_min", "score_max",
        }
        assert expected == metric_names

    def test_double_metrics_type(self, tmp_path):
        bundle("minmax_metrics", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        by_name = {f["name"]: f["dataType"] for f in schema["metricFieldSpecs"]}
        assert by_name["price_min"] == "DOUBLE"
        assert by_name["price_max"] == "DOUBLE"
        assert by_name["price_sum"] == "DOUBLE"

    def test_long_metrics_type(self, tmp_path):
        bundle("minmax_metrics", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        by_name = {f["name"]: f["dataType"] for f in schema["metricFieldSpecs"]}
        assert by_name["qty_min"] == "LONG"
        assert by_name["qty_max"] == "LONG"
        assert by_name["qty_total"] == "LONG"

    def test_float_metrics_map_to_double(self, tmp_path):
        """Druid floatSum/Min/Max should map to DOUBLE in Pinot."""
        bundle("minmax_metrics", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        by_name = {f["name"]: f["dataType"] for f in schema["metricFieldSpecs"]}
        assert by_name["score_sum"] == "DOUBLE"
        assert by_name["score_min"] == "DOUBLE"
        assert by_name["score_max"] == "DOUBLE"

    def test_rollup_risk_detected(self, tmp_path):
        bundle("minmax_metrics", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "ROLLUP_SEMANTIC_MISMATCH" in risk_ids

    def test_classification_is_rolled_up_additive(self):
        spec = str(FIXTURES / "minmax_metrics" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.classification == "rolled_up_additive"

    def test_ten_metric_fields_in_canonical(self):
        spec = str(FIXTURES / "minmax_metrics" / "spec.json")
        result = normalize_spec(spec)
        assert len(result.canonical.metrics) == 10


# ---------------------------------------------------------------------------
# 18. New fixture: kinesis_stream (payment_events via Kinesis)
# ---------------------------------------------------------------------------

class TestKinesisStream:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("kinesis_stream", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_realtime_table(self, tmp_path):
        bundle("kinesis_stream", tmp_path)
        assert (tmp_path / "table-realtime.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("kinesis_stream", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "payment_events"

    def test_source_kind_is_stream(self):
        spec = str(FIXTURES / "kinesis_stream" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.source_kind == "stream"

    def test_no_stream_source_mismatch_risk(self, tmp_path):
        # v0.13.0: dpm now emits a proper KinesisConsumerFactory streamConfigs
        # block, so the legacy STREAM_SOURCE_MISMATCH risk no longer fires.
        bundle("kinesis_stream", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "STREAM_SOURCE_MISMATCH" not in risk_ids

    def test_realtime_streamconfigs_targets_kinesis_factory(self, tmp_path):
        bundle("kinesis_stream", tmp_path)
        table = load_json(tmp_path / "table-realtime.json")
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["streamType"] == "kinesis"
        assert sc["stream.kinesis.consumer.factory.class.name"].endswith(
            "KinesisConsumerFactory"
        )

    def test_realtime_table_has_stream_configs(self, tmp_path):
        bundle("kinesis_stream", tmp_path)
        table = load_json(tmp_path / "table-realtime.json")
        assert "streamConfigs" in table["tableIndexConfig"]

    def test_classification_is_raw_event(self):
        spec = str(FIXTURES / "kinesis_stream" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.classification == "raw_event"

    def test_metric_fields_present(self, tmp_path):
        bundle("kinesis_stream", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        metric_names = {f["name"] for f in schema["metricFieldSpecs"]}
        assert {"tx_count", "amount_usd", "failure_count"} == metric_names


# ---------------------------------------------------------------------------
# 19. New fixture: custom_timestamp (access_logs with Apache log format)
# ---------------------------------------------------------------------------

class TestCustomTimestamp:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("custom_timestamp", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("custom_timestamp", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("custom_timestamp", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "access_logs"

    def test_time_column_name(self, tmp_path):
        bundle("custom_timestamp", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["dateTimeFieldSpecs"][0]["name"] == "log_time"

    def test_custom_timestamp_risk_detected(self, tmp_path):
        bundle("custom_timestamp", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "CUSTOM_TIMESTAMP_FORMAT" in risk_ids

    def test_custom_timestamp_risk_is_medium(self, tmp_path):
        bundle("custom_timestamp", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk = next(r for r in risks if r["risk_id"] == "CUSTOM_TIMESTAMP_FORMAT")
        assert risk["severity"] == "medium"

    def test_custom_timestamp_evidence_mentions_column(self, tmp_path):
        bundle("custom_timestamp", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk = next(r for r in risks if r["risk_id"] == "CUSTOM_TIMESTAMP_FORMAT")
        evidence_text = " ".join(risk["evidence"])
        assert "log_time" in evidence_text

    def test_custom_format_warning_present(self):
        spec = str(FIXTURES / "custom_timestamp" / "spec.json")
        result = normalize_spec(spec)
        # The normalizer should emit a warning about the custom format
        combined_warnings = " ".join(result.warnings)
        assert "dd/MMM/yyyy" in combined_warnings or "custom" in combined_warnings.lower()

    def test_unsupported_feature_recorded(self):
        spec = str(FIXTURES / "custom_timestamp" / "spec.json")
        result = normalize_spec(spec)
        feature_names = [uf.feature for uf in result.canonical.unsupported_features]
        assert any("custom_timestamp_format" in f for f in feature_names)


# ---------------------------------------------------------------------------
# 20. New fixture: append_mode (audit_trail with appendToExisting=true)
# ---------------------------------------------------------------------------

class TestAppendMode:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("append_mode", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("append_mode", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("append_mode", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "audit_trail"

    def test_ingestion_behavior_risk_detected(self, tmp_path):
        bundle("append_mode", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "INGESTION_BEHAVIOR_MISMATCH" in risk_ids

    def test_ingestion_behavior_risk_is_info(self, tmp_path):
        bundle("append_mode", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk = next(r for r in risks if r["risk_id"] == "INGESTION_BEHAVIOR_MISMATCH")
        assert risk["severity"] == "info"

    def test_append_to_existing_warning(self):
        spec = str(FIXTURES / "append_mode" / "spec.json")
        result = normalize_spec(spec)
        combined_warnings = " ".join(result.warnings)
        assert "appendToExisting" in combined_warnings

    def test_no_blocking_risks(self, tmp_path):
        bundle("append_mode", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        blocking = [r for r in risks if r["severity"] == "blocking"]
        assert blocking == []

    def test_classification_is_raw_event(self):
        spec = str(FIXTURES / "append_mode" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.classification == "raw_event"


# ---------------------------------------------------------------------------
# 21. New fixture: gcs_input (app_events with GCS inputSource)
# ---------------------------------------------------------------------------

class TestGcsInput:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("gcs_input", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("gcs_input", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("gcs_input", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "app_events"

    def test_dimension_count(self, tmp_path):
        bundle("gcs_input", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert len(schema["dimensionFieldSpecs"]) == 6

    def test_metric_types(self, tmp_path):
        bundle("gcs_input", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        by_name = {f["name"]: f["dataType"] for f in schema["metricFieldSpecs"]}
        assert by_name["event_count"] == "LONG"
        assert by_name["revenue"] == "DOUBLE"
        assert by_name["session_time"] == "LONG"

    def test_gcs_warning_in_normalizer(self):
        spec = str(FIXTURES / "gcs_input" / "spec.json")
        result = normalize_spec(spec)
        combined_warnings = " ".join(result.warnings)
        assert "gcs" in combined_warnings.lower() or "google" in combined_warnings.lower()

    def test_classification_is_raw_event(self):
        spec = str(FIXTURES / "gcs_input" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.classification == "raw_event"

    def test_no_blocking_risks(self, tmp_path):
        bundle("gcs_input", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        blocking = [r for r in risks if r["severity"] == "blocking"]
        assert blocking == []


# ---------------------------------------------------------------------------
# 22. New fixture: complex_transforms (enriched_clicks with 3 complex exprs)
# ---------------------------------------------------------------------------

class TestComplexTransforms:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("complex_transforms", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("complex_transforms", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("complex_transforms", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "enriched_clicks"

    def test_transform_portability_risk_detected(self, tmp_path):
        bundle("complex_transforms", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "TRANSFORM_PORTABILITY_RISK" in risk_ids

    def test_transform_portability_risk_is_medium(self, tmp_path):
        bundle("complex_transforms", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk = next(r for r in risks if r["risk_id"] == "TRANSFORM_PORTABILITY_RISK")
        assert risk["severity"] == "medium"

    def test_three_transforms_in_canonical(self):
        spec = str(FIXTURES / "complex_transforms" / "spec.json")
        result = normalize_spec(spec)
        assert len(result.canonical.transforms) == 3

    def test_transform_names_correct(self):
        spec = str(FIXTURES / "complex_transforms" / "spec.json")
        result = normalize_spec(spec)
        names = {t.name for t in result.canonical.transforms}
        assert names == {"normalized_url", "campaign_label", "risk_tier"}

    def test_transform_risk_evidence_mentions_transforms(self, tmp_path):
        bundle("complex_transforms", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk = next(r for r in risks if r["risk_id"] == "TRANSFORM_PORTABILITY_RISK")
        evidence_text = " ".join(risk["evidence"])
        assert len(evidence_text) > 0

    def test_classification_is_raw_event(self):
        spec = str(FIXTURES / "complex_transforms" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.classification == "raw_event"


# ---------------------------------------------------------------------------
# 23. New fixture: nested_json (api_requests with flattenSpec)
# ---------------------------------------------------------------------------

class TestNestedJson:
    def test_pipeline_succeeds(self, tmp_path):
        result, _ = bundle("nested_json", tmp_path)
        assert result.success, f"generate_bundle failed: {result.errors}"

    def test_emits_offline_table(self, tmp_path):
        bundle("nested_json", tmp_path)
        assert (tmp_path / "table-offline.json").exists()

    def test_schema_name(self, tmp_path):
        bundle("nested_json", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        assert schema["schemaName"] == "api_requests"

    def test_flatten_spec_risk_detected(self, tmp_path):
        bundle("nested_json", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "FLATTEN_SPEC_NOT_PORTABLE" in risk_ids

    def test_flatten_spec_risk_is_high(self, tmp_path):
        bundle("nested_json", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk = next(r for r in risks if r["risk_id"] == "FLATTEN_SPEC_NOT_PORTABLE")
        assert risk["severity"] == "high"

    def test_flatten_spec_unsupported_feature_recorded(self):
        spec = str(FIXTURES / "nested_json" / "spec.json")
        result = normalize_spec(spec)
        feature_names = [uf.feature for uf in result.canonical.unsupported_features]
        assert "flattenSpec" in feature_names

    def test_dimensions_present_in_schema(self, tmp_path):
        bundle("nested_json", tmp_path)
        schema = load_json(tmp_path / "schema.json")
        dim_names = {f["name"] for f in schema["dimensionFieldSpecs"]}
        expected = {
            "request_id", "endpoint", "method",
            "user_id", "tenant_id", "client_ip", "response_status",
        }
        assert expected == dim_names

    def test_transforms_present_in_canonical(self):
        """The nested_json fixture also has transforms in the transformSpec."""
        spec = str(FIXTURES / "nested_json" / "spec.json")
        result = normalize_spec(spec)
        assert len(result.canonical.transforms) == 2
        transform_names = {t.name for t in result.canonical.transforms}
        assert transform_names == {"user_id", "tenant_id"}

    def test_classification_is_raw_event(self):
        spec = str(FIXTURES / "nested_json" / "spec.json")
        result = normalize_spec(spec)
        assert result.canonical.classification == "raw_event"

    def test_flatten_spec_risk_confidence_is_certain(self, tmp_path):
        bundle("nested_json", tmp_path)
        risks = load_json(tmp_path / "reports" / "risks.json")["risks"]
        risk = next(r for r in risks if r["risk_id"] == "FLATTEN_SPEC_NOT_PORTABLE")
        assert risk["confidence"] == "certain"
