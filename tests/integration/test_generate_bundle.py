from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from migrator.translators.pipeline import generate_bundle

FIXTURES = Path(__file__).parent.parent / "fixtures"


class TestGenerateBundle:
    def test_generate_bundle_raw_batch_succeeds(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        result = generate_bundle(spec_path, out_dir=str(tmp_path))
        assert result.success, f"Expected success but got errors: {result.errors}"

    def test_generate_bundle_raw_batch_creates_schema(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        schema_path = tmp_path / "schema.json"
        assert schema_path.exists(), "schema.json not found"

    def test_generate_bundle_raw_batch_creates_offline_table(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        table_path = tmp_path / "table-offline.json"
        assert table_path.exists(), "table-offline.json not found"

    def test_generate_bundle_raw_batch_creates_migration_report(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        report_path = tmp_path / "reports" / "migration-report.json"
        assert report_path.exists(), "reports/migration-report.json not found"

    def test_generate_bundle_raw_batch_creates_markdown(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        md_path = tmp_path / "reports" / "migration-summary.md"
        assert md_path.exists(), "reports/migration-summary.md not found"

    def test_generated_schema_is_valid_json(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        schema_path = tmp_path / "schema.json"
        data = json.loads(schema_path.read_text())
        assert "schemaName" in data
        assert "dimensionFieldSpecs" in data
        assert "metricFieldSpecs" in data
        assert "dateTimeFieldSpecs" in data

    def test_generated_table_is_valid_json(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        table_path = tmp_path / "table-offline.json"
        data = json.loads(table_path.read_text())
        assert "tableName" in data
        assert "tableType" in data

    def test_generated_migration_report_is_valid_json(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        report_path = tmp_path / "reports" / "migration-report.json"
        data = json.loads(report_path.read_text())
        assert "datasource_name" in data
        assert "risks" in data

    def test_generated_markdown_is_non_empty(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        md_path = tmp_path / "reports" / "migration-summary.md"
        content = md_path.read_text()
        assert len(content) > 100
        assert "pageviews" in content

    def test_dry_run_writes_no_files(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        result = generate_bundle(spec_path, out_dir=str(tmp_path), dry_run=True)
        assert result.success
        assert result.files_written == []
        # No files should be created
        all_files = list(tmp_path.rglob("*"))
        assert all_files == []

    def test_generate_bundle_stream_creates_realtime_table(self, tmp_path):
        spec_path = str(FIXTURES / "raw_stream" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        table_path = tmp_path / "table-realtime.json"
        assert table_path.exists(), "table-realtime.json not found"

    def test_generate_bundle_unsupported_complex_succeeds(self, tmp_path):
        """Even unsupported complex fixtures should complete (with warnings/risks)."""
        spec_path = str(FIXTURES / "unsupported_complex" / "spec.json")
        result = generate_bundle(spec_path, out_dir=str(tmp_path))
        assert result.success

    def test_schema_name_matches_datasource(self, tmp_path):
        spec_path = str(FIXTURES / "raw_batch" / "spec.json")
        generate_bundle(spec_path, out_dir=str(tmp_path))
        data = json.loads((tmp_path / "schema.json").read_text())
        assert data["schemaName"] == "pageviews"
