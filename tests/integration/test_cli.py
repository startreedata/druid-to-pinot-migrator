from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from migrator.cli.app import app

FIXTURES = Path(__file__).parent.parent / "fixtures"
runner = CliRunner()


class TestInspectCommand:
    def test_inspect_raw_batch_exits_zero(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["inspect", spec])
        assert result.exit_code == 0, f"Non-zero exit: {result.output}"

    def test_inspect_raw_batch_json_is_valid(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["inspect", spec, "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "datasource_name" in data
        assert data["datasource_name"] == "pageviews"

    def test_inspect_raw_batch_json_has_classification(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["inspect", spec, "--json"])
        data = json.loads(result.output)
        assert "classification" in data
        assert data["classification"] == "raw_event"

    def test_inspect_raw_stream_exits_zero(self):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        result = runner.invoke(app, ["inspect", spec])
        assert result.exit_code == 0

    def test_inspect_rolled_up_exits_zero(self):
        spec = str(FIXTURES / "rolled_up" / "spec.json")
        result = runner.invoke(app, ["inspect", spec])
        assert result.exit_code == 0

    def test_inspect_missing_file_exits_nonzero(self):
        result = runner.invoke(app, ["inspect", "/nonexistent/path/spec.json"])
        assert result.exit_code != 0


class TestGenerateCommand:
    def test_generate_raw_batch_exits_zero(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["generate", spec, "--out", str(tmp_path)])
        assert result.exit_code == 0, f"Non-zero exit: {result.output}"

    def test_generate_raw_batch_creates_files(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        runner.invoke(app, ["generate", spec, "--out", str(tmp_path)])
        assert (tmp_path / "schema.json").exists()
        assert (tmp_path / "table-offline.json").exists()

    def test_generate_dry_run_exits_zero(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["generate", spec, "--out", str(tmp_path), "--dry-run"])
        assert result.exit_code == 0

    def test_generate_json_output_exits_zero(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["generate", spec, "--out", str(tmp_path), "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True

    def test_generate_stream_exits_zero(self, tmp_path):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        result = runner.invoke(app, ["generate", spec, "--out", str(tmp_path)])
        assert result.exit_code == 0


class TestValidateCommand:
    def test_validate_raw_batch_exits_zero(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["validate", spec])
        assert result.exit_code == 0, f"Non-zero exit: {result.output}"

    def test_validate_json_output_valid(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["validate", spec, "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "overall_status" in data
        assert "confidence_score" in data
        assert "checks" in data

    def test_validate_with_generated_dir(self, tmp_path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        # First generate
        runner.invoke(app, ["generate", spec, "--out", str(tmp_path)])
        # Then validate with generated dir
        result = runner.invoke(app, ["validate", spec, "--generated-dir", str(tmp_path)])
        assert result.exit_code == 0

    def test_validate_rolled_up_exits_zero(self):
        """Rolled-up spec has risks but validation should still run and exit 0 (warnings != fail)."""
        spec = str(FIXTURES / "rolled_up" / "spec.json")
        result = runner.invoke(app, ["validate", spec])
        # exit code 0 because static checks pass; risks don't make overall_status=fail
        assert result.exit_code == 0
