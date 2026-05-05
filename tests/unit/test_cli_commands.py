"""CLI command coverage via Typer's CliRunner.

These tests deliberately mock the network-bound surface (Druid/Pinot
clients, the cutover orchestrator, the deploy helper) so the CLI layer
itself — argument plumbing, error mapping, output formatting, exit
codes — gets exercised without a live cluster. The "did this command
do the right thing under the hood" check is the responsibility of the
unit tests for the underlying module.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from migrator.cli.app import app
from migrator.parity.models import ParityResult
from migrator.pinot.deployer import DeployReport, DeployResult
from migrator.preflight import PreflightCheck
from migrator.realtime.backfill_runner import BackfillResult
from migrator.realtime.cutover import CutoverReport, CutoverStepResult
from migrator.realtime.models import KafkaOffsetMap, KafkaPartitionOffset


FIXTURES = Path(__file__).parent.parent / "fixtures"
runner = CliRunner()


# ─────────────────────────────────────────────────────────────────────────────
# normalize
# ─────────────────────────────────────────────────────────────────────────────


class TestNormalizeCommand:
    def test_normalize_help(self):
        result = runner.invoke(app, ["normalize", "--help"])
        assert result.exit_code == 0
        assert "canonical" in result.output.lower()

    def test_normalize_raw_batch_emits_json_to_stdout(self):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["normalize", spec])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["datasource_name"] == "pageviews"

    def test_normalize_writes_to_file_when_out_given(self, tmp_path: Path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        out = tmp_path / "canonical.json"
        result = runner.invoke(app, ["normalize", spec, "--out", str(out)])
        assert result.exit_code == 0
        assert out.exists()
        assert json.loads(out.read_text())["datasource_name"] == "pageviews"

    def test_normalize_missing_spec_exits_nonzero(self):
        result = runner.invoke(app, ["normalize", "/nope/missing.json"])
        assert result.exit_code != 0


# ─────────────────────────────────────────────────────────────────────────────
# generate
# ─────────────────────────────────────────────────────────────────────────────


class TestGenerateCommand:
    def test_generate_help(self):
        result = runner.invoke(app, ["generate", "--help"])
        assert result.exit_code == 0

    def test_generate_writes_artifacts(self, tmp_path: Path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, ["generate", spec, "--out", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "schema.json").exists()
        assert (tmp_path / "table-offline.json").exists()

    def test_generate_realtime_writes_realtime_table(self, tmp_path: Path):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        result = runner.invoke(app, ["generate", spec, "--out", str(tmp_path)])
        assert result.exit_code == 0
        assert (tmp_path / "table-realtime.json").exists()


# ─────────────────────────────────────────────────────────────────────────────
# validate
# ─────────────────────────────────────────────────────────────────────────────


class TestValidateCommand:
    def test_validate_help(self):
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0

    def test_validate_emits_json(self, tmp_path: Path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        runner.invoke(app, ["generate", spec, "--out", str(tmp_path)])
        result = runner.invoke(
            app, ["validate", spec, "--generated-dir", str(tmp_path), "--json"],
        )
        assert result.exit_code == 0, result.output
        assert "overall_status" in json.loads(result.output)


# ─────────────────────────────────────────────────────────────────────────────
# plan-hybrid
# ─────────────────────────────────────────────────────────────────────────────


def _write_offset_map(path: Path, supervisor: str = "test_sup") -> None:
    """Stage a minimal offset-map file that the plan-hybrid loader accepts."""
    offset_map = KafkaOffsetMap(
        supervisor_id=supervisor,
        topic="test_topic",
        datasource="test_topic",
        watermark_iso="2024-01-01T00:00:00.000+00:00",
        watermark_ms=1704067200000,
        offsets=[
            KafkaPartitionOffset(partition=0, offset=42),
            KafkaPartitionOffset(partition=1, offset=43),
        ],
    )
    path.write_text(json.dumps(offset_map.model_dump(mode="json"), indent=2))


class TestPlanHybridCommand:
    def test_plan_hybrid_help(self):
        result = runner.invoke(app, ["plan-hybrid", "--help"])
        assert result.exit_code == 0

    def test_plan_hybrid_writes_artifacts(self, tmp_path: Path):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        offset_map = tmp_path / "offsets.json"
        _write_offset_map(offset_map)
        out = tmp_path / "plan"
        result = runner.invoke(app, [
            "plan-hybrid", spec,
            "--offset-map", str(offset_map),
            "--out", str(out),
        ])
        assert result.exit_code == 0, result.output
        # write_hybrid_plan emits at least the runbook + table configs
        assert any(out.iterdir())

    def test_plan_hybrid_json_output(self, tmp_path: Path):
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        offset_map = tmp_path / "offsets.json"
        _write_offset_map(offset_map)
        out = tmp_path / "plan"
        result = runner.invoke(app, [
            "plan-hybrid", spec,
            "--offset-map", str(offset_map),
            "--out", str(out),
            "--json",
        ])
        assert result.exit_code == 0
        assert json.loads(result.output)


# ─────────────────────────────────────────────────────────────────────────────
# extract-offsets
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractOffsetsCommand:
    def test_extract_offsets_help(self):
        result = runner.invoke(app, ["extract-offsets", "--help"])
        assert result.exit_code == 0

    def test_extract_offsets_writes_file(self, tmp_path: Path):
        offset_map = KafkaOffsetMap(
            supervisor_id="sup1",
            topic="t1",
            datasource="t1",
            watermark_iso="2024-01-01T00:00:00.000+00:00",
            watermark_ms=1704067200000,
            offsets=[KafkaPartitionOffset(partition=0, offset=99)],
        )
        with patch(
            "migrator.cli.commands.extract_offsets.DruidOverlordClient"
        ) as mock_client:
            mock_client.return_value.get_supervisor_offsets.return_value = offset_map
            out = tmp_path / "offsets.json"
            result = runner.invoke(app, [
                "extract-offsets",
                "--supervisor-id", "sup1",
                "--out", str(out),
            ])
        assert result.exit_code == 0, result.output
        assert out.exists()
        assert "sup1" in result.output

    def test_extract_offsets_json_mode(self, tmp_path: Path):
        offset_map = KafkaOffsetMap(
            supervisor_id="sup1", topic="t1",
            datasource="t1",
            watermark_iso="2024-01-01T00:00:00.000+00:00",
            watermark_ms=1704067200000,
            offsets=[KafkaPartitionOffset(partition=0, offset=99)],
        )
        with patch(
            "migrator.cli.commands.extract_offsets.DruidOverlordClient"
        ) as mock_client:
            mock_client.return_value.get_supervisor_offsets.return_value = offset_map
            out = tmp_path / "offsets.json"
            result = runner.invoke(app, [
                "extract-offsets",
                "--supervisor-id", "sup1",
                "--out", str(out),
                "--json",
            ])
        assert result.exit_code == 0
        assert json.loads(result.output)["supervisor_id"] == "sup1"

    def test_extract_offsets_invalid_auth_exits_2(self, tmp_path: Path):
        result = runner.invoke(app, [
            "extract-offsets",
            "--supervisor-id", "sup1",
            "--druid-auth", "garbage-no-colon",
            "--out", str(tmp_path / "x.json"),
        ])
        assert result.exit_code == 2
        assert "auth" in result.output.lower()


# ─────────────────────────────────────────────────────────────────────────────
# deploy
# ─────────────────────────────────────────────────────────────────────────────


def _ok_deploy_report() -> DeployReport:
    return DeployReport(results=[
        DeployResult(artifact="schema", name="t", status="created"),
        DeployResult(artifact="table-offline", name="t_OFFLINE", status="created"),
    ])


class TestDeployCommand:
    def test_deploy_help(self):
        result = runner.invoke(app, ["deploy", "--help"])
        assert result.exit_code == 0

    def test_deploy_with_artifacts_dir(self, tmp_path: Path):
        # Set up artifact files the deploy CLI can discover.
        (tmp_path / "schema.json").write_text(
            json.dumps({"schemaName": "t"})
        )
        (tmp_path / "table-offline.json").write_text(
            json.dumps({"tableName": "t_OFFLINE"})
        )
        with patch(
            "migrator.cli.commands.deploy.PinotDeployer"
        ) as mock_dep:
            mock_dep.return_value.deploy.return_value = _ok_deploy_report()
            result = runner.invoke(app, [
                "deploy", "--artifacts-dir", str(tmp_path),
            ])
        assert result.exit_code == 0, result.output
        assert "created" in result.output

    def test_deploy_no_inputs_exits_2(self):
        result = runner.invoke(app, ["deploy"])
        assert result.exit_code == 2
        assert "Nothing to deploy" in result.output

    def test_deploy_explicit_schema_flag(self, tmp_path: Path):
        schema = tmp_path / "schema.json"
        schema.write_text(json.dumps({"schemaName": "t"}))
        with patch(
            "migrator.cli.commands.deploy.PinotDeployer"
        ) as mock_dep:
            mock_dep.return_value.deploy.return_value = DeployReport(results=[
                DeployResult(artifact="schema", name="t", status="created"),
            ])
            result = runner.invoke(app, ["deploy", "--schema", str(schema)])
        assert result.exit_code == 0

    def test_deploy_invalid_auth_exits_2(self, tmp_path: Path):
        schema = tmp_path / "schema.json"
        schema.write_text(json.dumps({"schemaName": "t"}))
        result = runner.invoke(app, [
            "deploy", "--schema", str(schema),
            "--pinot-auth", "garbage-no-colon",
        ])
        assert result.exit_code == 2

    def test_deploy_failure_propagates_nonzero(self, tmp_path: Path):
        schema = tmp_path / "schema.json"
        schema.write_text(json.dumps({"schemaName": "t"}))
        with patch(
            "migrator.cli.commands.deploy.PinotDeployer"
        ) as mock_dep:
            mock_dep.return_value.deploy.return_value = DeployReport(results=[
                DeployResult(
                    artifact="schema", name="t", status="error",
                    detail="boom",
                ),
            ])
            result = runner.invoke(app, ["deploy", "--schema", str(schema)])
        assert result.exit_code == 1


# ─────────────────────────────────────────────────────────────────────────────
# backfill-batch
# ─────────────────────────────────────────────────────────────────────────────


class TestBackfillBatchCommand:
    def test_backfill_help(self):
        result = runner.invoke(app, ["backfill-batch", "--help"])
        assert result.exit_code == 0

    def test_backfill_default_mode(self, tmp_path: Path):
        with patch(
            "migrator.cli.commands.backfill_batch.run_backfill",
            return_value=BackfillResult(
                rows_dumped=5, pages_dumped=1, files_ingested=1,
                staging_dir=tmp_path,
            ),
        ):
            result = runner.invoke(app, [
                "backfill-batch",
                "--datasource", "ds",
                "--pinot-table", "ds",
                "--start-iso", "2024-01-01T00:00:00Z",
                "--end-iso",   "2024-02-01T00:00:00Z",
                "--staging-dir", str(tmp_path),
            ])
        assert result.exit_code == 0, result.output
        assert "5 rows" in result.output

    def test_backfill_uri_mode(self, tmp_path: Path):
        with patch(
            "migrator.cli.commands.backfill_batch.run_backfill",
            return_value=BackfillResult(
                rows_dumped=2, pages_dumped=1, files_ingested=1,
                staging_dir=tmp_path,
            ),
        ):
            result = runner.invoke(app, [
                "backfill-batch",
                "--datasource", "ds",
                "--pinot-table", "ds",
                "--start-iso", "2024-01-01T00:00:00Z",
                "--end-iso",   "2024-02-01T00:00:00Z",
                "--staging-dir", str(tmp_path),
                "--mode", "ingest-from-uri",
                "--uri-prefix", "file:///tmp/x/",
            ])
        assert result.exit_code == 0

    def test_backfill_invalid_mode_exits_2(self, tmp_path: Path):
        result = runner.invoke(app, [
            "backfill-batch",
            "--datasource", "ds",
            "--pinot-table", "ds",
            "--start-iso", "2024-01-01T00:00:00Z",
            "--end-iso",   "2024-02-01T00:00:00Z",
            "--staging-dir", str(tmp_path),
            "--mode", "made-up",
        ])
        assert result.exit_code == 2
        assert "made-up" in result.output

    def test_backfill_invalid_auth_exits_2(self, tmp_path: Path):
        result = runner.invoke(app, [
            "backfill-batch",
            "--datasource", "ds",
            "--pinot-table", "ds",
            "--start-iso", "2024-01-01T00:00:00Z",
            "--end-iso",   "2024-02-01T00:00:00Z",
            "--staging-dir", str(tmp_path),
            "--druid-auth", "garbage-no-colon",
        ])
        assert result.exit_code == 2


# ─────────────────────────────────────────────────────────────────────────────
# parity-check
# ─────────────────────────────────────────────────────────────────────────────


class TestParityCheckCommand:
    def test_parity_help(self):
        result = runner.invoke(app, ["parity-check", "--help"])
        assert result.exit_code == 0

    def test_parity_requires_one_of_queries_or_canonical(self):
        result = runner.invoke(app, [
            "parity-check",
            "--pinot-table", "t",
        ])
        assert result.exit_code == 2

    def test_parity_from_canonical_happy_path(self, tmp_path: Path):
        spec = FIXTURES / "raw_batch" / "spec.json"
        passing = ParityResult(
            label="row count", passed=True, detail="ok",
            druid_value=10, pinot_value=10,
        )
        with patch(
            "migrator.cli.commands.parity_check.run_parity",
            return_value=[passing],
        ):
            result = runner.invoke(app, [
                "parity-check",
                "--pinot-table", "pageviews",
                "--from-canonical", str(spec),
            ])
        assert result.exit_code == 0
        assert "PASS" in result.output

    def test_parity_failure_returns_nonzero(self, tmp_path: Path):
        spec = FIXTURES / "raw_batch" / "spec.json"
        failing = ParityResult(
            label="row count", passed=False, detail="mismatch",
            druid_value=10, pinot_value=9,
        )
        with patch(
            "migrator.cli.commands.parity_check.run_parity",
            return_value=[failing],
        ):
            result = runner.invoke(app, [
                "parity-check",
                "--pinot-table", "pageviews",
                "--from-canonical", str(spec),
            ])
        assert result.exit_code == 1

    def test_parity_json_output(self):
        spec = FIXTURES / "raw_batch" / "spec.json"
        passing = ParityResult(
            label="row count", passed=True, detail="ok",
            druid_value=1, pinot_value=1,
        )
        with patch(
            "migrator.cli.commands.parity_check.run_parity",
            return_value=[passing],
        ):
            result = runner.invoke(app, [
                "parity-check",
                "--pinot-table", "pageviews",
                "--from-canonical", str(spec),
                "--json",
            ])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["passed"] == 1

    def test_parity_invalid_auth_exits_2(self):
        spec = FIXTURES / "raw_batch" / "spec.json"
        result = runner.invoke(app, [
            "parity-check",
            "--pinot-table", "pageviews",
            "--from-canonical", str(spec),
            "--druid-auth", "garbage-no-colon",
        ])
        assert result.exit_code == 2

    def test_parity_canonical_load_failure_exits_2(self, tmp_path: Path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json{")
        result = runner.invoke(app, [
            "parity-check",
            "--pinot-table", "x",
            "--from-canonical", str(bad),
        ])
        assert result.exit_code == 2


# ─────────────────────────────────────────────────────────────────────────────
# cutover
# ─────────────────────────────────────────────────────────────────────────────


def _ok_cutover_report(out_dir: Path) -> CutoverReport:
    return CutoverReport(
        steps=[
            CutoverStepResult(step="offsets", status="ok", detail="captured"),
            CutoverStepResult(step="deploy", status="ok", detail="2 created"),
            CutoverStepResult(step="backfill", status="skipped", detail=""),
        ],
        out_dir=out_dir,
        parity=[ParityResult(
            label="row count", passed=True, detail="ok",
            druid_value=1, pinot_value=1,
        )],
    )


class TestCutoverCommand:
    def test_cutover_help(self):
        result = runner.invoke(app, ["cutover", "--help"])
        assert result.exit_code == 0

    def test_cutover_happy_path(self, tmp_path: Path):
        spec = FIXTURES / "raw_stream" / "spec.json"
        with patch(
            "migrator.cli.commands.cutover.run_cutover",
            return_value=_ok_cutover_report(tmp_path / "out"),
        ):
            result = runner.invoke(app, [
                "cutover",
                "--supervisor-id", "sup1",
                "--datasource", "ds",
                "--pinot-table", "ds",
                "--spec", str(spec),
                "--out", str(tmp_path / "out"),
                "--staging-dir", str(tmp_path / "staging"),
            ])
        assert result.exit_code == 0, result.output
        assert "Cutover" in result.output
        assert "Parity:" in result.output

    def test_cutover_failed_step_exits_nonzero(self, tmp_path: Path):
        spec = FIXTURES / "raw_stream" / "spec.json"
        bad = CutoverReport(
            steps=[CutoverStepResult(
                step="deploy", status="error", detail="boom",
            )],
            out_dir=tmp_path / "out",
        )
        with patch(
            "migrator.cli.commands.cutover.run_cutover",
            return_value=bad,
        ):
            result = runner.invoke(app, [
                "cutover",
                "--supervisor-id", "sup1",
                "--datasource", "ds",
                "--pinot-table", "ds",
                "--spec", str(spec),
                "--out", str(tmp_path / "out"),
            ])
        assert result.exit_code == 1

    def test_cutover_invalid_auth_exits_2(self, tmp_path: Path):
        spec = FIXTURES / "raw_stream" / "spec.json"
        result = runner.invoke(app, [
            "cutover",
            "--supervisor-id", "sup1",
            "--datasource", "ds",
            "--pinot-table", "ds",
            "--spec", str(spec),
            "--out", str(tmp_path / "out"),
            "--druid-auth", "garbage-no-colon",
        ])
        assert result.exit_code == 2

    def test_cutover_no_resume_flag_threads_through(self, tmp_path: Path):
        # Verify the --no-resume flag reaches the orchestrator's
        # CutoverConfig. We don't run a real cutover; we just check
        # the flag wiring by capturing the cfg passed to run_cutover.
        spec = FIXTURES / "raw_stream" / "spec.json"
        captured = {}

        def fake_run_cutover(cfg, **_kwargs):
            captured["cfg"] = cfg
            return _ok_cutover_report(tmp_path / "out")

        with patch(
            "migrator.cli.commands.cutover.run_cutover",
            side_effect=fake_run_cutover,
        ):
            result = runner.invoke(app, [
                "cutover",
                "--supervisor-id", "sup1",
                "--datasource", "ds",
                "--pinot-table", "ds",
                "--spec", str(spec),
                "--out", str(tmp_path / "out"),
                "--no-resume",
            ])
        assert result.exit_code == 0, result.output
        assert captured["cfg"].resume is False
        assert captured["cfg"].restart_from is None

    def test_cutover_restart_from_flag_threads_through(self, tmp_path: Path):
        spec = FIXTURES / "raw_stream" / "spec.json"
        captured = {}

        def fake_run_cutover(cfg, **_kwargs):
            captured["cfg"] = cfg
            return _ok_cutover_report(tmp_path / "out")

        with patch(
            "migrator.cli.commands.cutover.run_cutover",
            side_effect=fake_run_cutover,
        ):
            result = runner.invoke(app, [
                "cutover",
                "--supervisor-id", "sup1",
                "--datasource", "ds",
                "--pinot-table", "ds",
                "--spec", str(spec),
                "--out", str(tmp_path / "out"),
                "--restart-from", "parity",
            ])
        assert result.exit_code == 0, result.output
        assert captured["cfg"].resume is True
        assert captured["cfg"].restart_from == "parity"


# ─────────────────────────────────────────────────────────────────────────────
# translate-lookups
# ─────────────────────────────────────────────────────────────────────────────


class TestTranslateLookupsCommand:
    def test_translate_help(self):
        result = runner.invoke(app, ["translate-lookups", "--help"])
        assert result.exit_code == 0

    def test_translate_static_map(self, tmp_path: Path):
        result = runner.invoke(app, [
            "translate-lookups",
            "--lookups", str(FIXTURES / "lookups" / "static_map.json"),
            "--out", str(tmp_path),
        ])
        assert result.exit_code == 0, result.output
        # Two static-map lookups in the fixture, each gets its own dir.
        assert (tmp_path / "lookup_country_code_to_name").is_dir()
        assert (tmp_path / "lookup_country_code_to_name" / "data.json").exists()

    def test_translate_uri_csv(self, tmp_path: Path):
        result = runner.invoke(app, [
            "translate-lookups",
            "--lookups", str(FIXTURES / "lookups" / "uri_csv.json"),
            "--out", str(tmp_path),
        ])
        assert result.exit_code == 0
        # URI sources do NOT inline data.json
        d = tmp_path / "lookup_campaign_lookup"
        assert d.is_dir()
        assert not (d / "data.json").exists()

    def test_translate_json_output(self, tmp_path: Path):
        result = runner.invoke(app, [
            "translate-lookups",
            "--lookups", str(FIXTURES / "lookups" / "static_map.json"),
            "--out", str(tmp_path),
            "--json",
        ])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert len(payload) == 2
        assert {p["source_kind"] for p in payload} == {"static_map"}

    def test_translate_jdbc_rejected(self, tmp_path: Path):
        result = runner.invoke(app, [
            "translate-lookups",
            "--lookups", str(FIXTURES / "lookups" / "unsupported_jdbc.json"),
            "--out", str(tmp_path),
        ])
        assert result.exit_code == 2
        assert "jdbc" in result.output.lower()

    def test_translate_missing_file_exits_2(self, tmp_path: Path):
        result = runner.invoke(app, [
            "translate-lookups",
            "--lookups", "/nope/missing.json",
            "--out", str(tmp_path),
        ])
        assert result.exit_code == 2

    def test_translate_empty_input_exits_2(self, tmp_path: Path):
        empty = tmp_path / "empty.json"
        empty.write_text("{}")
        result = runner.invoke(app, [
            "translate-lookups",
            "--lookups", str(empty),
            "--out", str(tmp_path / "out"),
        ])
        assert result.exit_code == 2

    def test_translate_custom_prefix(self, tmp_path: Path):
        result = runner.invoke(app, [
            "translate-lookups",
            "--lookups", str(FIXTURES / "lookups" / "static_map.json"),
            "--out", str(tmp_path),
            "--table-name-prefix", "dim_",
        ])
        assert result.exit_code == 0
        assert (tmp_path / "dim_country_code_to_name").is_dir()


# ─────────────────────────────────────────────────────────────────────────────
# doctor
# ─────────────────────────────────────────────────────────────────────────────


def _ok(name: str, target: str, detail: str = "ok") -> PreflightCheck:
    return PreflightCheck(name=name, target=target, ok=True, detail=detail)


def _fail(name: str, target: str, detail: str = "failed") -> PreflightCheck:
    return PreflightCheck(name=name, target=target, ok=False, detail=detail)


class TestDoctorCommand:
    def test_doctor_help(self):
        result = runner.invoke(app, ["doctor", "--help"])
        assert result.exit_code == 0
        assert "preflight" in result.output.lower() or "probe" in result.output.lower()

    def test_doctor_all_green_exits_zero(self):
        with patch(
            "migrator.cli.commands.doctor.probe_druid_router",
            return_value=_ok("druid-router", "http://druid", "version 31.0.0"),
        ), patch(
            "migrator.cli.commands.doctor.probe_pinot_controller",
            return_value=_ok("pinot-controller", "http://pinot", "version 1.5.0"),
        ):
            result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0, result.output
        assert "Result: 2 ok, 0 failed" in result.output

    def test_doctor_failure_exits_one(self):
        with patch(
            "migrator.cli.commands.doctor.probe_druid_router",
            return_value=_fail("druid-router", "http://druid", "unreachable"),
        ), patch(
            "migrator.cli.commands.doctor.probe_pinot_controller",
            return_value=_ok("pinot-controller", "http://pinot"),
        ):
            result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 1
        assert "1 ok, 1 failed" in result.output

    def test_doctor_optional_probes_only_run_when_flagged(self):
        # Without --pinot-broker/--datasource/--pinot-tenant, those
        # probes should not be invoked. We assert by side-channel: the
        # mocks are NOT patched, so if they ran the test would fail
        # with a real network call. The Result line confirms 2 checks.
        with patch(
            "migrator.cli.commands.doctor.probe_druid_router",
            return_value=_ok("druid-router", "http://druid"),
        ), patch(
            "migrator.cli.commands.doctor.probe_pinot_controller",
            return_value=_ok("pinot-controller", "http://pinot"),
        ):
            result = runner.invoke(app, ["doctor"])
        assert "Result: 2 ok" in result.output

    def test_doctor_runs_optional_probes_when_flags_set(self):
        with patch(
            "migrator.cli.commands.doctor.probe_druid_router",
            return_value=_ok("druid-router", "http://druid"),
        ), patch(
            "migrator.cli.commands.doctor.probe_pinot_controller",
            return_value=_ok("pinot-controller", "http://pinot"),
        ), patch(
            "migrator.cli.commands.doctor.probe_pinot_broker",
            return_value=_ok("pinot-broker", "http://pinot:8099"),
        ), patch(
            "migrator.cli.commands.doctor.probe_druid_datasource",
            return_value=_ok("druid-datasource", "ds", "exists"),
        ), patch(
            "migrator.cli.commands.doctor.probe_pinot_tenant",
            return_value=_ok("pinot-tenant", "DefaultTenant", "exists"),
        ):
            result = runner.invoke(app, [
                "doctor",
                "--pinot-broker", "http://pinot:8099",
                "--datasource", "ds",
                "--pinot-tenant", "DefaultTenant",
            ])
        assert result.exit_code == 0
        assert "Result: 5 ok, 0 failed" in result.output

    def test_doctor_json_output(self):
        with patch(
            "migrator.cli.commands.doctor.probe_druid_router",
            return_value=_ok("druid-router", "http://druid", "version 31.0.0"),
        ), patch(
            "migrator.cli.commands.doctor.probe_pinot_controller",
            return_value=_fail("pinot-controller", "http://pinot", "HTTP 503"),
        ):
            result = runner.invoke(app, ["doctor", "--json"])
        assert result.exit_code == 1   # any failure → exit 1
        payload = json.loads(result.output)
        assert payload["ok"] is False
        assert len(payload["checks"]) == 2
        assert payload["checks"][0]["ok"] is True
        assert payload["checks"][1]["ok"] is False

    def test_doctor_invalid_auth_exits_2(self):
        result = runner.invoke(app, [
            "doctor", "--druid-auth", "garbage-no-colon",
        ])
        assert result.exit_code == 2
        assert "auth" in result.output.lower()


# ─────────────────────────────────────────────────────────────────────────────
# diff-spec
# ─────────────────────────────────────────────────────────────────────────────


SAMPLE_DIFF_SPEC = {
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


class TestDiffSpecCommand:
    def test_diff_spec_help(self):
        result = runner.invoke(app, ["diff-spec", "--help"])
        assert result.exit_code == 0

    def test_diff_spec_no_change_exit_zero(self, tmp_path: Path):
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        a.write_text(json.dumps(SAMPLE_DIFF_SPEC))
        b.write_text(json.dumps(SAMPLE_DIFF_SPEC))
        result = runner.invoke(app, ["diff-spec", str(a), str(b)])
        assert result.exit_code == 0
        assert "No semantic change" in result.output

    def test_diff_spec_with_change_renders_pretty_text(self, tmp_path: Path):
        import copy as _copy
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        a.write_text(json.dumps(SAMPLE_DIFF_SPEC))
        new_spec = _copy.deepcopy(SAMPLE_DIFF_SPEC)
        new_spec["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"].append("device")
        b.write_text(json.dumps(new_spec))
        result = runner.invoke(app, ["diff-spec", str(a), str(b)])
        assert result.exit_code == 0   # default: doesn't fail on change
        assert "device" in result.output
        assert "Pinot implications" in result.output

    def test_diff_spec_exit_on_change_returns_3(self, tmp_path: Path):
        import copy as _copy
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        a.write_text(json.dumps(SAMPLE_DIFF_SPEC))
        new_spec = _copy.deepcopy(SAMPLE_DIFF_SPEC)
        new_spec["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"].append("device")
        b.write_text(json.dumps(new_spec))
        result = runner.invoke(app, [
            "diff-spec", str(a), str(b), "--exit-on-change",
        ])
        # Custom exit code 3 distinguishes "spec changed" from
        # "command errored" (2). CI guard scripts can branch on it.
        assert result.exit_code == 3

    def test_diff_spec_json_output(self, tmp_path: Path):
        import copy as _copy
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        a.write_text(json.dumps(SAMPLE_DIFF_SPEC))
        new_spec = _copy.deepcopy(SAMPLE_DIFF_SPEC)
        new_spec["spec"]["dataSchema"]["dataSource"] = "events_v2"
        b.write_text(json.dumps(new_spec))
        result = runner.invoke(app, [
            "diff-spec", str(a), str(b), "--json",
        ])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["is_empty"] is False
        assert payload["datasource_name_changed"]["new"] == "events_v2"

    def test_diff_spec_unparseable_input_exits_2(self, tmp_path: Path):
        bad = tmp_path / "bad.json"
        bad.write_text("not even json{")
        good = tmp_path / "good.json"
        good.write_text(json.dumps(SAMPLE_DIFF_SPEC))
        result = runner.invoke(app, ["diff-spec", str(bad), str(good)])
        assert result.exit_code == 2
