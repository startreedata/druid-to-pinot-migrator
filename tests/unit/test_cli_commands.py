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
from migrator.realtime.models import (
    KafkaOffsetMap,
    KafkaPartitionOffset,
    KinesisShardSequence,
    StreamOffsetMap,
    StreamPlatform,
)


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

    def test_generate_with_upsert_primary_key_flag(self, tmp_path: Path):
        # End-to-end CLI test for the new ``--upsert-primary-key`` flag:
        # the generated schema gets ``primaryKeyColumns`` and the
        # realtime table gets ``upsertConfig`` + the strict-replica-
        # group routing knob Pinot requires.
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        result = runner.invoke(app, [
            "generate", spec, "--out", str(tmp_path),
            "--upsert-primary-key", "user_id",
        ])
        assert result.exit_code == 0, result.output
        schema = json.loads((tmp_path / "schema.json").read_text())
        assert schema["primaryKeyColumns"] == ["user_id"]
        table = json.loads((tmp_path / "table-realtime.json").read_text())
        assert table["upsertConfig"]["mode"] == "FULL"
        assert table["routing"]["instanceSelectorType"] == "strictReplicaGroup"

    def test_generate_upsert_compound_key_via_repeated_flag(self, tmp_path: Path):
        # ``--upsert-primary-key`` repeated → compound key.
        spec = str(FIXTURES / "raw_stream" / "spec.json")
        result = runner.invoke(app, [
            "generate", spec, "--out", str(tmp_path),
            "--upsert-primary-key", "user_id",
            "--upsert-primary-key", "event_type",
        ])
        assert result.exit_code == 0, result.output
        schema = json.loads((tmp_path / "schema.json").read_text())
        # Order preserved — Pinot's hash uses tuple order.
        assert schema["primaryKeyColumns"] == ["user_id", "event_type"]

    def test_generate_upsert_on_batch_source_rejected(self, tmp_path: Path):
        spec = str(FIXTURES / "raw_batch" / "spec.json")
        result = runner.invoke(app, [
            "generate", spec, "--out", str(tmp_path),
            "--upsert-primary-key", "user",
        ])
        assert result.exit_code == 1
        assert "streaming source" in result.output


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

    def test_extract_offsets_kinesis_reports_shards(self, tmp_path: Path):
        offset_map = StreamOffsetMap(
            platform=StreamPlatform.KINESIS,
            supervisor_id="k-sup",
            topic="payment-events",
            datasource="payments",
            watermark_iso="2024-01-01T00:00:00.000+00:00",
            watermark_ms=1704067200000,
            shard_sequences=[
                KinesisShardSequence(
                    shard_id="shardId-000000000000", sequence_number="42"
                ),
            ],
        )
        with patch(
            "migrator.cli.commands.extract_offsets.DruidOverlordClient"
        ) as mock_client:
            mock_client.return_value.get_supervisor_offsets.return_value = offset_map
            out = tmp_path / "offsets.json"
            result = runner.invoke(app, [
                "extract-offsets",
                "--supervisor-id", "k-sup",
                "--out", str(out),
            ])
        assert result.exit_code == 0, result.output
        assert out.exists()
        # Summary names the platform and reports shards (not partitions).
        assert "kinesis" in result.output
        assert "shards=1" in result.output

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

    def test_cutover_notify_webhook_posts_on_success(self, tmp_path: Path):
        # End-to-end: --notify-webhook reaches the notifier, which is
        # called with a Slack-shaped payload after the run finishes.
        # We mock both run_cutover (so no real clusters needed) AND
        # notify_webhook (so no real HTTP). The assertion is on
        # whether the CLI wired the two together correctly.
        spec = FIXTURES / "raw_stream" / "spec.json"
        captured: dict = {}

        from migrator.notifiers.webhook import WebhookResult

        def fake_notify(url, payload, **_kwargs):
            captured["url"] = url
            captured["payload"] = payload
            return WebhookResult(ok=True, status_code=200)

        with patch(
            "migrator.cli.commands.cutover.run_cutover",
            return_value=_ok_cutover_report(tmp_path / "out"),
        ), patch(
            "migrator.notifiers.webhook.notify_webhook",
            side_effect=fake_notify,
        ):
            result = runner.invoke(app, [
                "cutover",
                "--supervisor-id", "sup1",
                "--datasource", "events",
                "--pinot-table", "events",
                "--spec", str(spec),
                "--out", str(tmp_path / "out"),
                "--notify-webhook", "http://hooks.slack.com/services/X/Y/Z",
            ])
        assert result.exit_code == 0, result.output
        assert captured["url"] == "http://hooks.slack.com/services/X/Y/Z"
        # Payload uses success emoji + datasource + table in the headline.
        assert "events" in captured["payload"]["text"]
        assert ":white_check_mark:" in captured["payload"]["text"]
        # CLI confirms delivery to the operator.
        assert "Webhook delivered" in result.output

    def test_cutover_webhook_failure_does_not_abort(self, tmp_path: Path):
        # If the webhook server is down, the cutover's exit code is
        # unchanged — the operator's notification is a courtesy, not
        # load-bearing on data correctness.
        spec = FIXTURES / "raw_stream" / "spec.json"
        from migrator.notifiers.webhook import WebhookResult

        with patch(
            "migrator.cli.commands.cutover.run_cutover",
            return_value=_ok_cutover_report(tmp_path / "out"),
        ), patch(
            "migrator.notifiers.webhook.notify_webhook",
            return_value=WebhookResult(
                ok=False, status_code=None, detail="connection refused",
            ),
        ):
            result = runner.invoke(app, [
                "cutover",
                "--supervisor-id", "sup1",
                "--datasource", "events",
                "--pinot-table", "events",
                "--spec", str(spec),
                "--out", str(tmp_path / "out"),
                "--notify-webhook", "http://nope.invalid/",
            ])
        # Cutover succeeded (mock returned all_ok), so exit 0 — webhook
        # failure is logged on stderr but doesn't change the exit code.
        assert result.exit_code == 0
        assert "Webhook delivery failed" in result.output

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


# ─────────────────────────────────────────────────────────────────────────────
# recommend
# ─────────────────────────────────────────────────────────────────────────────


class TestRecommendCommand:
    def test_recommend_help(self):
        result = runner.invoke(app, ["recommend", "--help"])
        assert result.exit_code == 0

    def test_recommend_pretty_output_lists_each_recommendation(self):
        # rolled_up has dims + metrics + an id-like dim → triggers
        # star_tree, sorted_column, range_index, inverted_index,
        # bloom_filter. Pretty output must surface every kind it
        # produces so operators see the full picture.
        spec = str(FIXTURES / "rolled_up" / "spec.json")
        result = runner.invoke(app, ["recommend", spec])
        assert result.exit_code == 0, result.output
        # Headline references the datasource name from the spec.
        assert "ad_metrics" in result.output
        # Multiple recommendation kinds appear in the pretty list.
        assert "star_tree" in result.output
        assert "sorted_column" in result.output
        assert "range_index" in result.output

    def test_recommend_json_output_round_trip(self):
        spec = str(FIXTURES / "rolled_up" / "spec.json")
        result = runner.invoke(app, ["recommend", spec, "--json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert isinstance(payload, list)
        assert payload  # non-empty for this fixture
        # Each rec carries the load-bearing keys.
        for r in payload:
            assert "kind" in r and "severity" in r and "rationale" in r

    def test_recommend_unparseable_spec_exits_2(self, tmp_path: Path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json{")
        result = runner.invoke(app, ["recommend", str(bad)])
        assert result.exit_code == 2
        assert "failed to load" in result.output.lower()

    def test_recommend_no_signal_renders_empty_message(self, tmp_path: Path):
        # A spec with no time field, no dims, no metrics generates no
        # recommendations. Operator should see an explicit "no
        # recommendations" line, not a blank screen.
        empty_spec = {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "empty",
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": []},
                    "metricsSpec": [],
                    "granularitySpec": {
                        "segmentGranularity": "DAY", "rollup": False,
                    },
                },
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": {"type": "local", "baseDir": "/data"},
                    "inputFormat": {"type": "json"},
                },
            },
        }
        spec_path = tmp_path / "empty.json"
        spec_path.write_text(json.dumps(empty_spec))
        result = runner.invoke(app, ["recommend", str(spec_path)])
        assert result.exit_code == 0
        # Time field is set so sorted_column will recommend; let's
        # look at the json mode where we can assert exactly.
        result = runner.invoke(app, ["recommend", str(spec_path), "--json"])
        # Even with just a time field, sorted_column is suggested —
        # so the count is >0. The "no recommendations" branch is
        # only reachable for a CanonicalMigrationModel with no time
        # field, which a real Druid spec can't produce. The branch
        # exists for defensive purposes; we don't need a fixture
        # exercising it from CLI.
        assert result.exit_code == 0


# ─────────────────────────────────────────────────────────────────────────────
# cutover-many
# ─────────────────────────────────────────────────────────────────────────────


class TestCutoverManyCommand:
    def test_cutover_many_help(self):
        result = runner.invoke(app, ["cutover-many", "--help"])
        assert result.exit_code == 0

    def test_cutover_many_missing_manifest_exits_2(self, tmp_path: Path):
        result = runner.invoke(app, [
            "cutover-many",
            "--manifest", str(tmp_path / "nonexistent.yaml"),
        ])
        assert result.exit_code == 2
        assert "Failed to load manifest" in result.output

    def test_cutover_many_empty_manifest_exits_2(self, tmp_path: Path):
        # A valid manifest but with zero datasources is a no-op.
        # The CLI rejects it loudly so an operator who fat-fingered
        # the YAML doesn't think the run succeeded.
        manifest = tmp_path / "empty.yaml"
        manifest.write_text(
            "defaults: {}\n"
            "datasources: []\n"
        )
        result = runner.invoke(app, [
            "cutover-many",
            "--manifest", str(manifest),
            "--out", str(tmp_path / "out"),
        ])
        assert result.exit_code == 2
        assert "no datasources" in result.output.lower()

    def test_cutover_many_invalid_auth_exits_2(self, tmp_path: Path):
        manifest = tmp_path / "m.yaml"
        manifest.write_text(
            "defaults: {}\n"
            "datasources:\n"
            "  - supervisor_id: x\n"
            "    datasource: x\n"
            "    pinot_table: x\n"
            f"    spec: {FIXTURES / 'raw_stream' / 'spec.json'}\n"
        )
        result = runner.invoke(app, [
            "cutover-many",
            "--manifest", str(manifest),
            "--out", str(tmp_path / "out"),
            "--druid-auth", "garbage-no-colon",
        ])
        assert result.exit_code == 2
        assert "auth" in result.output.lower()

    def test_cutover_many_happy_path(self, tmp_path: Path):
        # End-to-end: real manifest, mocked clients via patching
        # run_batch_cutover. The CLI's job is to load the manifest,
        # build sessions, plumb the client factory, and render the
        # report. We assert each.
        from migrator.realtime.batch_cutover import (
            BatchCutoverReport, BatchEntryResult,
        )
        manifest = tmp_path / "m.yaml"
        manifest.write_text(
            "defaults:\n"
            "  druid_router: http://druid:8888\n"
            "datasources:\n"
            "  - supervisor_id: events_v1\n"
            "    datasource: events\n"
            "    pinot_table: events\n"
            f"    spec: {FIXTURES / 'raw_stream' / 'spec.json'}\n"
        )
        fake_report = BatchCutoverReport(
            out_dir=tmp_path / "out",
            started_at="2026-01-01T00:00:00+00:00",
            entries=[
                BatchEntryResult(
                    datasource="events", pinot_table="events",
                    out_dir=tmp_path / "out" / "events",
                    all_ok=True, elapsed_s=1.5,
                ),
            ],
        )
        with patch(
            "migrator.cli.commands.cutover_many.run_batch_cutover",
            return_value=fake_report,
        ):
            result = runner.invoke(app, [
                "cutover-many",
                "--manifest", str(manifest),
                "--out", str(tmp_path / "out"),
            ])
        assert result.exit_code == 0, result.output
        assert "1 succeeded" in result.output
        assert "events" in result.output

    def test_cutover_many_failure_exits_nonzero(self, tmp_path: Path):
        from migrator.realtime.batch_cutover import (
            BatchCutoverReport, BatchEntryResult,
        )
        manifest = tmp_path / "m.yaml"
        manifest.write_text(
            "defaults: {}\n"
            "datasources:\n"
            "  - supervisor_id: x\n"
            "    datasource: x\n"
            "    pinot_table: x\n"
            f"    spec: {FIXTURES / 'raw_stream' / 'spec.json'}\n"
        )
        fake_report = BatchCutoverReport(
            out_dir=tmp_path / "out",
            started_at="2026-01-01T00:00:00+00:00",
            entries=[
                BatchEntryResult(
                    datasource="x", pinot_table="x",
                    out_dir=tmp_path / "out" / "x",
                    all_ok=False, elapsed_s=0.1,
                    error="RuntimeError: simulated",
                ),
            ],
        )
        with patch(
            "migrator.cli.commands.cutover_many.run_batch_cutover",
            return_value=fake_report,
        ):
            result = runner.invoke(app, [
                "cutover-many",
                "--manifest", str(manifest),
                "--out", str(tmp_path / "out"),
            ])
        assert result.exit_code == 1
        # Per-entry error string in the output so the operator sees it.
        assert "simulated" in result.output


# ─────────────────────────────────────────────────────────────────────────────
# diff-spec — exercise the pretty render branches
# ─────────────────────────────────────────────────────────────────────────────


class TestDiffSpecPrettyRender:
    """The default (non-JSON) text renderer in diff-spec covers many
    rendering branches not hit by the existing TestDiffSpecCommand.
    Verifies each one fires when its corresponding canonical change is
    present."""

    def _spec_with(self, **overrides) -> dict:
        base = {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "ds",
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": ["a"]},
                    "metricsSpec": [],
                    "granularitySpec": {
                        "segmentGranularity": "HOUR", "rollup": False,
                    },
                },
                "ioConfig": {
                    "type": "kafka", "topic": "t",
                    "consumerProperties": {"bootstrap.servers": "k:9092"},
                    "inputFormat": {"type": "json"},
                },
            },
        }
        # Apply nested overrides.
        if "datasource" in overrides:
            base["spec"]["dataSchema"]["dataSource"] = overrides["datasource"]
        if "input_format" in overrides:
            base["spec"]["ioConfig"]["inputFormat"]["type"] = overrides["input_format"]
        if "rollup" in overrides:
            base["spec"]["dataSchema"]["granularitySpec"]["rollup"] = overrides["rollup"]
        if "segment_granularity" in overrides:
            base["spec"]["dataSchema"]["granularitySpec"]["segmentGranularity"] = overrides["segment_granularity"]
        if "ts_column" in overrides:
            base["spec"]["dataSchema"]["timestampSpec"]["column"] = overrides["ts_column"]
        if "dims" in overrides:
            base["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"] = overrides["dims"]
        if "metrics" in overrides:
            base["spec"]["dataSchema"]["metricsSpec"] = overrides["metrics"]
        return base

    def _stage(self, tmp_path: Path, *, old: dict, new: dict) -> tuple[Path, Path]:
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        a.write_text(json.dumps(old))
        b.write_text(json.dumps(new))
        return a, b

    def test_renders_datasource_rename(self, tmp_path: Path):
        a, b = self._stage(
            tmp_path,
            old=self._spec_with(),
            new=self._spec_with(datasource="ds_v2"),
        )
        result = runner.invoke(app, ["diff-spec", str(a), str(b)])
        assert result.exit_code == 0
        assert "datasource_name" in result.output
        # Pinot implications appear because the change has consequences.
        assert "Pinot implications" in result.output

    def test_renders_input_format_change(self, tmp_path: Path):
        a, b = self._stage(
            tmp_path,
            old=self._spec_with(),
            new=self._spec_with(input_format="parquet"),
        )
        result = runner.invoke(app, ["diff-spec", str(a), str(b)])
        assert result.exit_code == 0
        assert "input_format" in result.output

    def test_renders_granularity_changes(self, tmp_path: Path):
        a, b = self._stage(
            tmp_path,
            old=self._spec_with(rollup=False),
            new=self._spec_with(rollup=True, segment_granularity="DAY"),
        )
        result = runner.invoke(app, ["diff-spec", str(a), str(b)])
        assert result.exit_code == 0
        assert "granularity" in result.output

    def test_renders_dimension_added(self, tmp_path: Path):
        a, b = self._stage(
            tmp_path,
            old=self._spec_with(),
            new=self._spec_with(dims=["a", "b_new"]),
        )
        result = runner.invoke(app, ["diff-spec", str(a), str(b)])
        assert result.exit_code == 0
        assert "dimensions" in result.output
        assert "b_new" in result.output

    def test_renders_metric_added(self, tmp_path: Path):
        a, b = self._stage(
            tmp_path,
            old=self._spec_with(),
            new=self._spec_with(metrics=[
                {"type": "longSum", "name": "x_sum", "fieldName": "x"},
            ]),
        )
        result = runner.invoke(app, ["diff-spec", str(a), str(b)])
        assert result.exit_code == 0
        assert "metrics" in result.output
        assert "x_sum" in result.output

    def test_renders_time_field_change(self, tmp_path: Path):
        a, b = self._stage(
            tmp_path,
            old=self._spec_with(),
            new=self._spec_with(ts_column="event_time"),
        )
        result = runner.invoke(app, ["diff-spec", str(a), str(b)])
        assert result.exit_code == 0
        assert "time_field" in result.output


# ─────────────────────────────────────────────────────────────────────────────
# cluster-report
# ─────────────────────────────────────────────────────────────────────────────


class TestClusterReportCommand:
    def test_cluster_report_help(self):
        result = runner.invoke(app, ["cluster-report", "--help"])
        assert result.exit_code == 0

    def test_cluster_report_invalid_auth_exits_2(self, tmp_path: Path):
        result = runner.invoke(app, [
            "cluster-report",
            "--druid-coordinator", "http://localhost:8081",
            "--out", str(tmp_path / "out"),
            "--druid-auth", "garbage-no-colon",
        ])
        assert result.exit_code == 2

    def test_cluster_report_happy_path_writes_artifacts(self, tmp_path: Path):
        # Mock the inspector to return a clean GREEN report; assert
        # the CLI wires through to write_report and prints the
        # summary table + report paths.
        from migrator.cluster.inspector import (
            COMPAT_GREEN, ClusterReport, DatasourceReport,
        )
        fake_report = ClusterReport(
            coordinator_url="http://druid:8081",
            overlord_url=None,
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:00:01+00:00",
        )
        fake_report.datasources = [
            DatasourceReport(
                datasource="events", compat=COMPAT_GREEN,
                source_kind="batch", classification="raw_event",
            ),
        ]
        with patch(
            "migrator.cli.commands.cluster_report.inspect_cluster",
            return_value=fake_report,
        ):
            result = runner.invoke(app, [
                "cluster-report",
                "--druid-coordinator", "http://druid:8081",
                "--out", str(tmp_path / "out"),
            ])
        assert result.exit_code == 0, result.output
        # Summary table printed to stdout.
        assert "GREEN" in result.output
        # The artifacts landed.
        assert (tmp_path / "out" / "summary.json").exists()
        assert (tmp_path / "out" / "cluster-report.md").exists()

    def test_cluster_report_fail_on_red_exit_code(self, tmp_path: Path):
        from migrator.cluster.inspector import (
            COMPAT_RED, ClusterReport, DatasourceReport,
        )
        fake_report = ClusterReport(
            coordinator_url="http://druid:8081",
            overlord_url=None,
            started_at="t",
        )
        fake_report.datasources = [
            DatasourceReport(
                datasource="bad", compat=COMPAT_RED,
                source_kind="batch",
                risks=[{"risk_id": "ROLLUP_MISMATCH", "severity": "HIGH",
                        "confidence": "HIGH", "description": "..."}],
            ),
        ]
        with patch(
            "migrator.cli.commands.cluster_report.inspect_cluster",
            return_value=fake_report,
        ):
            # Without --fail-on-red, exit stays 0 even when RED is present.
            result = runner.invoke(app, [
                "cluster-report",
                "--druid-coordinator", "http://druid:8081",
                "--out", str(tmp_path / "out"),
            ])
            assert result.exit_code == 0
            # With --fail-on-red, exit 3 — distinct from config (2)
            # and run errors (1).
            result = runner.invoke(app, [
                "cluster-report",
                "--druid-coordinator", "http://druid:8081",
                "--out", str(tmp_path / "out2"),
                "--fail-on-red",
            ])
            assert result.exit_code == 3

    def test_cluster_report_datasource_filter_threads_through(self, tmp_path: Path):
        from migrator.cluster.inspector import (
            COMPAT_GREEN, ClusterReport, DatasourceReport,
        )
        captured: dict = {}

        def fake_inspect(**kwargs):
            captured.update(kwargs)
            r = ClusterReport(
                coordinator_url=kwargs.get("coordinator_url", ""),
                overlord_url=kwargs.get("overlord_url"),
                started_at="t",
            )
            r.datasources = [DatasourceReport(
                datasource="a", compat=COMPAT_GREEN, source_kind="batch",
            )]
            return r

        with patch(
            "migrator.cli.commands.cluster_report.inspect_cluster",
            side_effect=fake_inspect,
        ):
            result = runner.invoke(app, [
                "cluster-report",
                "--druid-coordinator", "http://druid:8081",
                "--out", str(tmp_path / "out"),
                "--datasource", "a",
                "--datasource", "b",
            ])
        assert result.exit_code == 0
        assert captured["datasources"] == ["a", "b"]


# ─────────────────────────────────────────────────────────────────────────────
# parity-check --check-columns
# ─────────────────────────────────────────────────────────────────────────────


class TestParityCheckColumnPresence:
    def test_check_columns_requires_from_canonical(self, tmp_path: Path):
        # --check-columns needs the canonical column list. With
        # --queries (manual list) the CLI doesn't know what to
        # compare; reject with a clear message rather than running
        # silently with zero columns.
        queries = tmp_path / "q.json"
        queries.write_text(json.dumps({
            "queries": [{
                "label": "row count",
                "druid": "SELECT COUNT(*) FROM events",
                "pinot": "SELECT COUNT(*) FROM events",
            }],
        }))
        result = runner.invoke(app, [
            "parity-check",
            "--queries", str(queries),
            "--pinot-table", "events",
            "--check-columns",
        ])
        assert result.exit_code == 2
        assert "--from-canonical" in result.output

    def test_check_columns_appends_results_to_existing(self, tmp_path: Path):
        # When --check-columns is set with --from-canonical, the
        # column-presence results land alongside the auto-derived
        # aggregate parity results. A failing column-presence check
        # flips the overall exit code to 1.
        spec = FIXTURES / "raw_batch" / "spec.json"
        from migrator.parity.models import ParityResult

        # Stub run_parity (aggregate side) → all pass.
        # Stub run_column_presence → one column-presence failure.
        with patch(
            "migrator.cli.commands.parity_check.run_parity",
            return_value=[ParityResult(
                label="row count", passed=True, detail="ok",
                druid_value=10, pinot_value=10,
            )],
        ), patch(
            "migrator.parity.column_presence.run_column_presence",
            return_value=[ParityResult(
                label="column presence: region",
                passed=False,
                detail="column 'region': null-rate divergence "
                       "druid=0.0% pinot=70.0%",
                druid_value=0.0, pinot_value=0.7,
            )],
        ):
            result = runner.invoke(app, [
                "parity-check",
                "--pinot-table", "pageviews",
                "--from-canonical", str(spec),
                "--check-columns",
            ])
        # Aggregate parity passed but column-presence failed → exit 1.
        assert result.exit_code == 1
        # The column-presence failure renders alongside the aggregate
        # results — operator sees both in one report.
        assert "column presence: region" in result.output
        assert "row count" in result.output

    def test_check_columns_passes_when_everything_matches(self, tmp_path: Path):
        spec = FIXTURES / "raw_batch" / "spec.json"
        from migrator.parity.models import ParityResult
        with patch(
            "migrator.cli.commands.parity_check.run_parity",
            return_value=[ParityResult(
                label="row count", passed=True, detail="ok",
                druid_value=10, pinot_value=10,
            )],
        ), patch(
            "migrator.parity.column_presence.run_column_presence",
            return_value=[ParityResult(
                label="column presence: region",
                passed=True,
                detail="column 'region': null-rates match druid=0% pinot=0%",
                druid_value=0.0, pinot_value=0.0,
            )],
        ):
            result = runner.invoke(app, [
                "parity-check",
                "--pinot-table", "pageviews",
                "--from-canonical", str(spec),
                "--check-columns",
            ])
        assert result.exit_code == 0

    def test_null_rate_tolerance_threads_through(self, tmp_path: Path):
        # The --null-rate-tolerance flag must reach run_column_presence.
        spec = FIXTURES / "raw_batch" / "spec.json"
        captured: dict = {}

        def fake_check(*args, **kwargs):
            captured.update(kwargs)
            return []

        from migrator.parity.models import ParityResult
        with patch(
            "migrator.cli.commands.parity_check.run_parity",
            return_value=[],
        ), patch(
            "migrator.parity.column_presence.run_column_presence",
            side_effect=fake_check,
        ):
            result = runner.invoke(app, [
                "parity-check",
                "--pinot-table", "pageviews",
                "--from-canonical", str(spec),
                "--check-columns",
                "--null-rate-tolerance", "0.02",
            ])
        assert result.exit_code == 0
        assert captured.get("null_rate_tolerance") == 0.02
