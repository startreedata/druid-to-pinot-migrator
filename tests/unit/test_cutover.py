"""Unit tests for the cutover orchestrator with stub clients."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.realtime.cutover import CutoverConfig, run_cutover
from migrator.realtime.models import KafkaOffsetMap, KafkaPartitionOffset


# ─────────────────────────────────────────────────────────────────────────────
# Stubs — minimal duck-typed substitutes for the real clients
# ─────────────────────────────────────────────────────────────────────────────


class StubOverlord:
    def __init__(self, watermark_iso: str = "2026-05-03T00:00:00.000Z") -> None:
        self._watermark = watermark_iso
        self.calls: list[str] = []

    def get_supervisor_offsets(self, supervisor_id: str) -> KafkaOffsetMap:
        self.calls.append(supervisor_id)
        return KafkaOffsetMap(
            platform="kafka",
            topic="t",
            supervisor_id=supervisor_id,
            datasource=supervisor_id,
            captured_at_iso=self._watermark,
            watermark_iso=self._watermark,
            watermark_ms=1746230400000,
            offsets=[KafkaPartitionOffset(partition=0, offset=42)],
        )


class StubDeployer:
    def __init__(self, all_ok: bool = True) -> None:
        self._all_ok = all_ok
        self.calls: list = []

    def deploy(self, artifacts):
        self.calls.append(artifacts)

        class _R:
            artifact = "schema"
            name = "ds"
            status = "created" if self._all_ok else "error"
            detail = "" if self._all_ok else "boom"

        class _Report:
            results = [_R()]
            all_ok = self._all_ok
            created = 1 if self._all_ok else 0
            already_exists = 0
            errored = 0 if self._all_ok else 1

        # Bind closure values at instance level so attribute access
        # returns concrete bools/ints rather than methods.
        _R.status = "created" if self._all_ok else "error"
        _R.detail = "" if self._all_ok else "boom"
        _Report.all_ok = self._all_ok
        _Report.created = 1 if self._all_ok else 0
        _Report.errored = 0 if self._all_ok else 1
        _Report.already_exists = 0
        return _Report()


class StubPager:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self._rows = rows if rows is not None else [{"a": 1}, {"a": 2}]
        self.calls: list = []

    def page_rows(self, datasource, *, start_iso, end_iso, page_rows):
        self.calls.append((datasource, start_iso, end_iso))
        if self._rows:
            yield self._rows


class StubSink:
    def __init__(self) -> None:
        self.received: list = []

    def ingest_file(self, path, table_name) -> None:
        self.received.append((Path(path), table_name))


class StubSqlClient:
    """Used as both DruidSqlClient and PinotSqlClient.

    Returns whatever ``responses`` says for each query — defaults to a
    single-row [{"v": 1}] / [[1]] respectively, which makes the
    auto-derived ``COUNT(*)`` query trivially pass.
    """

    def __init__(self, druid: bool, responses: dict | None = None) -> None:
        self.druid = druid
        self.responses = responses or {}
        self.calls: list[str] = []

    def query(self, sql: str):
        self.calls.append(sql)
        if sql in self.responses:
            return self.responses[sql]
        # Default: one row, value 1. Druid returns dict, Pinot returns list.
        return [{"v": 1}] if self.druid else [[1]]


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


SAMPLE_SPEC = {
    "type": "kafka",
    "spec": {
        "dataSchema": {
            "dataSource": "ds",
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


@pytest.fixture
def cfg(tmp_path: Path) -> CutoverConfig:
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(SAMPLE_SPEC))
    return CutoverConfig(
        supervisor_id="ds",
        datasource="ds",
        pinot_table="ds",
        spec_path=spec_path,
        out_dir=tmp_path / "out",
        staging_dir=tmp_path / "staging",
        # Stub clients can't actually report Pinot row counts, so the
        # post-backfill settle wait would otherwise time out at 300s
        # in every unit test. 0.5s is enough to exercise the code
        # path without hanging.
        backfill_settle_timeout_s=0.5,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Happy path: every phase runs and every phase succeeds
# ─────────────────────────────────────────────────────────────────────────────


class TestCutoverHappyPath:
    def test_all_phases_run_and_succeed(self, cfg: CutoverConfig):
        report = run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        names = [s.step for s in report.steps]
        assert names == [
            "extract_offsets", "plan_hybrid", "deploy", "backfill", "parity"
        ]
        assert all(s.status == "ok" for s in report.steps), \
            [(s.step, s.status, s.detail) for s in report.steps]
        assert report.all_ok

    def test_writes_top_level_report_json(self, cfg: CutoverConfig):
        run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        report_path = cfg.out_dir / "cutover-report.json"
        assert report_path.exists()
        data = json.loads(report_path.read_text())
        assert data["all_ok"] is True
        assert len(data["steps"]) == 5

    def test_writes_per_phase_artifacts(self, cfg: CutoverConfig):
        run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        # extract_offsets
        assert (cfg.out_dir / "offsets.json").exists()
        # plan_hybrid
        assert (cfg.out_dir / "hybrid").is_dir()
        assert (cfg.out_dir / "hybrid" / "schema.json").exists()
        # deploy
        assert (cfg.out_dir / "deploy-report.json").exists()
        # parity
        assert (cfg.out_dir / "parity-report.json").exists()


# ─────────────────────────────────────────────────────────────────────────────
# Skipping
# ─────────────────────────────────────────────────────────────────────────────


class TestCutoverSkipFlags:
    def test_skip_deploy_does_not_call_deployer(self, cfg: CutoverConfig):
        cfg.skip_deploy = True
        deployer = StubDeployer()
        report = run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=deployer,
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        assert deployer.calls == []
        deploy_step = next(s for s in report.steps if s.step == "deploy")
        assert deploy_step.status == "skipped"

    def test_skip_backfill_does_not_call_pager(self, cfg: CutoverConfig):
        cfg.skip_backfill = True
        pager = StubPager()
        report = run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=pager,
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        assert pager.calls == []
        bf_step = next(s for s in report.steps if s.step == "backfill")
        assert bf_step.status == "skipped"

    def test_skip_parity_does_not_call_sql_clients(self, cfg: CutoverConfig):
        cfg.skip_parity = True
        d, p = StubSqlClient(druid=True), StubSqlClient(druid=False)
        report = run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=d,
            pinot_sql_client=p,
        )
        assert d.calls == [] and p.calls == []
        parity_step = next(s for s in report.steps if s.step == "parity")
        assert parity_step.status == "skipped"


# ─────────────────────────────────────────────────────────────────────────────
# Abort vs continue-on-error
# ─────────────────────────────────────────────────────────────────────────────


class TestCutoverAbortBehaviour:
    def test_abort_on_error_short_circuits(self, cfg: CutoverConfig):
        # Deploy fails → backfill + parity should be skipped.
        report = run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=StubDeployer(all_ok=False),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        statuses = {s.step: s.status for s in report.steps}
        assert statuses["deploy"] == "error"
        assert statuses["backfill"] == "skipped"
        assert statuses["parity"] == "skipped"
        assert not report.all_ok

    def test_continue_on_error_runs_remaining(self, cfg: CutoverConfig):
        cfg.abort_on_error = False
        report = run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=StubDeployer(all_ok=False),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        statuses = {s.step: s.status for s in report.steps}
        assert statuses["deploy"] == "error"
        # Backfill + parity attempted despite the deploy failure.
        assert statuses["backfill"] == "ok"
        assert statuses["parity"] == "ok"
        # Overall report still flagged as not-OK.
        assert not report.all_ok


# ─────────────────────────────────────────────────────────────────────────────
# Parity-failure surfaces as a step error
# ─────────────────────────────────────────────────────────────────────────────


class TestCutoverParityFailures:
    def test_diverging_query_marks_parity_step_error(self, cfg: CutoverConfig):
        # Druid says 100, Pinot says 99 → COUNT(*) parity fails.
        druid = StubSqlClient(
            druid=True,
            responses={
                'SELECT COUNT(*) AS v FROM "ds"': [{"v": 100}],
                'SELECT region, COUNT(*) FROM "ds" GROUP BY "region" ORDER BY "region"':
                    [{"region": "us", "c": 100}],
            },
        )
        pinot = StubSqlClient(
            druid=False,
            responses={
                'SELECT COUNT(*) FROM "ds"': [[99]],
                'SELECT "region", COUNT(*) FROM "ds" GROUP BY "region" ORDER BY "region"':
                    [["us", 99]],
            },
        )
        report = run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=druid,
            pinot_sql_client=pinot,
        )
        parity_step = next(s for s in report.steps if s.step == "parity")
        assert parity_step.status == "error"
        assert "parity check" in parity_step.detail.lower()
        # The full per-query results are still on the report so callers
        # can render them.
        assert any(not r.passed for r in report.parity)


# ─────────────────────────────────────────────────────────────────────────────
# Resumability
# ─────────────────────────────────────────────────────────────────────────────


class _ExplodingDeployer:
    """Always errors on deploy() — used to make the first run fail at
    a known phase so the second run has something to resume past."""

    def __init__(self) -> None:
        self.calls: int = 0

    def deploy(self, artifacts):
        self.calls += 1
        raise RuntimeError("simulated deploy failure")


class _CountingOverlord(StubOverlord):
    """Like StubOverlord but counts calls so resume tests can assert
    'phase did NOT run again' by comparing call counts."""

    def __init__(self) -> None:
        super().__init__()
        self.call_count = 0

    def get_supervisor_offsets(self, supervisor_id: str):
        self.call_count += 1
        return super().get_supervisor_offsets(supervisor_id)


class TestCutoverResumeAfterFailure:
    def test_first_run_writes_checkpoint_with_completed_phases(
        self, cfg: CutoverConfig,
    ):
        # Run with an exploding deployer — extract_offsets and
        # plan_hybrid succeed; deploy errors and aborts the rest.
        run_cutover(
            cfg,
            overlord=StubOverlord(),
            deployer=_ExplodingDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        # Checkpoint file should exist with extract_offsets+plan_hybrid
        # marked ok and deploy marked error.
        ckpt_path = cfg.out_dir / "cutover-checkpoint.json"
        assert ckpt_path.exists()
        ck = json.loads(ckpt_path.read_text())
        assert ck["phases"]["extract_offsets"]["status"] == "ok"
        assert ck["phases"]["plan_hybrid"]["status"] == "ok"
        assert ck["phases"]["deploy"]["status"] == "error"
        # backfill + parity didn't even run, so they're absent.
        assert "backfill" not in ck["phases"]
        assert "parity" not in ck["phases"]

    def test_second_run_skips_completed_phases(self, cfg: CutoverConfig):
        # First run: deploy fails.
        overlord1 = _CountingOverlord()
        run_cutover(
            cfg, overlord=overlord1,
            deployer=_ExplodingDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        assert overlord1.call_count == 1

        # Second run: same config, but with a working deployer this time.
        overlord2 = _CountingOverlord()
        report = run_cutover(
            cfg, overlord=overlord2,
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        # Overlord must NOT have been called again — the first run
        # already completed extract_offsets, and the checkpoint says so.
        assert overlord2.call_count == 0
        # extract_offsets and plan_hybrid show up as 'ok' with a
        # ``resumed from checkpoint`` note.
        ext = next(s for s in report.steps if s.step == "extract_offsets")
        assert ext.status == "ok"
        assert "resumed from checkpoint" in ext.detail
        plan = next(s for s in report.steps if s.step == "plan_hybrid")
        assert plan.status == "ok"
        assert "resumed from checkpoint" in plan.detail
        # deploy actually ran this time and succeeded.
        deploy = next(s for s in report.steps if s.step == "deploy")
        assert deploy.status == "ok"
        assert "resumed" not in deploy.detail
        # Final outcome is all-ok.
        assert report.all_ok

    def test_resume_false_reruns_all_phases(self, cfg: CutoverConfig):
        # First run completes everything successfully.
        run_cutover(
            cfg, overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        # Second run with resume=False — overlord must be called again.
        overlord2 = _CountingOverlord()
        cfg.resume = False
        run_cutover(
            cfg, overlord=overlord2,
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        assert overlord2.call_count == 1

    def test_config_change_invalidates_checkpoint(self, cfg: CutoverConfig):
        # First run completes everything.
        run_cutover(
            cfg, overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        # Change a hashed field — datasource — and run again.
        cfg.datasource = "different_ds"
        cfg.pinot_table = "different_ds"
        # Need the spec to also reference the new datasource so plan
        # passes; for this test, easier to just rewrite the spec.
        spec = json.loads(cfg.spec_path.read_text())
        spec["spec"]["dataSchema"]["dataSource"] = "different_ds"
        cfg.spec_path.write_text(json.dumps(spec))
        overlord2 = _CountingOverlord()
        run_cutover(
            cfg, overlord=overlord2,
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        # Overlord called again — the prior checkpoint's hash didn't
        # match, so it was discarded.
        assert overlord2.call_count == 1


class TestCutoverRestartFrom:
    def test_restart_from_drops_named_and_later_phases(
        self, cfg: CutoverConfig,
    ):
        # First run completes everything.
        run_cutover(
            cfg, overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        # Second run: --restart-from parity. Earlier phases keep their
        # 'ok' status; only parity (and any later phase) re-runs.
        overlord2 = _CountingOverlord()
        deploy2 = StubDeployer()
        cfg.restart_from = "parity"
        report = run_cutover(
            cfg, overlord=overlord2,
            deployer=deploy2,
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        # Overlord, deployer not called — those phases are still 'ok'
        # in the checkpoint and get resumed.
        assert overlord2.call_count == 0
        assert len(deploy2.calls) == 0
        # parity ran (status=ok, no 'resumed' note)
        parity = next(s for s in report.steps if s.step == "parity")
        assert parity.status == "ok"
        assert "resumed" not in parity.detail


class TestCutoverCheckpointSchemaMismatch:
    def test_unrecognised_checkpoint_aborts_run(
        self, cfg: CutoverConfig,
    ):
        # Stage a corrupt checkpoint file under out_dir.
        cfg.out_dir.mkdir(parents=True, exist_ok=True)
        (cfg.out_dir / "cutover-checkpoint.json").write_text(
            json.dumps({"schema_version": 999, "config_hash": "x", "phases": {}})
        )
        # Default cfg has resume=True; orchestrator must refuse rather
        # than silently start over (which would mask a real format-bump
        # bug).
        with pytest.raises(RuntimeError, match="--no-resume"):
            run_cutover(
                cfg, overlord=StubOverlord(),
                deployer=StubDeployer(),
                pager=StubPager(),
                pinot_ingest_sink=StubSink(),
                druid_sql_client=StubSqlClient(druid=True),
                pinot_sql_client=StubSqlClient(druid=False),
            )

    def test_no_resume_overrides_corrupt_checkpoint(
        self, cfg: CutoverConfig,
    ):
        cfg.out_dir.mkdir(parents=True, exist_ok=True)
        (cfg.out_dir / "cutover-checkpoint.json").write_text("{not json")
        cfg.resume = False
        # Should run cleanly and overwrite the bad file.
        report = run_cutover(
            cfg, overlord=StubOverlord(),
            deployer=StubDeployer(),
            pager=StubPager(),
            pinot_ingest_sink=StubSink(),
            druid_sql_client=StubSqlClient(druid=True),
            pinot_sql_client=StubSqlClient(druid=False),
        )
        assert report.all_ok
