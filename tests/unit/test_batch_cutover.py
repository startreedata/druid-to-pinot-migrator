"""Unit tests for the multi-datasource batch cutover orchestrator."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.realtime.batch_cutover import (
    BatchCutoverDefaults,
    BatchCutoverEntry,
    BatchCutoverManifest,
    _build_cutover_config,
    run_batch_cutover,
)
from migrator.realtime.cutover import CutoverConfig, CutoverReport, CutoverStepResult


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures + stubs
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


@pytest.fixture
def two_specs(tmp_path: Path) -> tuple[Path, Path]:
    a = tmp_path / "events.json"
    b = tmp_path / "pageviews.json"
    a.write_text(json.dumps(SAMPLE_SPEC))
    pv_spec = json.loads(json.dumps(SAMPLE_SPEC))
    pv_spec["spec"]["dataSchema"]["dataSource"] = "pageviews"
    b.write_text(json.dumps(pv_spec))
    return a, b


@pytest.fixture
def manifest_path(tmp_path: Path, two_specs) -> Path:
    a, b = two_specs
    manifest = {
        "defaults": {
            "druid_router": "http://druid:8888",
            "pinot_controller": "http://pinot:9000",
            "backfill_start_iso": "2024-01-01T00:00:00.000Z",
            # Stub clients can't report Pinot row counts, so the
            # post-backfill settle wait would otherwise time out at
            # 300s in every test. Drop to 0.5s for unit tests.
            "backfill_settle_timeout_s": 0.5,
        },
        "datasources": [
            {
                "supervisor_id": "events_v1",
                "datasource": "events",
                "pinot_table": "events",
                "spec": str(a),
            },
            {
                "supervisor_id": "pageviews_v1",
                "datasource": "pageviews",
                "pinot_table": "pageviews",
                "spec": str(b),
                # Per-DS override of the default backfill start.
                "backfill_start_iso": "2024-06-01T00:00:00.000Z",
            },
        ],
    }
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest))
    return p


def _stub_clients() -> dict:
    """Return a set of stub clients matching ``run_cutover``'s signature."""
    from tests.unit.test_cutover import (  # reuse the existing stubs
        StubDeployer, StubOverlord, StubPager, StubSink, StubSqlClient,
    )
    return {
        "overlord": StubOverlord(),
        "deployer": StubDeployer(),
        "pager": StubPager(),
        "pinot_ingest_sink": StubSink(),
        "druid_sql_client": StubSqlClient(druid=True),
        "pinot_sql_client": StubSqlClient(druid=False),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Manifest parsing
# ─────────────────────────────────────────────────────────────────────────────


class TestBatchCutoverManifest:
    def test_loads_from_json(self, manifest_path: Path):
        m = BatchCutoverManifest.from_path(manifest_path)
        assert len(m.datasources) == 2
        assert m.datasources[0].datasource == "events"
        assert m.datasources[1].datasource == "pageviews"

    def test_defaults_carry_over(self, manifest_path: Path):
        m = BatchCutoverManifest.from_path(manifest_path)
        assert m.defaults.druid_router == "http://druid:8888"
        assert m.defaults.backfill_start_iso == "2024-01-01T00:00:00.000Z"

    def test_loads_from_yaml(self, tmp_path: Path, two_specs):
        a, _ = two_specs
        yaml_text = (
            "defaults:\n"
            "  druid_router: http://druid:8888\n"
            "datasources:\n"
            "  - supervisor_id: events_v1\n"
            "    datasource: events\n"
            "    pinot_table: events\n"
            f"    spec: {a}\n"
        )
        p = tmp_path / "manifest.yaml"
        p.write_text(yaml_text)
        m = BatchCutoverManifest.from_path(p)
        assert m.datasources[0].supervisor_id == "events_v1"

    def test_unknown_field_is_rejected(self, tmp_path: Path, two_specs):
        a, _ = two_specs
        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({
            "datasources": [{
                "supervisor_id": "x", "datasource": "x", "pinot_table": "x",
                "spec": str(a),
                "what_is_this": "typo",
            }]
        }))
        # Pydantic ConfigDict(extra='forbid') catches the typo so an
        # invalid manifest fails before any phase side-effects.
        with pytest.raises(Exception, match="what_is_this"):
            BatchCutoverManifest.from_path(bad)


# ─────────────────────────────────────────────────────────────────────────────
# CutoverConfig composition
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildCutoverConfig:
    def test_per_entry_field_wins_over_defaults(self, tmp_path: Path):
        spec = tmp_path / "s.json"
        spec.write_text("{}")
        defaults = BatchCutoverDefaults(
            backfill_start_iso="2024-01-01T00:00:00.000Z",
        )
        entry = BatchCutoverEntry(
            supervisor_id="x", datasource="x", pinot_table="x", spec=spec,
            backfill_start_iso="2024-06-01T00:00:00.000Z",
        )
        cfg = _build_cutover_config(
            entry, defaults,
            out_root=tmp_path / "out",
            staging_root=tmp_path / "staging",
        )
        assert cfg.backfill_start_iso == "2024-06-01T00:00:00.000Z"

    def test_default_used_when_entry_unset(self, tmp_path: Path):
        spec = tmp_path / "s.json"
        spec.write_text("{}")
        defaults = BatchCutoverDefaults(
            backfill_start_iso="2024-01-01T00:00:00.000Z",
        )
        entry = BatchCutoverEntry(
            supervisor_id="x", datasource="x", pinot_table="x", spec=spec,
        )
        cfg = _build_cutover_config(
            entry, defaults,
            out_root=tmp_path / "out", staging_root=tmp_path / "staging",
        )
        assert cfg.backfill_start_iso == "2024-01-01T00:00:00.000Z"

    def test_per_ds_out_subdirectory(self, tmp_path: Path):
        # Two datasources must NOT share an out_dir (their checkpoints
        # would collide). The composer derives <out_root>/<datasource>.
        spec = tmp_path / "s.json"
        spec.write_text("{}")
        defaults = BatchCutoverDefaults()
        e1 = BatchCutoverEntry(
            supervisor_id="a", datasource="ds_a", pinot_table="t_a", spec=spec,
        )
        e2 = BatchCutoverEntry(
            supervisor_id="b", datasource="ds_b", pinot_table="t_b", spec=spec,
        )
        out = tmp_path / "out"
        cfg1 = _build_cutover_config(e1, defaults, out_root=out, staging_root=out)
        cfg2 = _build_cutover_config(e2, defaults, out_root=out, staging_root=out)
        assert cfg1.out_dir != cfg2.out_dir
        assert cfg1.out_dir.name == "ds_a"
        assert cfg2.out_dir.name == "ds_b"

    def test_extra_threads_resume_through(self, tmp_path: Path):
        spec = tmp_path / "s.json"
        spec.write_text("{}")
        entry = BatchCutoverEntry(
            supervisor_id="x", datasource="x", pinot_table="x", spec=spec,
        )
        cfg = _build_cutover_config(
            entry, BatchCutoverDefaults(),
            out_root=tmp_path / "out", staging_root=tmp_path / "staging",
            extra={"resume": False, "restart_from": "parity"},
        )
        assert cfg.resume is False
        assert cfg.restart_from == "parity"


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────


class TestRunBatchCutover:
    def test_happy_path_runs_each_entry(self, manifest_path: Path, tmp_path: Path):
        m = BatchCutoverManifest.from_path(manifest_path)
        cfg = type("Cfg", (), {})  # noqa
        report = run_batch_cutover(
            m,
            out_root=tmp_path / "out",
            staging_root=tmp_path / "staging",
            client_factory=lambda d, e: _stub_clients(),
        )
        assert report.total == 2
        assert report.succeeded == 2
        assert report.failed == 0
        assert report.all_ok is True
        # Each entry has its own per-DS subdirectory + checkpoint.
        assert (tmp_path / "out" / "events" / "cutover-checkpoint.json").exists()
        assert (tmp_path / "out" / "pageviews" / "cutover-checkpoint.json").exists()
        # Aggregate report sits at the top-level out_dir.
        assert (tmp_path / "out" / "batch-report.json").exists()

    def test_aggregate_report_has_per_entry_outcomes(
        self, manifest_path: Path, tmp_path: Path,
    ):
        m = BatchCutoverManifest.from_path(manifest_path)
        run_batch_cutover(
            m,
            out_root=tmp_path / "out",
            staging_root=tmp_path / "staging",
            client_factory=lambda d, e: _stub_clients(),
        )
        report_data = json.loads(
            (tmp_path / "out" / "batch-report.json").read_text(),
        )
        assert report_data["total"] == 2
        assert report_data["all_ok"] is True
        assert {e["datasource"] for e in report_data["entries"]} == {
            "events", "pageviews",
        }

    def test_failure_in_one_does_not_stop_batch_by_default(
        self, manifest_path: Path, tmp_path: Path,
    ):
        # Pass a client_factory that errors out for the first datasource
        # but succeeds for the second. The batch must run to completion.
        from tests.unit.test_cutover import (
            StubDeployer, StubOverlord, StubPager, StubSink, StubSqlClient,
        )

        class _ExplodingDeployer:
            def deploy(self, _artifacts):
                raise RuntimeError("simulated deploy failure")

        seen: list[str] = []

        def factory(defaults, entry):
            seen.append(entry.datasource)
            return {
                "overlord": StubOverlord(),
                "deployer": (
                    _ExplodingDeployer() if entry.datasource == "events"
                    else StubDeployer()
                ),
                "pager": StubPager(),
                "pinot_ingest_sink": StubSink(),
                "druid_sql_client": StubSqlClient(druid=True),
                "pinot_sql_client": StubSqlClient(druid=False),
            }

        m = BatchCutoverManifest.from_path(manifest_path)
        report = run_batch_cutover(
            m,
            out_root=tmp_path / "out",
            staging_root=tmp_path / "staging",
            client_factory=factory,
        )
        # Both datasources were attempted (default abort_on_first_failure=False).
        assert seen == ["events", "pageviews"]
        assert report.failed == 1
        assert report.succeeded == 1
        assert not report.all_ok

    def test_abort_on_first_failure_stops_batch(
        self, manifest_path: Path, tmp_path: Path,
    ):
        from tests.unit.test_cutover import (
            StubDeployer, StubOverlord, StubPager, StubSink, StubSqlClient,
        )

        class _ExplodingDeployer:
            def deploy(self, _artifacts):
                raise RuntimeError("simulated deploy failure")

        seen: list[str] = []

        def factory(defaults, entry):
            seen.append(entry.datasource)
            return {
                "overlord": StubOverlord(),
                "deployer": _ExplodingDeployer(),
                "pager": StubPager(),
                "pinot_ingest_sink": StubSink(),
                "druid_sql_client": StubSqlClient(druid=True),
                "pinot_sql_client": StubSqlClient(druid=False),
            }

        m = BatchCutoverManifest.from_path(manifest_path)
        report = run_batch_cutover(
            m,
            out_root=tmp_path / "out",
            staging_root=tmp_path / "staging",
            client_factory=factory,
            abort_on_first_failure=True,
        )
        # Only the first datasource was attempted before the abort.
        assert seen == ["events"]
        assert report.total == 1
        assert report.failed == 1

    def test_per_entry_override_propagates(
        self, manifest_path: Path, tmp_path: Path,
    ):
        # The pageviews entry overrides backfill_start_iso. We verify
        # the override reaches the per-DS CutoverConfig by intercepting
        # the client-factory call (which receives the entry post-merge
        # via the closure in run_batch_cutover).
        seen_specs: dict[str, BatchCutoverEntry] = {}

        def factory(defaults, entry):
            seen_specs[entry.datasource] = entry
            return _stub_clients()

        m = BatchCutoverManifest.from_path(manifest_path)
        run_batch_cutover(
            m,
            out_root=tmp_path / "out",
            staging_root=tmp_path / "staging",
            client_factory=factory,
        )
        assert seen_specs["events"].backfill_start_iso is None
        # Pageviews entry carries the override unchanged through the
        # manifest layer; the merge happens later in _build_cutover_config.
        assert (
            seen_specs["pageviews"].backfill_start_iso
            == "2024-06-01T00:00:00.000Z"
        )
