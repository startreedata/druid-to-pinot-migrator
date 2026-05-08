"""Unit tests for the cluster-wide compatibility inspector."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.cluster.inspector import (
    COMPAT_ERROR,
    COMPAT_GREEN,
    COMPAT_RED,
    COMPAT_YELLOW,
    ClusterReport,
    DatasourceReport,
    inspect_cluster,
    inspect_one,
    render_markdown,
    write_report,
)
from migrator.druid.coordinator_client import (
    DruidCoordinatorClient,
    DruidCoordinatorError,
    SegmentMetadata,
)
from migrator.druid.overlord_client import DruidOverlordClient


# ─────────────────────────────────────────────────────────────────────────────
# Fakes — duck-typed Coordinator / Overlord clients keyed on datasource name
# so each test can preload a different topology.
# ─────────────────────────────────────────────────────────────────────────────


class FakeCoordinator:
    """Subclass of DruidCoordinatorClient that intercepts the fields
    the inspector actually uses. We don't construct a real one (it
    needs a session); we just expose the same method surface.
    """
    def __init__(
        self,
        datasources: list[str],
        summaries: dict | None = None,
        segment_metas: dict | None = None,
        list_raises: bool = False,
    ) -> None:
        self._datasources = datasources
        self._summaries = summaries or {}
        self._metas = segment_metas or {}
        self._list_raises = list_raises

    def list_datasources(self) -> list[str]:
        if self._list_raises:
            raise DruidCoordinatorError("simulated list failure")
        return list(self._datasources)

    def datasource_exists(self, name: str) -> bool:
        return name in self._datasources

    def get_datasource_summary(self, name: str) -> dict:
        if name not in self._summaries:
            return {"name": name, "segments": {}}
        return self._summaries[name]

    def get_segment_metadata(self, name: str) -> SegmentMetadata:
        if name in self._metas:
            return self._metas[name]
        # Default: tiny single-column meta — enough for the extractor
        # to build a synthetic spec without exploding.
        return SegmentMetadata(
            columns={
                "__time": {"type": "LONG"},
                "region": {"type": "STRING"},
            },
            intervals=["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        )


class FakeOverlord:
    """Same idea for the Overlord — only the bits the inspector hits."""

    def __init__(self, supervisor_specs: dict | None = None) -> None:
        # supervisor_specs: {datasource_name: supervisor_spec_dict}
        self._supervisor_specs = supervisor_specs or {}
        self._supervisor_id_for_ds = {
            name: f"{name}_supervisor" for name in self._supervisor_specs
        }

    def find_supervisor_for_datasource(self, name: str) -> str | None:
        return self._supervisor_id_for_ds.get(name)

    def get_supervisor_spec(self, sup_id: str) -> dict:
        for ds, sid in self._supervisor_id_for_ds.items():
            if sid == sup_id:
                return self._supervisor_specs[ds]
        raise ValueError(f"unknown supervisor {sup_id}")

    def list_supervisors(self) -> list[str]:
        return list(self._supervisor_id_for_ds.values())


def _kafka_supervisor_spec(datasource: str) -> dict:
    """Minimal Kafka supervisor that round-trips cleanly through the
    parser + normalizer + risk analyzer with NO blocking risks."""
    return {
        "type": "kafka",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {"column": "ts", "format": "millis"},
                "dimensionsSpec": {"dimensions": ["region"]},
                "metricsSpec": [
                    {"type": "count", "name": "events"},
                ],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "MINUTE",
                    "rollup": False,
                },
            },
            "ioConfig": {
                "type": "kafka",
                "topic": f"{datasource}_topic",
                "consumerProperties": {"bootstrap.servers": "k:9092"},
            },
        },
    }


def _kafka_supervisor_with_sketches(datasource: str) -> dict:
    """Kafka spec carrying a sketch metric — produces a HIGH-severity
    UnsupportedFeature in the canonical model, which flips the
    datasource to RED."""
    spec = _kafka_supervisor_spec(datasource)
    spec["spec"]["dataSchema"]["metricsSpec"].append({
        "type": "thetaSketch",
        "name": "users_sketch",
        "fieldName": "user_id",
    })
    return spec


# ─────────────────────────────────────────────────────────────────────────────
# inspect_one — single-datasource verdicts
# ─────────────────────────────────────────────────────────────────────────────


class TestInspectOneStreamPath:
    def test_clean_kafka_supervisor_is_green(self):
        ds = "events"
        coord = FakeCoordinator(datasources=[ds])
        ovr = FakeOverlord(supervisor_specs={ds: _kafka_supervisor_spec(ds)})
        report = inspect_one(ds, coordinator=coord, overlord=ovr)
        assert report.compat == COMPAT_GREEN
        assert report.source_kind == "stream"
        assert report.supervisor_id == f"{ds}_supervisor"
        # No risks, no unsupported.
        assert report.risks == []
        assert report.unsupported_features == []

    def test_sketch_metric_flips_to_red(self):
        ds = "events"
        coord = FakeCoordinator(datasources=[ds])
        ovr = FakeOverlord(
            supervisor_specs={ds: _kafka_supervisor_with_sketches(ds)},
        )
        report = inspect_one(ds, coordinator=coord, overlord=ovr)
        assert report.compat == COMPAT_RED
        # Unsupported features list carries the offender.
        feature_names = [u["feature"] for u in report.unsupported_features]
        assert any("complex_metric" in f for f in feature_names)


class TestInspectOneBatchPath:
    def test_no_supervisor_falls_back_to_segment_metadata(self):
        ds = "historical"
        coord = FakeCoordinator(datasources=[ds])
        ovr = FakeOverlord()  # no supervisor → batch path
        report = inspect_one(ds, coordinator=coord, overlord=ovr)
        # Should produce SOME verdict (green/yellow/red), not an
        # error — segment metadata is enough to synthesize a spec.
        assert report.compat != COMPAT_ERROR
        assert report.source_kind == "batch"

    def test_inspect_one_works_without_overlord(self):
        # Operator hasn't pointed at an Overlord (read-only env). The
        # inspector must still produce a verdict via the batch path.
        ds = "historical"
        coord = FakeCoordinator(datasources=[ds])
        report = inspect_one(ds, coordinator=coord, overlord=None)
        assert report.compat != COMPAT_ERROR


class TestInspectOneErrorPaths:
    def test_unknown_datasource_returns_error(self):
        coord = FakeCoordinator(datasources=["other"])
        report = inspect_one("missing", coordinator=coord, overlord=None)
        assert report.compat == COMPAT_ERROR
        assert "extract failed" in (report.error or "")

    def test_unexpected_exception_caught(self):
        # An unexpected exception inside the extractor must NOT
        # propagate — the report has to keep going for other DSes.
        class _ExplodingCoordinator(FakeCoordinator):
            def datasource_exists(self, name):
                raise RuntimeError("bizarre coordinator failure")
        coord = _ExplodingCoordinator(datasources=["events"])
        report = inspect_one("events", coordinator=coord, overlord=None)
        assert report.compat == COMPAT_ERROR
        assert "unexpected" in (report.error or "")


# ─────────────────────────────────────────────────────────────────────────────
# inspect_cluster — aggregation behaviour
# ─────────────────────────────────────────────────────────────────────────────


class TestInspectCluster:
    def test_walks_every_datasource(self):
        coord = FakeCoordinator(datasources=["a", "b", "c"])
        ovr = FakeOverlord(supervisor_specs={
            "a": _kafka_supervisor_spec("a"),
            "b": _kafka_supervisor_spec("b"),
            "c": _kafka_supervisor_spec("c"),
        })
        report = inspect_cluster(coordinator=coord, overlord=ovr)
        assert report.total == 3
        assert {d.datasource for d in report.datasources} == {"a", "b", "c"}

    def test_explicit_datasource_filter_skips_others(self):
        # ``datasources=["a"]`` → only "a" gets inspected, even when
        # the cluster has more.
        coord = FakeCoordinator(datasources=["a", "b", "c"])
        ovr = FakeOverlord(supervisor_specs={
            "a": _kafka_supervisor_spec("a"),
        })
        report = inspect_cluster(
            coordinator=coord, overlord=ovr, datasources=["a"],
        )
        assert report.total == 1
        assert report.datasources[0].datasource == "a"

    def test_failure_in_one_does_not_abort_whole_run(self):
        # ``b`` is a phantom — Coordinator says it exists but the
        # extractor will fail. The other two should still produce
        # GREEN verdicts.
        coord = FakeCoordinator(datasources=["a", "b", "c"])
        ovr = FakeOverlord(supervisor_specs={
            "a": _kafka_supervisor_spec("a"),
            "c": _kafka_supervisor_spec("c"),
        })
        # Override get_datasource_summary so b explodes during
        # extract_spec's batch path.
        original = coord.get_datasource_summary
        def boom(name):
            if name == "b":
                raise DruidCoordinatorError("simulated")
            return original(name)
        coord.get_datasource_summary = boom

        report = inspect_cluster(
            coordinator=coord, overlord=ovr, datasources=["a", "b", "c"],
        )
        assert report.total == 3
        statuses = {d.datasource: d.compat for d in report.datasources}
        assert statuses["a"] == COMPAT_GREEN
        assert statuses["b"] == COMPAT_ERROR
        assert statuses["c"] == COMPAT_GREEN

    def test_progress_callback_fires_per_datasource(self):
        coord = FakeCoordinator(datasources=["a", "b"])
        ovr = FakeOverlord(supervisor_specs={
            "a": _kafka_supervisor_spec("a"),
            "b": _kafka_supervisor_spec("b"),
        })
        events = []
        inspect_cluster(
            coordinator=coord, overlord=ovr,
            datasources=["a", "b"],
            progress_callback=lambda i, total, ds: events.append((i, total, ds)),
        )
        assert events == [(1, 2, "a"), (2, 2, "b")]

    def test_misbehaving_progress_callback_does_not_abort(self):
        coord = FakeCoordinator(datasources=["a", "b"])
        ovr = FakeOverlord(supervisor_specs={
            "a": _kafka_supervisor_spec("a"),
            "b": _kafka_supervisor_spec("b"),
        })

        def bad(*_):
            raise RuntimeError("operator's display crashed")

        report = inspect_cluster(
            coordinator=coord, overlord=ovr,
            datasources=["a", "b"],
            progress_callback=bad,
        )
        # Both DSes still inspected, both GREEN.
        assert report.total == 2
        assert all(d.compat == COMPAT_GREEN for d in report.datasources)

    def test_list_failure_yields_synthetic_error_row(self):
        # When the Coordinator can't even list DSes, the report has
        # one error row so the operator sees what happened — empty
        # report would be confusing.
        coord = FakeCoordinator(datasources=[], list_raises=True)
        report = inspect_cluster(coordinator=coord)
        assert report.total == 1
        assert report.datasources[0].compat == COMPAT_ERROR
        assert "list failed" in (report.datasources[0].error or "").lower()


# ─────────────────────────────────────────────────────────────────────────────
# Aggregation helpers
# ─────────────────────────────────────────────────────────────────────────────


class TestClusterReportAggregation:
    def _make(self, *, greens=0, yellows=0, reds=0, errors=0):
        report = ClusterReport(
            coordinator_url="http://c", overlord_url=None,
            started_at="2026-01-01T00:00:00+00:00",
        )
        for i in range(greens):
            report.datasources.append(DatasourceReport(
                datasource=f"g{i}", compat=COMPAT_GREEN,
                source_kind="batch", classification="raw_event",
            ))
        for i in range(yellows):
            report.datasources.append(DatasourceReport(
                datasource=f"y{i}", compat=COMPAT_YELLOW,
                source_kind="stream", classification="raw_event",
                risks=[{"risk_id": "MV_AMBIG", "severity": "MEDIUM",
                        "confidence": "HIGH", "description": "..."}],
            ))
        for i in range(reds):
            report.datasources.append(DatasourceReport(
                datasource=f"r{i}", compat=COMPAT_RED,
                source_kind="batch", classification="complex_aggregated",
                risks=[{"risk_id": "ROLLUP_MISMATCH", "severity": "HIGH",
                        "confidence": "HIGH", "description": "..."}],
                unsupported_features=[{
                    "feature": "complex_metric:users",
                    "reason": "thetaSketch",
                    "severity": "HIGH",
                }],
            ))
        for i in range(errors):
            report.datasources.append(DatasourceReport(
                datasource=f"e{i}", compat=COMPAT_ERROR, error="boom",
            ))
        return report

    def test_total_counts(self):
        report = self._make(greens=3, yellows=2, reds=1, errors=1)
        assert report.total == 7

    def test_by_status_breakdown(self):
        report = self._make(greens=3, yellows=2, reds=1, errors=1)
        assert report.by_status == {
            COMPAT_GREEN: 3, COMPAT_YELLOW: 2,
            COMPAT_RED: 1, COMPAT_ERROR: 1,
        }

    def test_by_classification_skips_errors(self):
        # Error rows have no meaningful classification and shouldn't
        # bleed into the breakdown.
        report = self._make(greens=2, yellows=1, errors=2)
        breakdown = report.by_classification
        assert "raw_event" in breakdown
        # Errors absent from the classification breakdown.
        assert "unknown" not in breakdown or breakdown.get("unknown", 0) == 0
        # Sum equals (greens + yellows), not totals.
        assert sum(breakdown.values()) == 3

    def test_top_blocking_issues_counts_high_and_blocking_only(self):
        report = self._make(reds=3, yellows=2)
        # 3 datasources hit ROLLUP_MISMATCH (HIGH) plus complex_metric.
        # The 2 yellow datasources each have a MEDIUM-severity risk
        # that should NOT count toward blocking issues.
        top = report.top_blocking_issues()
        issues = {issue: count for issue, count in top}
        assert issues.get("ROLLUP_MISMATCH") == 3
        assert issues.get("complex_metric:users") == 3
        # MV_AMBIG is MEDIUM — must not appear.
        assert "MV_AMBIG" not in issues

    def test_top_blocking_issues_respects_n(self):
        # Build 10 distinct blocking issues, ask for top 3.
        report = ClusterReport(
            coordinator_url="x", overlord_url=None, started_at="t",
        )
        for i in range(10):
            report.datasources.append(DatasourceReport(
                datasource=f"d{i}", compat=COMPAT_RED,
                risks=[{"risk_id": f"ISSUE_{i}", "severity": "HIGH",
                        "confidence": "HIGH", "description": "..."}],
            ))
        # Make ISSUE_3 occur 5 times, ISSUE_7 occur 3 times.
        for i in range(4):
            report.datasources.append(DatasourceReport(
                datasource=f"d3_{i}", compat=COMPAT_RED,
                risks=[{"risk_id": "ISSUE_3", "severity": "HIGH",
                        "confidence": "HIGH", "description": "..."}],
            ))
        top = report.top_blocking_issues(3)
        assert top[0] == ("ISSUE_3", 5)
        assert len(top) == 3


# ─────────────────────────────────────────────────────────────────────────────
# Markdown rendering
# ─────────────────────────────────────────────────────────────────────────────


class TestMarkdownRender:
    def test_renders_basic_sections(self):
        report = ClusterReport(
            coordinator_url="http://druid:8081",
            overlord_url="http://druid:8090",
            started_at="2026-01-01T00:00:00+00:00",
        )
        report.datasources.append(DatasourceReport(
            datasource="events", compat=COMPAT_GREEN,
            source_kind="stream", classification="raw_event",
        ))
        report.datasources.append(DatasourceReport(
            datasource="legacy", compat=COMPAT_RED,
            source_kind="batch", classification="complex_aggregated",
            risks=[{"risk_id": "ROLLUP_MISMATCH", "severity": "HIGH",
                    "confidence": "HIGH", "description": "..."}],
        ))

        md = render_markdown(report)
        # Headline + cluster URLs.
        assert "Cluster Compatibility Report" in md
        assert "http://druid:8081" in md
        assert "http://druid:8090" in md
        # Status table.
        assert "GREEN" in md and "RED" in md
        # Both datasources rendered.
        assert "events" in md and "legacy" in md
        # Worst-status DS rendered first (RED ahead of GREEN).
        red_pos = md.index("legacy")
        green_pos = md.index("events")
        assert red_pos < green_pos

    def test_renders_top_issues_section(self):
        report = ClusterReport(
            coordinator_url="x", overlord_url=None, started_at="t",
        )
        for i in range(5):
            report.datasources.append(DatasourceReport(
                datasource=f"d{i}", compat=COMPAT_RED,
                risks=[{"risk_id": "ROLLUP_MISMATCH", "severity": "HIGH",
                        "confidence": "HIGH", "description": "..."}],
            ))
        md = render_markdown(report)
        assert "Top blocking issues" in md
        assert "ROLLUP_MISMATCH" in md


# ─────────────────────────────────────────────────────────────────────────────
# Disk writer
# ─────────────────────────────────────────────────────────────────────────────


class TestWriteReport:
    def test_writes_summary_markdown_and_per_ds(self, tmp_path):
        report = ClusterReport(
            coordinator_url="x", overlord_url=None, started_at="t",
        )
        report.datasources.extend([
            DatasourceReport(datasource="events", compat=COMPAT_GREEN),
            DatasourceReport(datasource="legacy/v2", compat=COMPAT_RED),
        ])
        paths = write_report(report, tmp_path)

        assert (tmp_path / "summary.json").exists()
        assert (tmp_path / "cluster-report.md").exists()
        # Per-DS dir + sanitised filename for the slash-containing name.
        per_ds = list((tmp_path / "datasources").glob("*.json"))
        assert len(per_ds) == 2
        names = {p.name for p in per_ds}
        assert "events.json" in names
        # ``legacy/v2`` slash gets replaced with underscore.
        assert any("legacy_v2" in n for n in names)

        # summary.json round-trips.
        loaded = json.loads((tmp_path / "summary.json").read_text())
        assert loaded["total"] == 2
        assert {d["datasource"] for d in loaded["datasources"]} == {
            "events", "legacy/v2",
        }

    def test_per_ds_filenames_capped_at_120_chars(self, tmp_path):
        # A pathologically long datasource name — the writer caps at
        # 120 chars to stay below most filesystems' 255-char limit.
        long_name = "a" * 200
        report = ClusterReport(
            coordinator_url="x", overlord_url=None, started_at="t",
        )
        report.datasources.append(DatasourceReport(
            datasource=long_name, compat=COMPAT_GREEN,
        ))
        write_report(report, tmp_path)
        per_ds = list((tmp_path / "datasources").glob("*.json"))
        assert len(per_ds) == 1
        # Filename (without .json) capped.
        assert len(per_ds[0].stem) <= 120
