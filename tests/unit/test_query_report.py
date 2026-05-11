"""Unit tests for query-report aggregation, rendering, and CLI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from migrator.cli.app import app
from migrator.queries.classifier import (
    QueryClassification,
    QueryIssue,
    SEV_INCOMPATIBLE,
    SEV_RISKY,
    VERDICT_COMPATIBLE,
    VERDICT_INCOMPATIBLE,
    VERDICT_RISKY,
    classify_query,
)
from migrator.queries.report import QueryReport, render_markdown, write_report


def _q(qid: str, verdict: str, *issues: tuple[str, str]) -> QueryClassification:
    """Build a QueryClassification directly. Keeps the aggregation
    tests independent of the classifier."""
    return QueryClassification(
        query_id=qid,
        verdict=verdict,
        issues=[
            QueryIssue(pattern=p, severity=s, detail=p, pinot_equivalent="")
            for p, s in issues
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Aggregation
# ─────────────────────────────────────────────────────────────────────────────


class TestAggregation:

    def test_empty_report(self):
        r = QueryReport()
        assert r.total == 0
        assert r.by_verdict == {}
        assert r.top_patterns() == []

    def test_by_verdict_counts(self):
        r = QueryReport(queries=[
            _q("a", VERDICT_COMPATIBLE),
            _q("b", VERDICT_COMPATIBLE),
            _q("c", VERDICT_RISKY, ("JOIN", SEV_RISKY)),
            _q("d", VERDICT_INCOMPATIBLE, ("LOOKUP", SEV_INCOMPATIBLE)),
        ])
        assert r.by_verdict[VERDICT_COMPATIBLE] == 2
        assert r.by_verdict[VERDICT_RISKY] == 1
        assert r.by_verdict[VERDICT_INCOMPATIBLE] == 1

    def test_top_patterns_dedupe_per_query(self):
        # A query that uses LOOKUP three times should still only count
        # once in queries_affected — operators want "how many queries
        # break", not "how many call sites".
        r = QueryReport(queries=[
            _q("q1", VERDICT_INCOMPATIBLE,
               ("LOOKUP", SEV_INCOMPATIBLE),
               ("LOOKUP", SEV_INCOMPATIBLE),
               ("LOOKUP", SEV_INCOMPATIBLE)),
            _q("q2", VERDICT_INCOMPATIBLE, ("LOOKUP", SEV_INCOMPATIBLE)),
        ])
        top = dict(r.top_patterns())
        assert top["LOOKUP"] == 2

    def test_top_patterns_sorted_descending(self):
        r = QueryReport(queries=[
            _q("a", VERDICT_INCOMPATIBLE, ("LOOKUP", SEV_INCOMPATIBLE)),
            _q("b", VERDICT_INCOMPATIBLE, ("LOOKUP", SEV_INCOMPATIBLE)),
            _q("c", VERDICT_INCOMPATIBLE, ("LOOKUP", SEV_INCOMPATIBLE)),
            _q("d", VERDICT_RISKY, ("JOIN", SEV_RISKY)),
            _q("e", VERDICT_RISKY, ("JOIN", SEV_RISKY)),
        ])
        top = r.top_patterns()
        assert top[0] == ("LOOKUP", 3)
        assert top[1] == ("JOIN", 2)

    def test_to_dict_shape(self):
        r = QueryReport(queries=[
            _q("a", VERDICT_RISKY, ("JOIN", SEV_RISKY)),
        ])
        d = r.to_dict()
        assert d["total"] == 1
        assert d["by_verdict"]["risky"] == 1
        assert d["top_patterns"][0]["pattern"] == "JOIN"
        assert d["top_patterns"][0]["queries_affected"] == 1
        assert d["queries"][0]["query_id"] == "a"


# ─────────────────────────────────────────────────────────────────────────────
# Markdown rendering
# ─────────────────────────────────────────────────────────────────────────────


class TestRenderMarkdown:

    def test_includes_title(self):
        md = render_markdown(QueryReport())
        assert "Query Compatibility Report" in md
        assert "Total queries:** 0" in md

    def test_renders_each_query_block(self):
        r = QueryReport(queries=[
            _q("dash_42.sql", VERDICT_RISKY, ("JOIN", SEV_RISKY)),
        ])
        md = render_markdown(r)
        assert "`dash_42.sql`" in md
        assert "JOIN" in md

    def test_orders_incompatible_before_risky_before_compatible(self):
        r = QueryReport(queries=[
            _q("z_clean", VERDICT_COMPATIBLE),
            _q("m_risky", VERDICT_RISKY, ("JOIN", SEV_RISKY)),
            _q("a_broken", VERDICT_INCOMPATIBLE, ("LOOKUP", SEV_INCOMPATIBLE)),
        ])
        md = render_markdown(r)
        # Per-query detail ordering: INCOMPATIBLE first.
        # Anchor at the per-query section.
        section = md[md.index("## Per-query detail"):]
        bad = section.index("a_broken")
        risky = section.index("m_risky")
        clean = section.index("z_clean")
        assert bad < risky < clean

    def test_compatible_query_shows_no_issues_line(self):
        r = QueryReport(queries=[_q("clean", VERDICT_COMPATIBLE)])
        md = render_markdown(r)
        assert "No issues detected" in md


# ─────────────────────────────────────────────────────────────────────────────
# write_report — disk artifacts
# ─────────────────────────────────────────────────────────────────────────────


class TestWriteReport:

    def test_writes_summary_and_markdown(self, tmp_path: Path):
        r = QueryReport(queries=[
            _q("q1", VERDICT_INCOMPATIBLE, ("LOOKUP", SEV_INCOMPATIBLE)),
            _q("q2", VERDICT_COMPATIBLE),
        ])
        paths = write_report(r, tmp_path)
        assert paths["summary"].exists()
        assert paths["markdown"].exists()
        loaded = json.loads(paths["summary"].read_text())
        assert loaded["total"] == 2
        assert loaded["by_verdict"]["incompatible"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# CLI happy path
# ─────────────────────────────────────────────────────────────────────────────


class TestQueryReportCli:

    def setup_method(self):
        self.runner = CliRunner()

    def test_directory_input(self, tmp_path: Path):
        (tmp_path / "input").mkdir()
        (tmp_path / "input" / "a.sql").write_text(
            "SELECT a, b FROM events"
        )
        (tmp_path / "input" / "b.sql").write_text(
            "SELECT LOOKUP(c, 'k') FROM events"
        )
        out = tmp_path / "out"
        result = self.runner.invoke(
            app, ["query-report", str(tmp_path / "input"), "--out", str(out)],
        )
        assert result.exit_code == 0, result.output
        summary = json.loads((out / "summary.json").read_text())
        assert summary["total"] == 2
        verdicts = {q["query_id"]: q["verdict"] for q in summary["queries"]}
        assert verdicts["a.sql"] == VERDICT_COMPATIBLE
        assert verdicts["b.sql"] == VERDICT_INCOMPATIBLE

    def test_single_file_with_multiple_statements(self, tmp_path: Path):
        sqlfile = tmp_path / "queries.sql"
        sqlfile.write_text(
            "SELECT a FROM events;\n"
            "SELECT LOOKUP(c, 'k') FROM events;\n"
            "SELECT TIME_FLOOR(__time, 'PT1H') FROM events;\n"
        )
        out = tmp_path / "out"
        result = self.runner.invoke(
            app, ["query-report", str(sqlfile), "--out", str(out)],
        )
        assert result.exit_code == 0, result.output
        summary = json.loads((out / "summary.json").read_text())
        assert summary["total"] == 3
        assert summary["by_verdict"]["compatible"] == 1
        assert summary["by_verdict"]["incompatible"] == 1
        assert summary["by_verdict"]["risky"] == 1

    def test_fail_on_incompatible_exit_code(self, tmp_path: Path):
        sqlfile = tmp_path / "q.sql"
        sqlfile.write_text("SELECT LOOKUP(c, 'k') FROM events")
        out = tmp_path / "out"
        result = self.runner.invoke(
            app, ["query-report", str(sqlfile), "--out", str(out),
                  "--fail-on-incompatible"],
        )
        assert result.exit_code == 3

    def test_missing_path_exits_with_config_error(self, tmp_path: Path):
        result = self.runner.invoke(
            app, ["query-report", str(tmp_path / "does_not_exist"),
                  "--out", str(tmp_path / "out")],
        )
        assert result.exit_code == 2

    def test_empty_input_directory_exits_with_config_error(
        self, tmp_path: Path,
    ):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = self.runner.invoke(
            app, ["query-report", str(empty), "--out", str(tmp_path / "out")],
        )
        assert result.exit_code == 2

    def test_top_patterns_appear_in_terminal_output(self, tmp_path: Path):
        sqlfile = tmp_path / "q.sql"
        sqlfile.write_text(
            "SELECT LOOKUP(a, 'x') FROM e1;\n"
            "SELECT LOOKUP(b, 'x') FROM e2;\n"
            "SELECT LOOKUP(c, 'x') FROM e3;\n"
        )
        out = tmp_path / "out"
        result = self.runner.invoke(
            app, ["query-report", str(sqlfile), "--out", str(out)],
        )
        assert result.exit_code == 0
        assert "LOOKUP" in result.output
