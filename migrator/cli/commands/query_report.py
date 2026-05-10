"""``dpm query-report`` — Druid → Pinot SQL compatibility report.

Operators have a Druid cluster running thousands of dashboards and
saved queries. Before cutting over to Pinot they need to know which
queries will fail, which need rewriting, and which will run unchanged.
This command takes a corpus of Druid SQL — one file containing
multiple queries (separated by ``;``), or one query per file in a
directory — and emits the same shape of report as ``dpm cluster-report``
emits for ingestion specs:

  - ``<out>/summary.json`` — structured per-query verdicts plus the
    cluster-wide top-pattern aggregation.
  - ``<out>/query-report.md`` — pretty markdown for the migration
    review document.

The classifier doesn't rewrite queries (out of scope; rewrite is
context-dependent and risky). It surfaces what needs human attention.
"""

from __future__ import annotations

import sys
from pathlib import Path

import typer
from rich.console import Console

from migrator.queries.classifier import (
    VERDICT_COMPATIBLE,
    VERDICT_INCOMPATIBLE,
    VERDICT_RISKY,
    classify_query,
)
from migrator.queries.report import QueryReport, write_report


_console = Console()


def _split_sql_file(text: str) -> list[str]:
    """Split a multi-statement SQL file on ``;`` boundaries, ignoring
    semicolons inside string literals. Operators routinely paste a
    dashboard's full query catalogue into one file."""
    out: list[str] = []
    buf: list[str] = []
    in_str = False
    quote = ""
    i = 0
    while i < len(text):
        ch = text[i]
        if in_str:
            buf.append(ch)
            if ch == quote:
                # Doubled-quote escape: '' inside ''-string.
                if i + 1 < len(text) and text[i + 1] == quote:
                    buf.append(text[i + 1])
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch in ("'", '"'):
            in_str = True
            quote = ch
            buf.append(ch)
            i += 1
            continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    final = "".join(buf).strip()
    if final:
        out.append(final)
    return out


def _load_queries(path: Path) -> list[tuple[str, str]]:
    """Resolve ``path`` to a ``[(query_id, sql), ...]`` list.

    ``path`` may be:
      - a directory: every ``*.sql`` and ``*.txt`` file becomes one
        query, ``query_id`` = the filename relative to the dir.
      - a single file with one or more ``;``-separated statements.
        ``query_id`` = ``<filename>::<n>`` where N is the 1-indexed
        position.
    """
    pairs: list[tuple[str, str]] = []
    if path.is_dir():
        for f in sorted(path.rglob("*")):
            if not f.is_file():
                continue
            if f.suffix.lower() not in (".sql", ".txt"):
                continue
            txt = f.read_text().strip()
            if not txt:
                continue
            pairs.append((str(f.relative_to(path)), txt))
        return pairs

    text = path.read_text()
    statements = _split_sql_file(text)
    if len(statements) == 1:
        pairs.append((path.name, statements[0]))
    else:
        for idx, stmt in enumerate(statements, start=1):
            pairs.append((f"{path.name}::{idx}", stmt))
    return pairs


def command(
    input_path: Path = typer.Argument(
        ...,
        help=(
            "Path to a directory of .sql files OR a single SQL file "
            "containing one or more ';'-separated statements."
        ),
    ),
    out: Path = typer.Option(
        Path("./query-report"),
        "--out",
        help="Output directory for the report files.",
    ),
    fail_on_incompatible: bool = typer.Option(
        False, "--fail-on-incompatible",
        help=(
            "Exit non-zero (3) when any query is classified "
            "INCOMPATIBLE. Useful in CI to gate the migration on "
            "a clean query corpus."
        ),
    ),
) -> None:
    """Classify Druid SQL queries by Pinot compatibility risk."""
    if not input_path.exists():
        typer.echo(f"Path does not exist: {input_path}", err=True)
        raise typer.Exit(code=2)

    try:
        pairs = _load_queries(input_path)
    except OSError as exc:
        typer.echo(f"Failed to read {input_path}: {exc}", err=True)
        raise typer.Exit(code=2)

    if not pairs:
        typer.echo(
            "No SQL files found. Pass a directory containing .sql/.txt "
            "files or a single SQL file with ';'-separated statements.",
            err=True,
        )
        raise typer.Exit(code=2)

    report = QueryReport()
    for qid, sql in pairs:
        report.queries.append(classify_query(sql, query_id=qid))

    paths = write_report(report, out)

    _console.print("")
    _console.print(f"[bold]{report.total} query(ies) classified[/bold]")
    by = report.by_verdict
    color = {
        VERDICT_COMPATIBLE: "green",
        VERDICT_RISKY: "yellow",
        VERDICT_INCOMPATIBLE: "red",
    }
    for v in (VERDICT_COMPATIBLE, VERDICT_RISKY, VERDICT_INCOMPATIBLE):
        n = by.get(v, 0)
        _console.print(
            f"  [{color[v]}]{v.upper():<14s}[/{color[v]}] {n}"
        )

    top = report.top_patterns(5)
    if top:
        _console.print("")
        _console.print("[bold]Top compatibility patterns[/bold]")
        for pattern, n in top:
            _console.print(f"  [yellow]{n:>3d}×[/yellow] {pattern}")

    _console.print("")
    _console.print(f"Report: {paths['markdown']}")
    _console.print(f"        {paths['summary']}")

    if fail_on_incompatible and by.get(VERDICT_INCOMPATIBLE, 0) > 0:
        # Exit code 3 distinguishes "incompatible queries detected"
        # from exit 2 (config error) and exit 1 (unexpected). CI guard
        # scripts can branch on it the same way cluster-report does.
        raise typer.Exit(code=3)
