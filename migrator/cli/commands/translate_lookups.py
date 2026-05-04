"""``dpm translate-lookups`` — Druid lookup config → Pinot dim tables."""

from __future__ import annotations

import json as _json
from pathlib import Path

import typer

from migrator.lookups.generator import generate_lookup_artifacts
from migrator.lookups.parser import (
    LookupParseError,
    UnsupportedLookupError,
    parse_lookups_config,
)


def command(
    lookups: Path = typer.Option(
        ...,
        "--lookups",
        help=(
            "Druid lookup config JSON. Either the tier-keyed form "
            "returned by Druid's Coordinator "
            "(/druid/coordinator/v1/lookups/config) or a flat "
            "{name: spec} form. Both are auto-detected."
        ),
    ),
    out: Path = typer.Option(
        Path("./lookups-out"),
        "--out",
        help=(
            "Directory to write per-lookup Pinot artifacts. Each "
            "lookup gets its own subdirectory containing "
            "schema.json, table-offline.json, and (for static_map "
            "lookups) data.json."
        ),
    ),
    table_name_prefix: str = typer.Option(
        "lookup_",
        "--table-name-prefix",
        help=(
            "Prefix for the generated Pinot dim-table names. "
            "Default 'lookup_' keeps the dim-table namespace clear; "
            "use '' (empty) if you've already prefixed your lookup "
            "names upstream."
        ),
    ),
    json_output: bool = typer.Option(
        False, "--json",
        help="Print a structured JSON summary instead of the pretty text.",
    ),
) -> None:
    """Translate Druid lookup configs into Pinot dim-table artifacts.

    Supported source shapes:
      - cachedNamespace + staticMap (inline key/value map)
      - cachedNamespace + uri (CSV with 2 columns, or simpleJson)

    Unsupported (the parser surfaces an explicit error):
      - JDBC, kafka, polling lookups
      - URI lookups with arbitrary multi-column CSV

    For each recognised lookup the command writes:
      <out>/<table_name>/schema.json
      <out>/<table_name>/table-offline.json
      <out>/<table_name>/data.json     (only for static_map sources)
      <out>/<table_name>/notes.md
    """
    try:
        raw = _json.loads(lookups.read_text())
    except Exception as exc:
        typer.echo(f"Failed to read {lookups}: {exc}", err=True)
        raise typer.Exit(code=2)

    try:
        canonical_lookups = parse_lookups_config(
            raw, source_file=str(lookups),
        )
    except (LookupParseError, UnsupportedLookupError) as exc:
        typer.echo(f"Lookup parse failed: {exc}", err=True)
        raise typer.Exit(code=2)

    if not canonical_lookups:
        typer.echo("No lookups found in input.", err=True)
        raise typer.Exit(code=2)

    out.mkdir(parents=True, exist_ok=True)

    summary: list[dict] = []
    for lookup in canonical_lookups:
        artifacts = generate_lookup_artifacts(
            lookup, table_name_prefix=table_name_prefix,
        )
        lookup_dir = out / artifacts.table_name
        lookup_dir.mkdir(parents=True, exist_ok=True)

        (lookup_dir / "schema.json").write_text(
            _json.dumps(artifacts.schema_, indent=2) + "\n"
        )
        (lookup_dir / "table-offline.json").write_text(
            _json.dumps(artifacts.table, indent=2) + "\n"
        )
        if artifacts.inline_data is not None:
            with (lookup_dir / "data.json").open("w") as fh:
                for row in artifacts.inline_data:
                    fh.write(_json.dumps(row) + "\n")

        notes_path = lookup_dir / "notes.md"
        notes_body = (
            f"# Lookup: {lookup.name}\n\n"
            f"- Source kind: `{lookup.source_kind}`\n"
            f"- Pinot table: `{artifacts.table_name}_OFFLINE`\n\n"
            "## Notes\n\n"
            + "\n".join(f"- {n}" for n in artifacts.notes)
            + "\n"
        )
        notes_path.write_text(notes_body)

        summary.append({
            "lookup": lookup.name,
            "source_kind": lookup.source_kind,
            "table_name": artifacts.table_name,
            "out_dir": str(lookup_dir),
            "rows_inline": (
                len(artifacts.inline_data)
                if artifacts.inline_data is not None
                else None
            ),
        })

    if json_output:
        typer.echo(_json.dumps(summary, indent=2))
        return

    typer.echo(f"Translated {len(summary)} lookup(s) → {out}")
    for s in summary:
        details = (
            f"static_map ({s['rows_inline']} rows)"
            if s["source_kind"] == "static_map"
            else s["source_kind"]
        )
        typer.echo(
            f"  • {s['lookup']:<24s}  → {s['table_name']:<30s}  ({details})"
        )
    typer.echo("")
    typer.echo(
        "Next: deploy each lookup directory's schema + table via\n"
        "  `dpm deploy --artifacts-dir <out>/<table_name> --pinot-controller ...`"
    )
