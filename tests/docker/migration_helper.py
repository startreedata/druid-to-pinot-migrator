"""
Helpers that sit between the migrator library and the live Pinot cluster.

These cover the "last mile" that the generator tool cannot do:
  - Writing an inline Druid spec file from a datasource definition
  - Pushing generated schema + table config to Pinot via REST
  - Ingesting data into Pinot's OFFLINE table from a JSON file
  - Running matched queries against both clusters and comparing results
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import requests

from migrator.translators.pipeline import generate_bundle
from tests.docker.cluster_clients import DruidClient, PinotClient


# ─────────────────────────────────────────────────────────────────────────────
# Spec builder — construct a Druid spec from a datasource description
# ─────────────────────────────────────────────────────────────────────────────

def build_druid_spec(
    datasource: str,
    timestamp_col: str = "timestamp",
    timestamp_format: str = "millis",
    dimensions: list[str] | None = None,
    metrics: list[dict] | None = None,
    rollup: bool = False,
    segment_granularity: str = "DAY",
    query_granularity: str = "NONE",
    intervals: list[str] | None = None,
    input_type: str = "inline",
) -> dict:
    """
    Return a minimal Druid index_parallel spec dict that can be serialised
    to a file and passed to the migration tool.

    timestamp_format defaults to "millis" so both Druid and Pinot handle
    epoch-millisecond integer timestamps without conversion issues.
    """
    return {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {
                    "column": timestamp_col,
                    "format": timestamp_format,
                },
                "dimensionsSpec": {
                    "dimensions": dimensions or [],
                },
                "metricsSpec": metrics or [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": segment_granularity,
                    "queryGranularity": query_granularity,
                    "rollup": rollup,
                    "intervals": intervals or ["2024-01-01/2025-01-01"],
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": input_type,
                    "baseDir": "/data",
                    "filter": "*.json",
                },
                "inputFormat": {"type": "json"},
            },
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Full migration pipeline: Druid spec file -> Pinot cluster
# ─────────────────────────────────────────────────────────────────────────────

def migrate_and_deploy(
    spec: dict,
    pinot: PinotClient,
    out_dir: str | Path,
) -> dict:
    """
    1. Write the Druid spec to a temp JSON file.
    2. Run generate_bundle() to produce Pinot artifacts.
    3. Push schema and table config to the live Pinot cluster.

    Returns the GenerateResult as a dict plus the paths to key artifacts.
    """
    out_dir = Path(out_dir)
    spec_path = out_dir / "druid_spec.json"
    spec_path.write_text(json.dumps(spec, indent=2))

    result = generate_bundle(str(spec_path), out_dir=str(out_dir))
    if not result.success:
        raise RuntimeError(
            f"generate_bundle failed for {spec['spec']['dataSchema']['dataSource']}: "
            f"{result.errors}"
        )

    schema_path = out_dir / "schema.json"
    schema = json.loads(schema_path.read_text())

    # Determine table file
    offline_path = out_dir / "table-offline.json"
    realtime_path = out_dir / "table-realtime.json"
    if offline_path.exists():
        table = json.loads(offline_path.read_text())
    elif realtime_path.exists():
        table = json.loads(realtime_path.read_text())
    else:
        raise RuntimeError("No table config file produced by generate_bundle")

    # Push to Pinot
    pinot.create_schema(schema)
    pinot.create_table(table)

    return {
        "generate_result": result,
        "schema": schema,
        "table": table,
        "out_dir": out_dir,
        "spec_path": spec_path,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Data ingestion into Pinot OFFLINE table via /ingestFromFile
# ─────────────────────────────────────────────────────────────────────────────

def ingest_records_into_pinot(
    pinot: PinotClient,
    table_name: str,
    records: list[dict],
    tmp_dir: str | Path,
) -> None:
    """
    Write records as NDJSON and POST to Pinot's ingestFromFile endpoint.
    The table must already be created before calling this.
    """
    tmp_dir = Path(tmp_dir)
    data_file = tmp_dir / f"{table_name}_records.json"
    with data_file.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")

    url = (
        f"{pinot.controller_url}/ingestFromFile"
        f"?tableNameWithType={table_name}_OFFLINE"
        f"&batchConfigMapStr={json.dumps({'inputFormat': 'json'})}"
    )
    with data_file.open("rb") as fh:
        r = requests.post(
            url,
            files={"file": ("data.json", fh, "application/octet-stream")},
            timeout=120,
        )
    if r.status_code not in (200, 201):
        raise RuntimeError(
            f"Pinot ingestFromFile failed for {table_name}: {r.status_code} {r.text}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Query comparison helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_rows(rows: list[dict]) -> list[dict]:
    """
    Sort rows and normalise numeric types for comparison.
    Floats are rounded to 6 significant figures to absorb float precision deltas.
    """
    def normalise_value(v: Any) -> Any:
        if isinstance(v, float):
            return round(v, 6)
        return v

    normalised = [
        {k: normalise_value(v) for k, v in row.items()}
        for row in rows
    ]
    # Sort deterministically
    return sorted(normalised, key=lambda r: json.dumps(r, sort_keys=True, default=str))


def assert_row_counts_match(
    druid: DruidClient,
    pinot: PinotClient,
    druid_datasource: str,
    pinot_table: str,
) -> None:
    """Assert that total row counts match between Druid and Pinot."""
    druid_rows = druid.sql_query(
        f'SELECT COUNT(*) AS cnt FROM "{druid_datasource}"'
    )
    pinot_rows = pinot.sql_query(
        f"SELECT COUNT(*) AS cnt FROM {pinot_table}"
    )
    druid_cnt = druid_rows[0]["cnt"]
    pinot_cnt = pinot_rows[0]["cnt"]
    assert druid_cnt == pinot_cnt, (
        f"Row count mismatch: Druid={druid_cnt}, Pinot={pinot_cnt} "
        f"for {druid_datasource} -> {pinot_table}"
    )


def assert_dimension_values_match(
    druid: DruidClient,
    pinot: PinotClient,
    druid_datasource: str,
    pinot_table: str,
    dimension: str,
) -> None:
    """Assert that the distinct values of a dimension column match."""
    druid_rows = druid.sql_query(
        f'SELECT DISTINCT "{dimension}" FROM "{druid_datasource}" ORDER BY 1'
    )
    pinot_rows = pinot.sql_query(
        f"SELECT DISTINCT {dimension} FROM {pinot_table} ORDER BY 1"
    )
    druid_vals = {r[dimension] for r in druid_rows}
    pinot_vals = {r[dimension] for r in pinot_rows}
    assert druid_vals == pinot_vals, (
        f"Dimension '{dimension}' values differ: "
        f"only in Druid={druid_vals - pinot_vals}, "
        f"only in Pinot={pinot_vals - druid_vals}"
    )


def assert_aggregated_query_matches(
    druid: DruidClient,
    pinot: PinotClient,
    druid_sql: str,
    pinot_sql: str,
    tolerance: float = 1e-4,
) -> None:
    """
    Run matching SQL queries on both clusters and assert the result sets are
    equivalent (modulo float tolerance and row order).
    """
    druid_rows = _normalise_rows(druid.sql_query(druid_sql))
    pinot_rows = _normalise_rows(pinot.sql_query(pinot_sql))

    assert len(druid_rows) == len(pinot_rows), (
        f"Result row count differs: Druid={len(druid_rows)}, Pinot={len(pinot_rows)}\n"
        f"Druid SQL: {druid_sql}\nPinot SQL: {pinot_sql}\n"
        f"Druid: {druid_rows[:5]}\nPinot: {pinot_rows[:5]}"
    )

    for i, (dr, pr) in enumerate(zip(druid_rows, pinot_rows)):
        assert set(dr.keys()) == set(pr.keys()), (
            f"Row {i}: column set differs: Druid={set(dr.keys())}, Pinot={set(pr.keys())}"
        )
        for col in dr:
            dv, pv = dr[col], pr[col]
            if isinstance(dv, (int, float)) and isinstance(pv, (int, float)):
                assert abs(float(dv) - float(pv)) <= tolerance, (
                    f"Row {i} col '{col}': Druid={dv} Pinot={pv} (delta > {tolerance})"
                )
            else:
                assert str(dv) == str(pv), (
                    f"Row {i} col '{col}': Druid={dv!r} Pinot={pv!r}"
                )
