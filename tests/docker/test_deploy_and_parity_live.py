"""
Live integration tests for the v0.4.0 ``dpm parity-check`` and v0.5.0
``dpm deploy`` features.

These exercise the production code paths against the no-auth Druid +
Pinot stack from ``tests/docker/docker-compose.yml`` (the auth path is
covered separately in ``tests/docker/auth/test_auth_live.py``).

Coverage:

  TestPinotDeployerLive
    * Deploy a fresh schema + OFFLINE table → 200/201
    * Re-run the same deploy → 409s treated as ``already_exists`` (idempotent)
    * Report.all_ok stays True across both runs

  TestParityCheckLive
    * Auto-derive parity queries from a Druid spec, run them against
      a populated Druid datasource + Pinot table, all checks PASS
    * Inject divergent data into Pinot → at least one query FAILs;
      orchestrator surfaces it without aborting the run
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.parity.clients import DruidHttpSqlClient, PinotHttpSqlClient
from migrator.parity.query_builder import derive_queries_from_canonical
from migrator.parity.runner import run_parity
from migrator.pinot.deployer import (
    DeployArtifacts,
    PinotDeployer,
    discover_artifacts,
)
from tests.docker.cluster_clients import (
    DRUID_ROUTER_URL,
    PINOT_BROKER_URL,
    PINOT_CONTROLLER_URL,
)
from tests.docker.migration_helper import ingest_records_into_pinot


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixture: minimal pageviews-like Druid datasource + matching Pinot
# table populated with the same rows. Used by both deploy and parity tests.
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pageviews_state(druid, pinot, tmp_path_factory):
    """Ingest a small pageviews dataset into Druid + Pinot for the live tests.

    Module-scoped so the two parity tests share the same ingested data
    (the divergence test relies on the matching Pinot table from the
    happy-path test). Cleanup is done in-fixture rather than via the
    function-scoped ``druid_datasource_factory`` / ``pinot_table_factory``
    — pytest disallows a module-scoped fixture from depending on a
    function-scoped one.
    """
    ds = "pv_v05_live"
    records = []
    base = 1_704_067_200_000  # 2024-01-01T00:00:00Z
    for i in range(120):
        records.append({
            "timestamp": base + i * 60_000,
            "region": "us-east" if i % 2 == 0 else "us-west",
            "platform": ["desktop", "mobile", "tablet"][i % 3],
            "user_id": f"user_{i % 30}",
        })

    # ── Druid ─────────────────────────────────────────────────────────────
    druid.ingest_inline(
        datasource=ds,
        records=records,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["region", "platform", "user_id"],
        metrics=[],
        rollup=False,
    )
    druid.wait_for_datasource(ds, timeout=180)

    # ── Pinot schema + OFFLINE table ──────────────────────────────────────
    schema = {
        "schemaName": ds,
        "dateTimeFieldSpecs": [
            {"dataType": "LONG", "format": "1:MILLISECONDS:EPOCH",
             "granularity": "1:MILLISECONDS", "name": "timestamp"},
        ],
        "dimensionFieldSpecs": [
            {"dataType": "STRING", "name": "region"},
            {"dataType": "STRING", "name": "platform"},
            {"dataType": "STRING", "name": "user_id"},
        ],
        "metricFieldSpecs": [],
    }
    table_offline = {
        "tableName": f"{ds}_OFFLINE",
        "tableType": "OFFLINE",
        "segmentsConfig": {
            "timeColumnName": "timestamp",
            "timeType": "MILLISECONDS",
            "replication": "1",
            "retentionTimeUnit": "DAYS",
            "retentionTimeValue": "365",
        },
        "tenants": {"broker": "DefaultTenant", "server": "DefaultTenant"},
        "tableIndexConfig": {"loadMode": "MMAP"},
        "metadata": {"customConfigs": {}},
    }
    pinot.create_schema(schema)
    pinot.create_table(table_offline)

    work = tmp_path_factory.mktemp("pv_v05_live")
    # Use the migration_helper ingest path that the other live tests
    # use — it sends `{"inputFormat":"json"}` (the simple form Pinot
    # 1.5+ accepts), not the nested recordReaderSpec form which 1.5
    # rejects with a Jackson "Cannot deserialize String from Object"
    # error.
    ingest_records_into_pinot(pinot, ds, records, str(work))
    pinot.wait_for_table_queryable(f"{ds}_OFFLINE", timeout=360)

    # ── A Druid spec to feed --from-canonical ─────────────────────────────
    spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": ds,
                "timestampSpec": {"column": "timestamp", "format": "millis"},
                "dimensionsSpec": {
                    "dimensions": ["region", "platform", "user_id"],
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "NONE",
                    "rollup": False,
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {"type": "local", "baseDir": "/tmp",
                                 "filter": "*.json"},
                "inputFormat": {"type": "json"},
            },
            "tuningConfig": {"type": "index_parallel"},
        },
    }
    spec_path = work / "spec.json"
    spec_path.write_text(json.dumps(spec))

    yield {
        "ds": ds,
        "pinot_table": ds,
        "row_count": len(records),
        "spec_path": spec_path,
    }

    # ── teardown ──────────────────────────────────────────────────────────
    # Best-effort: if Druid/Pinot are already torn down by the session
    # fixture this just no-ops. We swallow exceptions so a teardown
    # blip can't mask a real test failure in the report.
    try:
        druid.drop_datasource(ds)
    except Exception:
        pass
    try:
        pinot.delete_table(ds)
    except Exception:
        pass
    try:
        pinot.delete_schema(ds)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# 1. PinotDeployer — live deploy + idempotency
# ─────────────────────────────────────────────────────────────────────────────


class TestPinotDeployerLive:
    """Exercises ``migrator.pinot.deployer.PinotDeployer`` against the
    live Pinot controller. The deploy path the CLI uses is exactly this
    module — testing it here covers the CLI's deployment behaviour
    without forking a subprocess."""

    def test_deploy_fresh_schema_and_offline_table(
        self, pinot, tmp_path: Path,
    ):
        # Distinct table name so we don't collide with the shared fixture.
        ds = "deploy_live_fresh"
        schema = {
            "schemaName": ds,
            "dateTimeFieldSpecs": [
                {"dataType": "LONG", "format": "1:MILLISECONDS:EPOCH",
                 "granularity": "1:MILLISECONDS", "name": "timestamp"},
            ],
            "dimensionFieldSpecs": [
                {"dataType": "STRING", "name": "k"},
            ],
            "metricFieldSpecs": [],
        }
        table_offline = {
            "tableName": f"{ds}_OFFLINE",
            "tableType": "OFFLINE",
            "segmentsConfig": {
                "timeColumnName": "timestamp",
                "timeType": "MILLISECONDS",
                "replication": "1",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {"broker": "DefaultTenant", "server": "DefaultTenant"},
            "tableIndexConfig": {"loadMode": "MMAP"},
            "metadata": {"customConfigs": {}},
        }
        schema_path = tmp_path / "schema.json"
        offline_path = tmp_path / "table-offline.json"
        schema_path.write_text(json.dumps(schema))
        offline_path.write_text(json.dumps(table_offline))

        deployer = PinotDeployer(PINOT_CONTROLLER_URL)
        report = deployer.deploy(DeployArtifacts(
            schema=schema_path, offline_table=offline_path,
        ))
        # Manual cleanup via try/finally: the schema/table came from
        # PinotDeployer (the system-under-test) rather than
        # pinot_table_factory, so the factory's auto-cleanup doesn't
        # cover it.
        try:
            assert report.all_ok, [r.detail for r in report.results]
            assert report.created == 2

            # Verify with the live controller.
            schemas = requests.get(
                f"{PINOT_CONTROLLER_URL}/schemas", timeout=10,
            ).json()
            assert ds in schemas, f"schema {ds!r} not visible in /schemas"

            tables = requests.get(
                f"{PINOT_CONTROLLER_URL}/tables", timeout=10,
            ).json()
            assert ds in tables.get("tables", []), (
                f"table {ds!r} not visible in /tables: {tables}"
            )
        finally:
            requests.delete(
                f"{PINOT_CONTROLLER_URL}/tables/{ds}?type=offline", timeout=10,
            )
            requests.delete(
                f"{PINOT_CONTROLLER_URL}/schemas/{ds}", timeout=10,
            )

    def test_deploy_is_idempotent_on_409(
        self, pinot, tmp_path: Path,
    ):
        """Deploying the same schema/table twice must not error."""
        ds = "deploy_live_idemp"
        schema = {
            "schemaName": ds,
            "dateTimeFieldSpecs": [
                {"dataType": "LONG", "format": "1:MILLISECONDS:EPOCH",
                 "granularity": "1:MILLISECONDS", "name": "timestamp"},
            ],
            "dimensionFieldSpecs": [
                {"dataType": "STRING", "name": "k"},
            ],
            "metricFieldSpecs": [],
        }
        table_offline = {
            "tableName": f"{ds}_OFFLINE",
            "tableType": "OFFLINE",
            "segmentsConfig": {
                "timeColumnName": "timestamp",
                "timeType": "MILLISECONDS",
                "replication": "1",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {"broker": "DefaultTenant", "server": "DefaultTenant"},
            "tableIndexConfig": {"loadMode": "MMAP"},
            "metadata": {"customConfigs": {}},
        }
        schema_path = tmp_path / "schema.json"
        offline_path = tmp_path / "table-offline.json"
        schema_path.write_text(json.dumps(schema))
        offline_path.write_text(json.dumps(table_offline))

        deployer = PinotDeployer(PINOT_CONTROLLER_URL)
        try:
            first = deployer.deploy(DeployArtifacts(
                schema=schema_path, offline_table=offline_path,
            ))
            assert first.all_ok
            assert first.created == 2

            # Second run: schema returns 200 (Pinot's behaviour for a
            # re-post of an identical schema), table returns 409 →
            # mapped to ``already_exists``. Both flow as soft success.
            second = deployer.deploy(DeployArtifacts(
                schema=schema_path, offline_table=offline_path,
            ))
            assert second.all_ok, [r.detail for r in second.results]
            # The table half is the one Pinot 409s on for re-creates.
            statuses = [r.status for r in second.results]
            assert "already_exists" in statuses, (
                f"expected at least one already_exists across {statuses}"
            )
        finally:
            requests.delete(
                f"{PINOT_CONTROLLER_URL}/tables/{ds}?type=offline", timeout=10,
            )
            requests.delete(
                f"{PINOT_CONTROLLER_URL}/schemas/{ds}", timeout=10,
            )

    def test_discover_artifacts_drives_full_deploy(self, tmp_path: Path):
        """The CLI's ``--artifacts-dir`` mode goes through
        ``discover_artifacts`` → ``PinotDeployer.deploy``. Exercise that
        path end-to-end."""
        ds = "deploy_live_disc"
        # Lay out files exactly as ``dpm generate`` would.
        (tmp_path / "schema.json").write_text(json.dumps({
            "schemaName": ds,
            "dateTimeFieldSpecs": [
                {"dataType": "LONG", "format": "1:MILLISECONDS:EPOCH",
                 "granularity": "1:MILLISECONDS", "name": "timestamp"},
            ],
            "dimensionFieldSpecs": [{"dataType": "STRING", "name": "k"}],
            "metricFieldSpecs": [],
        }))
        (tmp_path / "table-offline.json").write_text(json.dumps({
            "tableName": f"{ds}_OFFLINE",
            "tableType": "OFFLINE",
            "segmentsConfig": {
                "timeColumnName": "timestamp",
                "timeType": "MILLISECONDS",
                "replication": "1",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {"broker": "DefaultTenant", "server": "DefaultTenant"},
            "tableIndexConfig": {"loadMode": "MMAP"},
            "metadata": {"customConfigs": {}},
        }))
        # No table-realtime.json — discovery must skip it cleanly.

        artifacts = discover_artifacts(tmp_path)
        assert artifacts.schema is not None
        assert artifacts.offline_table is not None
        assert artifacts.realtime_table is None

        deployer = PinotDeployer(PINOT_CONTROLLER_URL)
        try:
            report = deployer.deploy(artifacts)
            assert report.all_ok
            # Only two slots had files → only two deploys.
            assert len(report.results) == 2
        finally:
            requests.delete(
                f"{PINOT_CONTROLLER_URL}/tables/{ds}?type=offline", timeout=10,
            )
            requests.delete(
                f"{PINOT_CONTROLLER_URL}/schemas/{ds}", timeout=10,
            )


# ─────────────────────────────────────────────────────────────────────────────
# 2. parity-check — auto-derived queries against live Druid + Pinot
# ─────────────────────────────────────────────────────────────────────────────


class TestParityCheckLive:
    """Exercises ``derive_queries_from_canonical`` + ``run_parity``
    against the live Druid + Pinot stack — exactly what the
    ``dpm parity-check --from-canonical`` CLI does end-to-end."""

    def test_parity_passes_when_data_matches(self, pageviews_state):
        spec = json.loads(Path(pageviews_state["spec_path"]).read_text())
        parsed = DruidSpecParser().parse(spec)
        canonical = DruidNormalizer().normalize(parsed.parsed_spec).canonical

        queries = derive_queries_from_canonical(
            canonical, pinot_table=pageviews_state["pinot_table"],
        )
        # The auto-derived set for our shape: 1 total + 0 metrics +
        # 3 dimensions = 4 queries.
        assert len(queries) == 4
        labels = {q.label for q in queries}
        assert "Total event count" in labels
        assert "events by region" in labels
        assert "events by platform" in labels
        assert "events by user_id" in labels

        druid = DruidHttpSqlClient(DRUID_ROUTER_URL)
        pinot = PinotHttpSqlClient(PINOT_BROKER_URL)
        results = run_parity(queries, druid=druid, pinot=pinot)

        failed = [r for r in results if not r.passed]
        assert not failed, (
            f"unexpected parity failures:\n"
            + "\n".join(f"  {r.label}: {r.detail}" for r in failed)
        )
        # Sanity: row counts match what we ingested.
        total = next(r for r in results if r.label == "Total event count")
        assert total.druid_value == pageviews_state["row_count"]
        assert total.pinot_value == pageviews_state["row_count"]

    def test_parity_surfaces_divergence(
        self, pageviews_state, druid_datasource_factory,
    ):
        """Inject extra rows into Druid only → parity must fail with a
        clear count divergence (NOT an exception)."""
        # Reuse the factory to bump row count on the Druid side only.
        # The factory is idempotent on the same ``name``, so create a
        # second datasource for divergence so we don't poison the
        # passes-when-data-matches test.
        diverge_ds = "pv_v05_diverge"
        records = [
            {"timestamp": 1_704_067_200_000 + i * 60_000,
             "region": "us-east", "platform": "desktop", "user_id": "u"}
            for i in range(50)
        ]
        druid_datasource_factory(
            name=diverge_ds,
            records=records,
            timestamp_col="timestamp",
            dimensions=["region", "platform", "user_id"],
            metrics=[],
            rollup=False,
        )

        # Point the parity query at the matching pageviews_state Pinot
        # table — it has 120 rows, Druid has 50 → divergence guaranteed.
        spec = json.loads(Path(pageviews_state["spec_path"]).read_text())
        # Override the datasource for the auto-derive so queries hit
        # diverge_ds on the Druid side.
        spec["spec"]["dataSchema"]["dataSource"] = diverge_ds
        parsed = DruidSpecParser().parse(spec)
        canonical = DruidNormalizer().normalize(parsed.parsed_spec).canonical

        queries = derive_queries_from_canonical(
            canonical, pinot_table=pageviews_state["pinot_table"],
        )
        druid = DruidHttpSqlClient(DRUID_ROUTER_URL)
        pinot = PinotHttpSqlClient(PINOT_BROKER_URL)
        results = run_parity(queries, druid=druid, pinot=pinot)

        # The total count check must report Druid=50 vs Pinot=120 →
        # passed=False without raising.
        total = next(r for r in results if r.label == "Total event count")
        assert total.passed is False
        assert total.druid_value == 50
        assert total.pinot_value == 120
