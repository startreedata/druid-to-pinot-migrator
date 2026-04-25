"""
Live Docker integration tests for the Druid → Pinot migration tool.

These tests:
1. Bring up real Druid and Pinot clusters via docker-compose (session scope).
2. Ingest several representative datasets into Druid.
3. Run the migration tool (generate_bundle) to produce Pinot artifacts.
4. Deploy those artifacts to the live Pinot cluster.
5. Ingest the same data into Pinot via the OFFLINE ingest API.
6. Run matched SQL queries on both clusters and assert result equivalence.
7. Verify that the generated Pinot table config contains expected index settings.

Run with:
    LIVE_DOCKER_TESTS=1 python -m pytest tests/docker/ -v --tb=short -s

Skip without the env var (default CI behaviour).

Datasets exercised
──────────────────
DS1  raw_events        plain batch, no rollup, 3 string dims, 0 metrics
DS2  rolled_up_daily   batch with rollup, count + longSum + doubleSum
DS3  typed_dimensions  long + float + double dimension types
DS4  transforms_ds     transformSpec with expression (validated structurally)
DS5  index_check       index/table config parity verification
DS6  determinism       same spec → identical output across two runs
DS7  validation        full validate_spec with generated artifacts
DS8  minmax_metrics    doubleMin/Max/Sum metrics with rollup (query parity)
DS9  hourly_gran       HOUR segment granularity + MINUTE query granularity
DS10 append_mode       appendToExisting=true → INGESTION_BEHAVIOR_MISMATCH risk
DS11 multivalue_dims   MV dimensions → MULTIVALUE_AMBIGUITY risk + schema check
DS12 float_metrics     floatSum/Min/Max → DOUBLE in Pinot schema
DS13 hash_partitioned  hashed partitionsSpec → PARTITIONING_CONFIG_REQUIRED risk
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from tests.docker.cluster_clients import DruidClient, PinotClient
from tests.docker.migration_helper import (
    assert_aggregated_query_matches,
    assert_dimension_values_match,
    assert_row_counts_match,
    build_druid_spec,
    ingest_records_into_pinot,
    migrate_and_deploy,
)

# ─────────────────────────────────────────────────────────────────────────────
# Shared test data — timestamps as epoch-milliseconds.
#
# Pinot's ingestFromFile stores the time column as LONG, so ISO strings cause
# type-conversion errors.  Using epoch-ms integers works with both Druid
# (format="millis") and Pinot (EPOCH|MILLISECONDS format).
#
# Reference:
#   2024-03-01T00:00:00Z = 1709251200000
#   2024-03-01T01:00:00Z = 1709254800000
#   2024-03-02T00:00:00Z = 1709337600000
#   2024-03-02T06:00:00Z = 1709359200000
#   2024-03-03T00:00:00Z = 1709424000000
# ─────────────────────────────────────────────────────────────────────────────

RAW_EVENTS_RECORDS = [
    {"timestamp": 1709251200000, "page": "home",    "user": "alice", "region": "us-east"},
    {"timestamp": 1709254800000, "page": "about",   "user": "bob",   "region": "eu-west"},
    {"timestamp": 1709337600000, "page": "home",    "user": "carol", "region": "us-west"},
    {"timestamp": 1709359200000, "page": "contact", "user": "alice", "region": "us-east"},
    {"timestamp": 1709424000000, "page": "home",    "user": "dave",  "region": "ap-south"},
]

ROLLED_UP_RECORDS = [
    {"timestamp": 1709251200000, "campaign": "spring_sale",     "country": "US", "click_count": 10, "revenue": 25.50},
    {"timestamp": 1709272800000, "campaign": "spring_sale",     "country": "US", "click_count": 5,  "revenue": 12.75},
    {"timestamp": 1709294400000, "campaign": "brand_awareness", "country": "GB", "click_count": 8,  "revenue": 0.0},
    {"timestamp": 1709337600000, "campaign": "spring_sale",     "country": "US", "click_count": 20, "revenue": 55.00},
    {"timestamp": 1709337600000, "campaign": "brand_awareness", "country": "DE", "click_count": 3,  "revenue": 0.0},
    {"timestamp": 1709424000000, "campaign": "spring_sale",     "country": "US", "click_count": 15, "revenue": 37.50},
]

TYPED_DIM_RECORDS = [
    {"timestamp": 1711929600000, "name": "widget_a", "user_id": 101, "price": 9.99,  "score": 4.5},
    {"timestamp": 1711951200000, "name": "widget_b", "user_id": 102, "price": 19.99, "score": 3.8},
    {"timestamp": 1712016000000, "name": "widget_a", "user_id": 103, "price": 9.99,  "score": 4.2},
    {"timestamp": 1712059200000, "name": "widget_c", "user_id": 101, "price": 29.99, "score": 4.9},
]

TRANSFORMS_RECORDS = [
    {"timestamp": 1714521600000, "event_type": "click",    "platform": "mobile",  "duration_ms": 1200},
    {"timestamp": 1714543200000, "event_type": "view",     "platform": "desktop", "duration_ms": 300},
    {"timestamp": 1714608000000, "event_type": "click",    "platform": "mobile",  "duration_ms": 800},
    {"timestamp": 1714651200000, "event_type": "purchase", "platform": "mobile",  "duration_ms": 2000},
]


# ─────────────────────────────────────────────────────────────────────────────
# Session-scoped per-dataset setup fixtures
#
# Each fixture runs once per test session: ingests data into Druid,
# runs the migration tool, deploys to Pinot, ingests data into Pinot.
# Tests receive the shared state dict via a fixture.
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def raw_events_state(druid: DruidClient, pinot: PinotClient,
                     tmp_path_factory) -> dict:
    """Session-scoped state for the raw_events dataset."""
    DS = "live_raw_events"
    out_dir = tmp_path_factory.mktemp("raw_events")

    # Ingest into Druid
    druid.ingest_inline(
        datasource=DS,
        records=RAW_EVENTS_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["page", "user", "region"],
    )
    druid.wait_for_datasource(DS, timeout=180)

    # Generate + deploy to Pinot
    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["page", "user", "region"],
    )
    info = migrate_and_deploy(spec, pinot, out_dir)

    # Ingest data into Pinot
    ingest_records_into_pinot(pinot, DS, RAW_EVENTS_RECORDS, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE",
           "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def rolled_up_state(druid: DruidClient, pinot: PinotClient,
                    tmp_path_factory) -> dict:
    """Session-scoped state for the rolled_up dataset."""
    DS = "live_rolled_up"
    out_dir = tmp_path_factory.mktemp("rolled_up")

    druid_metrics = [
        {"type": "count",     "name": "impressions"},
        {"type": "longSum",   "name": "clicks",  "fieldName": "click_count"},
        {"type": "doubleSum", "name": "revenue",  "fieldName": "revenue"},
    ]
    druid.ingest_inline(
        datasource=DS,
        records=ROLLED_UP_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["campaign", "country"],
        metrics=druid_metrics,
        rollup=True,
        query_granularity="DAY",
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["campaign", "country"],
        metrics=druid_metrics,
        rollup=True,
        query_granularity="DAY",
    )
    info = migrate_and_deploy(spec, pinot, out_dir)

    # Remap field names to match Pinot metric names:
    # 'click_count' (fieldName) → 'clicks' (metric name)
    pinot_records = [
        {**r, "clicks": r["click_count"], "impressions": 1}
        for r in ROLLED_UP_RECORDS
    ]
    ingest_records_into_pinot(pinot, DS, pinot_records, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def typed_dims_state(druid: DruidClient, pinot: PinotClient,
                     tmp_path_factory) -> dict:
    """Session-scoped state for the typed_dimensions dataset."""
    DS = "live_typed_dims"
    out_dir = tmp_path_factory.mktemp("typed_dims")

    druid_dims = [
        "name",
        {"type": "long",   "name": "user_id"},
        {"type": "float",  "name": "price"},
        {"type": "double", "name": "score"},
    ]
    druid.ingest_inline(
        datasource=DS,
        records=TYPED_DIM_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=druid_dims,
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=druid_dims,
    )
    info = migrate_and_deploy(spec, pinot, out_dir)

    ingest_records_into_pinot(pinot, DS, TYPED_DIM_RECORDS, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def transforms_state(druid: DruidClient, pinot: PinotClient,
                     tmp_path_factory) -> dict:
    """Session-scoped state for the transforms dataset."""
    DS = "live_transforms"
    out_dir = tmp_path_factory.mktemp("transforms")

    druid_metrics = [
        {"type": "count",     "name": "event_count"},
        {"type": "doubleSum", "name": "duration_sum", "fieldName": "duration_ms"},
    ]
    druid.ingest_inline(
        datasource=DS,
        records=TRANSFORMS_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["event_type", "platform"],
        metrics=druid_metrics,
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["event_type", "platform"],
        metrics=druid_metrics,
    )
    spec["spec"]["dataSchema"]["transformSpec"] = {
        "transforms": [{
            "type": "expression",
            "name": "event_category",
            "expression": "concat(event_type, '_', platform)",
        }]
    }
    info = migrate_and_deploy(spec, pinot, out_dir)

    ingest_records_into_pinot(pinot, DS, TRANSFORMS_RECORDS, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def index_check_state(druid: DruidClient, pinot: PinotClient,
                      tmp_path_factory) -> dict:
    """Session-scoped state for the index_check dataset."""
    DS = "live_index_check"
    out_dir = tmp_path_factory.mktemp("index_check")

    druid.ingest_inline(
        datasource=DS,
        records=RAW_EVENTS_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["page", "user", "region"],
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["page", "user", "region"],
    )
    info = migrate_and_deploy(spec, pinot, out_dir)

    ingest_records_into_pinot(pinot, DS, RAW_EVENTS_RECORDS, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def validation_state(druid: DruidClient, pinot: PinotClient,
                     tmp_path_factory) -> dict:
    """Session-scoped state for the validation dataset."""
    DS = "live_validation"
    out_dir = tmp_path_factory.mktemp("validation")

    druid.ingest_inline(
        datasource=DS,
        records=RAW_EVENTS_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["page", "user", "region"],
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["page", "user", "region"],
    )
    info = migrate_and_deploy(spec, pinot, out_dir)
    spec_path = str(info["spec_path"])

    ingest_records_into_pinot(pinot, DS, RAW_EVENTS_RECORDS, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE",
           "spec_path": spec_path, "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


# ─────────────────────────────────────────────────────────────────────────────
# DS1 — Raw events (no rollup, no metrics)
# ─────────────────────────────────────────────────────────────────────────────

class TestRawEventsMigration:
    def test_schema_has_correct_name(self, raw_events_state):
        assert raw_events_state["schema"]["schemaName"] == raw_events_state["ds"]

    def test_schema_has_three_dimensions(self, raw_events_state):
        assert len(raw_events_state["schema"]["dimensionFieldSpecs"]) == 3

    def test_schema_dimensions_are_strings(self, raw_events_state):
        for f in raw_events_state["schema"]["dimensionFieldSpecs"]:
            assert f["dataType"] == "STRING", f"Expected STRING for {f['name']}"

    def test_schema_has_no_metrics(self, raw_events_state):
        assert raw_events_state["schema"]["metricFieldSpecs"] == []

    def test_table_is_offline(self, raw_events_state):
        assert raw_events_state["table"]["tableType"] == "OFFLINE"

    def test_table_name_matches(self, raw_events_state):
        assert raw_events_state["table"]["tableName"] == raw_events_state["pinot_table"]

    def test_table_time_column_matches_schema(self, raw_events_state):
        schema_time = raw_events_state["schema"]["dateTimeFieldSpecs"][0]["name"]
        table_time = raw_events_state["table"]["segmentsConfig"]["timeColumnName"]
        assert schema_time == table_time

    def test_row_count_matches(self, raw_events_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot,
            raw_events_state["ds"], raw_events_state["pinot_table"]
        )

    def test_page_dimension_values_match(self, raw_events_state, druid, pinot):
        # Use GROUP BY instead of DISTINCT to avoid Pinot multi-stage engine requirement
        ds = raw_events_state["ds"]
        tbl = raw_events_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT page, COUNT(*) AS cnt FROM "{ds}" GROUP BY page ORDER BY page',
            pinot_sql=f"SELECT page, COUNT(*) AS cnt FROM {tbl} GROUP BY page ORDER BY page",
        )

    def test_region_dimension_values_match(self, raw_events_state, druid, pinot):
        # Use GROUP BY instead of DISTINCT to avoid Pinot multi-stage engine requirement
        ds = raw_events_state["ds"]
        tbl = raw_events_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT region, COUNT(*) AS cnt FROM "{ds}" GROUP BY region ORDER BY region',
            pinot_sql=f"SELECT region, COUNT(*) AS cnt FROM {tbl} GROUP BY region ORDER BY region",
        )

    def test_count_by_region_matches(self, raw_events_state, druid, pinot):
        ds = raw_events_state["ds"]
        tbl = raw_events_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT region, COUNT(*) AS cnt FROM "{ds}" GROUP BY region ORDER BY region',
            pinot_sql=f"SELECT region, COUNT(*) AS cnt FROM {tbl} GROUP BY region ORDER BY region",
        )

    def test_count_by_page_matches(self, raw_events_state, druid, pinot):
        ds = raw_events_state["ds"]
        tbl = raw_events_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT page, COUNT(*) AS cnt FROM "{ds}" GROUP BY page ORDER BY page',
            pinot_sql=f"SELECT page, COUNT(*) AS cnt FROM {tbl} GROUP BY page ORDER BY page",
        )

    def test_count_per_user_matches(self, raw_events_state, druid, pinot):
        ds = raw_events_state["ds"]
        tbl = raw_events_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT "user", COUNT(*) AS cnt FROM "{ds}" GROUP BY "user" ORDER BY "user"',
            pinot_sql=f'SELECT "user", COUNT(*) AS cnt FROM {tbl} GROUP BY "user" ORDER BY "user"',
        )

    def test_pinot_table_exists_in_controller(self, raw_events_state, pinot):
        # list_tables() returns base names without type suffix (e.g. "live_raw_events")
        assert raw_events_state["ds"] in pinot.list_tables()

    def test_pinot_has_at_least_one_segment(self, raw_events_state, pinot):
        count = pinot.get_segments_count(raw_events_state["pinot_table"])
        assert count >= 1


# ─────────────────────────────────────────────────────────────────────────────
# DS2 — Rolled-up additive metrics
# ─────────────────────────────────────────────────────────────────────────────

class TestRolledUpMigration:
    def test_schema_has_metric_fields(self, rolled_up_state):
        names = {f["name"] for f in rolled_up_state["schema"]["metricFieldSpecs"]}
        assert {"impressions", "clicks", "revenue"}.issubset(names)

    def test_impressions_is_long(self, rolled_up_state):
        by_name = {f["name"]: f["dataType"] for f in rolled_up_state["schema"]["metricFieldSpecs"]}
        assert by_name["impressions"] == "LONG"

    def test_clicks_is_long(self, rolled_up_state):
        by_name = {f["name"]: f["dataType"] for f in rolled_up_state["schema"]["metricFieldSpecs"]}
        assert by_name["clicks"] == "LONG"

    def test_revenue_is_double(self, rolled_up_state):
        by_name = {f["name"]: f["dataType"] for f in rolled_up_state["schema"]["metricFieldSpecs"]}
        assert by_name["revenue"] == "DOUBLE"

    def test_table_is_offline(self, rolled_up_state):
        assert rolled_up_state["table"]["tableType"] == "OFFLINE"

    def test_migration_report_has_rollup_risk(self, rolled_up_state):
        risks_path = rolled_up_state["out_dir"] / "reports" / "risks.json"
        risks_data = json.loads(risks_path.read_text())
        risk_ids = [r["risk_id"] for r in risks_data["risks"]]
        assert "ROLLUP_SEMANTIC_MISMATCH" in risk_ids

    def test_total_clicks_match(self, rolled_up_state, druid, pinot):
        # After Druid rollup: metric name is 'clicks' (not fieldName 'click_count')
        ds = rolled_up_state["ds"]
        tbl = rolled_up_state["pinot_table"]
        druid_rows = druid.sql_query(f'SELECT SUM(clicks) AS tc FROM "{ds}"')
        pinot_rows = pinot.sql_query(f"SELECT SUM(clicks) AS tc FROM {tbl}")
        assert druid_rows[0]["tc"] == pinot_rows[0]["tc"]

    def test_total_revenue_match(self, rolled_up_state, druid, pinot):
        ds = rolled_up_state["ds"]
        tbl = rolled_up_state["pinot_table"]
        # Avoid ROUND(x, 2) — Pinot 1.4 rounds to nearest multiple, not decimal places
        druid_rows = druid.sql_query(f'SELECT SUM(revenue) AS rev FROM "{ds}"')
        pinot_rows = pinot.sql_query(f"SELECT SUM(revenue) AS rev FROM {tbl}")
        assert abs(float(druid_rows[0]["rev"]) - float(pinot_rows[0]["rev"])) < 0.01

    def test_group_by_campaign_clicks_sum_matches(self, rolled_up_state, druid, pinot):
        # After Druid rollup, rows merge by (day, campaign, country) but SUM(clicks) is preserved.
        # Both Druid and Pinot should return the same SUM regardless of row count differences.
        ds = rolled_up_state["ds"]
        tbl = rolled_up_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT campaign, SUM(clicks) AS tc FROM "{ds}" GROUP BY campaign ORDER BY campaign',
            pinot_sql=f"SELECT campaign, SUM(clicks) AS tc FROM {tbl} GROUP BY campaign ORDER BY campaign",
        )

    def test_group_by_country_revenue_matches(self, rolled_up_state, druid, pinot):
        ds = rolled_up_state["ds"]
        tbl = rolled_up_state["pinot_table"]
        # Avoid ROUND(x, 2) — Pinot 1.4 rounds to nearest multiple, not decimal places
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT country, SUM(revenue) AS rev FROM "{ds}" GROUP BY country ORDER BY country',
            pinot_sql=f"SELECT country, SUM(revenue) AS rev FROM {tbl} GROUP BY country ORDER BY country",
        )

    def test_campaign_country_clicks_match(self, rolled_up_state, druid, pinot):
        # After Druid rollup: metric name is 'clicks' (not fieldName 'click_count').
        # SUM is used so results match regardless of how many rows Druid merged.
        ds = rolled_up_state["ds"]
        tbl = rolled_up_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT campaign, country, SUM(clicks) AS clicks FROM "{ds}" GROUP BY campaign, country ORDER BY campaign, country',
            pinot_sql=f"SELECT campaign, country, SUM(clicks) AS clicks FROM {tbl} GROUP BY campaign, country ORDER BY campaign, country",
        )


# ─────────────────────────────────────────────────────────────────────────────
# DS3 — Typed dimensions (long, float, double)
# ─────────────────────────────────────────────────────────────────────────────

class TestTypedDimensionsMigration:
    def test_user_id_is_long(self, typed_dims_state):
        by_name = {f["name"]: f["dataType"] for f in typed_dims_state["schema"]["dimensionFieldSpecs"]}
        assert by_name["user_id"] == "LONG"

    def test_price_is_float(self, typed_dims_state):
        by_name = {f["name"]: f["dataType"] for f in typed_dims_state["schema"]["dimensionFieldSpecs"]}
        assert by_name["price"] == "FLOAT"

    def test_score_is_double(self, typed_dims_state):
        by_name = {f["name"]: f["dataType"] for f in typed_dims_state["schema"]["dimensionFieldSpecs"]}
        assert by_name["score"] == "DOUBLE"

    def test_name_is_string(self, typed_dims_state):
        by_name = {f["name"]: f["dataType"] for f in typed_dims_state["schema"]["dimensionFieldSpecs"]}
        assert by_name["name"] == "STRING"

    def test_row_count_matches(self, typed_dims_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot, typed_dims_state["ds"], typed_dims_state["pinot_table"]
        )

    def test_avg_price_by_name_matches(self, typed_dims_state, druid, pinot):
        # Avoid ROUND(x, n) — Pinot 1.4 interprets n as a rounding interval, not decimal places
        ds = typed_dims_state["ds"]
        tbl = typed_dims_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT name, AVG(price) AS avg_price FROM "{ds}" GROUP BY name ORDER BY name',
            pinot_sql=f"SELECT name, AVG(price) AS avg_price FROM {tbl} GROUP BY name ORDER BY name",
        )

    def test_max_score_by_name_matches(self, typed_dims_state, druid, pinot):
        ds = typed_dims_state["ds"]
        tbl = typed_dims_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT name, MAX(score) AS max_score FROM "{ds}" GROUP BY name ORDER BY name',
            pinot_sql=f"SELECT name, MAX(score) AS max_score FROM {tbl} GROUP BY name ORDER BY name",
        )

    def test_distinct_user_ids_match(self, typed_dims_state, druid, pinot):
        # Use GROUP BY instead of DISTINCT to avoid Pinot multi-stage engine requirement
        ds = typed_dims_state["ds"]
        tbl = typed_dims_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT user_id, COUNT(*) AS cnt FROM "{ds}" GROUP BY user_id ORDER BY user_id',
            pinot_sql=f"SELECT user_id, COUNT(*) AS cnt FROM {tbl} GROUP BY user_id ORDER BY user_id",
        )


# ─────────────────────────────────────────────────────────────────────────────
# DS4 — TransformSpec (structure + risk validated; query parity on raw cols)
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformsMigration:
    def test_transform_portability_risk_emitted(self, transforms_state):
        risks_path = transforms_state["out_dir"] / "reports" / "risks.json"
        risks_data = json.loads(risks_path.read_text())
        risk_ids = [r["risk_id"] for r in risks_data["risks"]]
        assert "TRANSFORM_PORTABILITY_RISK" in risk_ids

    def test_risks_json_mentions_transforms(self, transforms_state):
        # Transform risk is emitted in risks.json (not warnings.json)
        risks_path = transforms_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "TRANSFORM_PORTABILITY_RISK" in risk_ids

    def test_schema_includes_event_type_dimension(self, transforms_state):
        names = {f["name"] for f in transforms_state["schema"]["dimensionFieldSpecs"]}
        assert "event_type" in names

    def test_schema_includes_platform_dimension(self, transforms_state):
        names = {f["name"] for f in transforms_state["schema"]["dimensionFieldSpecs"]}
        assert "platform" in names

    def test_generate_result_has_no_errors(self, transforms_state):
        assert transforms_state["generate_result"].errors == []

    def test_row_count_matches(self, transforms_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot, transforms_state["ds"], transforms_state["pinot_table"]
        )

    def test_count_by_platform_matches(self, transforms_state, druid, pinot):
        ds = transforms_state["ds"]
        tbl = transforms_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT platform, COUNT(*) AS cnt FROM "{ds}" GROUP BY platform ORDER BY platform',
            pinot_sql=f"SELECT platform, COUNT(*) AS cnt FROM {tbl} GROUP BY platform ORDER BY platform",
        )

    def test_row_count_by_event_type_matches(self, transforms_state, druid, pinot):
        # Test COUNT(*) by event_type — works on both systems without metric name mapping issues.
        # Note: Druid stores metrics by their 'name' (not fieldName), so raw records
        # ingested into Pinot use source field names, not the Druid metric names.
        ds = transforms_state["ds"]
        tbl = transforms_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT event_type, COUNT(*) AS cnt FROM "{ds}" GROUP BY event_type ORDER BY event_type',
            pinot_sql=f"SELECT event_type, COUNT(*) AS cnt FROM {tbl} GROUP BY event_type ORDER BY event_type",
        )


# ─────────────────────────────────────────────────────────────────────────────
# DS5 — Index / table config parity checks
# ─────────────────────────────────────────────────────────────────────────────

class TestIndexConfigParity:
    def test_generated_table_has_load_mode(self, index_check_state):
        load_mode = index_check_state["table"].get("tableIndexConfig", {}).get("loadMode")
        assert load_mode == "MMAP", f"Expected MMAP, got {load_mode}"

    def test_live_pinot_table_has_load_mode(self, index_check_state, pinot):
        idx_cfg = pinot.get_indexes(index_check_state["pinot_table"])
        load_mode = idx_cfg.get("loadMode")
        assert load_mode == "MMAP", f"Live Pinot loadMode={load_mode}"

    def test_generated_table_has_retention(self, index_check_state):
        seg_cfg = index_check_state["table"].get("segmentsConfig", {})
        assert "retentionTimeUnit" in seg_cfg
        assert "retentionTimeValue" in seg_cfg

    def test_generated_table_has_tenants(self, index_check_state):
        tenants = index_check_state["table"].get("tenants", {})
        assert tenants.get("broker") == "DefaultTenant"
        assert tenants.get("server") == "DefaultTenant"

    def test_live_pinot_table_exists(self, index_check_state, pinot):
        # list_tables() returns base names without type suffix
        assert index_check_state["ds"] in pinot.list_tables()

    def test_live_pinot_schema_name_matches(self, index_check_state, pinot):
        live_schema = pinot.get_schema(index_check_state["ds"])
        assert live_schema["schemaName"] == index_check_state["ds"]

    def test_live_pinot_schema_has_all_dimensions(self, index_check_state, pinot):
        live_schema = pinot.get_schema(index_check_state["ds"])
        live_dims = {f["name"] for f in live_schema.get("dimensionFieldSpecs", [])}
        generated_dims = {f["name"] for f in index_check_state["schema"]["dimensionFieldSpecs"]}
        assert live_dims == generated_dims

    def test_live_pinot_schema_time_col_matches_generated(self, index_check_state, pinot):
        live_schema = pinot.get_schema(index_check_state["ds"])
        live_dt = live_schema.get("dateTimeFieldSpecs", [{}])[0].get("name")
        generated_dt = index_check_state["schema"]["dateTimeFieldSpecs"][0]["name"]
        assert live_dt == generated_dt

    def test_live_pinot_has_segments(self, index_check_state, pinot):
        count = pinot.get_segments_count(index_check_state["pinot_table"])
        assert count >= 1, "No segments found in Pinot table"


# ─────────────────────────────────────────────────────────────────────────────
# DS6 — Cross-datasource determinism: same spec → identical artifacts twice
# ─────────────────────────────────────────────────────────────────────────────

class TestDeterministicOutput:
    def test_schema_output_is_identical_across_runs(self, tmp_path_factory):
        from migrator.translators.pipeline import generate_bundle

        spec = build_druid_spec(
            datasource="determinism_test",
            timestamp_col="timestamp",
            dimensions=["a", "c", "b"],  # intentionally unsorted
            metrics=[
                {"type": "count",     "name": "cnt"},
                {"type": "doubleSum", "name": "total", "fieldName": "val"},
            ],
        )
        out1 = tmp_path_factory.mktemp("det1")
        out2 = tmp_path_factory.mktemp("det2")

        for out in (out1, out2):
            spec_path = out / "spec.json"
            spec_path.write_text(json.dumps(spec))
            generate_bundle(str(spec_path), out_dir=str(out))

        assert (out1 / "schema.json").read_text() == (out2 / "schema.json").read_text()
        assert (out1 / "table-offline.json").read_text() == (out2 / "table-offline.json").read_text()

    def test_migration_report_is_deterministic(self, tmp_path_factory):
        from migrator.translators.pipeline import generate_bundle

        spec = build_druid_spec(
            datasource="report_det_test",
            dimensions=["x", "y"],
        )
        out1 = tmp_path_factory.mktemp("rdet1")
        out2 = tmp_path_factory.mktemp("rdet2")

        for out in (out1, out2):
            sp = out / "spec.json"
            sp.write_text(json.dumps(spec))
            generate_bundle(str(sp), out_dir=str(out))

        r1 = json.loads((out1 / "reports" / "risks.json").read_text())
        r2 = json.loads((out2 / "reports" / "risks.json").read_text())
        assert r1 == r2


# ─────────────────────────────────────────────────────────────────────────────
# DS7 — Migration validation report end-to-end
# ─────────────────────────────────────────────────────────────────────────────

class TestValidationReportLive:
    def test_validate_spec_with_generated_artifacts_passes(self, validation_state):
        from migrator.translators.pipeline import validate_spec
        result = validate_spec(validation_state["spec_path"],
                               generated_dir=str(validation_state["out_dir"]))
        assert result.success, (
            "validate_spec failed: "
            + "; ".join(c.message for c in result.report.checks if c.status == "fail")
        )

    def test_validation_report_datasource_name_correct(self, validation_state):
        from migrator.translators.pipeline import validate_spec
        result = validate_spec(validation_state["spec_path"],
                               generated_dir=str(validation_state["out_dir"]))
        assert result.report.datasource_name == validation_state["ds"]

    def test_confidence_score_is_high_for_clean_spec(self, validation_state):
        from migrator.translators.pipeline import validate_spec
        result = validate_spec(validation_state["spec_path"],
                               generated_dir=str(validation_state["out_dir"]))
        assert result.report.confidence_score >= 0.9, (
            f"Expected confidence >= 0.9, got {result.report.confidence_score}"
        )

    def test_no_fail_checks_in_report(self, validation_state):
        from migrator.translators.pipeline import validate_spec
        result = validate_spec(validation_state["spec_path"],
                               generated_dir=str(validation_state["out_dir"]))
        failed = [c for c in result.report.checks if c.status == "fail"]
        assert failed == [], f"Unexpected failing checks: {[c.message for c in failed]}"

    def test_live_pinot_row_count_matches_druid(self, validation_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot, validation_state["ds"], validation_state["pinot_table"]
        )


# ─────────────────────────────────────────────────────────────────────────────
# Records for new datasets
# ─────────────────────────────────────────────────────────────────────────────

# DS8 — min/max/sum metrics with rollup
#   product_id × seller_id pairs ingested at minute granularity
#   Pinot receives pre-mapped field names (price_min/max/sum, qty_min/max/total)
MINMAX_RECORDS = [
    {"timestamp": 1709251200000, "product_id": "P001", "seller_id": "S10",
     "price": 9.99,  "quantity": 5},
    {"timestamp": 1709251500000, "product_id": "P001", "seller_id": "S10",
     "price": 10.50, "quantity": 3},
    {"timestamp": 1709254800000, "product_id": "P002", "seller_id": "S20",
     "price": 24.99, "quantity": 10},
    {"timestamp": 1709337600000, "product_id": "P001", "seller_id": "S11",
     "price": 8.75,  "quantity": 7},
    {"timestamp": 1709337600000, "product_id": "P002", "seller_id": "S20",
     "price": 23.50, "quantity": 4},
]

# DS9 — HOUR segment granularity, MINUTE query granularity
HOURLY_GRAN_RECORDS = [
    {"timestamp": 1709251200000, "sensor": "T001", "region": "north", "reading": 22.5},
    {"timestamp": 1709251260000, "sensor": "T001", "region": "north", "reading": 22.8},
    {"timestamp": 1709254800000, "sensor": "T002", "region": "south", "reading": 18.3},
    {"timestamp": 1709258400000, "sensor": "T001", "region": "north", "reading": 23.1},
    {"timestamp": 1709337600000, "sensor": "T002", "region": "south", "reading": 19.0},
]

# DS10 — appendToExisting=true (structural/risk test only; same records as raw_events)
APPEND_MODE_RECORDS = [
    {"timestamp": 1709251200000, "entity": "order",   "action": "create", "actor": "user_1"},
    {"timestamp": 1709337600000, "entity": "order",   "action": "update", "actor": "user_2"},
    {"timestamp": 1709424000000, "entity": "product",  "action": "create", "actor": "admin"},
]

# DS11 — multi-value dimensions
#   tags field contains pipe-delimited string to simulate MV; actual MV ingest
#   requires special Druid config so we test schema/risk, not query parity.
MULTIVALUE_RECORDS = [
    {"timestamp": 1709251200000, "content_id": "C001", "author": "alice",
     "tags": "python", "language": "en"},
    {"timestamp": 1709337600000, "content_id": "C002", "author": "bob",
     "tags": "java",   "language": "en"},
    {"timestamp": 1709424000000, "content_id": "C003", "author": "alice",
     "tags": "python", "language": "fr"},
]

# DS12 — floatSum / floatMin / floatMax metrics
FLOAT_METRICS_RECORDS = [
    {"timestamp": 1709251200000, "category": "A", "score": 4.5,  "weight": 1.0},
    {"timestamp": 1709337600000, "category": "A", "score": 3.8,  "weight": 2.0},
    {"timestamp": 1709424000000, "category": "B", "score": 4.9,  "weight": 1.5},
    {"timestamp": 1709510400000, "category": "B", "score": 2.1,  "weight": 0.5},
]

# DS13 — hashed partitionsSpec risk
HASH_PARTITIONED_RECORDS = [
    {"timestamp": 1709251200000, "customer_id": "CU01", "product_id": "PR10",
     "status": "completed", "amount": 150.0},
    {"timestamp": 1709337600000, "customer_id": "CU02", "product_id": "PR20",
     "status": "pending",   "amount": 75.0},
    {"timestamp": 1709424000000, "customer_id": "CU01", "product_id": "PR30",
     "status": "completed", "amount": 200.0},
]


# ─────────────────────────────────────────────────────────────────────────────
# Session-scoped fixtures for DS8–DS13
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def minmax_state(druid: DruidClient, pinot: PinotClient, tmp_path_factory) -> dict:
    """DS8 — doubleMin/Max/Sum + longMin/Max/Sum with rollup=true."""
    DS = "live_minmax"
    out_dir = tmp_path_factory.mktemp("minmax")

    druid_metrics = [
        {"type": "count",     "name": "price_updates"},
        {"type": "doubleMin", "name": "price_min",  "fieldName": "price"},
        {"type": "doubleMax", "name": "price_max",  "fieldName": "price"},
        {"type": "doubleSum", "name": "price_sum",  "fieldName": "price"},
        {"type": "longMin",   "name": "qty_min",    "fieldName": "quantity"},
        {"type": "longMax",   "name": "qty_max",    "fieldName": "quantity"},
        {"type": "longSum",   "name": "qty_total",  "fieldName": "quantity"},
    ]
    druid.ingest_inline(
        datasource=DS,
        records=MINMAX_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["product_id", "seller_id"],
        metrics=druid_metrics,
        rollup=True,
        query_granularity="DAY",
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["product_id", "seller_id"],
        metrics=druid_metrics,
        rollup=True,
        query_granularity="DAY",
    )
    info = migrate_and_deploy(spec, pinot, out_dir)

    # Pinot receives per-row data; map field names → metric names for ingestFromFile
    pinot_records = [
        {
            "timestamp": r["timestamp"],
            "product_id": r["product_id"],
            "seller_id":  r["seller_id"],
            "price_updates": 1,
            "price_min":  r["price"],
            "price_max":  r["price"],
            "price_sum":  r["price"],
            "qty_min":    r["quantity"],
            "qty_max":    r["quantity"],
            "qty_total":  r["quantity"],
        }
        for r in MINMAX_RECORDS
    ]
    ingest_records_into_pinot(pinot, DS, pinot_records, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def hourly_gran_state(druid: DruidClient, pinot: PinotClient, tmp_path_factory) -> dict:
    """DS9 — HOUR segment granularity, MINUTE query granularity."""
    DS = "live_hourly_gran"
    out_dir = tmp_path_factory.mktemp("hourly_gran")

    druid_metrics = [
        {"type": "count",     "name": "reading_count"},
        {"type": "doubleSum", "name": "reading_sum", "fieldName": "reading"},
    ]
    druid.ingest_inline(
        datasource=DS,
        records=HOURLY_GRAN_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["sensor", "region"],
        metrics=druid_metrics,
        rollup=False,
        segment_granularity="HOUR",
        query_granularity="MINUTE",
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["sensor", "region"],
        metrics=druid_metrics,
        rollup=False,
        segment_granularity="HOUR",
        query_granularity="MINUTE",
    )
    info = migrate_and_deploy(spec, pinot, out_dir)

    # Pinot schema has metric columns reading_count / reading_sum (not 'reading'),
    # so map source field 'reading' to those names before ingesting.
    pinot_records = [
        {
            "timestamp": r["timestamp"],
            "sensor": r["sensor"],
            "region": r["region"],
            "reading_count": 1,
            "reading_sum": r["reading"],
        }
        for r in HOURLY_GRAN_RECORDS
    ]
    ingest_records_into_pinot(pinot, DS, pinot_records, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def append_mode_state(druid: DruidClient, pinot: PinotClient, tmp_path_factory) -> dict:
    """DS10 — appendToExisting=true spec; validates risk detection end-to-end."""
    DS = "live_append_mode"
    out_dir = tmp_path_factory.mktemp("append_mode")

    druid.ingest_inline(
        datasource=DS,
        records=APPEND_MODE_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["entity", "action", "actor"],
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["entity", "action", "actor"],
    )
    # Inject appendToExisting=true into the spec
    spec["spec"]["ioConfig"]["appendToExisting"] = True
    info = migrate_and_deploy(spec, pinot, out_dir)

    ingest_records_into_pinot(pinot, DS, APPEND_MODE_RECORDS, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def multivalue_dims_state(druid: DruidClient, pinot: PinotClient, tmp_path_factory) -> dict:
    """DS11 — MV dimension spec; validates risk detection + schema correctness."""
    DS = "live_multivalue_dims"
    out_dir = tmp_path_factory.mktemp("multivalue_dims")

    druid.ingest_inline(
        datasource=DS,
        records=MULTIVALUE_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["content_id", "author", "tags", "language"],
    )
    druid.wait_for_datasource(DS, timeout=180)

    # Build spec with MV dimension declaration
    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=[
            "content_id",
            "author",
            {"type": "string", "name": "tags",
             "multiValueHandling": "SORTED_ARRAY"},
            "language",
        ],
    )
    info = migrate_and_deploy(spec, pinot, out_dir)

    ingest_records_into_pinot(pinot, DS, MULTIVALUE_RECORDS, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def float_metrics_state(druid: DruidClient, pinot: PinotClient, tmp_path_factory) -> dict:
    """DS12 — floatSum/Min/Max metrics map to DOUBLE in Pinot schema."""
    DS = "live_float_metrics"
    out_dir = tmp_path_factory.mktemp("float_metrics")

    druid_metrics = [
        {"type": "count",    "name": "event_count"},
        {"type": "floatSum", "name": "score_sum",  "fieldName": "score"},
        {"type": "floatMin", "name": "score_min",  "fieldName": "score"},
        {"type": "floatMax", "name": "score_max",  "fieldName": "score"},
        {"type": "floatSum", "name": "weight_sum", "fieldName": "weight"},
    ]
    druid.ingest_inline(
        datasource=DS,
        records=FLOAT_METRICS_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["category"],
        metrics=druid_metrics,
        rollup=False,
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["category"],
        metrics=druid_metrics,
        rollup=False,
    )
    info = migrate_and_deploy(spec, pinot, out_dir)

    # Pinot schema metric columns: event_count, score_sum, score_min, score_max,
    # weight_sum (not raw 'score' / 'weight'). Map before ingesting.
    pinot_records = [
        {
            "timestamp": r["timestamp"],
            "category": r["category"],
            "event_count": 1,
            "score_sum": r["score"],
            "score_min": r["score"],
            "score_max": r["score"],
            "weight_sum": r["weight"],
        }
        for r in FLOAT_METRICS_RECORDS
    ]
    ingest_records_into_pinot(pinot, DS, pinot_records, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


@pytest.fixture(scope="session")
def hash_partitioned_state(druid: DruidClient, pinot: PinotClient, tmp_path_factory) -> dict:
    """DS13 — hashed partitionsSpec → PARTITIONING_CONFIG_REQUIRED risk detected."""
    DS = "live_hash_partitioned"
    out_dir = tmp_path_factory.mktemp("hash_partitioned")

    druid_metrics = [
        {"type": "count",     "name": "order_count"},
        {"type": "doubleSum", "name": "total_amount", "fieldName": "amount"},
    ]
    druid.ingest_inline(
        datasource=DS,
        records=HASH_PARTITIONED_RECORDS,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["customer_id", "product_id", "status"],
        metrics=druid_metrics,
        rollup=False,
    )
    druid.wait_for_datasource(DS, timeout=180)

    spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["customer_id", "product_id", "status"],
        metrics=druid_metrics,
        rollup=False,
    )
    # Inject partitionsSpec into tuningConfig
    spec["spec"]["tuningConfig"] = {
        "type": "index_parallel",
        "partitionsSpec": {
            "type": "hashed",
            "numShards": 4,
            "partitionDimensions": ["customer_id", "product_id"],
        },
    }
    info = migrate_and_deploy(spec, pinot, out_dir)

    # Pinot schema metric columns: order_count, total_amount (not 'amount').
    pinot_records = [
        {
            "timestamp": r["timestamp"],
            "customer_id": r["customer_id"],
            "product_id": r["product_id"],
            "status": r["status"],
            "order_count": 1,
            "total_amount": r["amount"],
        }
        for r in HASH_PARTITIONED_RECORDS
    ]
    ingest_records_into_pinot(pinot, DS, pinot_records, out_dir)
    pinot.wait_for_table_queryable(f"{DS}_OFFLINE", timeout=120)

    yield {**info, "ds": DS, "pinot_table": f"{DS}_OFFLINE", "out_dir": out_dir}

    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


# ─────────────────────────────────────────────────────────────────────────────
# DS8 — MinMax metrics with rollup
# ─────────────────────────────────────────────────────────────────────────────

class TestMinmaxMetricsMigration:
    def test_schema_has_minmax_metrics(self, minmax_state):
        names = {f["name"] for f in minmax_state["schema"]["metricFieldSpecs"]}
        expected = {"price_updates", "price_min", "price_max", "price_sum",
                    "qty_min", "qty_max", "qty_total"}
        assert expected == names

    def test_double_metrics_are_double(self, minmax_state):
        by_name = {f["name"]: f["dataType"] for f in minmax_state["schema"]["metricFieldSpecs"]}
        assert by_name["price_min"] == "DOUBLE"
        assert by_name["price_max"] == "DOUBLE"
        assert by_name["price_sum"] == "DOUBLE"

    def test_long_metrics_are_long(self, minmax_state):
        by_name = {f["name"]: f["dataType"] for f in minmax_state["schema"]["metricFieldSpecs"]}
        assert by_name["qty_min"] == "LONG"
        assert by_name["qty_max"] == "LONG"
        assert by_name["qty_total"] == "LONG"

    def test_rollup_risk_emitted(self, minmax_state):
        risks_path = minmax_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        assert any(r["risk_id"] == "ROLLUP_SEMANTIC_MISMATCH" for r in risks)

    def test_table_is_offline(self, minmax_state):
        assert minmax_state["table"]["tableType"] == "OFFLINE"

    def test_total_price_sum_matches(self, minmax_state, druid, pinot):
        ds = minmax_state["ds"]
        tbl = minmax_state["pinot_table"]
        druid_rows = druid.sql_query(f'SELECT SUM(price_sum) AS ps FROM "{ds}"')
        pinot_rows = pinot.sql_query(f"SELECT SUM(price_sum) AS ps FROM {tbl}")
        assert abs(float(druid_rows[0]["ps"]) - float(pinot_rows[0]["ps"])) < 0.01

    def test_min_price_by_product_matches(self, minmax_state, druid, pinot):
        ds = minmax_state["ds"]
        tbl = minmax_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT product_id, MIN(price_min) AS mp FROM "{ds}" '
                      f'GROUP BY product_id ORDER BY product_id',
            pinot_sql=f"SELECT product_id, MIN(price_min) AS mp FROM {tbl} "
                      f"GROUP BY product_id ORDER BY product_id",
        )

    def test_total_qty_sum_matches(self, minmax_state, druid, pinot):
        ds = minmax_state["ds"]
        tbl = minmax_state["pinot_table"]
        druid_rows = druid.sql_query(f'SELECT SUM(qty_total) AS qt FROM "{ds}"')
        pinot_rows = pinot.sql_query(f"SELECT SUM(qty_total) AS qt FROM {tbl}")
        assert druid_rows[0]["qt"] == pinot_rows[0]["qt"]

    def test_live_pinot_table_exists(self, minmax_state, pinot):
        assert minmax_state["ds"] in pinot.list_tables()


# ─────────────────────────────────────────────────────────────────────────────
# DS9 — Hourly granularity
# ─────────────────────────────────────────────────────────────────────────────

class TestHourlyGranularityMigration:
    def test_schema_name_correct(self, hourly_gran_state):
        assert hourly_gran_state["schema"]["schemaName"] == hourly_gran_state["ds"]

    def test_table_is_offline(self, hourly_gran_state):
        assert hourly_gran_state["table"]["tableType"] == "OFFLINE"

    def test_schema_has_reading_sum_metric(self, hourly_gran_state):
        names = {f["name"] for f in hourly_gran_state["schema"]["metricFieldSpecs"]}
        assert "reading_count" in names
        assert "reading_sum" in names

    def test_reading_sum_is_double(self, hourly_gran_state):
        by_name = {f["name"]: f["dataType"]
                   for f in hourly_gran_state["schema"]["metricFieldSpecs"]}
        assert by_name["reading_sum"] == "DOUBLE"

    def test_schema_has_sensor_and_region_dims(self, hourly_gran_state):
        dim_names = {f["name"] for f in hourly_gran_state["schema"]["dimensionFieldSpecs"]}
        assert "sensor" in dim_names
        assert "region" in dim_names

    def test_row_count_matches(self, hourly_gran_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot, hourly_gran_state["ds"], hourly_gran_state["pinot_table"]
        )

    def test_sum_reading_by_sensor_matches(self, hourly_gran_state, druid, pinot):
        ds = hourly_gran_state["ds"]
        tbl = hourly_gran_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT sensor, SUM(reading_sum) AS rs FROM "{ds}" '
                      f'GROUP BY sensor ORDER BY sensor',
            pinot_sql=f"SELECT sensor, SUM(reading_sum) AS rs FROM {tbl} "
                      f"GROUP BY sensor ORDER BY sensor",
        )

    def test_live_pinot_table_exists(self, hourly_gran_state, pinot):
        assert hourly_gran_state["ds"] in pinot.list_tables()

    def test_no_blocking_risks(self, hourly_gran_state):
        risks_path = hourly_gran_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        blocking = [r for r in risks if r["severity"] == "blocking"]
        assert blocking == []


# ─────────────────────────────────────────────────────────────────────────────
# DS10 — appendToExisting=true risk detection
# ─────────────────────────────────────────────────────────────────────────────

class TestAppendModeMigration:
    def test_ingestion_behavior_mismatch_risk_emitted(self, append_mode_state):
        risks_path = append_mode_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "INGESTION_BEHAVIOR_MISMATCH" in risk_ids

    def test_ingestion_behavior_risk_is_info_severity(self, append_mode_state):
        risks_path = append_mode_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk = next(r for r in risks if r["risk_id"] == "INGESTION_BEHAVIOR_MISMATCH")
        assert risk["severity"] == "info"

    def test_no_blocking_risks(self, append_mode_state):
        risks_path = append_mode_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        blocking = [r for r in risks if r["severity"] == "blocking"]
        assert blocking == []

    def test_schema_name_correct(self, append_mode_state):
        assert append_mode_state["schema"]["schemaName"] == append_mode_state["ds"]

    def test_table_is_offline(self, append_mode_state):
        assert append_mode_state["table"]["tableType"] == "OFFLINE"

    def test_row_count_matches(self, append_mode_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot, append_mode_state["ds"], append_mode_state["pinot_table"]
        )

    def test_count_by_entity_matches(self, append_mode_state, druid, pinot):
        ds = append_mode_state["ds"]
        tbl = append_mode_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT entity, COUNT(*) AS cnt FROM "{ds}" '
                      f'GROUP BY entity ORDER BY entity',
            pinot_sql=f"SELECT entity, COUNT(*) AS cnt FROM {tbl} "
                      f"GROUP BY entity ORDER BY entity",
        )

    def test_live_pinot_table_exists(self, append_mode_state, pinot):
        assert append_mode_state["ds"] in pinot.list_tables()


# ─────────────────────────────────────────────────────────────────────────────
# DS11 — Multi-value dimensions risk detection + schema
# ─────────────────────────────────────────────────────────────────────────────

class TestMultivalueDimsMigration:
    def test_multivalue_ambiguity_risk_emitted(self, multivalue_dims_state):
        risks_path = multivalue_dims_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "MULTIVALUE_AMBIGUITY" in risk_ids

    def test_multivalue_risk_is_medium(self, multivalue_dims_state):
        risks_path = multivalue_dims_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk = next(r for r in risks if r["risk_id"] == "MULTIVALUE_AMBIGUITY")
        assert risk["severity"] == "medium"

    def test_multivalue_evidence_mentions_tags(self, multivalue_dims_state):
        risks_path = multivalue_dims_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk = next(r for r in risks if r["risk_id"] == "MULTIVALUE_AMBIGUITY")
        assert "tags" in " ".join(risk["evidence"])

    def test_schema_has_all_dimensions(self, multivalue_dims_state):
        dim_names = {f["name"] for f in multivalue_dims_state["schema"]["dimensionFieldSpecs"]}
        assert {"content_id", "author", "tags", "language"} == dim_names

    def test_table_is_offline(self, multivalue_dims_state):
        assert multivalue_dims_state["table"]["tableType"] == "OFFLINE"

    def test_row_count_matches(self, multivalue_dims_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot,
            multivalue_dims_state["ds"], multivalue_dims_state["pinot_table"]
        )

    def test_count_by_author_matches(self, multivalue_dims_state, druid, pinot):
        ds = multivalue_dims_state["ds"]
        tbl = multivalue_dims_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT author, COUNT(*) AS cnt FROM "{ds}" '
                      f'GROUP BY author ORDER BY author',
            pinot_sql=f"SELECT author, COUNT(*) AS cnt FROM {tbl} "
                      f"GROUP BY author ORDER BY author",
        )

    def test_live_pinot_table_exists(self, multivalue_dims_state, pinot):
        assert multivalue_dims_state["ds"] in pinot.list_tables()


# ─────────────────────────────────────────────────────────────────────────────
# DS12 — Float metrics (floatSum/Min/Max → DOUBLE in Pinot)
# ─────────────────────────────────────────────────────────────────────────────

class TestFloatMetricsMigration:
    def test_float_metrics_map_to_double(self, float_metrics_state):
        by_name = {f["name"]: f["dataType"]
                   for f in float_metrics_state["schema"]["metricFieldSpecs"]}
        assert by_name["score_sum"] == "DOUBLE"
        assert by_name["score_min"] == "DOUBLE"
        assert by_name["score_max"] == "DOUBLE"
        assert by_name["weight_sum"] == "DOUBLE"

    def test_event_count_is_long(self, float_metrics_state):
        by_name = {f["name"]: f["dataType"]
                   for f in float_metrics_state["schema"]["metricFieldSpecs"]}
        assert by_name["event_count"] == "LONG"

    def test_table_is_offline(self, float_metrics_state):
        assert float_metrics_state["table"]["tableType"] == "OFFLINE"

    def test_no_blocking_risks(self, float_metrics_state):
        risks_path = float_metrics_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        blocking = [r for r in risks if r["severity"] == "blocking"]
        assert blocking == []

    def test_row_count_matches(self, float_metrics_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot, float_metrics_state["ds"], float_metrics_state["pinot_table"]
        )

    def test_sum_score_by_category_matches(self, float_metrics_state, druid, pinot):
        ds = float_metrics_state["ds"]
        tbl = float_metrics_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT category, SUM(score_sum) AS ss FROM "{ds}" '
                      f'GROUP BY category ORDER BY category',
            pinot_sql=f"SELECT category, SUM(score_sum) AS ss FROM {tbl} "
                      f"GROUP BY category ORDER BY category",
        )

    def test_max_score_by_category_matches(self, float_metrics_state, druid, pinot):
        ds = float_metrics_state["ds"]
        tbl = float_metrics_state["pinot_table"]
        assert_aggregated_query_matches(
            druid, pinot,
            druid_sql=f'SELECT category, MAX(score_max) AS ms FROM "{ds}" '
                      f'GROUP BY category ORDER BY category',
            pinot_sql=f"SELECT category, MAX(score_max) AS ms FROM {tbl} "
                      f"GROUP BY category ORDER BY category",
        )

    def test_live_pinot_table_exists(self, float_metrics_state, pinot):
        assert float_metrics_state["ds"] in pinot.list_tables()


# ─────────────────────────────────────────────────────────────────────────────
# DS13 — Hash-partitioned spec → PARTITIONING_CONFIG_REQUIRED risk
# ─────────────────────────────────────────────────────────────────────────────

class TestHashPartitionedMigration:
    def test_partitioning_risk_emitted(self, hash_partitioned_state):
        risks_path = hash_partitioned_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk_ids = [r["risk_id"] for r in risks]
        assert "PARTITIONING_CONFIG_REQUIRED" in risk_ids

    def test_partitioning_risk_is_medium(self, hash_partitioned_state):
        risks_path = hash_partitioned_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk = next(r for r in risks if r["risk_id"] == "PARTITIONING_CONFIG_REQUIRED")
        assert risk["severity"] == "medium"

    def test_partitioning_risk_evidence_mentions_hashed(self, hash_partitioned_state):
        risks_path = hash_partitioned_state["out_dir"] / "reports" / "risks.json"
        risks = json.loads(risks_path.read_text())["risks"]
        risk = next(r for r in risks if r["risk_id"] == "PARTITIONING_CONFIG_REQUIRED")
        evidence_text = " ".join(risk["evidence"])
        assert "hashed" in evidence_text.lower()

    def test_schema_has_correct_dimensions(self, hash_partitioned_state):
        dim_names = {f["name"] for f in hash_partitioned_state["schema"]["dimensionFieldSpecs"]}
        assert {"customer_id", "product_id", "status"} == dim_names

    def test_schema_has_metric_fields(self, hash_partitioned_state):
        names = {f["name"] for f in hash_partitioned_state["schema"]["metricFieldSpecs"]}
        assert "order_count" in names
        assert "total_amount" in names

    def test_table_is_offline(self, hash_partitioned_state):
        assert hash_partitioned_state["table"]["tableType"] == "OFFLINE"

    def test_row_count_matches(self, hash_partitioned_state, druid, pinot):
        assert_row_counts_match(
            druid, pinot,
            hash_partitioned_state["ds"], hash_partitioned_state["pinot_table"]
        )

    def test_total_amount_matches(self, hash_partitioned_state, druid, pinot):
        ds = hash_partitioned_state["ds"]
        tbl = hash_partitioned_state["pinot_table"]
        druid_rows = druid.sql_query(f'SELECT SUM(total_amount) AS ta FROM "{ds}"')
        pinot_rows = pinot.sql_query(f"SELECT SUM(total_amount) AS ta FROM {tbl}")
        assert abs(float(druid_rows[0]["ta"]) - float(pinot_rows[0]["ta"])) < 0.01

    def test_live_pinot_table_exists(self, hash_partitioned_state, pinot):
        assert hash_partitioned_state["ds"] in pinot.list_tables()
