"""
Live end-to-end test for ``dpm extract-spec`` against a real Druid cluster.

Two scenarios are exercised — both use the same docker-compose stack as
the existing live tests:

1. **Batch path** — ingest a small inline dataset via Druid's native
   batch task, then call ``extract_spec`` and verify the resulting JSON
   round-trips through ``dpm generate`` to produce a valid Pinot
   schema.

2. **Stream path** — produce events to Kafka, submit a Druid Kafka
   supervisor, then call ``extract_spec`` (with overlord_url set) and
   verify the recovered spec carries the right topic and bootstrap
   server, and round-trips through ``dpm generate`` to produce a
   REALTIME table.

Both scenarios exit cleanly: the inline batch path drops its
datasource at teardown; the stream path terminates the supervisor
and drops the datasource.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import pytest

from migrator.druid.classifiers import classify_datasource
from migrator.druid.coordinator_client import DruidCoordinatorClient
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.overlord_client import DruidOverlordClient
from migrator.druid.parser import DruidSpecParser
from migrator.druid.spec_extractor import extract_spec
from migrator.pinot.schema_generator import PinotSchemaGenerator
from migrator.pinot.table_generator import PinotTableGenerator
from tests.docker.cluster_clients import (
    DRUID_COORDINATOR_URL,
    DRUID_ROUTER_URL,
    KAFKA_BOOTSTRAP_INTERNAL,
    DruidClient,
    DruidSupervisorClient,
    KafkaTestClient,
)


# ─── Batch (no supervisor) ────────────────────────────────────────────────


@pytest.fixture(scope="module")
def batch_extract_state(druid: DruidClient, tmp_path_factory) -> Iterator[dict]:
    DS = "ext_batch"
    out_dir = tmp_path_factory.mktemp("ext_batch")

    # Ingest a deterministic inline batch dataset
    records = [
        {"timestamp": 1709251200000 + i * 60_000,
         "country": ["us", "fr", "jp"][i % 3],
         "platform": ["mobile", "desktop"][i % 2],
         "revenue": float(10 + i),
         "clicks": 5 + i}
        for i in range(20)
    ]
    druid.ingest_inline(
        datasource=DS,
        records=records,
        timestamp_col="timestamp",
        timestamp_format="millis",
        dimensions=["country", "platform"],
        metrics=[
            {"type": "longSum",   "name": "clicks",  "fieldName": "clicks"},
            {"type": "doubleSum", "name": "revenue", "fieldName": "revenue"},
        ],
        rollup=False,
    )
    druid.wait_for_datasource(DS, timeout=180)

    yield {"ds": DS, "out_dir": out_dir}

    druid.drop_datasource(DS)


class TestExtractSpecBatchLive:
    def test_extract_returns_batch_spec(self, batch_extract_state):
        coord = DruidCoordinatorClient(
            coordinator_url=DRUID_COORDINATOR_URL,
            broker_url=DRUID_ROUTER_URL,
        )
        result = extract_spec(batch_extract_state["ds"], coordinator=coord)
        assert result.source_kind == "batch"
        assert result.spec["type"] == "index_parallel"
        assert (
            result.spec["spec"]["dataSchema"]["dataSource"]
            == batch_extract_state["ds"]
        )

    def test_extracted_spec_recovers_dimension_and_metric_names(
        self, batch_extract_state
    ):
        coord = DruidCoordinatorClient(
            coordinator_url=DRUID_COORDINATOR_URL,
            broker_url=DRUID_ROUTER_URL,
        )
        result = extract_spec(batch_extract_state["ds"], coordinator=coord)
        ds = result.spec["spec"]["dataSchema"]
        # Dims arrive as bare strings (no MV) — order is alphabetical
        dims = [d if isinstance(d, str) else d["name"]
                for d in ds["dimensionsSpec"]["dimensions"]]
        assert set(dims) >= {"country", "platform"}
        # Metrics: clicks (LONG) + revenue (DOUBLE)
        met_names = {m["name"] for m in ds["metricsSpec"]}
        assert {"clicks", "revenue"} <= met_names

    def test_extracted_spec_round_trips_through_generate(
        self, batch_extract_state
    ):
        coord = DruidCoordinatorClient(
            coordinator_url=DRUID_COORDINATOR_URL,
            broker_url=DRUID_ROUTER_URL,
        )
        result = extract_spec(batch_extract_state["ds"], coordinator=coord)

        # Parse → normalise → generate (no I/O against Pinot)
        parsed = DruidSpecParser().parse(result.spec)
        assert parsed.success, parsed.errors
        norm = DruidNormalizer().normalize(parsed.parsed_spec)
        assert norm.success, norm.errors

        canonical = norm.canonical
        canonical.classification = classify_datasource(canonical).value

        schema = PinotSchemaGenerator().generate(canonical)
        assert schema["schemaName"] == batch_extract_state["ds"]
        # The generated schema includes the dimensions we ingested
        names = {f["name"] for f in schema["dimensionFieldSpecs"]}
        assert {"country", "platform"} <= names

        table = PinotTableGenerator().generate(canonical)
        assert table["tableType"] == "OFFLINE"

    def test_extract_warns_about_input_source_placeholder(
        self, batch_extract_state
    ):
        coord = DruidCoordinatorClient(
            coordinator_url=DRUID_COORDINATOR_URL,
            broker_url=DRUID_ROUTER_URL,
        )
        result = extract_spec(batch_extract_state["ds"], coordinator=coord)
        assert any("inputSource" in w for w in result.warnings)


# ─── Stream (with supervisor) ─────────────────────────────────────────────


@pytest.fixture(scope="module")
def stream_extract_state(
    druid: DruidClient,
    supervisor_client: DruidSupervisorClient,
    kafka_client: KafkaTestClient,
    tmp_path_factory,
) -> Iterator[dict]:
    DS = "ext_stream"
    TOPIC = "ext_stream_topic"
    out_dir = tmp_path_factory.mktemp("ext_stream")

    kafka_client.create_topic(TOPIC, partitions=1)
    kafka_client.produce_json(TOPIC, [
        {"timestamp": 1710000000000 + i * 1000, "user_id": f"u_{i}"}
        for i in range(20)
    ])
    sup_id = supervisor_client.submit_kafka_supervisor(
        datasource=DS,
        topic=TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_INTERNAL,
        dimensions=["user_id"],
    )
    supervisor_client.wait_for_offsets(sup_id, min_total_offset=10, timeout=240)
    druid.wait_for_datasource(DS, timeout=180)

    yield {"ds": DS, "topic": TOPIC, "supervisor_id": sup_id, "out_dir": out_dir}

    supervisor_client.terminate_supervisor(sup_id)
    druid.drop_datasource(DS)


class TestExtractSpecStreamLive:
    def test_extract_returns_stream_spec(self, stream_extract_state):
        coord = DruidCoordinatorClient(
            coordinator_url=DRUID_COORDINATOR_URL,
            broker_url=DRUID_ROUTER_URL,
        )
        overlord = DruidOverlordClient(DRUID_ROUTER_URL)
        result = extract_spec(
            stream_extract_state["ds"],
            coordinator=coord,
            overlord=overlord,
        )
        assert result.source_kind == "stream"
        assert result.supervisor_id == stream_extract_state["supervisor_id"]
        assert result.spec["type"] == "kafka"

    def test_extracted_stream_spec_carries_topic_and_bootstrap(
        self, stream_extract_state
    ):
        coord = DruidCoordinatorClient(
            coordinator_url=DRUID_COORDINATOR_URL,
            broker_url=DRUID_ROUTER_URL,
        )
        overlord = DruidOverlordClient(DRUID_ROUTER_URL)
        result = extract_spec(
            stream_extract_state["ds"],
            coordinator=coord,
            overlord=overlord,
        )
        iocfg = result.spec["spec"]["ioConfig"]
        assert iocfg["topic"] == stream_extract_state["topic"]
        assert (
            iocfg["consumerProperties"]["bootstrap.servers"]
            == KAFKA_BOOTSTRAP_INTERNAL
        )
        # Most important — the type must be propagated so the migrator's
        # normalizer recognises this as a stream source.
        assert iocfg["type"] == "kafka"

    def test_extracted_stream_spec_round_trips_to_realtime_table(
        self, stream_extract_state
    ):
        coord = DruidCoordinatorClient(
            coordinator_url=DRUID_COORDINATOR_URL,
            broker_url=DRUID_ROUTER_URL,
        )
        overlord = DruidOverlordClient(DRUID_ROUTER_URL)
        result = extract_spec(
            stream_extract_state["ds"],
            coordinator=coord,
            overlord=overlord,
        )

        parsed = DruidSpecParser().parse(result.spec)
        assert parsed.success, parsed.errors
        norm = DruidNormalizer().normalize(parsed.parsed_spec)
        assert norm.success, norm.errors
        canonical = norm.canonical
        canonical.classification = classify_datasource(canonical).value

        # Should classify as stream and emit a REALTIME table
        assert canonical.source_kind == "stream"
        table = PinotTableGenerator().generate(canonical)
        assert table["tableType"] == "REALTIME"
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kafka.topic.name"] == stream_extract_state["topic"]
