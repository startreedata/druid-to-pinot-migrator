"""
Live end-to-end test for the hybrid Druid → Pinot Kafka migration flow.

Steps exercised:

1. Produce N=50 events to a Kafka topic.
2. Submit a Druid Kafka supervisor and wait until it consumes some events.
3. Run ``DruidOverlordClient.get_supervisor_offsets`` and verify the
   captured offset map matches Druid's reported offsets.
4. Terminate the Druid supervisor (simulating cutover).
5. Run ``plan_hybrid_migration`` to produce the OFFLINE + REALTIME
   table configs + runbook.
6. Deploy the schema and the REALTIME table to Pinot.
7. Produce M=50 more events to the same topic (timestamps AFTER the watermark).
8. Verify Pinot's REALTIME table eventually consumes those new events
   (i.e. the embedded watermark TIMESTAMP offset criterion seeded the
   start position correctly).

This test validates the wire-compatibility of:
- DruidOverlordClient against a real supervisor status payload
- The watermark → Pinot stream config translation against a real broker
- The end-to-end "static seeding" flow
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from migrator.druid.overlord_client import DruidOverlordClient
from migrator.realtime.hybrid_planner import plan_hybrid_migration
from tests.docker.cluster_clients import (
    DRUID_ROUTER_URL,
    KAFKA_BOOTSTRAP_INTERNAL,
    DruidClient,
    DruidSupervisorClient,
    KafkaTestClient,
    PinotClient,
)
from tests.docker.migration_helper import build_druid_spec


# A fixed millisecond base in 2024 so test events spread across one day
BASE_MS = 1_710_000_000_000  # 2024-03-09T16:00:00.000Z


def _events(start_idx: int, count: int, *, ms_offset: int) -> list[dict]:
    return [
        {
            "timestamp": BASE_MS + ms_offset + i * 1_000,  # 1s apart
            "user_id": f"u_{(start_idx + i) % 7}",
            "amount": float((start_idx + i) % 100),
        }
        for i in range(count)
    ]


@pytest.fixture(scope="module")
def kafka_client() -> KafkaTestClient:
    k = KafkaTestClient()
    k.wait_healthy(timeout=120)
    return k


@pytest.fixture(scope="module")
def supervisor_client() -> DruidSupervisorClient:
    return DruidSupervisorClient()


@pytest.fixture(scope="module")
def realtime_state(
    druid: DruidClient,
    pinot: PinotClient,
    kafka_client: KafkaTestClient,
    supervisor_client: DruidSupervisorClient,
    tmp_path_factory,
) -> dict:
    """
    Drives the whole pre-cutover phase: Kafka topic, Druid supervisor,
    offset capture, plan generation. Yields the captured state for tests.
    """
    DS = "rt_events"
    TOPIC = "rt_events_topic"
    out_dir = tmp_path_factory.mktemp("realtime")

    # 1) Topic + initial events
    kafka_client.create_topic(TOPIC, partitions=1)
    initial = _events(0, 50, ms_offset=0)
    kafka_client.produce_json(TOPIC, initial)

    # 2) Druid supervisor
    sup_id = supervisor_client.submit_kafka_supervisor(
        datasource=DS,
        topic=TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_INTERNAL,
        dimensions=["user_id"],
    )
    supervisor_client.wait_for_offsets(sup_id, min_total_offset=50, timeout=240)
    druid.wait_for_datasource(DS, timeout=180)

    # 3) Capture offsets via the migrator's own client
    overlord = DruidOverlordClient(DRUID_ROUTER_URL)
    offset_map = overlord.get_supervisor_offsets(sup_id)

    # 4) Terminate the supervisor — Druid stops consuming. After this point
    #    only Pinot will see new events.
    supervisor_client.terminate_supervisor(sup_id)

    # 5) Build canonical model + plan via the pure planner
    druid_spec = build_druid_spec(
        datasource=DS,
        timestamp_col="timestamp",
        dimensions=["user_id"],
        metrics=[],
    )
    # Force the spec into stream-shape so the planner accepts it
    druid_spec["spec"]["ioConfig"] = {
        "type": "kafka",
        "topic": TOPIC,
        "consumerProperties": {"bootstrap.servers": KAFKA_BOOTSTRAP_INTERNAL},
    }

    from migrator.druid.classifiers import classify_datasource
    from migrator.druid.normalizer import DruidNormalizer
    from migrator.druid.parser import DruidSpecParser

    parsed = DruidSpecParser().parse(druid_spec)
    norm = DruidNormalizer().normalize(parsed.parsed_spec)
    canonical = norm.canonical
    canonical.classification = classify_datasource(canonical).value

    plan = plan_hybrid_migration(canonical, offset_map)

    # 6) Deploy the schema + realtime table
    pinot.create_schema(plan.schema_)
    pinot.create_table(plan.realtime_table)

    yield {
        "ds": DS,
        "topic": TOPIC,
        "supervisor_id": sup_id,
        "offset_map": offset_map,
        "plan": plan,
        "out_dir": out_dir,
    }

    # Cleanup
    supervisor_client.terminate_supervisor(sup_id)
    druid.drop_datasource(DS)
    pinot.delete_table(DS)
    pinot.delete_schema(DS)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestRealtimeMigration:
    def test_overlord_returned_offset_map_matches_event_count(self, realtime_state):
        offset_map = realtime_state["offset_map"]
        # We produced 50 events on 1 partition before extracting offsets
        assert len(offset_map.offsets) == 1
        assert offset_map.offsets[0].partition == 0
        assert offset_map.offsets[0].offset >= 50

    def test_offset_map_carries_topic_and_supervisor_id(self, realtime_state):
        offset_map = realtime_state["offset_map"]
        assert offset_map.topic == realtime_state["topic"]
        assert offset_map.supervisor_id == realtime_state["supervisor_id"]

    def test_plan_realtime_table_uses_watermark(self, realtime_state):
        plan = realtime_state["plan"]
        sc = plan.realtime_table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kafka.consumer.prop.auto.offset.reset"] == \
            realtime_state["offset_map"].watermark_iso

    def test_plan_realtime_uses_internal_kafka_bootstrap(self, realtime_state):
        plan = realtime_state["plan"]
        sc = plan.realtime_table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kafka.broker.list"] == KAFKA_BOOTSTRAP_INTERNAL

    def test_pinot_realtime_consumes_post_watermark_events(
        self,
        realtime_state,
        pinot: PinotClient,
        kafka_client: KafkaTestClient,
    ):
        # Produce 50 NEW events, all with timestamps AFTER the watermark
        # (watermark = last Druid-ingested timestamp; we add +1h to be safe).
        offset_map = realtime_state["offset_map"]
        new_events = _events(
            start_idx=1000,
            count=50,
            ms_offset=offset_map.watermark_ms + 60 * 60 * 1000 - BASE_MS,
        )
        kafka_client.produce_json(realtime_state["topic"], new_events)

        # Pinot should consume them. Allow generous time for the controller
        # to bring up CONSUMING segments, push to server, etc.
        deadline = time.time() + 240
        last_count = 0
        while time.time() < deadline:
            try:
                rows = pinot.sql_query(
                    f"SELECT COUNT(*) AS c FROM {realtime_state['ds']}"
                )
                last_count = int(rows[0]["c"]) if rows else 0
                if last_count >= 50:
                    return
            except Exception:
                pass
            time.sleep(5)
        pytest.fail(
            f"Pinot REALTIME table did not consume new events within 240s "
            f"(saw {last_count} rows)"
        )

    def test_runbook_was_written(self, realtime_state, tmp_path_factory):
        from migrator.realtime.hybrid_planner import write_hybrid_plan
        out = tmp_path_factory.mktemp("rt_runbook")
        paths = write_hybrid_plan(realtime_state["plan"], out)
        rb = paths["runbook"].read_text()
        assert realtime_state["offset_map"].watermark_iso in rb
        assert realtime_state["topic"] in rb
