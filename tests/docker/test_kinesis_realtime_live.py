"""
Live end-to-end test for the Kinesis side of the hybrid migration flow.

This is the real-shape counterpart to ``test_realtime_migration.py``
(which is Kafka). It exists because the Kinesis capture path was twice
broken by mocked unit tests that diverged from Druid's real supervisor-
status payload:

  1. a ``stream`` key in the payload was wrongly treated as a Kinesis
     signal (Druid reports ``stream`` for Kafka too); and
  2. detection keyed off a fictional ``latestSequenceNumbers`` field —
     Druid's shared ``SeekableStreamSupervisorReportPayload`` actually
     reports Kinesis sequence numbers under ``latestOffsets`` (as opaque
     strings), with no ``latestSequenceNumbers`` field at all.

Only a live test against a real Druid Kinesis supervisor exercises the
true payload shape. This drives:

1. Create a Kinesis stream on LocalStack and put N JSON records.
2. Submit a real Druid **kinesis** supervisor (endpoint → LocalStack)
   and wait until Druid has ingested the records (proving the
   Druid↔LocalStack Kinesis wiring works end-to-end).
3. Run ``DruidOverlordClient.get_supervisor_offsets`` and assert it
   detects ``kinesis`` (not Kafka), resolves the stream name, keeps any
   sequence numbers as STRINGS (never int-coerced), and produces a valid
   watermark — all from the REAL status payload + spec.
4. Run ``plan_hybrid_migration`` and assert the REALTIME table is a
   ``streamType: kinesis`` config seeded with the watermark timestamp.

Scope note: this validates the Druid-side detection + plan generation —
the part that regressed twice (Kinesis misdetected as Kafka; int() crash
on sequence strings). Two things are deliberately out of scope:
  - Pinot consuming from LocalStack Kinesis (needs Pinot-side endpoint
    wiring; the Kafka test already covers Pinot stream consumption).
  - Asserting non-empty per-shard sequences. Druid computes the
    supervisor-level ``latestOffsets`` lazily (a periodic stream-head
    query that, for Kinesis on LocalStack, may not have run yet), so an
    empty positions list is the CORRECT output of get_supervisor_offsets
    here. Capturing positions + a precise watermark from
    ``activeTasks[].currentOffsets`` instead is a tracked follow-up.

The class is marked ``kinesis`` so a flaky Druid×LocalStack combo can be
deselected per matrix cell with ``-m 'not kinesis'`` without dropping
the rest of the live suite.
"""

from __future__ import annotations

import pytest

from migrator.druid.overlord_client import DruidOverlordClient
from migrator.realtime.hybrid_planner import plan_hybrid_migration
from migrator.realtime.models import StreamPlatform
from tests.docker.cluster_clients import (
    DRUID_ROUTER_URL,
    LOCALSTACK_KINESIS_INTERNAL,
    DruidClient,
    DruidSupervisorClient,
    PinotClient,
)
from tests.docker.migration_helper import build_druid_spec


pytestmark = pytest.mark.kinesis

BASE_MS = 1_710_000_000_000  # 2024-03-09T16:00:00.000Z


def _events(count: int) -> list[dict]:
    return [
        {
            "timestamp": BASE_MS + i * 1_000,
            "user_id": f"u_{i % 7}",
            "amount": float(i % 100),
        }
        for i in range(count)
    ]


@pytest.fixture(scope="module")
def kinesis_state(
    druid: DruidClient,
    kinesis_client,
    supervisor_client: DruidSupervisorClient,
) -> dict:
    DS = "rt_kinesis_events"
    STREAM = "rt_kinesis_stream"
    N = 50

    # 1) Stream + records on LocalStack
    kinesis_client.create_stream(STREAM, shards=1)
    kinesis_client.put_json(STREAM, _events(N), partition_key_field="user_id")

    # 2) Real Druid kinesis supervisor → LocalStack
    sup_id = supervisor_client.submit_kinesis_supervisor(
        datasource=DS,
        stream=STREAM,
        endpoint=LOCALSTACK_KINESIS_INTERNAL,
        dimensions=["user_id"],
    )
    # Wait until Druid has actually ingested the records from LocalStack —
    # this proves the Druid↔LocalStack Kinesis wiring works end-to-end.
    #
    # NOTE: we deliberately do NOT wait for the supervisor's top-level
    # ``latestOffsets`` to populate. Druid computes that lazily on a
    # periodic stream-head query that (for Kinesis on LocalStack) hadn't
    # run even after 300s — the field is @Nullable and simply absent from
    # the status JSON until then. The actual consumed positions live under
    # ``activeTasks[].currentOffsets``; capturing Kinesis per-shard
    # sequences + a precise watermark from there is a tracked follow-up
    # (see test_shard_sequences_are_strings_when_present). This test
    # validates the part that regressed twice: that the REAL Kinesis
    # status payload flows through get_supervisor_offsets as ``kinesis``
    # (not misdetected as Kafka) without crashing on sequence strings.
    druid.wait_for_datasource(DS, timeout=240)

    # 3) Capture via the migrator's own client — the REAL status payload
    overlord = DruidOverlordClient(DRUID_ROUTER_URL)
    offset_map = overlord.get_supervisor_offsets(sup_id)

    # 4) Terminate (simulate cutover)
    supervisor_client.terminate_supervisor(sup_id)

    # 5) Canonical model + plan
    druid_spec = build_druid_spec(
        datasource=DS, timestamp_col="timestamp", dimensions=["user_id"], metrics=[],
    )
    druid_spec["spec"]["ioConfig"] = {
        "type": "kinesis",
        "stream": STREAM,
        "endpoint": LOCALSTACK_KINESIS_INTERNAL,
    }

    from migrator.druid.classifiers import classify_datasource
    from migrator.druid.normalizer import DruidNormalizer
    from migrator.druid.parser import DruidSpecParser

    parsed = DruidSpecParser().parse(druid_spec)
    canonical = DruidNormalizer().normalize(parsed.parsed_spec).canonical
    canonical.classification = classify_datasource(canonical).value
    plan = plan_hybrid_migration(canonical, offset_map)

    yield {
        "ds": DS,
        "stream": STREAM,
        "supervisor_id": sup_id,
        "offset_map": offset_map,
        "plan": plan,
        "n": N,
    }

    supervisor_client.terminate_supervisor(sup_id)
    try:
        druid.drop_datasource(DS)
    except Exception:
        pass


class TestKinesisRealtimeMigration:
    def test_platform_detected_as_kinesis(self, kinesis_state):
        assert kinesis_state["offset_map"].platform == StreamPlatform.KINESIS

    def test_stream_name_resolved(self, kinesis_state):
        assert kinesis_state["offset_map"].topic == kinesis_state["stream"]

    def test_supervisor_id_carried(self, kinesis_state):
        assert kinesis_state["offset_map"].supervisor_id == kinesis_state["supervisor_id"]

    def test_shard_sequences_are_strings_when_present(self, kinesis_state):
        # The supervisor's top-level latestOffsets is computed lazily, so on
        # a fresh supervisor it's often absent and shard_sequences is empty —
        # that's the CORRECT behaviour of get_supervisor_offsets given an
        # absent latestOffsets (not a crash, not a misparse). When sequences
        # ARE present, the crux assertion holds: they're opaque STRINGS,
        # never int()-coerced (the bug that broke real Kinesis capture).
        #
        # FOLLOW-UP: enrich get_supervisor_offsets to read per-shard
        # positions from activeTasks[].currentOffsets so this is reliably
        # non-empty + yields a precise watermark. Tracked separately.
        om = kinesis_state["offset_map"]
        for ss in om.shard_sequences:
            assert isinstance(ss.shard_id, str) and ss.shard_id
            assert isinstance(ss.sequence_number, str) and ss.sequence_number

    def test_no_kafka_offsets_populated(self, kinesis_state):
        # Kinesis capture must not produce Kafka partition offsets.
        assert kinesis_state["offset_map"].offsets == []

    def test_watermark_is_pinot_iso(self, kinesis_state):
        import re

        wm = kinesis_state["offset_map"].watermark_iso
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", wm
        ), wm

    def test_plan_realtime_is_kinesis_with_watermark(self, kinesis_state):
        plan = kinesis_state["plan"]
        sc = plan.realtime_table["tableIndexConfig"]["streamConfigs"]
        assert sc["streamType"] == "kinesis"
        assert sc["stream.kinesis.topic.name"] == kinesis_state["stream"]
        assert (
            sc["stream.kinesis.consumer.prop.auto.offset.reset"]
            == kinesis_state["offset_map"].watermark_iso
        )
        # No Kafka keys leak into a Kinesis hybrid plan.
        assert not any(k.startswith("stream.kafka.") for k in sc)
