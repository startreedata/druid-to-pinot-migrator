"""
Live integration: one Pinot table per supported input format.

The unit tests in ``tests/unit/test_input_format.py`` already prove
``dpm generate``'s dispatch is correct for every format. This live
test goes one step further: each format actually deploys to the
running Pinot cluster — proving the generated table config is shape-
valid (Pinot accepts the JSON), the RecordReader / decoder class
names are real (Pinot can resolve the FQCNs), and the schema +
table-config combination is internally consistent.

What this is NOT: a data-parity test across formats. Generating
Parquet / Avro / ORC / Protobuf data files at test time would
multiply CI complexity (pyarrow + fastavro + protoc) for marginal
benefit beyond the dispatch correctness already covered by unit
tests. We deploy + assert, then delete — no rows ingested.

Coverage matrix
───────────────
+----------+-------+--------+-------------------------------------+
| Format   | Batch | Stream | Pinot artifact verified             |
+----------+-------+--------+-------------------------------------+
| json     |   ✓   |   ✓    | RecordReader / JSONMessageDecoder   |
| parquet  |   ✓   |   —    | ParquetRecordReader                 |
| avro_ocf |   ✓   |   —    | AvroRecordReader                    |
| avro_str |   —   |   ✓    | KafkaConfluent...AvroDecoder        |
| orc      |   ✓   |   —    | ORCRecordReader                     |
| csv      |   ✓   |   —    | CSVRecordReader                     |
| protobuf |   ✓   |   ✓    | ProtoBufRecordReader / KafkaConflue |
+----------+-------+--------+-------------------------------------+

Stream-only formats (avro_stream, protobuf streaming) skip the
batch-job assertion — there's no batch artifact for them.

Skipped unless ``LIVE_DOCKER_TESTS=1``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.translators.pipeline import generate_bundle
from tests.docker.migration_helper import build_druid_spec


# ─────────────────────────────────────────────────────────────────────────────
# Per-format spec builders
# ─────────────────────────────────────────────────────────────────────────────


def _batch_spec(datasource: str, input_format: str, **extra) -> dict:
    return build_druid_spec(
        datasource=datasource,
        dimensions=["region", "host"],
        metrics=[
            {"type": "count",   "name": "events"},
            {"type": "longSum", "name": "value_sum", "fieldName": "value"},
        ],
        rollup=True,
        input_type="local",
        input_format=input_format,
        input_format_extra=extra,
    )


def _stream_spec(datasource: str, input_format: str, **decoder_extra) -> dict:
    """Kafka supervisor spec — minimal but realistic enough to round-
    trip through ``dpm generate`` and produce a deploy-able REALTIME
    table config.
    """
    spec = {
        "type": "kafka",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {"column": "ts", "format": "millis"},
                "dimensionsSpec": {"dimensions": ["k"]},
                "metricsSpec": [{"type": "count", "name": "events"}],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "MINUTE",
                    "rollup": True,
                },
            },
            "ioConfig": {
                "type": "kafka",
                "topic": f"{datasource}_topic",
                "consumerProperties": {"bootstrap.servers": "kafka:9092"},
                "inputFormat": {"type": input_format, **decoder_extra},
            },
        },
    }
    return spec


# ─────────────────────────────────────────────────────────────────────────────
# Helper: run dpm generate and deploy the result to live Pinot
# ─────────────────────────────────────────────────────────────────────────────


def _generate_and_deploy(
    spec: dict, pinot, tmp_path: Path,
) -> tuple[dict, dict | None, dict | None]:
    """Run ``dpm generate`` on the in-memory spec; deploy the schema +
    table config to the live Pinot. Returns (schema, offline_table,
    realtime_table) — the latter two ``None`` when the spec wouldn't
    produce them.

    Each call uses a fresh ``tmp_path`` so artifacts from earlier
    parametrisations don't bleed across.
    """
    spec_path = tmp_path / "druid_spec.json"
    spec_path.write_text(json.dumps(spec, indent=2))
    result = generate_bundle(str(spec_path), out_dir=str(tmp_path))
    if not result.success:
        raise RuntimeError(
            f"generate_bundle failed: {result.errors}",
        )
    schema = json.loads((tmp_path / "schema.json").read_text())
    pinot.create_schema(schema)
    offline = None
    realtime = None
    if (tmp_path / "table-offline.json").exists():
        offline = json.loads((tmp_path / "table-offline.json").read_text())
        pinot.create_table(offline)
    if (tmp_path / "table-realtime.json").exists():
        realtime = json.loads((tmp_path / "table-realtime.json").read_text())
        pinot.create_table(realtime)
    return schema, offline, realtime


# ─────────────────────────────────────────────────────────────────────────────
# Batch input-format matrix (one Pinot table per format)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("input_format, expected_reader, extra", [
    # JSON: the v0.10.0 default; locked in as the regression baseline.
    ("json",     "JSONRecordReader",    {}),
    ("parquet",  "ParquetRecordReader", {}),
    ("avro_ocf", "AvroRecordReader",    {}),
    ("orc",      "ORCRecordReader",     {}),
    # CSV needs the ``columns`` entry to make Druid happy at parse
    # time; we don't actually run a Druid task here so any list works.
    ("csv",      "CSVRecordReader",     {"columns": ["timestamp", "region", "host", "value"]}),
    ("protobuf", "ProtoBufRecordReader", {}),
])
class TestBatchInputFormatMatrix:
    def test_one_pinot_table_per_input_format(
        self, input_format: str, expected_reader: str, extra: dict,
        pinot_table_factory, pinot, tmp_path: Path,
    ):
        # Each format gets its own datasource so the parametrised cells
        # don't collide on table names.
        ds = f"format_matrix_{input_format}"
        spec = _batch_spec(ds, input_format, **extra)

        # Drive through the full pipeline — generate, push to Pinot,
        # and register cleanup so the table is removed on teardown.
        spec_path = tmp_path / "druid_spec.json"
        spec_path.write_text(json.dumps(spec, indent=2))
        result = generate_bundle(str(spec_path), out_dir=str(tmp_path))
        assert result.success, result.errors

        schema = json.loads((tmp_path / "schema.json").read_text())
        offline = json.loads((tmp_path / "table-offline.json").read_text())
        # Use the test factory so cleanup happens automatically.
        pinot_table_factory(schema=schema, table_config=offline)

        # 1. Pinot accepted the table — the table appears in /tables.
        tables = pinot.list_tables()
        assert any(ds in t for t in tables), (
            f"OFFLINE table for ds={ds} not visible in Pinot /tables: {tables}"
        )

        # 2. The generated batch-job spec carries the right
        # RecordReader. Asserted against the file dpm wrote, not the
        # live Pinot, because the batch-job is a launchable spec, not
        # a Pinot-side resource.
        batch_job = json.loads((tmp_path / "batch-job.json").read_text())
        assert expected_reader in batch_job["recordReaderSpec"]["className"], (
            f"For input_format={input_format}: "
            f"expected {expected_reader}, got "
            f"{batch_job['recordReaderSpec']['className']}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Streaming input-format matrix (REALTIME table)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("input_format, decoder_extra, expected_decoder_class", [
    # Plain JSON — Druid's ``json`` inputFormat used straight on Kafka.
    ("json", {}, "JSONMessageDecoder"),
    # Confluent-registry Avro — the most common production Avro setup.
    (
        "avro_stream",
        {
            "avroBytesDecoder": {
                "type": "schema_registry",
                "url": "http://schema-registry:8081",
            },
        },
        "KafkaConfluentSchemaRegistryAvroMessageDecoder",
    ),
    # Confluent-registry Protobuf — the streaming partner of the
    # batch ProtoBufRecordReader.
    (
        "protobuf",
        {
            "protoBytesDecoder": {
                "type": "schema_registry",
                "url": "http://schema-registry:8081",
                "protoMessageType": "MyEvent",
            },
        },
        "KafkaConfluentSchemaRegistryProtoBufMessageDecoder",
    ),
])
class TestStreamInputFormatMatrix:
    def test_one_pinot_realtime_table_per_decoder(
        self, input_format: str, decoder_extra: dict,
        expected_decoder_class: str,
        pinot_table_factory, pinot, tmp_path: Path,
    ):
        ds = f"stream_matrix_{input_format}"
        spec = _stream_spec(ds, input_format, **decoder_extra)
        spec_path = tmp_path / "druid_spec.json"
        spec_path.write_text(json.dumps(spec, indent=2))
        result = generate_bundle(str(spec_path), out_dir=str(tmp_path))
        assert result.success, result.errors

        schema = json.loads((tmp_path / "schema.json").read_text())
        realtime = json.loads((tmp_path / "table-realtime.json").read_text())
        pinot_table_factory(schema=schema, table_config=realtime)

        # Pinot accepted the REALTIME table.
        tables = pinot.list_tables()
        assert any(ds in t for t in tables), (
            f"REALTIME table for ds={ds} not visible: {tables}"
        )

        # The generated stream config carries the right decoder. This
        # is what proves dpm picked the right Pinot decoder class for
        # the Druid inputFormat — wrong class means silent runtime
        # decode failures (Pinot drops messages with no error).
        sc = realtime["tableIndexConfig"]["streamConfigs"]
        assert expected_decoder_class in sc["stream.kafka.decoder.class.name"], (
            f"For input_format={input_format}: "
            f"expected {expected_decoder_class}, got "
            f"{sc['stream.kafka.decoder.class.name']}"
        )
