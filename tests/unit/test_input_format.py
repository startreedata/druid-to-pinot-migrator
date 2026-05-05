"""Unit tests for inputFormat handling: parser → canonical → generator.

Focus: Parquet is the new format added in v0.11-dev, but the same
plumbing has to keep working for JSON (existing default) and produce
sensible output for Avro / ORC / CSV / Protobuf via the same
RecordReader dispatch.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.pinot.ingestion_generator import (
    _PINOT_FS,
    _PINOT_RECORD_READERS,
    PinotIngestionGenerator,
    _pinot_fs_spec,
    _record_reader_spec,
)


FIXTURES = Path(__file__).parent.parent / "fixtures"


def _normalize(raw: dict):
    parsed = DruidSpecParser().parse(raw)
    norm = DruidNormalizer().normalize(parsed.parsed_spec)
    return norm


def _canonical(raw: dict):
    return _normalize(raw).canonical


# ─────────────────────────────────────────────────────────────────────────────
# Normalizer: input_format detection
# ─────────────────────────────────────────────────────────────────────────────


_BASE_SPEC = {
    "type": "index_parallel",
    "spec": {
        "dataSchema": {
            "dataSource": "x",
            "timestampSpec": {"column": "ts", "format": "millis"},
            "dimensionsSpec": {"dimensions": ["a"]},
            "metricsSpec": [],
            "granularitySpec": {"segmentGranularity": "DAY", "rollup": False},
        },
        "ioConfig": {
            "type": "index_parallel",
            "inputSource": {"type": "local", "baseDir": "/data"},
            "inputFormat": {"type": "json"},
        },
    },
}


def _spec(input_format_type: str | None, **input_format_extra) -> dict:
    spec = json.loads(json.dumps(_BASE_SPEC))  # deep copy via roundtrip
    if input_format_type is None:
        spec["spec"]["ioConfig"].pop("inputFormat", None)
    else:
        spec["spec"]["ioConfig"]["inputFormat"] = {
            "type": input_format_type, **input_format_extra,
        }
    return spec


class TestNormalizerInputFormat:
    def test_json_is_default_when_unset(self):
        c = _canonical(_spec(None))
        assert c.input_format == "json"

    @pytest.mark.parametrize("druid_type, expected", [
        ("json",     "json"),
        ("parquet",  "parquet"),
        ("avro",     "avro"),
        ("orc",      "orc"),
        ("csv",      "csv"),
        ("protobuf", "protobuf"),
    ])
    def test_known_formats_pass_through(self, druid_type: str, expected: str):
        c = _canonical(_spec(druid_type))
        assert c.input_format == expected

    def test_case_normalised_lowercase(self):
        # Operators occasionally write `"PARQUET"` or `"Parquet"`;
        # canonicalising avoids the spec-author writing the same
        # format two ways and getting two different results.
        c = _canonical(_spec("PARQUET"))
        assert c.input_format == "parquet"
        c = _canonical(_spec("Parquet"))
        assert c.input_format == "parquet"

    def test_tsv_aliased_to_csv(self):
        # Pinot's CSVRecordReader handles both via a delimiter knob;
        # collapsing the alias keeps downstream dispatch simple.
        c = _canonical(_spec("tsv"))
        assert c.input_format == "csv"

    def test_unknown_format_falls_back_to_json_with_warning(self):
        result = _normalize(_spec("xml"))
        assert result.canonical.input_format == "json"
        # Warning surfaces the original value so the operator can spot
        # whether it was a typo or a genuinely unsupported format.
        assert any(
            "xml" in w and "JSON" in w for w in result.warnings
        ), result.warnings

    def test_parquet_binary_as_string_emits_warning(self):
        result = _normalize(_spec("parquet", binaryAsString=True))
        assert result.canonical.input_format == "parquet"
        assert any(
            "binaryAsString" in w and "Pinot" in w for w in result.warnings
        ), result.warnings

    def test_parquet_without_binary_as_string_no_warning(self):
        # The compatibility footgun warning must NOT fire when
        # binaryAsString is unset / false; otherwise every Parquet
        # spec (the common case) shows a noise warning.
        result = _normalize(_spec("parquet"))
        assert not any("binaryAsString" in w for w in result.warnings)


# ─────────────────────────────────────────────────────────────────────────────
# RecordReader dispatch
# ─────────────────────────────────────────────────────────────────────────────


class TestRecordReaderSpec:
    @pytest.mark.parametrize("fmt, data_format, class_suffix", [
        ("json",     "json",    "JSONRecordReader"),
        ("parquet",  "parquet", "ParquetRecordReader"),
        ("avro",     "avro",    "AvroRecordReader"),
        ("orc",      "orc",     "ORCRecordReader"),
        ("csv",      "csv",     "CSVRecordReader"),
        ("protobuf", "proto",   "ProtoBufRecordReader"),
    ])
    def test_spec_picks_matching_reader(
        self, fmt: str, data_format: str, class_suffix: str,
    ):
        spec = _record_reader_spec(fmt)
        assert spec["dataFormat"] == data_format
        assert spec["className"].endswith(class_suffix)

    def test_unknown_format_falls_back_to_json(self):
        # The normalizer SHOULD have already coerced unknowns to
        # ``json``; this is a defensive belt-and-suspenders check
        # in case a caller bypasses the normalizer.
        spec = _record_reader_spec("xml")
        assert spec["dataFormat"] == "json"
        assert "JSONRecordReader" in spec["className"]

    def test_known_formats_all_have_unique_classes(self):
        # No two formats should map to the same RecordReader; if they
        # do, dispatch is broken.
        classes = {fmt: cls for fmt, (_, cls) in _PINOT_RECORD_READERS.items()}
        assert len(set(classes.values())) == len(classes)


# ─────────────────────────────────────────────────────────────────────────────
# pinotFSSpec dispatch
# ─────────────────────────────────────────────────────────────────────────────


class TestPinotFsSpec:
    @pytest.mark.parametrize("uri, expected_scheme, fs_suffix", [
        ("/data/local",                       "file", "LocalPinotFS"),
        ("file:///data/local",                "file", "LocalPinotFS"),
        ("s3://bucket/key",                   "s3",   "S3PinotFS"),
        ("gs://bucket/key",                   "gs",   "GcsPinotFS"),
        ("gcs://bucket/key",                  "gs",   "GcsPinotFS"),
        ("hdfs://nn/path",                    "hdfs", "HadoopPinotFS"),
    ])
    def test_scheme_dispatch(
        self, uri: str, expected_scheme: str, fs_suffix: str,
    ):
        spec = _pinot_fs_spec(uri)
        assert spec["scheme"] == expected_scheme
        assert spec["className"].endswith(fs_suffix)

    def test_unknown_scheme_falls_back_to_local(self):
        spec = _pinot_fs_spec("ftp://server/path")
        assert spec["scheme"] == "file"


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end: spec → canonical → generated batch-job
# ─────────────────────────────────────────────────────────────────────────────


class TestEndToEndParquet:
    def test_fixture_produces_parquet_record_reader(self):
        raw = json.loads((FIXTURES / "parquet_input" / "spec.json").read_text())
        canonical = _canonical(raw)
        assert canonical.input_format == "parquet"
        job = PinotIngestionGenerator().generate_batch_job(canonical)
        # Right RecordReader class
        assert "ParquetRecordReader" in job["recordReaderSpec"]["className"]
        assert job["recordReaderSpec"]["dataFormat"] == "parquet"

    def test_fixture_produces_s3_pinot_fs(self):
        # Fixture's inputSource is s3 with uris[0] starting "s3://".
        # Generator must pick S3PinotFS, not the LocalPinotFS default.
        raw = json.loads((FIXTURES / "parquet_input" / "spec.json").read_text())
        canonical = _canonical(raw)
        job = PinotIngestionGenerator().generate_batch_job(canonical)
        fs = job["pinotFSSpecs"][0]
        assert fs["scheme"] == "s3"
        assert "S3PinotFS" in fs["className"]
        # InputDir comes from the spec's uris[0], not the local default.
        assert job["inputDirURI"].startswith("s3://prod-telemetry/")

    def test_existing_json_fixture_unchanged(self):
        # Backward compatibility: the raw_batch fixture (JSON,
        # local FS) must still produce identical output to v0.10.0.
        raw = json.loads((FIXTURES / "raw_batch" / "spec.json").read_text())
        canonical = _canonical(raw)
        assert canonical.input_format == "json"
        job = PinotIngestionGenerator().generate_batch_job(canonical)
        assert job["recordReaderSpec"]["dataFormat"] == "json"
        assert "JSONRecordReader" in job["recordReaderSpec"]["className"]
        assert job["pinotFSSpecs"][0]["scheme"] == "file"


# ─────────────────────────────────────────────────────────────────────────────
# Avro: avro_ocf (batch) + avro_stream (Kafka with schema registry)
# ─────────────────────────────────────────────────────────────────────────────


from migrator.pinot.table_generator import (
    KAFKA_AVRO_REGISTRY_DECODER,
    KAFKA_AVRO_SIMPLE_DECODER,
    KAFKA_JSON_DECODER,
    PinotTableGenerator,
    avro_decoder_config_from_io,
)


class TestNormalizerAvroSubtypes:
    def test_avro_ocf_collapses_to_avro(self):
        # OCF (batch, embedded schema) is the simpler avro variant —
        # no registry / inline schema needed because each file carries
        # its own header. Just maps to canonical ``avro``.
        c = _canonical(_spec("avro_ocf"))
        assert c.input_format == "avro"

    def test_avro_stream_collapses_to_avro(self):
        c = _canonical(_spec("avro_stream"))
        assert c.input_format == "avro"

    def test_avro_stream_missing_url_warns(self):
        # schema_registry decoder without a URL is a load-bearing
        # missing field — the operator gets nothing back from the
        # decoder until they fill it in. Warn loudly.
        spec = _spec("avro_stream", avroBytesDecoder={"type": "schema_registry"})
        result = _normalize(spec)
        assert any(
            "schema_registry" in w and "url" in w for w in result.warnings
        ), result.warnings

    def test_avro_stream_with_url_no_warning(self):
        spec = _spec("avro_stream", avroBytesDecoder={
            "type": "schema_registry",
            "url": "http://sr:8081",
        })
        result = _normalize(spec)
        assert not any(
            "schema_registry" in w and "url" in w for w in result.warnings
        ), result.warnings

    def test_avro_stream_inline_missing_schema_warns(self):
        spec = _spec("avro_stream", avroBytesDecoder={"type": "schema_inline"})
        result = _normalize(spec)
        assert any(
            "schema_inline" in w for w in result.warnings
        ), result.warnings


class TestAvroDecoderConfigFromIo:
    def test_schema_registry_picks_confluent_decoder(self):
        io = {
            "inputFormat": {
                "type": "avro_stream",
                "avroBytesDecoder": {
                    "type": "schema_registry",
                    "url": "http://sr:8081",
                },
            },
        }
        klass, props = avro_decoder_config_from_io(io)
        assert klass == KAFKA_AVRO_REGISTRY_DECODER
        # Pinot expects this exact key — typo'ing it silently produces
        # a no-op decoder, so lock it in.
        assert props == {"schema.registry.rest.url": "http://sr:8081"}

    def test_schema_registry_missing_url_yields_empty_props(self):
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {"type": "schema_registry"},
        }}
        klass, props = avro_decoder_config_from_io(io)
        assert klass == KAFKA_AVRO_REGISTRY_DECODER
        assert props == {}   # Operator must add the URL post-generation

    def test_schema_inline_dict_serialised_as_json(self):
        # Druid accepts an inline schema as either a JSON string OR a
        # parsed dict; Pinot's SimpleAvroMessageDecoder needs a string
        # (it parses it itself). Make sure dpm doesn't shove a dict
        # into the streamConfigs (which would silently break Pinot).
        schema_dict = {"type": "record", "name": "X", "fields": []}
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {"type": "schema_inline", "schema": schema_dict},
        }}
        klass, props = avro_decoder_config_from_io(io)
        assert klass == KAFKA_AVRO_SIMPLE_DECODER
        assert isinstance(props["schema"], str)
        # Round-trip back through JSON to confirm fidelity.
        assert json.loads(props["schema"]) == schema_dict

    def test_unknown_decoder_type_falls_back_to_registry(self):
        # If an operator misspells the avroBytesDecoder.type, default
        # to the registry decoder (most common case) — they'll see the
        # missing-URL warning from the normalizer and fix it.
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {"type": "weird_thing_we_do_not_know"},
        }}
        klass, _ = avro_decoder_config_from_io(io)
        assert klass == KAFKA_AVRO_REGISTRY_DECODER


class TestEndToEndAvroBatch:
    def test_avro_ocf_fixture_produces_avro_record_reader(self):
        raw = json.loads((FIXTURES / "avro_ocf" / "spec.json").read_text())
        canonical = _canonical(raw)
        assert canonical.input_format == "avro"
        job = PinotIngestionGenerator().generate_batch_job(canonical)
        assert job["recordReaderSpec"]["dataFormat"] == "avro"
        assert "AvroRecordReader" in job["recordReaderSpec"]["className"]
        # Source is S3 → S3PinotFS, not LocalPinotFS.
        assert "S3PinotFS" in job["pinotFSSpecs"][0]["className"]


class TestEndToEndAvroStream:
    def test_avro_stream_fixture_produces_confluent_decoder(self):
        raw = json.loads((FIXTURES / "avro_stream" / "spec.json").read_text())
        canonical = _canonical(raw)
        assert canonical.input_format == "avro"
        # Source kind must come through as stream — Avro on Kafka.
        assert canonical.source_kind == "stream"
        table = PinotTableGenerator().generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        # Decoder swapped from JSON default to the Confluent Avro one.
        assert sc["stream.kafka.decoder.class.name"] == KAFKA_AVRO_REGISTRY_DECODER
        # And the schema-registry URL threaded all the way through.
        assert (
            sc["stream.kafka.decoder.prop.schema.registry.rest.url"]
            == "http://schema-registry-prod:8081"
        )
        # No accidental JSON decoder keys lingering.
        assert "JSONMessageDecoder" not in sc["stream.kafka.decoder.class.name"]

    def test_existing_json_kafka_fixture_unchanged(self):
        # Backward compatibility: the raw_stream Kafka fixture (JSON
        # decoder) must still produce JSONMessageDecoder in v0.11+.
        raw = json.loads((FIXTURES / "raw_stream" / "spec.json").read_text())
        canonical = _canonical(raw)
        assert canonical.input_format == "json"
        table = PinotTableGenerator().generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kafka.decoder.class.name"] == KAFKA_JSON_DECODER
        # And no Avro decoder-prop keys snuck in.
        assert not any(
            k.startswith("stream.kafka.decoder.prop.") for k in sc
        )

    def test_inline_schema_emits_simple_decoder(self):
        # Build an Avro+Kafka spec on the fly with schema_inline, since
        # we don't ship a dedicated fixture for it — the runtime path
        # is what matters.
        spec = json.loads((FIXTURES / "avro_stream" / "spec.json").read_text())
        spec["spec"]["ioConfig"]["inputFormat"]["avroBytesDecoder"] = {
            "type": "schema_inline",
            "schema": {"type": "record", "name": "X", "fields": []},
        }
        canonical = _canonical(spec)
        table = PinotTableGenerator().generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kafka.decoder.class.name"] == KAFKA_AVRO_SIMPLE_DECODER
        # The inline schema rides through as a JSON string under the
        # Pinot-specific decoder.prop.schema key.
        assert "stream.kafka.decoder.prop.schema" in sc
