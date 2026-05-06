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
    KAFKA_PROTOBUF_FILE_DECODER,
    KAFKA_PROTOBUF_REGISTRY_DECODER,
    PinotTableGenerator,
    avro_decoder_config_from_io,
    protobuf_decoder_config_from_io,
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


class TestEndToEndOrcBatch:
    """The ``orc_input`` fixture mirrors ``parquet_input`` but with a
    GCS source — exercises the GcsPinotFS dispatch as a side effect."""

    def test_orc_fixture_produces_orc_record_reader(self):
        raw = json.loads((FIXTURES / "orc_input" / "spec.json").read_text())
        canonical = _canonical(raw)
        assert canonical.input_format == "orc"
        job = PinotIngestionGenerator().generate_batch_job(canonical)
        assert job["recordReaderSpec"]["dataFormat"] == "orc"
        assert "ORCRecordReader" in job["recordReaderSpec"]["className"]

    def test_orc_fixture_emits_gcs_pinot_fs(self):
        # Druid spec uses ``inputSource.type=google`` with gs:// URIs;
        # the generator should pick GcsPinotFS, not LocalPinotFS.
        raw = json.loads((FIXTURES / "orc_input" / "spec.json").read_text())
        canonical = _canonical(raw)
        job = PinotIngestionGenerator().generate_batch_job(canonical)
        fs = job["pinotFSSpecs"][0]
        assert fs["scheme"] == "gs"
        assert "GcsPinotFS" in fs["className"]
        assert job["inputDirURI"].startswith("gs://prod-warehouse/")


class TestEndToEndCsvBatch:
    """CSV is the ``oddball`` of the supported formats — Druid carries
    extra knobs (``columns``, ``delimiter``, ``skipHeaderRows``) that
    Pinot's CSVRecordReader can read out of the same dispatch. The
    fixture is local-FS to lock the LocalPinotFS path too."""

    def test_csv_fixture_produces_csv_record_reader(self):
        raw = json.loads((FIXTURES / "csv_input" / "spec.json").read_text())
        canonical = _canonical(raw)
        assert canonical.input_format == "csv"
        job = PinotIngestionGenerator().generate_batch_job(canonical)
        assert job["recordReaderSpec"]["dataFormat"] == "csv"
        assert "CSVRecordReader" in job["recordReaderSpec"]["className"]

    def test_csv_fixture_uses_local_pinot_fs(self):
        raw = json.loads((FIXTURES / "csv_input" / "spec.json").read_text())
        canonical = _canonical(raw)
        job = PinotIngestionGenerator().generate_batch_job(canonical)
        assert job["pinotFSSpecs"][0]["scheme"] == "file"
        assert "LocalPinotFS" in job["pinotFSSpecs"][0]["className"]


class TestAllFixtureInputFormatCoverage:
    """Belt-and-suspenders: assert every input format mentioned in
    ``_PINOT_RECORD_READERS`` has at least one fixture-backed
    end-to-end test in this module. If someone adds a new format
    without a fixture, this test fails loudly so the gap can't go
    unnoticed.

    JSON is the implicit default and is exercised by every existing
    Druid fixture (raw_batch / raw_stream / etc), so it's allowed to
    not have a dedicated ``json_input`` directory.
    """
    EXPECTED_FIXTURE_DIRS = {
        # input_format → at least one fixture directory under tests/fixtures/
        "json":     {"raw_batch", "raw_stream"},
        "parquet":  {"parquet_input"},
        "avro":     {"avro_ocf", "avro_stream", "avro_stream_auth"},
        "orc":      {"orc_input"},
        "csv":      {"csv_input"},
        "protobuf": {"protobuf_stream", "protobuf_file"},
    }

    @pytest.mark.parametrize("input_format, expected_dirs", list(EXPECTED_FIXTURE_DIRS.items()))
    def test_each_format_has_at_least_one_fixture(
        self, input_format: str, expected_dirs: set[str],
    ):
        present = {p.name for p in FIXTURES.iterdir() if p.is_dir()}
        overlap = expected_dirs & present
        assert overlap, (
            f"input_format={input_format!r} has no fixture directory "
            f"under tests/fixtures/. Expected one of: {sorted(expected_dirs)}. "
            f"Add a fixture and an end-to-end test in test_input_format.py."
        )


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


# ─────────────────────────────────────────────────────────────────────────────
# Avro schema registry — auth + multi-URL + capacity
# ─────────────────────────────────────────────────────────────────────────────


class TestAvroSchemaRegistryAuth:
    def test_basic_auth_user_info_threaded(self):
        # Camel-case is the spelling Druid >= 0.22 uses.
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {
                "type": "schema_registry",
                "url": "http://sr:8081",
                "config": {
                    "basicAuthCredentialsSource": "USER_INFO",
                    "basicAuthUserInfo": "u:p",
                },
            },
        }}
        _, props = avro_decoder_config_from_io(io)
        assert props["basic.auth.credentials.source"] == "USER_INFO"
        assert props["basic.auth.user.info"] == "u:p"

    def test_basic_auth_dotted_keys_also_supported(self):
        # Older Druid configs use the Pinot-style dotted key directly.
        # We accept both spellings so dpm doesn't force a hand-edit.
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {
                "type": "schema_registry",
                "url": "http://sr:8081",
                "config": {
                    "basic.auth.credentials.source": "USER_INFO",
                    "basic.auth.user.info": "u:p",
                },
            },
        }}
        _, props = avro_decoder_config_from_io(io)
        assert props["basic.auth.credentials.source"] == "USER_INFO"
        assert props["basic.auth.user.info"] == "u:p"

    def test_urls_array_comma_joined(self):
        # HA registries: Druid takes ``urls`` array; Pinot takes the
        # same comma-joined into ``schema.registry.rest.url``.
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {
                "type": "schema_registry",
                "urls": ["http://sr-1:8081", "http://sr-2:8081"],
            },
        }}
        _, props = avro_decoder_config_from_io(io)
        assert props["schema.registry.rest.url"] == \
            "http://sr-1:8081,http://sr-2:8081"

    def test_url_singular_wins_over_urls_array(self):
        # When both fields are set (common when migrating off an old
        # config), the singular form wins — that's the field Druid
        # treats as authoritative.
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {
                "type": "schema_registry",
                "url": "http://primary:8081",
                "urls": ["http://other:8081"],
            },
        }}
        _, props = avro_decoder_config_from_io(io)
        assert props["schema.registry.rest.url"] == "http://primary:8081"

    def test_capacity_threaded(self):
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {
                "type": "schema_registry",
                "url": "http://sr:8081",
                "capacity": 5000,
            },
        }}
        _, props = avro_decoder_config_from_io(io)
        assert props["schema.registry.cache.capacity"] == "5000"

    def test_no_auth_no_capacity_no_extra_props(self):
        # Auth-less registries are common in dev — no spurious props
        # should leak into the output for them.
        io = {"inputFormat": {
            "type": "avro_stream",
            "avroBytesDecoder": {
                "type": "schema_registry",
                "url": "http://sr:8081",
            },
        }}
        _, props = avro_decoder_config_from_io(io)
        assert set(props.keys()) == {"schema.registry.rest.url"}


class TestAvroStreamAuthFixture:
    """End-to-end via the avro_stream_auth fixture (multi-URL + auth)."""

    def test_fixture_produces_full_decoder_props(self):
        raw = json.loads(
            (FIXTURES / "avro_stream_auth" / "spec.json").read_text()
        )
        canonical = _canonical(raw)
        assert canonical.input_format == "avro"
        table = PinotTableGenerator().generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        # Multi-URL → comma-joined.
        assert sc["stream.kafka.decoder.prop.schema.registry.rest.url"] == (
            "http://schema-registry-1:8081,http://schema-registry-2:8081"
        )
        # Auth carried through.
        assert sc["stream.kafka.decoder.prop.basic.auth.credentials.source"] == "USER_INFO"
        assert sc["stream.kafka.decoder.prop.basic.auth.user.info"] == "client:s3cret"
        # Cache capacity.
        assert sc["stream.kafka.decoder.prop.schema.registry.cache.capacity"] == "1000"


# ─────────────────────────────────────────────────────────────────────────────
# Protobuf streaming — Confluent registry + descriptor file
# ─────────────────────────────────────────────────────────────────────────────


class TestProtobufDecoderConfigFromIo:
    def test_schema_registry_picks_confluent_decoder(self):
        io = {"inputFormat": {
            "type": "protobuf",
            "protoBytesDecoder": {
                "type": "schema_registry",
                "url": "http://sr:8081",
                "protoMessageType": "MyEvent",
            },
        }}
        klass, props = protobuf_decoder_config_from_io(io)
        assert klass == KAFKA_PROTOBUF_REGISTRY_DECODER
        assert props["schema.registry.rest.url"] == "http://sr:8081"
        # Protobuf-specific: the message type is required by the
        # Confluent decoder, mapped to ``schemaName``.
        assert props["schemaName"] == "MyEvent"

    def test_file_decoder_with_descriptor(self):
        io = {"inputFormat": {
            "type": "protobuf",
            "protoBytesDecoder": {
                "type": "file",
                "descriptor": "/data/proto.desc",
                "protoMessageType": "MyEvent",
            },
        }}
        klass, props = protobuf_decoder_config_from_io(io)
        assert klass == KAFKA_PROTOBUF_FILE_DECODER
        # Pinot's file-based protobuf decoder uses these EXACT prop
        # keys; getting the names wrong silently no-ops the decoder.
        assert props["descriptorFile"] == "/data/proto.desc"
        assert props["protoClassName"] == "MyEvent"

    def test_registry_inherits_auth_from_helper(self):
        # The Avro registry-prop helper is shared, so auth on a
        # protobuf registry config should land in the same Pinot
        # decoder.prop keys as on Avro.
        io = {"inputFormat": {
            "type": "protobuf",
            "protoBytesDecoder": {
                "type": "schema_registry",
                "url": "http://sr:8081",
                "protoMessageType": "X",
                "config": {
                    "basicAuthCredentialsSource": "USER_INFO",
                    "basicAuthUserInfo": "u:p",
                },
            },
        }}
        _, props = protobuf_decoder_config_from_io(io)
        assert props["basic.auth.credentials.source"] == "USER_INFO"
        assert props["basic.auth.user.info"] == "u:p"

    def test_unknown_decoder_type_falls_back_to_registry(self):
        io = {"inputFormat": {
            "type": "protobuf",
            "protoBytesDecoder": {"type": "weird"},
        }}
        klass, _ = protobuf_decoder_config_from_io(io)
        assert klass == KAFKA_PROTOBUF_REGISTRY_DECODER

    def test_no_protoBytesDecoder_block_returns_registry_with_empty_props(self):
        # Druid spec without protoBytesDecoder is technically invalid
        # but operators do this when they're still wiring things up;
        # we shouldn't crash, just return the default decoder.
        io = {"inputFormat": {"type": "protobuf"}}
        klass, props = protobuf_decoder_config_from_io(io)
        assert klass == KAFKA_PROTOBUF_REGISTRY_DECODER
        assert props == {}


class TestProtobufStreamFixtures:
    def test_protobuf_stream_registry_fixture(self):
        raw = json.loads(
            (FIXTURES / "protobuf_stream" / "spec.json").read_text()
        )
        canonical = _canonical(raw)
        assert canonical.input_format == "protobuf"
        assert canonical.source_kind == "stream"
        table = PinotTableGenerator().generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kafka.decoder.class.name"] == KAFKA_PROTOBUF_REGISTRY_DECODER
        assert sc["stream.kafka.decoder.prop.schemaName"] == "TelemetryEvent"
        assert (
            sc["stream.kafka.decoder.prop.schema.registry.rest.url"]
            == "http://schema-registry-prod:8081"
        )

    def test_protobuf_file_fixture(self):
        raw = json.loads(
            (FIXTURES / "protobuf_file" / "spec.json").read_text()
        )
        canonical = _canonical(raw)
        assert canonical.input_format == "protobuf"
        table = PinotTableGenerator().generate_realtime(canonical)
        sc = table["tableIndexConfig"]["streamConfigs"]
        assert sc["stream.kafka.decoder.class.name"] == KAFKA_PROTOBUF_FILE_DECODER
        assert sc["stream.kafka.decoder.prop.descriptorFile"] == \
            "/etc/pinot/metrics.desc"
        assert sc["stream.kafka.decoder.prop.protoClassName"] == "Metric"


class TestProtobufStreamWarnings:
    """Normalizer-side warnings for protobuf streams missing config."""

    def _kafka_spec_with_proto(self, **proto_decoder) -> dict:
        spec = {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "x",
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": ["a"]},
                    "metricsSpec": [],
                    "granularitySpec": {
                        "segmentGranularity": "HOUR", "rollup": False,
                    },
                },
                "ioConfig": {
                    "type": "kafka",
                    "topic": "t",
                    "consumerProperties": {"bootstrap.servers": "k:9092"},
                    "inputFormat": {
                        "type": "protobuf",
                        "protoBytesDecoder": proto_decoder,
                    },
                },
            },
        }
        return spec

    def test_schema_registry_missing_url_warns(self):
        result = _normalize(self._kafka_spec_with_proto(
            type="schema_registry", protoMessageType="X",
        ))
        assert any(
            "schema_registry" in w and "url" in w for w in result.warnings
        )

    def test_schema_registry_missing_protoMessageType_warns(self):
        result = _normalize(self._kafka_spec_with_proto(
            type="schema_registry", url="http://sr:8081",
        ))
        assert any(
            "protoMessageType" in w for w in result.warnings
        )

    def test_file_missing_descriptor_warns(self):
        result = _normalize(self._kafka_spec_with_proto(
            type="file", protoMessageType="X",
        ))
        assert any(
            "descriptor" in w.lower() for w in result.warnings
        )

    def test_file_missing_protoMessageType_warns(self):
        result = _normalize(self._kafka_spec_with_proto(
            type="file", descriptor="/foo.desc",
        ))
        assert any(
            "protoMessageType" in w for w in result.warnings
        )

    def test_unknown_decoder_type_warns(self):
        result = _normalize(self._kafka_spec_with_proto(type="weird"))
        assert any(
            "weird" in w and "schema_registry" in w for w in result.warnings
        )

    def test_batch_protobuf_does_not_emit_stream_warnings(self):
        # A batch protobuf spec (not source_kind=stream) shouldn't
        # trigger the stream-specific warning matrix.
        spec = {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "x",
                    "timestampSpec": {"column": "ts", "format": "millis"},
                    "dimensionsSpec": {"dimensions": ["a"]},
                    "metricsSpec": [],
                    "granularitySpec": {
                        "segmentGranularity": "DAY", "rollup": False,
                    },
                },
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": {"type": "local", "baseDir": "/data"},
                    "inputFormat": {"type": "protobuf"},
                },
            },
        }
        result = _normalize(spec)
        # No proto-stream-specific warnings — the batch path uses
        # ProtoBufRecordReader, not the Kafka decoder.
        assert not any(
            "protoBytesDecoder" in w or "schemaName" in w
            for w in result.warnings
        )
