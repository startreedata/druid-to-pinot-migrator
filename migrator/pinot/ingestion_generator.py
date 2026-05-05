from __future__ import annotations

from migrator.core.models import CanonicalMigrationModel


# Pinot RecordReader plugin map: canonical input_format → (dataFormat
# string the batch-job spec wants, FQCN of the RecordReader class).
# ``json`` and ``csv`` ship as part of the core Pinot distribution;
# the others are loadable plugins that an operator must drop into
# ``${PINOT_HOME}/plugins/`` if not already present (the standard
# apachepinot/pinot Docker image carries them all).
_PINOT_RECORD_READERS: dict[str, tuple[str, str]] = {
    "json": (
        "json",
        "org.apache.pinot.plugin.inputformat.json.JSONRecordReader",
    ),
    "parquet": (
        "parquet",
        "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader",
    ),
    "avro": (
        "avro",
        "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader",
    ),
    "orc": (
        "orc",
        "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader",
    ),
    "csv": (
        "csv",
        "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader",
    ),
    "protobuf": (
        "proto",
        "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReader",
    ),
}


def _record_reader_spec(input_format: str) -> dict[str, str]:
    """Pick the right Pinot RecordReader for the canonical input_format.

    Falls back to JSON for anything not in the table above — the
    normalizer has already emitted a warning at that point, so we
    don't spam the artifact with another one.
    """
    data_format, class_name = _PINOT_RECORD_READERS.get(
        input_format, _PINOT_RECORD_READERS["json"],
    )
    return {"dataFormat": data_format, "className": class_name}


# Pinot input-source ``scheme`` registry — needed when the source URI
# is on object storage. Wired as ``pinotFSSpecs`` on the batch-job spec.
_PINOT_FS: dict[str, tuple[str, str]] = {
    "file":  ("file", "org.apache.pinot.spi.filesystem.LocalPinotFS"),
    "s3":    ("s3",   "org.apache.pinot.plugin.filesystem.S3PinotFS"),
    "gs":    ("gs",   "org.apache.pinot.plugin.filesystem.GcsPinotFS"),
    "gcs":   ("gs",   "org.apache.pinot.plugin.filesystem.GcsPinotFS"),
    "hdfs":  ("hdfs", "org.apache.pinot.plugin.filesystem.HadoopPinotFS"),
}


def _pinot_fs_spec(input_uri: str) -> dict[str, str]:
    """Return the right pinotFSSpec entry for the given URI's scheme.
    Defaults to LocalPinotFS for unknown / scheme-less paths."""
    scheme = ""
    if "://" in input_uri:
        scheme = input_uri.split("://", 1)[0].lower()
    fs = _PINOT_FS.get(scheme, _PINOT_FS["file"])
    return {"scheme": fs[0], "className": fs[1]}


class PinotIngestionGenerator:
    """Generate Pinot ingestion job specs."""

    def generate_batch_job(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate a Pinot offline batch ingestion job spec.

        The RecordReader is picked from ``canonical.input_format`` —
        JSON, Parquet, Avro, ORC, CSV, or Protobuf are all wired up
        with the correct FQCN so the resulting job spec is
        copy-pasteable into ``LaunchDataIngestionJob``.
        """
        io = canonical.raw_io_config or {}
        input_source = io.get("inputSource", {})
        input_type = input_source.get("type", "local")

        # Resolve the input directory from the Druid source spec.
        # ``baseDir`` (local), ``uris`` (S3/GCS/HTTP), or fallback.
        input_dir = input_source.get("baseDir", "/data/input")
        if input_type == "s3":
            uris = input_source.get("uris", [])
            input_dir = uris[0] if uris else "s3://your-bucket/path/"
        elif input_type in ("google", "gs"):
            uris = input_source.get("uris", [])
            input_dir = uris[0] if uris else "gs://your-bucket/path/"

        return {
            "jobType": "SegmentCreationAndTarPush",
            "inputDirURI": input_dir,
            "outputDirURI": f"/tmp/pinot-output/{canonical.datasource_name}",
            "overwriteOutput": True,
            "pinotFSSpecs": [_pinot_fs_spec(input_dir)],
            "recordReaderSpec": _record_reader_spec(canonical.input_format),
            "tableSpec": {
                "tableName": canonical.datasource_name,
                "schemaURI": f"http://localhost:9000/schemas/{canonical.datasource_name}",
                "tableConfigURI": f"http://localhost:9000/tables/{canonical.datasource_name}",
            },
            "pinotClusterSpecs": [
                {
                    "controllerURI": "http://localhost:9000",
                }
            ],
        }

    def generate_stream_config(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate a Pinot stream ingestion config snippet."""
        io = canonical.raw_io_config or {}
        consumer_props = io.get("consumerProperties", {})
        broker_list = consumer_props.get("bootstrap.servers", "localhost:9092")
        topic = io.get("topic", canonical.datasource_name)

        return {
            "streamType": "kafka",
            "stream.kafka.topic.name": topic,
            "stream.kafka.broker.list": broker_list,
            "stream.kafka.consumer.type": "lowlevel",
            "stream.kafka.consumer.factory.class.name": (
                "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory"
            ),
            "stream.kafka.decoder.class.name": (
                "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"
            ),
            "realtime.segment.flush.threshold.rows": "1000000",
            "realtime.segment.flush.threshold.time": "1h",
        }
