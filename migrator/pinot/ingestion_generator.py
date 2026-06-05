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
# The ``azure`` key maps the Druid scheme to Pinot's ADLS Gen2 plugin
# (Pinot's scheme is ``adl2``, not ``azure``); ``adl2`` is also present
# so an already-rewritten URI dispatches correctly.
_PINOT_FS: dict[str, tuple[str, str]] = {
    "file":  ("file", "org.apache.pinot.spi.filesystem.LocalPinotFS"),
    "s3":    ("s3",   "org.apache.pinot.plugin.filesystem.S3PinotFS"),
    "gs":    ("gs",   "org.apache.pinot.plugin.filesystem.GcsPinotFS"),
    "gcs":   ("gs",   "org.apache.pinot.plugin.filesystem.GcsPinotFS"),
    "hdfs":  ("hdfs", "org.apache.pinot.plugin.filesystem.HadoopPinotFS"),
    "azure": ("adl2", "org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS"),
    "adl2":  ("adl2", "org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS"),
    "abfss": ("adl2", "org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS"),
}


# Placeholder values for pinotFSSpec ``configs`` keys that can't be
# recovered from the Druid spec. Deliberately loud + greppable: an
# unreplaced value makes Pinot fail fast (invalid region / project /
# account) rather than silently read from the wrong place.
_REPLACE_REGION = "REPLACE_WITH_AWS_REGION"
_REPLACE_GCP_PROJECT = "REPLACE_WITH_GCP_PROJECT_ID"
_REPLACE_AZURE_ACCOUNT = "REPLACE_WITH_AZURE_STORAGE_ACCOUNT"
_REPLACE_AZURE_FS = "REPLACE_WITH_AZURE_FILESYSTEM"


def _azure_filesystem_from_uri(uri: str) -> str | None:
    """Pull the container / filesystem name out of a Druid
    ``azure://<container>/<path>`` URI. For ADLS Gen2 the container IS
    the filesystem, so this maps straight to ``fileSystemName``.
    Returns None for non-azure / malformed URIs."""
    if "://" not in uri:
        return None
    scheme, rest = uri.split("://", 1)
    if scheme.lower() not in ("azure", "adl2", "abfss"):
        return None
    host = rest.split("/", 1)[0]
    # ``abfss://fs@account.dfs.core.windows.net/...`` — strip the
    # ``@account...`` suffix if present; the filesystem is before the @.
    host = host.split("@", 1)[0]
    return host or None


def _rewrite_azure_uri(uri: str) -> str:
    """Rewrite a Druid ``azure://`` URI to Pinot's ``adl2://`` scheme so
    it matches the registered ADLSGen2PinotFS. Leaves already-``adl2`` /
    ``abfss`` URIs untouched, and non-azure URIs unchanged."""
    if uri.startswith("azure://"):
        return "adl2://" + uri[len("azure://"):]
    return uri


def _pinot_fs_configs(scheme: str, input_source: dict | None) -> dict[str, str]:
    """Build the ``configs`` block for a pinotFSSpec.

    Returns only the *non-secret* structural keys Pinot needs and that
    can't be sourced from the server's ambient environment — region,
    project, account, filesystem. Credentials (S3 access/secret keys,
    GCS ``gcpKey``, Azure access key / SAS) are deliberately omitted:
    production Pinot servers carry them via IAM roles, workload
    identity, or env vars, and writing them into the committed
    ``batch-job.json`` would leak a secret into source control — the
    same stance the Kinesis stream config takes.

    Values are derived from the Druid ``inputSource`` when present
    (rare — most live in Druid's *global* runtime props, not the spec),
    otherwise emitted as a loud ``REPLACE_WITH_*`` placeholder.
    Local / HDFS schemes need no configs and get an empty dict.
    """
    src = input_source or {}
    if scheme == "s3":
        region = (
            src.get("region")
            or (src.get("properties") or {}).get("region")
            or (src.get("endpointConfig") or {}).get("region")
        )
        return {"region": region or _REPLACE_REGION}
    if scheme == "gs":
        return {"projectId": src.get("projectId") or _REPLACE_GCP_PROJECT}
    if scheme == "adl2":
        account = src.get("account") or src.get("accountName")
        fs_name = src.get("fileSystemName") or src.get("container")
        if not fs_name:
            uris = src.get("uris") or []
            if uris:
                fs_name = _azure_filesystem_from_uri(uris[0])
        return {
            "accountName": account or _REPLACE_AZURE_ACCOUNT,
            "fileSystemName": fs_name or _REPLACE_AZURE_FS,
        }
    return {}


def _pinot_fs_spec(
    input_uri: str, input_source: dict | None = None,
) -> dict:
    """Return the right pinotFSSpec entry for the given URI's scheme.
    Defaults to LocalPinotFS for unknown / scheme-less paths.

    When the resolved scheme needs structural config (object stores),
    a ``configs`` block is attached — derived from ``input_source``
    where possible, placeholder otherwise. Local / HDFS get no
    ``configs`` key so their artifacts stay byte-identical to before.
    """
    scheme = ""
    if "://" in input_uri:
        scheme = input_uri.split("://", 1)[0].lower()
    fs = _PINOT_FS.get(scheme, _PINOT_FS["file"])
    spec: dict = {"scheme": fs[0], "className": fs[1]}
    configs = _pinot_fs_configs(fs[0], input_source)
    if configs:
        spec["configs"] = configs
    return spec


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
        # ``baseDir`` (local), ``uris`` (S3/GCS/Azure), or fallback.
        input_dir = input_source.get("baseDir", "/data/input")
        if input_type == "s3":
            uris = input_source.get("uris", [])
            input_dir = uris[0] if uris else "s3://your-bucket/path/"
        elif input_type in ("google", "gs"):
            uris = input_source.get("uris", [])
            input_dir = uris[0] if uris else "gs://your-bucket/path/"
        elif input_type in ("azure", "abs", "adl2", "abfss"):
            uris = input_source.get("uris", [])
            raw = uris[0] if uris else "azure://your-container/path/"
            # Pinot's ADLS Gen2 plugin registers the ``adl2`` scheme, so
            # the inputDirURI must use it too or no PinotFS will claim
            # the URI at deploy time.
            input_dir = _rewrite_azure_uri(raw)

        return {
            "jobType": "SegmentCreationAndTarPush",
            "inputDirURI": input_dir,
            "outputDirURI": f"/tmp/pinot-output/{canonical.datasource_name}",
            "overwriteOutput": True,
            "pinotFSSpecs": [_pinot_fs_spec(input_dir, input_source)],
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
