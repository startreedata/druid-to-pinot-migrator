from __future__ import annotations

from migrator.core.models import CanonicalMigrationModel


class PinotIngestionGenerator:
    """Generate Pinot ingestion job specs."""

    def generate_batch_job(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate a Pinot offline batch ingestion job spec."""
        io = canonical.raw_io_config or {}
        input_source = io.get("inputSource", {})
        input_type = input_source.get("type", "local")

        # Map Druid input source types to Pinot equivalents
        pinot_input_format = "json"
        input_dir = input_source.get("baseDir", "/data/input")
        if input_type == "s3":
            uris = input_source.get("uris", [])
            input_dir = uris[0] if uris else "s3://your-bucket/path/"

        return {
            "jobType": "SegmentCreationAndTarPush",
            "inputDirURI": input_dir,
            "outputDirURI": f"/tmp/pinot-output/{canonical.datasource_name}",
            "overwriteOutput": True,
            "pinotFSSpecs": [
                {
                    "scheme": "file",
                    "className": "org.apache.pinot.spi.filesystem.LocalPinotFS",
                }
            ],
            "recordReaderSpec": {
                "dataFormat": pinot_input_format,
                "className": "org.apache.pinot.plugin.inputformat.json.JSONRecordReader",
            },
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
