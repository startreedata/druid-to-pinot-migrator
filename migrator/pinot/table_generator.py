from __future__ import annotations

from migrator.core.enums import SourceKind
from migrator.core.models import CanonicalMigrationModel


class PinotTableGenerator:
    """Generate Pinot offline and realtime table config dicts."""

    def generate_offline(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate an OFFLINE table configuration."""
        time_column = canonical.time_field.column_name if canonical.time_field else "__time"
        table_name = f"{canonical.datasource_name}_OFFLINE"

        return {
            "tableName": table_name,
            "tableType": "OFFLINE",
            "segmentsConfig": {
                "timeColumnName": time_column,
                "timeType": "MILLISECONDS",
                "replication": "1",
                "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {
                "broker": "DefaultTenant",
                "server": "DefaultTenant",
            },
            "tableIndexConfig": {
                "loadMode": "MMAP",
            },
            "ingestionConfig": {
                "batchIngestionConfig": {
                    "segmentIngestionType": "APPEND",
                    "segmentIngestionFrequency": "DAILY",
                }
            },
            "metadata": {
                "customConfigs": {}
            },
        }

    def generate_realtime(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate a REALTIME table configuration."""
        time_column = canonical.time_field.column_name if canonical.time_field else "__time"
        table_name = f"{canonical.datasource_name}_REALTIME"

        # Extract kafka info from raw io_config if available
        io = canonical.raw_io_config or {}
        consumer_props = io.get("consumerProperties", {})
        broker_list = consumer_props.get("bootstrap.servers", "localhost:9092")
        topic = io.get("topic", canonical.datasource_name)

        stream_configs = {
            "streamType": "kafka",
            "stream.kafka.topic.name": topic,
            "stream.kafka.broker.list": broker_list,
            "stream.kafka.consumer.type": "lowlevel",
            "stream.kafka.consumer.factory.class.name": (
                "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory"
            ),
            "stream.kafka.decoder.class.name": (
                "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"
            ),
            "realtime.segment.flush.threshold.rows": "1000000",
            "realtime.segment.flush.threshold.time": "1h",
        }

        return {
            "tableName": table_name,
            "tableType": "REALTIME",
            "segmentsConfig": {
                "timeColumnName": time_column,
                "timeType": "MILLISECONDS",
                "replication": "1",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {
                "broker": "DefaultTenant",
                "server": "DefaultTenant",
                "tagOverrideConfig": {},
            },
            "tableIndexConfig": {
                "loadMode": "MMAP",
                "streamConfigs": stream_configs,
            },
            "metadata": {
                "customConfigs": {}
            },
        }

    def generate(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate the appropriate table config based on source kind."""
        if canonical.source_kind == SourceKind.STREAM.value:
            return self.generate_realtime(canonical)
        return self.generate_offline(canonical)
