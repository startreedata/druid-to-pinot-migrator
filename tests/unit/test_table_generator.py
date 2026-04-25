from __future__ import annotations

import json
from pathlib import Path

from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.pinot.table_generator import PinotTableGenerator

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _canonical_from_fixture(name: str):
    raw = json.loads((FIXTURES / name / "spec.json").read_text())
    parser = DruidSpecParser()
    parse_result = parser.parse(raw)
    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)
    return norm_result.canonical


class TestPinotTableGenerator:
    def setup_method(self):
        self.gen = PinotTableGenerator()

    def test_batch_generates_offline_table(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        assert table["tableType"] == "OFFLINE"

    def test_stream_generates_realtime_table(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        assert table["tableType"] == "REALTIME"

    def test_table_name_contains_datasource_offline(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        assert "pageviews" in table["tableName"]

    def test_table_name_contains_datasource_realtime(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        assert "clickstream" in table["tableName"]

    def test_offline_table_has_segments_config(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        assert "segmentsConfig" in table
        assert table["segmentsConfig"]["replication"] == "1"

    def test_offline_table_has_retention(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        seg_cfg = table["segmentsConfig"]
        assert "retentionTimeUnit" in seg_cfg
        assert "retentionTimeValue" in seg_cfg

    def test_realtime_table_has_stream_configs(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        stream_configs = table["tableIndexConfig"]["streamConfigs"]
        assert stream_configs["streamType"] == "kafka"
        assert "stream.kafka.topic.name" in stream_configs
        assert "stream.kafka.broker.list" in stream_configs
        assert "stream.kafka.consumer.type" in stream_configs
        assert "stream.kafka.decoder.class.name" in stream_configs

    def test_realtime_topic_from_io_config(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        stream_configs = table["tableIndexConfig"]["streamConfigs"]
        assert stream_configs["stream.kafka.topic.name"] == "clickstream-events"

    def test_generate_dispatches_to_offline_for_batch(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate(canonical)
        assert table["tableType"] == "OFFLINE"

    def test_generate_dispatches_to_realtime_for_stream(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate(canonical)
        assert table["tableType"] == "REALTIME"

    def test_offline_table_name_has_offline_suffix(self):
        canonical = _canonical_from_fixture("raw_batch")
        table = self.gen.generate_offline(canonical)
        assert table["tableName"].endswith("_OFFLINE")

    def test_realtime_table_name_has_realtime_suffix(self):
        canonical = _canonical_from_fixture("raw_stream")
        table = self.gen.generate_realtime(canonical)
        assert table["tableName"].endswith("_REALTIME")
