from __future__ import annotations

import json
from pathlib import Path

from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.pinot.schema_generator import PinotSchemaGenerator

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _canonical_from_fixture(name: str):
    raw = json.loads((FIXTURES / name / "spec.json").read_text())
    parser = DruidSpecParser()
    parse_result = parser.parse(raw)
    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)
    return norm_result.canonical


class TestPinotSchemaGenerator:
    def setup_method(self):
        self.gen = PinotSchemaGenerator()

    def test_schema_name_matches_datasource(self):
        canonical = _canonical_from_fixture("raw_batch")
        schema = self.gen.generate(canonical)
        assert schema["schemaName"] == "pageviews"

    def test_time_field_in_datetime_specs(self):
        canonical = _canonical_from_fixture("raw_batch")
        schema = self.gen.generate(canonical)
        assert len(schema["dateTimeFieldSpecs"]) >= 1
        assert schema["dateTimeFieldSpecs"][0]["name"] == "timestamp"

    def test_dimensions_in_dimension_field_specs(self):
        canonical = _canonical_from_fixture("raw_batch")
        schema = self.gen.generate(canonical)
        dim_names = {d["name"] for d in schema["dimensionFieldSpecs"]}
        assert "page" in dim_names
        assert "user" in dim_names
        assert "region" in dim_names

    def test_metrics_in_metric_field_specs(self):
        canonical = _canonical_from_fixture("rolled_up")
        schema = self.gen.generate(canonical)
        metric_names = {m["name"] for m in schema["metricFieldSpecs"]}
        assert "impressions" in metric_names
        assert "clicks" in metric_names
        assert "revenue" in metric_names

    def test_empty_metrics_for_raw(self):
        canonical = _canonical_from_fixture("raw_batch")
        schema = self.gen.generate(canonical)
        assert schema["metricFieldSpecs"] == []

    def test_dimensions_sorted_alphabetically(self):
        canonical = _canonical_from_fixture("raw_batch")
        schema = self.gen.generate(canonical)
        dim_names = [d["name"] for d in schema["dimensionFieldSpecs"]]
        assert dim_names == sorted(dim_names)

    def test_metrics_sorted_alphabetically(self):
        canonical = _canonical_from_fixture("rolled_up")
        schema = self.gen.generate(canonical)
        metric_names = [m["name"] for m in schema["metricFieldSpecs"]]
        assert metric_names == sorted(metric_names)

    def test_ordering_is_deterministic(self):
        """Two calls to generate() on the same canonical should produce identical output."""
        canonical = _canonical_from_fixture("rolled_up")
        schema1 = self.gen.generate(canonical)
        schema2 = self.gen.generate(canonical)
        assert json.dumps(schema1, sort_keys=True) == json.dumps(schema2, sort_keys=True)

    def test_millis_time_format(self):
        canonical = _canonical_from_fixture("raw_stream")
        schema = self.gen.generate(canonical)
        dt_spec = schema["dateTimeFieldSpecs"][0]
        assert dt_spec["name"] == "event_time"
        assert "EPOCH" in dt_spec["format"]

    def test_iso_time_format(self):
        canonical = _canonical_from_fixture("raw_batch")
        schema = self.gen.generate(canonical)
        dt_spec = schema["dateTimeFieldSpecs"][0]
        assert "SIMPLE_DATE_FORMAT" in dt_spec["format"] or "EPOCH" in dt_spec["format"]

    def test_complex_metrics_appear_as_bytes(self):
        canonical = _canonical_from_fixture("unsupported_complex")
        schema = self.gen.generate(canonical)
        bytes_metrics = [m for m in schema["metricFieldSpecs"] if m["dataType"] == "BYTES"]
        assert len(bytes_metrics) > 0
