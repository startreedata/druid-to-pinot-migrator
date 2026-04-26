from __future__ import annotations

import json
from pathlib import Path

from migrator.core.enums import DatasourceClassification
from migrator.druid.classifiers import classify_datasource
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _canonical_from_fixture(name: str):
    raw = json.loads((FIXTURES / name / "spec.json").read_text())
    parser = DruidSpecParser()
    parse_result = parser.parse(raw)
    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)
    return norm_result.canonical


class TestClassifier:
    def test_raw_batch_is_raw_event(self):
        canonical = _canonical_from_fixture("raw_batch")
        result = classify_datasource(canonical)
        assert result == DatasourceClassification.RAW_EVENT

    def test_raw_stream_is_raw_event(self):
        canonical = _canonical_from_fixture("raw_stream")
        result = classify_datasource(canonical)
        assert result == DatasourceClassification.RAW_EVENT

    def test_rolled_up_is_rolled_up_additive(self):
        canonical = _canonical_from_fixture("rolled_up")
        result = classify_datasource(canonical)
        assert result == DatasourceClassification.ROLLED_UP_ADDITIVE

    def test_unsupported_complex_is_complex_aggregated(self):
        canonical = _canonical_from_fixture("unsupported_complex")
        result = classify_datasource(canonical)
        assert result == DatasourceClassification.COMPLEX_AGGREGATED

    def test_transforms_fixture_is_raw_event(self):
        """transforms fixture has no rollup and only simple metrics -> RAW_EVENT."""
        canonical = _canonical_from_fixture("transforms")
        result = classify_datasource(canonical)
        assert result == DatasourceClassification.RAW_EVENT
