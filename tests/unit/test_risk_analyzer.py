from __future__ import annotations

import json
from pathlib import Path

from migrator.core.enums import RiskSeverity
from migrator.druid.classifiers import classify_datasource
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.risks.analyzer import RiskAnalyzer
from migrator.risks.taxonomy import (
    APPROX_AGGREGATOR_MISMATCH,
    ROLLUP_SEMANTIC_MISMATCH,
    TRANSFORM_PORTABILITY_RISK,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _canonical_from_fixture(name: str):
    raw = json.loads((FIXTURES / name / "spec.json").read_text())
    parser = DruidSpecParser()
    parse_result = parser.parse(raw)
    normalizer = DruidNormalizer()
    norm_result = normalizer.normalize(parse_result.parsed_spec)
    canonical = norm_result.canonical
    canonical.classification = classify_datasource(canonical).value
    return canonical


class TestRiskAnalyzer:
    def setup_method(self):
        self.analyzer = RiskAnalyzer()

    def test_rolled_up_has_rollup_semantic_mismatch(self):
        canonical = _canonical_from_fixture("rolled_up")
        result = self.analyzer.analyze(canonical)
        risk_ids = [r.risk_id for r in result.risks]
        assert ROLLUP_SEMANTIC_MISMATCH in risk_ids

    def test_rollup_risk_is_high_severity(self):
        canonical = _canonical_from_fixture("rolled_up")
        result = self.analyzer.analyze(canonical)
        rollup_risks = [r for r in result.risks if r.risk_id == ROLLUP_SEMANTIC_MISMATCH]
        assert rollup_risks[0].severity == RiskSeverity.HIGH.value

    def test_transforms_has_transform_portability_risk(self):
        canonical = _canonical_from_fixture("transforms")
        result = self.analyzer.analyze(canonical)
        risk_ids = [r.risk_id for r in result.risks]
        assert TRANSFORM_PORTABILITY_RISK in risk_ids

    def test_unsupported_complex_has_approx_aggregator_mismatch(self):
        canonical = _canonical_from_fixture("unsupported_complex")
        result = self.analyzer.analyze(canonical)
        risk_ids = [r.risk_id for r in result.risks]
        assert APPROX_AGGREGATOR_MISMATCH in risk_ids

    def test_approx_aggregator_mismatch_is_blocking(self):
        canonical = _canonical_from_fixture("unsupported_complex")
        result = self.analyzer.analyze(canonical)
        blocking_risks = [r for r in result.risks if r.risk_id == APPROX_AGGREGATOR_MISMATCH]
        assert blocking_risks[0].severity == RiskSeverity.BLOCKING.value

    def test_raw_batch_no_rollup_risk(self):
        canonical = _canonical_from_fixture("raw_batch")
        result = self.analyzer.analyze(canonical)
        risk_ids = [r.risk_id for r in result.risks]
        assert ROLLUP_SEMANTIC_MISMATCH not in risk_ids

    def test_raw_batch_no_approx_risk(self):
        canonical = _canonical_from_fixture("raw_batch")
        result = self.analyzer.analyze(canonical)
        risk_ids = [r.risk_id for r in result.risks]
        assert APPROX_AGGREGATOR_MISMATCH not in risk_ids

    def test_raw_stream_has_no_high_risks(self):
        canonical = _canonical_from_fixture("raw_stream")
        result = self.analyzer.analyze(canonical)
        high_or_blocking = [
            r for r in result.risks
            if r.severity in (RiskSeverity.HIGH.value, RiskSeverity.BLOCKING.value)
        ]
        assert len(high_or_blocking) == 0

    def test_risk_has_evidence(self):
        canonical = _canonical_from_fixture("rolled_up")
        result = self.analyzer.analyze(canonical)
        rollup_risks = [r for r in result.risks if r.risk_id == ROLLUP_SEMANTIC_MISMATCH]
        assert len(rollup_risks[0].evidence) > 0

    def test_risk_has_remediation(self):
        canonical = _canonical_from_fixture("unsupported_complex")
        result = self.analyzer.analyze(canonical)
        approx_risks = [r for r in result.risks if r.risk_id == APPROX_AGGREGATOR_MISMATCH]
        assert approx_risks[0].remediation != ""
