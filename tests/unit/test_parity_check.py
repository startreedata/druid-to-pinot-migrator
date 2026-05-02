"""Unit tests for the parity-check runner with stub clients."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from migrator.parity.loader import load_queries
from migrator.parity.models import ParityQuery, ParityQueryFile
from migrator.parity.runner import _values_equal, run_parity


# ─────────────────────────────────────────────────────────────────────────────
# Stub clients
# ─────────────────────────────────────────────────────────────────────────────


class StubDruid:
    """In-memory Druid stub. Maps SQL → list[dict] (resultFormat=object)."""

    def __init__(self, responses: dict[str, list[dict]]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def query(self, sql: str) -> list[dict]:
        self.calls.append(sql)
        if sql not in self.responses:
            raise RuntimeError(f"unexpected druid sql: {sql!r}")
        return self.responses[sql]


class StubPinot:
    """In-memory Pinot stub. Maps SQL → list[list]."""

    def __init__(self, responses: dict[str, list[list]]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def query(self, sql: str) -> list[list]:
        self.calls.append(sql)
        if sql not in self.responses:
            raise RuntimeError(f"unexpected pinot sql: {sql!r}")
        return self.responses[sql]


# ─────────────────────────────────────────────────────────────────────────────
# _values_equal
# ─────────────────────────────────────────────────────────────────────────────


class TestValuesEqual:
    @pytest.mark.parametrize("d, p, expected", [
        (1500, 1500, True),
        (1500, 1500.0, True),    # int / float crosscheck
        (1500, 1501, False),
        ("a", "a", True),
        ("a", "b", False),
        (None, None, True),
        (None, 0, False),
        (0, None, False),
        ([1, 2], [1, 2], True),  # falls back to default == for non-numeric
    ])
    def test_no_tolerance(self, d, p, expected):
        assert _values_equal(d, p, 0) is expected

    def test_relative_tolerance_within(self):
        # 0.5% diff inside a 1% tolerance window → equal.
        assert _values_equal(1_000_000, 1_005_000, 0.01) is True

    def test_relative_tolerance_outside(self):
        # 5% diff outside a 1% window → not equal.
        assert _values_equal(1_000_000, 1_050_000, 0.01) is False

    def test_tolerance_handles_zero_denominator(self):
        # Both zero — 0/max(0,0,1)=0 ≤ tolerance.
        assert _values_equal(0, 0, 0.01) is True
        # One zero, one not — relative error vs the non-zero value.
        assert _values_equal(0, 0.001, 0.01) is True
        assert _values_equal(0, 1, 0.01) is False


# ─────────────────────────────────────────────────────────────────────────────
# run_parity — scalar
# ─────────────────────────────────────────────────────────────────────────────


class TestRunParityScalar:
    def test_scalar_match(self):
        q = ParityQuery(label="count", druid="D", pinot="P")
        druid = StubDruid({"D": [{"c": 1500}]})
        pinot = StubPinot({"P": [[1500]]})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert len(out) == 1
        assert out[0].passed is True
        assert out[0].druid_value == 1500
        assert out[0].pinot_value == 1500

    def test_scalar_int_vs_float_match(self):
        # Druid integer COUNT(*) vs Pinot float SUM-of-LONG: both are 1500
        # numerically; should pass.
        q = ParityQuery(label="count", druid="D", pinot="P")
        druid = StubDruid({"D": [{"c": 1500}]})
        pinot = StubPinot({"P": [[1500.0]]})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert out[0].passed is True

    def test_scalar_mismatch(self):
        q = ParityQuery(label="count", druid="D", pinot="P")
        druid = StubDruid({"D": [{"c": 1500}]})
        pinot = StubPinot({"P": [[1499]]})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert out[0].passed is False
        assert "1500" in out[0].detail and "1499" in out[0].detail

    def test_scalar_within_tolerance(self):
        # 0.07% diff with 1% tolerance → pass.
        q = ParityQuery(label="sum", druid="D", pinot="P", tolerance=0.01)
        druid = StubDruid({"D": [{"v": 1_000_000}]})
        pinot = StubPinot({"P": [[1_000_700.0]]})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert out[0].passed is True

    def test_scalar_empty_response(self):
        # Both engines return zero rows → both values None → considered equal.
        q = ParityQuery(label="empty", druid="D", pinot="P")
        druid = StubDruid({"D": []})
        pinot = StubPinot({"P": []})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert out[0].passed is True
        assert out[0].druid_value is None
        assert out[0].pinot_value is None


# ─────────────────────────────────────────────────────────────────────────────
# run_parity — groupby
# ─────────────────────────────────────────────────────────────────────────────


class TestRunParityGroupby:
    def test_groupby_match(self):
        q = ParityQuery(label="g", druid="D", pinot="P", type="groupby")
        druid = StubDruid({"D": [
            {"region": "us-east", "c": 100},
            {"region": "us-west", "c": 200},
        ]})
        pinot = StubPinot({"P": [["us-east", 100], ["us-west", 200]]})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert out[0].passed is True
        assert "2 groups" in out[0].detail

    def test_groupby_match_unsorted(self):
        # The runner sorts both sides, so different row orders are fine.
        q = ParityQuery(label="g", druid="D", pinot="P", type="groupby")
        druid = StubDruid({"D": [
            {"region": "us-east", "c": 100},
            {"region": "us-west", "c": 200},
        ]})
        pinot = StubPinot({"P": [["us-west", 200], ["us-east", 100]]})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert out[0].passed is True

    def test_groupby_size_mismatch(self):
        q = ParityQuery(label="g", druid="D", pinot="P", type="groupby")
        druid = StubDruid({"D": [{"region": "us-east", "c": 100}]})
        pinot = StubPinot({"P": [["us-east", 100], ["us-west", 200]]})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert out[0].passed is False
        assert "groups=1" in out[0].detail and "groups=2" in out[0].detail

    def test_groupby_value_mismatch(self):
        q = ParityQuery(label="g", druid="D", pinot="P", type="groupby")
        druid = StubDruid({"D": [
            {"region": "us-east", "c": 100},
            {"region": "us-west", "c": 200},
        ]})
        pinot = StubPinot({"P": [["us-east", 100], ["us-west", 999]]})
        out = run_parity([q], druid=druid, pinot=pinot)
        assert out[0].passed is False
        assert "first diff" in out[0].detail


# ─────────────────────────────────────────────────────────────────────────────
# Error handling: failures don't abort the run
# ─────────────────────────────────────────────────────────────────────────────


class TestRunParityErrorHandling:
    def test_one_query_error_does_not_abort_run(self):
        q1 = ParityQuery(label="ok", druid="D1", pinot="P1")
        q2 = ParityQuery(label="bad", druid="D2", pinot="P2")
        druid = StubDruid({"D1": [{"c": 1}], "D2": [{"c": 5}]})
        # Pinot stub will RuntimeError on D2's pair (P2 isn't in responses).
        pinot = StubPinot({"P1": [[1]]})
        out = run_parity([q1, q2], druid=druid, pinot=pinot)
        assert len(out) == 2
        assert out[0].passed is True
        assert out[1].passed is False
        assert "ERROR" in out[1].detail


# ─────────────────────────────────────────────────────────────────────────────
# loader (YAML + JSON)
# ─────────────────────────────────────────────────────────────────────────────


class TestLoadQueries:
    def test_yaml(self, tmp_path: Path):
        path = tmp_path / "q.yaml"
        path.write_text(yaml.safe_dump({
            "queries": [
                {"label": "count",
                 "druid": "SELECT COUNT(*) FROM x",
                 "pinot": "SELECT COUNT(*) FROM x"},
                {"label": "by_region",
                 "druid": "SELECT region, COUNT(*) FROM x GROUP BY region",
                 "pinot": "SELECT region, COUNT(*) FROM x GROUP BY region",
                 "type": "groupby"},
            ]
        }))
        spec = load_queries(path)
        assert isinstance(spec, ParityQueryFile)
        assert len(spec.queries) == 2
        assert spec.queries[1].type == "groupby"

    def test_json(self, tmp_path: Path):
        path = tmp_path / "q.json"
        path.write_text(json.dumps({
            "queries": [
                {"label": "count", "druid": "SELECT 1", "pinot": "SELECT 1"},
            ]
        }))
        spec = load_queries(path)
        assert len(spec.queries) == 1
        assert spec.queries[0].label == "count"

    def test_missing_label_rejected(self, tmp_path: Path):
        path = tmp_path / "q.yaml"
        path.write_text(yaml.safe_dump({"queries": [{"druid": "x", "pinot": "y"}]}))
        with pytest.raises(Exception):
            load_queries(path)

    def test_unknown_field_rejected(self, tmp_path: Path):
        # extra="forbid" on the model — typos like ``rolling_window`` are
        # caught at load time instead of being silently ignored.
        path = tmp_path / "q.yaml"
        path.write_text(yaml.safe_dump({
            "queries": [{
                "label": "x", "druid": "y", "pinot": "z",
                "rolling_window": "1h",   # not a field
            }]
        }))
        with pytest.raises(Exception):
            load_queries(path)

    def test_bad_type_rejected(self, tmp_path: Path):
        path = tmp_path / "q.yaml"
        path.write_text(yaml.safe_dump({
            "queries": [{
                "label": "x", "druid": "y", "pinot": "z",
                "type": "histogram",  # only scalar / groupby allowed
            }]
        }))
        with pytest.raises(Exception):
            load_queries(path)
