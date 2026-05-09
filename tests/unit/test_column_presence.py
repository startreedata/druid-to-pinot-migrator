"""Unit tests for column-presence parity.

Scope (per operator's note): we check column presence + null-rate
divergence. Aggregate VALUE comparisons (SUM, COUNT, etc.) are
out — operator-specific aggregation logic varies too much to lock in.

Three failure modes the check should catch:
  1. Column missing from the deployed Pinot schema (Pinot rejects
     the SQL with "unknown column").
  2. Column present but null-rate jumped — usually a type-mapping
     accident or a broken ingestion transform.
  3. Both rates are similar (within tolerance) — passes; the
     ``--check-columns`` flag is meant to surface real problems,
     not flap on noise.
"""

from __future__ import annotations

import pytest

from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    MetricField,
    TimeField,
)
from migrator.parity.column_presence import run_column_presence


# ─────────────────────────────────────────────────────────────────────────────
# Fakes — tiny SQL clients that return canned per-column responses.
# Each test parametrises the responses to exercise one branch.
# ─────────────────────────────────────────────────────────────────────────────


class _FakeDruid:
    """Returns canned ``[{"total": ..., "nulls": ...}]`` for the
    Druid SQL the helper builds, indexed by column name. Missing
    column → query() raises (matches DruidHttpSqlClient's behaviour
    on a non-existent column)."""
    def __init__(self, responses: dict[str, dict | Exception]) -> None:
        self._r = responses
        self.queries: list[str] = []

    def query(self, sql: str):
        self.queries.append(sql)
        # Naive column extraction from the SQL we generate.
        col = self._extract_column(sql)
        v = self._r.get(col)
        if isinstance(v, Exception):
            raise v
        if v is None:
            return []
        return [v]

    @staticmethod
    def _extract_column(sql: str) -> str | None:
        # The helper produces ``COUNT(*) - COUNT("<col>")``; pull
        # the quoted column name out.
        import re
        m = re.search(r'COUNT\("([^"]+)"\)', sql)
        return m.group(1) if m else None


class _FakePinot:
    """Same shape but Pinot's PinotHttpSqlClient returns rows as
    lists of cell values, not dicts. Test stubs both shapes."""
    def __init__(
        self,
        responses: dict[str, list | Exception | None] | None = None,
    ) -> None:
        self._r = responses or {}
        self.queries: list[str] = []

    def query(self, sql: str):
        self.queries.append(sql)
        col = _FakeDruid._extract_column(sql)
        v = self._r.get(col, "MISSING")
        if isinstance(v, Exception):
            raise v
        if v == "MISSING":
            # Default: "column not found" — flips the helper to
            # MISSING_FROM_PINOT verdict.
            raise RuntimeError(f"Pinot SQL error: cannot find column {col}")
        if v is None:
            return []
        return v


def _canon(**overrides) -> CanonicalMigrationModel:
    base = dict(
        datasource_name="events",
        source_kind="batch",
        time_field=TimeField(column_name="ts", format="millis"),
        dimensions=[
            DimensionField(name="region", druid_type="string", pinot_type="STRING"),
            DimensionField(name="device", druid_type="string", pinot_type="STRING"),
        ],
        metrics=[
            MetricField(name="amount_sum", druid_type="longSum",
                        field_name="amount", pinot_type="LONG", aggregation="SUM"),
        ],
    )
    base.update(overrides)
    return CanonicalMigrationModel(**base)


# ─────────────────────────────────────────────────────────────────────────────
# Happy path
# ─────────────────────────────────────────────────────────────────────────────


class TestColumnPresenceHappyPath:
    def test_all_columns_present_and_matching_null_rates(self):
        # Druid: every column 0% null.
        # Pinot: same.
        # Verdict: every check passes.
        c = _canon()
        druid = _FakeDruid({
            "ts":         {"total": 1000.0, "nulls": 0.0},
            "region":     {"total": 1000.0, "nulls": 0.0},
            "device":     {"total": 1000.0, "nulls": 0.0},
            "amount_sum": {"total": 1000.0, "nulls": 0.0},
        })
        pinot = _FakePinot({
            "ts":         [[1000.0, 0.0]],
            "region":     [[1000.0, 0.0]],
            "device":     [[1000.0, 0.0]],
            "amount_sum": [[1000.0, 0.0]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        assert len(results) == 4   # ts + 2 dims + 1 metric
        assert all(r.passed for r in results)

    def test_returns_one_result_per_canonical_column(self):
        # Belt-and-suspenders ordering check. The result sequence
        # follows the canonical model's traversal order: time,
        # then dimensions, then metrics.
        c = _canon()
        druid = _FakeDruid({
            col: {"total": 100.0, "nulls": 0.0}
            for col in ("ts", "region", "device", "amount_sum")
        })
        pinot = _FakePinot({
            col: [[100.0, 0.0]]
            for col in ("ts", "region", "device", "amount_sum")
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        labels = [r.label for r in results]
        # Time first.
        assert labels[0].endswith(": ts")
        # Dims in declared order.
        assert "region" in labels[1]
        assert "device" in labels[2]
        # Metric last.
        assert "amount_sum" in labels[3]


# ─────────────────────────────────────────────────────────────────────────────
# Failure mode 1: column missing from Pinot
# ─────────────────────────────────────────────────────────────────────────────


class TestMissingColumn:
    def test_unknown_column_in_pinot_fails(self):
        # Pinot rejects the query with a "cannot find column"
        # message → MISSING_FROM_PINOT verdict.
        c = _canon(metrics=[])  # one less column to keep test small
        druid = _FakeDruid({
            "ts":     {"total": 100.0, "nulls": 0.0},
            "region": {"total": 100.0, "nulls": 0.0},
            "device": {"total": 100.0, "nulls": 0.0},
        })
        pinot = _FakePinot({
            "ts":     [[100.0, 0.0]],
            "region": [[100.0, 0.0]],
            # ``device`` defaults to MISSING in the stub → raises
            # RuntimeError with "cannot find column".
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        device_result = next(r for r in results if "device" in r.label)
        assert not device_result.passed
        assert "unknown column" in device_result.detail

    def test_other_pinot_error_does_not_flag_missing(self):
        # If Pinot errors with a non-column-not-found message
        # (timeout, broker down, etc.), we don't claim "missing
        # column"; we report no signal.
        c = _canon(metrics=[])
        druid = _FakeDruid({
            col: {"total": 100.0, "nulls": 0.0}
            for col in ("ts", "region", "device")
        })
        pinot = _FakePinot({
            "ts":     [[100.0, 0.0]],
            "region": RuntimeError("Pinot SQL error: query timeout"),
            "device": [[100.0, 0.0]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        region_result = next(r for r in results if r.label.endswith(": region"))
        # Timeout → no signal, NOT a MISSING_FROM_PINOT.
        assert region_result.passed   # default "no signal" is a pass
        assert "no signal" in region_result.detail or "match" in region_result.detail
        assert "unknown column" not in region_result.detail


# ─────────────────────────────────────────────────────────────────────────────
# Failure mode 2: null-rate divergence
# ─────────────────────────────────────────────────────────────────────────────


class TestNullRateDivergence:
    def test_pinot_higher_null_rate_fails(self):
        # Druid 0% null, Pinot 50% null → divergence.
        # Most common cause: type mapping turned text into NULL.
        c = _canon(metrics=[])
        druid = _FakeDruid({
            "ts":     {"total": 1000.0, "nulls": 0.0},
            "region": {"total": 1000.0, "nulls": 0.0},
            "device": {"total": 1000.0, "nulls": 0.0},
        })
        pinot = _FakePinot({
            "ts":     [[1000.0, 0.0]],
            "region": [[1000.0, 500.0]],     # 50% null
            "device": [[1000.0, 0.0]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        region = next(r for r in results if r.label.endswith(": region"))
        assert not region.passed
        assert "divergence" in region.detail
        # Detail surfaces both rates so the operator can eyeball.
        assert "druid=0.0%" in region.detail
        assert "pinot=50.0%" in region.detail

    def test_within_tolerance_passes(self):
        # Default tolerance is 10pp. Druid 0%, Pinot 5% → pass.
        c = _canon(metrics=[])
        druid = _FakeDruid({
            col: {"total": 1000.0, "nulls": 0.0}
            for col in ("ts", "region", "device")
        })
        pinot = _FakePinot({
            "ts":     [[1000.0, 0.0]],
            "region": [[1000.0, 50.0]],     # 5% null — within tolerance
            "device": [[1000.0, 0.0]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        region = next(r for r in results if r.label.endswith(": region"))
        assert region.passed

    def test_pinot_lower_null_rate_does_not_fail(self):
        # Pinot null rate < Druid's — that's not a sign of data
        # loss, so we don't flag it. (Could happen if Pinot's
        # default-value-for-missing-int is 0 and Druid was treating
        # those as null.)
        c = _canon(metrics=[])
        druid = _FakeDruid({
            "ts":     {"total": 1000.0, "nulls": 200.0},
            "region": {"total": 1000.0, "nulls": 0.0},
            "device": {"total": 1000.0, "nulls": 0.0},
        })
        pinot = _FakePinot({
            "ts":     [[1000.0, 0.0]],     # 0% < 20% → pass
            "region": [[1000.0, 0.0]],
            "device": [[1000.0, 0.0]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        ts_result = next(r for r in results if r.label.endswith(": ts"))
        assert ts_result.passed

    @pytest.mark.parametrize("tolerance, druid_null, pinot_null, should_pass", [
        # Tighter tolerance (2pp): 0% → 5% fails.
        (0.02, 0.0, 0.05, False),
        # Loose tolerance (50pp): even 0% → 40% passes.
        (0.50, 0.0, 0.40, True),
        # Equal rates: any tolerance passes.
        (0.0,  0.10, 0.10, True),
    ])
    def test_tolerance_knob(
        self, tolerance: float, druid_null: float,
        pinot_null: float, should_pass: bool,
    ):
        c = _canon(metrics=[], dimensions=[
            DimensionField(name="x", druid_type="string", pinot_type="STRING"),
        ])
        druid = _FakeDruid({
            "ts": {"total": 100.0, "nulls": 0.0},
            "x":  {"total": 100.0, "nulls": druid_null * 100},
        })
        pinot = _FakePinot({
            "ts": [[100.0, 0.0]],
            "x":  [[100.0, pinot_null * 100]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
            null_rate_tolerance=tolerance,
        )
        x_result = next(r for r in results if r.label.endswith(": x"))
        assert x_result.passed is should_pass


# ─────────────────────────────────────────────────────────────────────────────
# No-signal cases — the helper returns a "passed but no signal" result
# rather than failing, so an unreliable Druid doesn't generate noise.
# ─────────────────────────────────────────────────────────────────────────────


class TestNoSignal:
    def test_druid_returns_no_rows_yields_no_signal(self):
        c = _canon(metrics=[], dimensions=[
            DimensionField(name="x", druid_type="string", pinot_type="STRING"),
        ])
        druid = _FakeDruid({"ts": None, "x": None})  # empty result set
        pinot = _FakePinot({
            "ts": [[100.0, 0.0]],
            "x":  [[100.0, 0.0]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        x_result = next(r for r in results if r.label.endswith(": x"))
        assert x_result.passed
        assert "no signal" in x_result.detail

    def test_druid_returns_zero_total_yields_no_signal(self):
        # COUNT(*) = 0 → can't compute a null-rate (0/0). Skip.
        c = _canon(metrics=[], dimensions=[
            DimensionField(name="x", druid_type="string", pinot_type="STRING"),
        ])
        druid = _FakeDruid({
            "ts": {"total": 0.0, "nulls": 0.0},
            "x":  {"total": 0.0, "nulls": 0.0},
        })
        pinot = _FakePinot({
            "ts": [[0.0, 0.0]],
            "x":  [[0.0, 0.0]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        for r in results:
            assert r.passed
            assert "no signal" in r.detail

    def test_druid_query_exception_yields_no_signal(self):
        # Any unexpected Druid error → skip the column rather than
        # falsely report divergence. The operator's parity-check
        # session can absorb a bad column without crashing.
        c = _canon(metrics=[], dimensions=[
            DimensionField(name="x", druid_type="string", pinot_type="STRING"),
        ])
        druid = _FakeDruid({
            "ts": {"total": 100.0, "nulls": 0.0},
            "x":  RuntimeError("Druid SQL: something exploded"),
        })
        pinot = _FakePinot({
            "ts": [[100.0, 0.0]],
            "x":  [[100.0, 0.0]],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        x_result = next(r for r in results if r.label.endswith(": x"))
        assert x_result.passed
        assert "no signal" in x_result.detail


# ─────────────────────────────────────────────────────────────────────────────
# Wire-format detail — Druid returns dicts, Pinot returns lists; both
# should work identically through the helper.
# ─────────────────────────────────────────────────────────────────────────────


class TestResultShape:
    def test_pinot_dict_response_shape_also_works(self):
        # Some PinotHttpSqlClient builds normalize the rows into
        # dicts. The helper should handle both shapes without
        # silently mishashing nulls.
        c = _canon(metrics=[], dimensions=[
            DimensionField(name="x", druid_type="string", pinot_type="STRING"),
        ])
        druid = _FakeDruid({
            "ts": {"total": 100.0, "nulls": 0.0},
            "x":  {"total": 100.0, "nulls": 0.0},
        })
        pinot = _FakePinot({
            "ts": [{"total": 100.0, "nulls": 0.0}],
            "x":  [{"total": 100.0, "nulls": 0.0}],
        })
        results = run_column_presence(
            c, druid_client=druid, pinot_client=pinot,
            pinot_table="events",
        )
        # Both passed — dict and list shapes parse equivalently.
        assert all(r.passed for r in results)
