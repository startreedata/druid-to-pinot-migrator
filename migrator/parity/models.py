"""Pydantic models for parity-check query specs and results."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ParityQuery(BaseModel):
    """One Druid → Pinot query pair to compare.

    Each query asserts that running ``druid`` against the Druid Broker /
    Router and running ``pinot`` against the Pinot Broker should produce
    the same result. Two comparison kinds are supported:

    - ``scalar`` (default): both queries return a single value. The
      first cell of the first row from each side is compared.
    - ``groupby``: each query returns ``(key, value)`` rows; the two
      sets of rows are compared as sorted ``(key, value)`` tuples.
    """
    model_config = ConfigDict(extra="forbid")

    label: str = Field(..., description="Human-readable label printed in the report.")
    druid: str = Field(..., description="SQL to execute against the Druid Router/Broker.")
    pinot: str = Field(..., description="SQL to execute against the Pinot Broker.")
    type: Literal["scalar", "groupby"] = "scalar"
    # Numeric tolerance for scalar comparisons. 0 means exact match.
    # Useful for SUM-of-floating-point cases where engines disagree on
    # the last bit; the migration semantics are still correct.
    tolerance: float = 0.0


class ParityQueryFile(BaseModel):
    """Top-level shape of the YAML/JSON file passed via ``--queries``."""
    model_config = ConfigDict(extra="forbid")

    queries: list[ParityQuery]


class ParityResult(BaseModel):
    """Outcome of running one ``ParityQuery``."""
    label: str
    passed: bool
    detail: str
    # Optional structured fields — populated for scalar checks so callers
    # that JSON-serialise the report can compute their own diffs.
    druid_value: object | None = None
    pinot_value: object | None = None
