"""Pydantic models for Druid → Pinot lookup migration.

Druid lookups are an enrichment mechanism — given a dimension value
(e.g. ``country_code = "us"``), look up an associated value (``country_name = "United States"``).
They live at the **cluster level** in Druid (managed via the
Coordinator's ``/druid/coordinator/v1/lookups/config`` endpoint), not
per-datasource.

Pinot's equivalent is a **dimension table** — a regular OFFLINE table
config with ``isDimTable: true`` that gets replicated to every server
so JOINs and ``LOOKUP()`` UDFs can reach it from any query without
shuffle. The migrator builds one Pinot dim-table per Druid lookup.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StaticMapEntry(BaseModel):
    """One key/value pair from a Druid ``staticMap`` lookup."""
    key: str
    value: str


class CanonicalLookup(BaseModel):
    """The migration-internal representation of one Druid lookup.

    Three source shapes are supported (the parser surfaces an
    explicit error for everything else):

    - ``static_map`` — ``staticMap`` with the key/value pairs inline
      in the lookup config. The most common shape for small,
      slow-changing dictionaries (country code → name, etc.).
    - ``uri_csv`` — CSV file referenced by URI; first column is the
      key, second is the value.
    - ``uri_json`` — JSON file referenced by URI, formatted as a
      ``{"key": "value"}`` map.
    """
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Druid lookup tier-local name.")
    source_kind: Literal["static_map", "uri_csv", "uri_json"]
    key_column: str = "key"
    value_column: str = "value"

    # Populated when source_kind == "static_map":
    entries: list[StaticMapEntry] = Field(default_factory=list)

    # Populated when source_kind starts with "uri_":
    uri: str | None = None

    # Where the original Druid lookup definition came from. Useful for
    # forensics + the runbook ("regenerate from this file"). Optional
    # — programmatic callers may have only the dict.
    source_file: str | None = None

    notes: str = ""


class LookupArtifacts(BaseModel):
    """Pinot artifacts generated for one lookup."""
    model_config = ConfigDict(extra="forbid")

    table_name: str
    schema_: dict = Field(alias="schema")
    table: dict
    inline_data: list[dict] | None = None  # for static_map sources
    notes: list[str] = Field(default_factory=list)
