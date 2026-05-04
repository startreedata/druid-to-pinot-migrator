"""Unit tests for the Druid → Pinot lookups migration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.lookups.generator import generate_lookup_artifacts
from migrator.lookups.models import CanonicalLookup, StaticMapEntry
from migrator.lookups.parser import (
    LookupParseError,
    UnsupportedLookupError,
    parse_lookups_config,
)


FIXTURES = Path(__file__).parent.parent / "fixtures" / "lookups"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())


# ─────────────────────────────────────────────────────────────────────────────
# Parser
# ─────────────────────────────────────────────────────────────────────────────


class TestParserStaticMap:
    def test_parses_two_static_map_lookups(self):
        out = parse_lookups_config(_load("static_map"))
        assert len(out) == 2
        names = {l.name for l in out}
        assert names == {"country_code_to_name", "platform_short_to_long"}

    def test_static_map_entries_preserved(self):
        out = parse_lookups_config(_load("static_map"))
        country = next(l for l in out if l.name == "country_code_to_name")
        assert country.source_kind == "static_map"
        keys = {e.key for e in country.entries}
        assert keys == {"us", "ca", "mx"}
        us = next(e for e in country.entries if e.key == "us")
        assert us.value == "United States"

    def test_default_key_value_columns(self):
        # static_map source: caller hasn't told us a column name, so
        # we default to the dpm-canonical 'key' / 'value'.
        out = parse_lookups_config(_load("static_map"))
        country = next(l for l in out if l.name == "country_code_to_name")
        assert country.key_column == "key"
        assert country.value_column == "value"

    def test_source_file_threaded_through(self):
        out = parse_lookups_config(_load("static_map"), source_file="x.json")
        assert all(l.source_file == "x.json" for l in out)


class TestParserUriCsv:
    def test_parses_uri_csv_lookup(self):
        out = parse_lookups_config(_load("uri_csv"))
        assert len(out) == 1
        l = out[0]
        assert l.name == "campaign_lookup"
        assert l.source_kind == "uri_csv"
        assert l.uri == "file:///opt/lookups/campaigns.csv"
        # Columns from the CSV namespaceParseSpec.
        assert l.key_column == "campaign_id"
        assert l.value_column == "campaign_name"

    def test_csv_with_wrong_column_count_rejected(self):
        # A 3-column CSV has no obvious key/value mapping.
        cfg = {
            "__default": {
                "x": {
                    "version": "v1",
                    "lookupExtractorFactory": {
                        "type": "cachedNamespace",
                        "extractionNamespace": {
                            "type": "uri",
                            "uri": "file:///x.csv",
                            "namespaceParseSpec": {
                                "format": "csv",
                                "columns": ["a", "b", "c"],
                            },
                        },
                    },
                },
            }
        }
        with pytest.raises(UnsupportedLookupError, match="2-column CSV"):
            parse_lookups_config(cfg)


class TestParserUnsupported:
    def test_jdbc_rejected_with_pointer_to_supported_set(self):
        with pytest.raises(UnsupportedLookupError) as exc_info:
            parse_lookups_config(_load("unsupported_jdbc"))
        msg = str(exc_info.value)
        # The error must (a) name the unsupported type and (b) point
        # at what IS supported, so operators can fix and retry.
        assert "jdbc" in msg
        assert "staticMap" in msg or "uri" in msg

    def test_non_cached_factory_rejected(self):
        cfg = {
            "__default": {
                "polling_lookup": {
                    "version": "v1",
                    "lookupExtractorFactory": {
                        "type": "pollingLookup",
                        "extractionNamespace": {"type": "staticMap", "map": {}},
                    },
                },
            }
        }
        with pytest.raises(UnsupportedLookupError, match="cachedNamespace"):
            parse_lookups_config(cfg)


class TestParserShape:
    def test_flat_form_autodetected(self):
        # Some operators read the lookup file from disk where it's
        # already flat (no tier wrapper).
        flat = {
            "country_code_to_name": _load("static_map")["__default"]["country_code_to_name"],
        }
        out = parse_lookups_config(flat)
        assert len(out) == 1
        assert out[0].name == "country_code_to_name"

    def test_multiple_tiers_flatten(self):
        cfg = {
            "__default": {
                "a": {"version": "v1", "lookupExtractorFactory": {
                    "type": "cachedNamespace",
                    "extractionNamespace": {"type": "staticMap", "map": {"k": "v"}},
                }},
            },
            "tier-2": {
                "b": {"version": "v1", "lookupExtractorFactory": {
                    "type": "cachedNamespace",
                    "extractionNamespace": {"type": "staticMap", "map": {"k": "v"}},
                }},
            },
        }
        out = parse_lookups_config(cfg)
        assert {l.name for l in out} == {"a", "b"}

    def test_malformed_top_level_rejected(self):
        with pytest.raises(LookupParseError):
            parse_lookups_config([1, 2, 3])  # noqa - intentional wrong type

    def test_missing_extractor_factory_rejected(self):
        with pytest.raises(LookupParseError, match="lookupExtractorFactory"):
            parse_lookups_config({
                "__default": {
                    "x": {"version": "v1"},  # no factory
                },
            })


# ─────────────────────────────────────────────────────────────────────────────
# Generator
# ─────────────────────────────────────────────────────────────────────────────


def _static_map_lookup() -> CanonicalLookup:
    return CanonicalLookup(
        name="country",
        source_kind="static_map",
        entries=[
            StaticMapEntry(key="us", value="United States"),
            StaticMapEntry(key="ca", value="Canada"),
        ],
    )


class TestGeneratorSchema:
    def test_table_name_uses_default_prefix(self):
        out = generate_lookup_artifacts(_static_map_lookup())
        assert out.table_name == "lookup_country"
        assert out.table["tableName"] == "lookup_country_OFFLINE"

    def test_custom_prefix(self):
        out = generate_lookup_artifacts(
            _static_map_lookup(), table_name_prefix="dim_",
        )
        assert out.table_name == "dim_country"

    def test_empty_prefix(self):
        out = generate_lookup_artifacts(
            _static_map_lookup(), table_name_prefix="",
        )
        assert out.table_name == "country"

    def test_schema_has_key_value_dimensions(self):
        out = generate_lookup_artifacts(_static_map_lookup())
        dims = {d["name"] for d in out.schema_["dimensionFieldSpecs"]}
        assert dims == {"key", "value"}
        assert all(
            d["dataType"] == "STRING"
            for d in out.schema_["dimensionFieldSpecs"]
        )

    def test_schema_declares_primary_key(self):
        # The primaryKeyColumns hint lets Pinot's LOOKUP() UDF use a
        # hash index instead of a scan — material on a dim-table that
        # gets queried per-row.
        out = generate_lookup_artifacts(_static_map_lookup())
        assert out.schema_["primaryKeyColumns"] == ["key"]

    def test_no_metrics(self):
        out = generate_lookup_artifacts(_static_map_lookup())
        assert out.schema_["metricFieldSpecs"] == []

    def test_placeholder_time_column(self):
        # Pinot requires a time column on every table; for a static
        # dim table it's a placeholder.
        out = generate_lookup_artifacts(_static_map_lookup())
        dt = out.schema_["dateTimeFieldSpecs"]
        assert len(dt) == 1
        assert dt[0]["name"] == "ingest_timestamp"
        seg = out.table["segmentsConfig"]
        assert seg["timeColumnName"] == "ingest_timestamp"


class TestGeneratorTable:
    def test_is_dim_table_flag(self):
        out = generate_lookup_artifacts(_static_map_lookup())
        assert out.table["isDimTable"] is True
        assert out.table["dimTableConfig"]["disablePreload"] is False

    def test_offline_table_type(self):
        out = generate_lookup_artifacts(_static_map_lookup())
        assert out.table["tableType"] == "OFFLINE"

    def test_metadata_records_druid_source(self):
        # Forensics — a quick way to figure out where this Pinot
        # table came from after the fact.
        out = generate_lookup_artifacts(_static_map_lookup())
        m = out.table["metadata"]["customConfigs"]
        assert m["druid_source_lookup_name"] == "country"
        assert m["druid_source_kind"] == "static_map"


class TestGeneratorInlineData:
    def test_static_map_inlined_as_ndjson_rows(self):
        out = generate_lookup_artifacts(_static_map_lookup())
        assert out.inline_data is not None
        assert len(out.inline_data) == 2
        # Each row carries key, value, and the placeholder time.
        first = out.inline_data[0]
        assert set(first.keys()) == {"key", "value", "ingest_timestamp"}

    def test_uri_sources_have_no_inline_data(self):
        l = CanonicalLookup(
            name="campaigns",
            source_kind="uri_csv",
            uri="file:///x.csv",
            key_column="id",
            value_column="name",
        )
        out = generate_lookup_artifacts(l)
        assert out.inline_data is None

    def test_uri_sources_emit_a_pointer_note(self):
        l = CanonicalLookup(
            name="campaigns",
            source_kind="uri_csv",
            uri="file:///x.csv",
            key_column="id",
            value_column="name",
        )
        out = generate_lookup_artifacts(l)
        # Some note must reference the URI so readers don't lose it.
        assert any("file:///x.csv" in n for n in out.notes)


class TestGeneratorRoundTrip:
    def test_uri_csv_has_columns_from_parse_spec(self):
        # The dim columns come from namespaceParseSpec.columns, not
        # from defaults.
        from_parser = parse_lookups_config(_load("uri_csv"))
        assert len(from_parser) == 1
        out = generate_lookup_artifacts(from_parser[0])
        dims = {d["name"] for d in out.schema_["dimensionFieldSpecs"]}
        assert dims == {"campaign_id", "campaign_name"}
        assert out.schema_["primaryKeyColumns"] == ["campaign_id"]
