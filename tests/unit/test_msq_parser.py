"""Unit tests for MSQ (Multi-Stage Query) ingestion-spec parsing.

Druid 25+ ships MSQ as the modern ingestion path: specs are SQL
strings submitted to /druid/v2/sql/task rather than the historical
JSON dataSchema/ioConfig shape. The parser ingests both forms and
produces the same ``DruidParsedSpec`` so the rest of the pipeline
(normalize → generate → validate) is shape-agnostic.

Tests sit at three layers:

  1. ``looks_like_msq`` detection — the dispatch fork.
  2. ``parse_msq_spec`` direct API — extraction of the load-bearing
     fields (target, time column, dimensions, metrics, granularity,
     EXTERN URI / format / schema).
  3. End-to-end via ``DruidSpecParser.parse`` + ``DruidNormalizer``
     against the fixtures, locking in the canonical-model shape.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrator.core.errors import ParseError
from migrator.druid.msq_parser import (
    looks_like_msq,
    parse_msq_spec,
)
from migrator.druid.parser import DruidSpecParser
from migrator.druid.normalizer import DruidNormalizer


FIXTURES = Path(__file__).parent.parent / "fixtures"


def _spec(query: str) -> dict:
    return {"query": query, "context": {"executionMode": "ASYNC"}}


# ─────────────────────────────────────────────────────────────────────────────
# Detection
# ─────────────────────────────────────────────────────────────────────────────


class TestLooksLikeMsq:
    def test_replace_into_detected(self):
        assert looks_like_msq(_spec("REPLACE INTO events SELECT * FROM TABLE(EXTERN(...))"))

    def test_insert_into_detected(self):
        assert looks_like_msq(_spec("INSERT INTO events SELECT * FROM TABLE(EXTERN(...))"))

    def test_case_insensitive(self):
        # Operators sometimes lowercase keywords; we shouldn't miss
        # those just because of capitalisation.
        assert looks_like_msq(_spec("insert into events SELECT * FROM EXTERN()"))

    def test_sql_alias_field_accepted(self):
        # Some operator-saved specs use ``sql`` instead of ``query``;
        # accept both since Druid docs vary.
        assert looks_like_msq({"sql": "INSERT INTO x SELECT * FROM y"})

    def test_classic_index_parallel_not_msq(self):
        assert not looks_like_msq({"type": "index_parallel", "spec": {"dataSchema": {}}})

    def test_random_select_not_msq(self):
        # A bare SELECT (operator's analytics query, not an ingestion
        # spec) should NOT be picked up. MSQ ingest specifically
        # writes via REPLACE/INSERT INTO.
        assert not looks_like_msq(_spec("SELECT 1"))

    def test_non_dict_returns_false(self):
        assert not looks_like_msq("INSERT INTO x SELECT * FROM y")
        assert not looks_like_msq([])
        assert not looks_like_msq(None)


# ─────────────────────────────────────────────────────────────────────────────
# parse_msq_spec — happy paths
# ─────────────────────────────────────────────────────────────────────────────


class TestParseMsqHappyPath:
    def test_simple_insert_extracts_target_and_columns(self):
        spec = _spec(
            "INSERT INTO events "
            "SELECT __time, region, COUNT(*) AS events "
            "FROM TABLE(EXTERN('s3://x/', '{\"type\":\"json\"}', "
            "'[{\"name\":\"__time\",\"type\":\"long\"},"
            "{\"name\":\"region\",\"type\":\"string\"}]')) "
            "GROUP BY 1, 2 "
            "PARTITIONED BY DAY"
        )
        parsed, warnings = parse_msq_spec(spec)
        assert parsed.datasource_name == "events"
        # Plain ``__time`` (no TIME_FLOOR wrapper) → time column with
        # NONE query granularity.
        assert parsed.timestamp_spec.column == "__time"
        assert parsed.granularity_spec.queryGranularity == "NONE"
        # ``region`` is a dimension; ``events`` is a metric.
        assert [d["name"] for d in parsed.dimensions_spec.dimensions] == ["region"]
        assert [(m.type, m.name) for m in parsed.metrics_spec] == [("count", "events")]

    def test_replace_into_treated_like_insert(self):
        # The OVERWRITE WHERE / REPLACE INTO clauses don't change
        # the resulting Pinot dataSchema — we strip them, run the
        # rest as if it were INSERT INTO, and stash the filter in
        # raw_sections for forensics.
        spec = _spec(
            "REPLACE INTO events "
            "OVERWRITE WHERE __time >= TIMESTAMP '2024-01-01' "
            "SELECT __time, region, COUNT(*) AS c "
            "FROM TABLE(EXTERN('s3://x/', '{\"type\":\"json\"}', "
            "'[{\"name\":\"__time\",\"type\":\"long\"},"
            "{\"name\":\"region\",\"type\":\"string\"}]')) "
            "GROUP BY 1, 2 "
            "PARTITIONED BY HOUR"
        )
        parsed, _ = parse_msq_spec(spec)
        assert parsed.datasource_name == "events"
        # OVERWRITE filter preserved as a forensics note.
        assert "msq_overwrite_filter" in parsed.raw_sections.get("msq", {})
        # Granularity from PARTITIONED BY survived the strip.
        assert parsed.granularity_spec.segmentGranularity == "HOUR"

    def test_time_floor_extracts_query_granularity(self):
        # TIME_FLOOR(__time, 'PT1H') AS __time → time column +
        # queryGranularity HOUR.
        spec = _spec(
            "INSERT INTO events "
            "SELECT TIME_FLOOR(__time, 'PT1H') AS __time, region, COUNT(*) AS c "
            "FROM TABLE(EXTERN('s3://x/', '{\"type\":\"json\"}', '[]')) "
            "GROUP BY 1, 2 "
            "PARTITIONED BY HOUR"
        )
        parsed, _ = parse_msq_spec(spec)
        assert parsed.timestamp_spec.column == "__time"
        assert parsed.granularity_spec.queryGranularity == "HOUR"

    @pytest.mark.parametrize("partitioned_by, expected", [
        ("HOUR", "HOUR"),
        ("DAY", "DAY"),
        ("MONTH", "MONTH"),
        ("YEAR", "YEAR"),
        ("ALL", "ALL"),
        ("WEEK", "WEEK"),
        # TIME_FLOOR period form normalises to bucket name.
        ("TIME_FLOOR(__time, 'PT1H')", "HOUR"),
        # FLOOR(... TO HOUR) form.
        ("FLOOR(__time TO HOUR)", "HOUR"),
    ])
    def test_partitioned_by_normalised(self, partitioned_by: str, expected: str):
        spec = _spec(
            "INSERT INTO events "
            "SELECT __time, region "
            "FROM TABLE(EXTERN('s3://x/', '{\"type\":\"json\"}', '[]')) "
            f"PARTITIONED BY {partitioned_by}"
        )
        parsed, _ = parse_msq_spec(spec)
        assert parsed.granularity_spec.segmentGranularity == expected

    def test_clustered_by_emits_warning(self):
        # CLUSTERED BY has no clean Pinot analogue; we warn so the
        # operator can pick a single sortedColumn manually.
        spec = _spec(
            "INSERT INTO events "
            "SELECT __time, region "
            "FROM TABLE(EXTERN('s3://x/', '{\"type\":\"json\"}', '[]')) "
            "PARTITIONED BY DAY "
            "CLUSTERED BY region"
        )
        parsed, warnings = parse_msq_spec(spec)
        assert any("CLUSTERED BY" in w for w in warnings)
        assert parsed.raw_sections["msq"]["msq_clustered_by"] == ["region"]


class TestExternSourceDispatch:
    @pytest.mark.parametrize("uri, expected_type", [
        ("s3://b/k",            "s3"),
        ("s3a://b/k",           "s3"),
        ("gs://b/k",            "google"),
        ("gcs://b/k",           "google"),
        ("hdfs://nn/p",         "hdfs"),
        ("https://api/data",    "http"),
        ("/local/path",         "local"),
    ])
    def test_input_source_type_from_scheme(self, uri: str, expected_type: str):
        spec = _spec(
            f"INSERT INTO x "
            f"SELECT __time "
            f"FROM TABLE(EXTERN('{uri}', '{{\"type\":\"json\"}}', '[]')) "
            f"PARTITIONED BY DAY"
        )
        parsed, _ = parse_msq_spec(spec)
        assert parsed.io_config.inputSource["type"] == expected_type

    @pytest.mark.parametrize("druid_format, canonical_format", [
        ("json", "json"),
        ("parquet", "parquet"),
        ("avro_ocf", "avro"),
        ("orc", "orc"),
        ("csv", "csv"),
    ])
    def test_input_format_threaded_through(
        self, druid_format: str, canonical_format: str,
    ):
        # The EXTERN() inputFormat dict is preserved through to the
        # canonical model's input_format field — same dispatch path
        # that classic specs use.
        spec = _spec(
            f"INSERT INTO x "
            f"SELECT __time "
            f"FROM TABLE(EXTERN('s3://b/k', '{{\"type\":\"{druid_format}\"}}', '[]')) "
            f"PARTITIONED BY DAY"
        )
        parsed, _ = parse_msq_spec(spec)
        assert parsed.io_config.inputFormat["type"] == druid_format
        # End-to-end: normalizer collapses to canonical format.
        n = DruidNormalizer().normalize(parsed)
        assert n.canonical.input_format == canonical_format


class TestAggregateClassification:
    """Different SQL aggregates → matching Druid metric types."""

    def _parse(self, agg_sql: str):
        spec = _spec(
            "INSERT INTO x "
            f"SELECT __time, {agg_sql} "
            "FROM TABLE(EXTERN('s3://b/k', '{\"type\":\"json\"}', '[]')) "
            "GROUP BY 1 "
            "PARTITIONED BY DAY"
        )
        parsed, _ = parse_msq_spec(spec)
        return parsed.metrics_spec[0]

    def test_count_star(self):
        m = self._parse("COUNT(*) AS c")
        assert (m.type, m.name, m.fieldName) == ("count", "c", "")

    def test_sum_named_column(self):
        m = self._parse("SUM(amount) AS s")
        assert m.type == "longSum"
        assert m.fieldName == "amount"

    def test_max_named_column(self):
        m = self._parse("MAX(latency) AS lmax")
        assert m.type == "longMax"
        assert m.fieldName == "latency"

    def test_min_named_column(self):
        m = self._parse("MIN(latency) AS lmin")
        assert m.type == "longMin"

    def test_avg_named_column_emits_avg_marker(self):
        # Druid has no native avg in metricsSpec. We emit type="avg"
        # so the normalizer can warn the operator to rewrite as
        # SUM/COUNT post-migration — silent acceptance would produce
        # an unmappable Pinot column.
        m = self._parse("AVG(amount) AS a")
        assert m.type == "avg"


# ─────────────────────────────────────────────────────────────────────────────
# parse_msq_spec — error paths
# ─────────────────────────────────────────────────────────────────────────────


class TestParseMsqErrors:
    def test_empty_query_raises(self):
        with pytest.raises(ParseError, match="no SQL"):
            parse_msq_spec({"query": ""})

    def test_missing_query_raises(self):
        with pytest.raises(ParseError, match="no SQL"):
            parse_msq_spec({})

    def test_join_rejected_with_clear_message(self):
        # JOIN makes the dataSchema ambiguous — refuse rather than
        # produce a half-shaped Pinot table.
        spec = _spec(
            "INSERT INTO x "
            "SELECT a.k, b.v "
            "FROM TABLE(EXTERN('s3://a/', '{\"type\":\"json\"}', '[]')) a "
            "JOIN TABLE(EXTERN('s3://b/', '{\"type\":\"json\"}', '[]')) b "
            "ON a.k = b.k "
            "PARTITIONED BY DAY"
        )
        with pytest.raises(ParseError, match="JOIN"):
            parse_msq_spec(spec)

    def test_non_extern_source_rejected(self):
        # Druid-datasource sources (``FROM events``) and the like
        # aren't supported — operators should use EXTERN for the
        # auto-translatable case. Error names what IS supported.
        spec = _spec(
            "INSERT INTO new_events "
            "SELECT __time FROM events "
            "PARTITIONED BY DAY"
        )
        with pytest.raises(ParseError, match="EXTERN"):
            parse_msq_spec(spec)

    def test_non_insert_top_level_rejected(self):
        # Some MSQ-shaped specs are pure analytic queries; refuse
        # to treat them as ingestion.
        with pytest.raises(ParseError):
            parse_msq_spec({"query": "SELECT 1"})

    def test_unparseable_sql_raises(self):
        with pytest.raises(ParseError, match="failed to parse"):
            parse_msq_spec({"query": "INSERT INTO x SELECT garbage % @ #"})


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end via DruidSpecParser
# ─────────────────────────────────────────────────────────────────────────────


class TestDruidSpecParserMsqDispatch:
    def test_parser_dispatches_to_msq_path(self):
        # The classic parser detects MSQ-shape and dispatches to
        # ``parse_msq_spec``; the result is a successful ParseResult.
        spec = _spec(
            "INSERT INTO events "
            "SELECT __time, region "
            "FROM TABLE(EXTERN('s3://x/', '{\"type\":\"json\"}', '[]')) "
            "PARTITIONED BY DAY"
        )
        result = DruidSpecParser().parse(spec)
        assert result.success
        assert result.parsed_spec is not None
        assert result.parsed_spec.datasource_name == "events"

    def test_classic_spec_still_uses_classic_path(self):
        # Backward-compat: the existing index_parallel fixture still
        # parses through the classic dataSchema/ioConfig path. MSQ
        # detection must not steal classic specs.
        raw = json.loads(
            (FIXTURES / "raw_batch" / "spec.json").read_text()
        )
        result = DruidSpecParser().parse(raw)
        assert result.success
        # The classic parser produces the datasource from
        # spec.dataSchema.dataSource — that name in raw_batch is
        # ``pageviews``.
        assert result.parsed_spec.datasource_name == "pageviews"

    def test_parse_error_surfaces_through_parse_result(self):
        # An MSQ spec with invalid SQL produces a ParseResult with
        # success=False and the message in errors — same shape the
        # classic parser uses.
        result = DruidSpecParser().parse({
            "query": "INSERT INTO x SELECT a b c",
        })
        assert not result.success
        assert any("MSQ" in e or "parse" in e.lower() for e in result.errors)


class TestMsqFixturesEndToEnd:
    """The fixture files are the canonical operator-facing examples;
    they need to round-trip through the full pipeline."""

    def test_msq_replace_fixture_round_trips(self):
        raw = json.loads(
            (FIXTURES / "msq_replace" / "spec.json").read_text()
        )
        result = DruidSpecParser().parse(raw)
        assert result.success
        canonical = DruidNormalizer().normalize(result.parsed_spec).canonical
        assert canonical.datasource_name == "events"
        # OVERWRITE WHERE captured.
        msq_notes = result.parsed_spec.raw_sections.get("msq", {})
        assert "msq_overwrite_filter" in msq_notes
        # CLUSTERED BY captured.
        assert msq_notes["msq_clustered_by"] == ["region", "device"]
        # PARTITIONED BY HOUR.
        assert canonical.granularity.segment_granularity == "HOUR"
        # Time column survives TIME_FLOOR aliasing.
        assert canonical.time_field.column_name == "__time"
        # Two dims (region + device), three metrics.
        dim_names = {d.name for d in canonical.dimensions}
        assert dim_names == {"region", "device"}
        metric_names = {m.name for m in canonical.metrics}
        assert metric_names == {"event_count", "amount_sum", "latency_max"}
        # S3 inputSource picked up.
        assert canonical.raw_io_config["inputSource"]["type"] == "s3"

    def test_msq_insert_fixture_with_gcs_source(self):
        raw = json.loads(
            (FIXTURES / "msq_insert" / "spec.json").read_text()
        )
        result = DruidSpecParser().parse(raw)
        assert result.success
        canonical = DruidNormalizer().normalize(result.parsed_spec).canonical
        assert canonical.datasource_name == "sessions"
        # gs:// → google inputSource.
        assert canonical.raw_io_config["inputSource"]["type"] == "google"
        # PARTITIONED BY DAY.
        assert canonical.granularity.segment_granularity == "DAY"

    def test_msq_parquet_fixture(self):
        raw = json.loads(
            (FIXTURES / "msq_parquet" / "spec.json").read_text()
        )
        result = DruidSpecParser().parse(raw)
        assert result.success
        canonical = DruidNormalizer().normalize(result.parsed_spec).canonical
        # Parquet inputFormat threads through.
        assert canonical.input_format == "parquet"
