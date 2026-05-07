"""
Parse a Druid MSQ (Multi-Stage Query) ingestion spec into the same
``DruidParsedSpec`` shape the classic ``index_parallel`` / ``kafka``
parser produces.

Druid 25+ ships MSQ as the modern ingestion path. Specs are submitted
to ``/druid/v2/sql/task`` as ``{"query": "<SQL string>", "context":
{...}}`` rather than the historical ``{"type":"index_parallel",
"spec": {...}}`` shape. The SQL extends standard ANSI with three
Druid-only clauses dpm has to handle:

  - ``REPLACE INTO <table> OVERWRITE WHERE <expr>`` — replace the
    rows under that WHERE filter. We strip this in pre-processing
    (sqlglot doesn't understand it) and treat the statement as
    INSERT INTO for the purposes of dataSchema extraction. The
    overwrite clause itself doesn't change the generated Pinot
    artifact — it's a Druid-side overwrite-vs-append knob.
  - ``PARTITIONED BY <granularity>`` — direct analogue of Druid's
    classic ``granularitySpec.segmentGranularity``.
  - ``CLUSTERED BY <cols>`` — segment-internal sort. Surfaced as
    a note; doesn't map cleanly to Pinot's sortedColumn (which is
    a single column).

What's deliberately NOT in scope:

  - JOINs, subqueries, CTEs (``WITH ...``), unions. These produce
    a non-standard dataSchema that the canonical model doesn't
    represent. The parser surfaces a clear error so operators
    don't silently get a half-shaped Pinot table.
  - Non-EXTERN sources (Kafka MSQ, Druid datasource → Druid
    rewrite, etc.). EXTERN is the operator-friendly entry point
    and covers the common file/object-store case.
  - Multiple statements in one spec.

Wire-up: ``DruidSpecParser.parse`` calls ``looks_like_msq(raw)``
first; on a hit it dispatches to ``parse_msq_spec`` and returns the
resulting ``DruidParsedSpec`` directly.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from migrator.core.errors import ParseError
from migrator.druid.models import (
    DruidDimensionsSpec,
    DruidGranularitySpec,
    DruidIoConfig,
    DruidMetricSpec,
    DruidParsedSpec,
    DruidTimestampSpec,
)


# ─────────────────────────────────────────────────────────────────────────────
# Detection
# ─────────────────────────────────────────────────────────────────────────────


def looks_like_msq(raw: dict) -> bool:
    """True when ``raw`` is shaped like an MSQ task submission.

    Heuristic, not exhaustive — operators usually save MSQ specs as
    the literal POST body to ``/druid/v2/sql/task``: ``{"query":
    "...", "context": {...}}``. We accept either:

      - top-level ``query`` containing a string starting with
        REPLACE INTO / INSERT INTO (case-insensitive), OR
      - top-level ``sql`` carrying the same — Druid documentation
        sometimes uses this name interchangeably.

    Specs without these markers fall through to the classic parser
    (which then handles ``index_parallel``, ``kafka``, ``kinesis``).
    """
    if not isinstance(raw, dict):
        return False
    sql = raw.get("query") or raw.get("sql")
    if not isinstance(sql, str):
        return False
    head = sql.lstrip().upper()
    return head.startswith("REPLACE INTO") or head.startswith("INSERT INTO")


# ─────────────────────────────────────────────────────────────────────────────
# Preprocessing — strip Druid-only clauses before handing to sqlglot
# ─────────────────────────────────────────────────────────────────────────────


# REPLACE INTO is Druid-only; treat as INSERT INTO for parsing.
_REPLACE_INTO_RE = re.compile(r"\breplace\s+into\b", re.IGNORECASE)

# OVERWRITE WHERE <expr>; we capture the expression but strip the
# clause so sqlglot doesn't choke. The expression matches up to the
# next major clause (SELECT) — non-greedy to handle multi-line.
_OVERWRITE_WHERE_RE = re.compile(
    r"\boverwrite\s+where\b\s+(?P<expr>.+?)(?=\bselect\b)",
    re.IGNORECASE | re.DOTALL,
)
# OVERWRITE ALL → drop the whole partition; same handling — strip,
# emit a note.
_OVERWRITE_ALL_RE = re.compile(r"\boverwrite\s+all\b", re.IGNORECASE)

# PARTITIONED BY <token>; <token> is one of HOUR / DAY / MONTH / YEAR
# / ALL or TIME_FLOOR(...). We capture just the token; complex
# TIME_FLOOR forms are normalised to their second arg (the period
# string).
_PARTITIONED_BY_RE = re.compile(
    r"\bpartitioned\s+by\b\s+(?P<gran>[A-Z_]+(?:\([^)]*\))?|\([^)]*\))",
    re.IGNORECASE,
)

_CLUSTERED_BY_RE = re.compile(
    r"\bclustered\s+by\b\s+(?P<cols>[^;]+?)(?=\s*(?:;|$|partitioned\s+by))",
    re.IGNORECASE,
)


@dataclass
class _MsqClauses:
    """Druid-only clauses extracted via regex, removed before sqlglot.

    None values mean "not present in the spec". ``overwrite_filter``
    is the SQL expression as a string — we don't try to parse it
    further; it's surfaced verbatim in the parsed-spec notes.
    """
    overwrite_filter: str | None = None
    overwrite_all: bool = False
    partitioned_by: str | None = None
    clustered_by: list[str] | None = None


def _extract_and_strip_msq_clauses(sql: str) -> tuple[str, _MsqClauses]:
    """Pull Druid-specific clauses out of the SQL string and return
    (stripped_sql, clauses). ``stripped_sql`` is what sqlglot can
    actually parse."""
    clauses = _MsqClauses()

    m = _OVERWRITE_WHERE_RE.search(sql)
    if m:
        clauses.overwrite_filter = m.group("expr").strip().rstrip(";").strip()
        sql = _OVERWRITE_WHERE_RE.sub("", sql)
    if _OVERWRITE_ALL_RE.search(sql):
        clauses.overwrite_all = True
        sql = _OVERWRITE_ALL_RE.sub("", sql)

    m = _PARTITIONED_BY_RE.search(sql)
    if m:
        clauses.partitioned_by = _normalise_partitioned_by(m.group("gran"))
        sql = _PARTITIONED_BY_RE.sub("", sql)

    m = _CLUSTERED_BY_RE.search(sql)
    if m:
        cols_raw = m.group("cols")
        # Split on commas, strip whitespace + trailing parens.
        cols = [
            c.strip().strip(',').strip('"').strip()
            for c in cols_raw.split(",") if c.strip()
        ]
        clauses.clustered_by = cols
        sql = _CLUSTERED_BY_RE.sub("", sql)

    sql = _REPLACE_INTO_RE.sub("INSERT INTO", sql)
    return sql, clauses


# Druid period strings → segmentGranularity bucket.
_PERIOD_TO_GRANULARITY = {
    "PT1H": "HOUR", "PT15M": "FIFTEEN_MINUTE", "PT30M": "THIRTY_MINUTE",
    "PT1M": "MINUTE", "PT1S": "SECOND",
    "P1D": "DAY", "P1W": "WEEK", "P1M": "MONTH", "P3M": "QUARTER",
    "P1Y": "YEAR",
}


def _normalise_partitioned_by(token: str) -> str:
    """Normalise the PARTITIONED BY argument into a Druid
    granularitySpec.segmentGranularity string.

    Handles:
      - bare keywords (HOUR / DAY / MONTH / YEAR / ALL).
      - ``TIME_FLOOR(__time, 'PT1H')`` → HOUR (period mapped via the
        table above).
      - ``FLOOR(__time TO HOUR)`` → HOUR.
      - Anything unknown → returned as-is (the normalizer downstream
        will surface it as a custom granularity).
    """
    t = token.strip().upper()
    if t in {"HOUR", "DAY", "MONTH", "YEAR", "ALL", "WEEK", "QUARTER", "MINUTE", "SECOND"}:
        return t
    # TIME_FLOOR(__time, 'PT1H')
    m = re.match(r"TIME_FLOOR\s*\([^,]+,\s*'([^']+)'\s*\)", t)
    if m:
        return _PERIOD_TO_GRANULARITY.get(m.group(1), m.group(1))
    # FLOOR(__time TO HOUR)
    m = re.match(r"FLOOR\s*\([^)]+\bTO\s+([A-Z]+)\)", t)
    if m:
        return m.group(1)
    return token.strip()


# ─────────────────────────────────────────────────────────────────────────────
# EXTERN(...) extraction
# ─────────────────────────────────────────────────────────────────────────────


# Druid type → canonical-model column-spec dict.
_DRUID_COL_TYPE_MAP = {
    "string": {"type": "string"},
    "long":   {"type": "long"},
    "float":  {"type": "float"},
    "double": {"type": "double"},
}


def _extern_args(select: exp.Select) -> tuple[str, dict, list[dict]] | None:
    """Find the EXTERN(...) call inside the SELECT and return
    (uri, input_format_dict, schema_list).

    Returns None when the SELECT doesn't read from an EXTERN — the
    caller surfaces this as an unsupported case (Druid datasource
    sources, JOINs, etc.).
    """
    for node in select.find_all(exp.Anonymous):
        if node.name.upper() != "EXTERN":
            continue
        args = node.expressions
        if len(args) < 2:
            continue
        uri = _strip_quotes(args[0].sql())
        try:
            input_format = json.loads(_strip_quotes(args[1].sql()))
        except json.JSONDecodeError:
            input_format = {"type": _strip_quotes(args[1].sql())}
        schema_list: list[dict] = []
        if len(args) >= 3:
            try:
                schema_list = json.loads(_strip_quotes(args[2].sql()))
            except json.JSONDecodeError:
                schema_list = []
        return uri, input_format, schema_list
    return None


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1]
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Top-level: SQL → DruidParsedSpec
# ─────────────────────────────────────────────────────────────────────────────


# sqlglot tags every aggregate with one of these.
_AGG_TYPES = (
    exp.Sum, exp.Count, exp.Max, exp.Min, exp.Avg, exp.AggFunc,
    exp.ApproxDistinct, exp.Quantile,
)


def parse_msq_spec(raw: dict) -> tuple[DruidParsedSpec, list[str]]:
    """Translate an MSQ spec dict to the DruidParsedSpec shape the
    rest of the pipeline expects.

    Returns (parsed_spec, warnings). Raises ``ParseError`` for
    unsupported shapes (joins, subqueries, non-EXTERN sources).
    """
    sql = raw.get("query") or raw.get("sql") or ""
    if not isinstance(sql, str) or not sql.strip():
        raise ParseError("MSQ spec has no SQL ``query``")

    warnings: list[str] = []
    stripped, clauses = _extract_and_strip_msq_clauses(sql)

    try:
        parsed = sqlglot.parse_one(stripped)
    except sqlglot.errors.ParseError as exc:
        raise ParseError(f"failed to parse MSQ SQL: {exc}") from exc

    if not isinstance(parsed, exp.Insert):
        raise ParseError(
            "MSQ spec must start with REPLACE INTO or INSERT INTO; "
            f"got top-level node {type(parsed).__name__}"
        )

    target = parsed.this
    # ``INSERT INTO events`` parses as Table; pull the name.
    if isinstance(target, exp.Schema):
        target = target.this
    if isinstance(target, exp.Table):
        datasource_name = target.name
    else:
        datasource_name = str(target)

    select = parsed.expression
    if not isinstance(select, exp.Select):
        raise ParseError(
            "MSQ spec's INSERT body is not a SELECT — joins / unions / "
            "CTEs are not supported."
        )

    # Reject joins / multiple sources up-front; the resulting
    # dataSchema would be ambiguous.
    joins = list(select.find_all(exp.Join))
    if joins:
        raise ParseError(
            "MSQ specs with JOIN clauses are not supported — Pinot "
            "schema shape can't be inferred unambiguously. Materialise "
            "the join into a single table upstream and re-run."
        )

    extern = _extern_args(select)
    if extern is None:
        raise ParseError(
            "MSQ spec's SELECT does not read from EXTERN(...) — "
            "Druid-datasource sources and Kafka EXTERN are out of "
            "scope. Use the EXTERN form for file/object-store inputs."
        )
    extern_uri, input_format, schema_list = extern

    # Walk the SELECT list: anything aggregating is a metric, anything
    # else is a dimension. The time column is whichever output is
    # named __time (the Druid convention) or the first TIME_FLOOR
    # alias if no __time output exists.
    dimensions: list[dict] = []
    metrics: list[DruidMetricSpec] = []
    time_column = "__time"
    query_granularity = "NONE"

    schema_by_name = {s.get("name"): s for s in schema_list if isinstance(s, dict)}

    for select_expr in select.expressions:
        alias = select_expr.alias_or_name
        # An aggregate expression → metric.
        is_agg = any(
            isinstance(node, _AGG_TYPES) for node in select_expr.walk()
        )
        if is_agg:
            metric_type, field_name = _classify_aggregate(select_expr)
            metrics.append(DruidMetricSpec(
                type=metric_type, name=alias, fieldName=field_name,
            ))
            continue
        # TIME_FLOOR(__time, 'PT1H') AS __time → time column with
        # query granularity HOUR.
        time_floor = _find_time_floor(select_expr)
        if time_floor:
            time_column, query_granularity = time_floor
            continue
        # Plain ``__time`` column (no aggregate, no TIME_FLOOR) is
        # also the time field — Druid uses ``__time`` as the magic
        # time-column name, so ``SELECT __time, ...`` carries it
        # through verbatim. Recognise it and skip the dim list.
        if alias == "__time":
            time_column = "__time"
            continue
        # Plain column → dimension. Look up Druid type from the
        # EXTERN schema; fall back to string.
        col_type = "string"
        if alias in schema_by_name:
            col_type = schema_by_name[alias].get("type", "string")
        dimensions.append({"type": col_type, "name": alias})

    # GROUP BY presence → rollup is intended.
    rollup = bool(metrics) and select.args.get("group") is not None

    granularity_spec = DruidGranularitySpec(
        type="uniform",
        segmentGranularity=clauses.partitioned_by or "DAY",
        queryGranularity=query_granularity,
        rollup=rollup,
    )

    timestamp_spec = DruidTimestampSpec(
        column=time_column,
        # MSQ specs always wire __time as epoch millis; the operator
        # is responsible for the TIME_PARSE in the SELECT if their
        # EXTERN data has a different shape.
        format="millis",
    )

    dimensions_spec = DruidDimensionsSpec(dimensions=dimensions)

    # ioConfig: synthesised from EXTERN. inputSource scheme drops
    # out of the URI; the rest of the pipeline picks the right
    # PinotFS via the existing ``ingestion_generator`` dispatch.
    io_config = DruidIoConfig(
        type="index_parallel",
        inputSource=_input_source_from_uri(extern_uri),
        inputFormat=input_format,
        appendToExisting=False,
    )

    raw_io_config = {
        "type": "index_parallel",
        "inputSource": io_config.inputSource,
        "inputFormat": io_config.inputFormat,
    }

    notes: dict = {}
    if clauses.overwrite_filter:
        notes["msq_overwrite_filter"] = clauses.overwrite_filter
    if clauses.overwrite_all:
        notes["msq_overwrite_all"] = True
    if clauses.clustered_by:
        notes["msq_clustered_by"] = clauses.clustered_by
        warnings.append(
            f"MSQ CLUSTERED BY columns {clauses.clustered_by!r} have no "
            "direct Pinot equivalent; consider using "
            f"``tableIndexConfig.sortedColumn=[{clauses.clustered_by[0]!r}]`` "
            "post-generation."
        )

    parsed_spec = DruidParsedSpec(
        datasource_name=datasource_name,
        timestamp_spec=timestamp_spec,
        dimensions_spec=dimensions_spec,
        metrics_spec=metrics,
        granularity_spec=granularity_spec,
        io_config=io_config,
        raw_io_config=raw_io_config,
        raw_sections={"msq": notes} if notes else {},
    )
    return parsed_spec, warnings


def _find_time_floor(node: exp.Expression) -> tuple[str, str] | None:
    """Detect a ``TIME_FLOOR(<col>, '<period>')`` inside ``node``.
    Returns (column_name, granularity) when found, else None.

    Druid MSQ uses TIME_FLOOR to bucket the time column at SELECT
    time; the bucket period maps directly to Druid's
    ``queryGranularity``. Only the literal-period form is handled —
    expression-based periods (``TIME_FLOOR(__time, ?)``) fall through
    so the operator can fix manually.
    """
    for n in node.walk():
        if isinstance(n, exp.Anonymous) and n.name.upper() == "TIME_FLOOR":
            args = n.expressions
            if len(args) < 2:
                continue
            col = args[0].sql().strip().strip('"')
            period = _strip_quotes(args[1].sql())
            return col, _PERIOD_TO_GRANULARITY.get(period, period)
    return None


def _classify_aggregate(node: exp.Expression) -> tuple[str, str]:
    """Map a SELECT-list aggregate expression to a Druid metricsSpec
    entry: ``(metric_type, field_name)``.

    Examples:
      ``COUNT(*)``           → ("count", "")
      ``SUM(x)``             → ("longSum"|"doubleSum", "x")  — operator
                               picks the int/double flavour at runtime;
                               default to ``longSum`` here, the
                               normalizer downgrades when needed.
      ``MAX(x)`` / ``MIN(x)`` → ("longMax"/"longMin", "x")

    Best-effort — unrecognised aggregates produce ``"unknown"``
    which the canonical-model normalizer surfaces as a warning.
    """
    inner = node.this if isinstance(node, exp.Alias) else node
    if isinstance(inner, exp.Count):
        return "count", ""
    if isinstance(inner, exp.Sum):
        target = inner.this
        col = target.sql().strip().strip('"') if target else ""
        return "longSum", col
    if isinstance(inner, exp.Max):
        col = inner.this.sql().strip().strip('"') if inner.this else ""
        return "longMax", col
    if isinstance(inner, exp.Min):
        col = inner.this.sql().strip().strip('"') if inner.this else ""
        return "longMin", col
    if isinstance(inner, exp.Avg):
        col = inner.this.sql().strip().strip('"') if inner.this else ""
        # Druid has no native avg; surfaced as "avg" so the
        # downstream normalizer can warn.
        return "avg", col
    return "unknown", ""


def _input_source_from_uri(uri: str) -> dict:
    """Pick the Druid inputSource shape from a URI scheme.

    The resulting dict feeds the ``ingestion_generator`` PinotFS
    dispatch (``s3`` → S3PinotFS, ``gs``/``gcs`` → GcsPinotFS,
    ``hdfs`` → HadoopPinotFS, otherwise local).
    """
    scheme = ""
    if "://" in uri:
        scheme = uri.split("://", 1)[0].lower()
    if scheme in ("s3", "s3a", "s3n"):
        return {"type": "s3", "uris": [uri]}
    if scheme in ("gs", "gcs"):
        return {"type": "google", "uris": [uri]}
    if scheme == "hdfs":
        return {"type": "hdfs", "uris": [uri]}
    if scheme in ("http", "https"):
        return {"type": "http", "uris": [uri]}
    # Local / no scheme — Druid's local inputSource takes baseDir.
    return {"type": "local", "baseDir": uri}
