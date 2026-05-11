"""
Classify a Druid SQL query by Pinot compatibility risk.

This is the *query-side* counterpart to the spec-side risk analyzer:
``RiskAnalyzer`` decides whether a Druid datasource's *ingestion spec*
will translate cleanly to Pinot; this module decides whether the
*queries that hit that datasource* will translate cleanly. Operators
need both — a datasource can ingest fine but break every dashboard
that queries it because the query uses ``LOOKUP()`` and Pinot has no
equivalent.

The classifier deliberately does **not rewrite** queries. Rewriting
is risky (semantic drift), context-dependent (Pinot extensions vary
by deployment), and out of scope for an automated tool. Instead the
classifier emits a verdict + per-issue list so a human can decide:

  - **COMPATIBLE** — query parses with Druid dialect and contains no
    pattern flagged below. Should run on Pinot unchanged (modulo
    table-name suffix conventions like ``_OFFLINE`` / ``_REALTIME``,
    which are out of scope here).
  - **RISKY** — query uses a function or feature whose Pinot
    equivalent has different name, different semantics, or limited
    support. Likely portable with manual review.
  - **INCOMPATIBLE** — query uses a Druid-only feature with no Pinot
    equivalent (sketch wire formats, LOOKUP, MV_* family) or fails to
    parse altogether. Will not run on Pinot as-is.

Detection runs on the sqlglot AST. Druid-specific functions parse as
``Anonymous`` nodes; we match on the function name (case-insensitive)
against the Druid-specific lists below. Joins, windows, subqueries
are detected via their concrete AST node classes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp


# ─────────────────────────────────────────────────────────────────────────────
# Verdicts and severities
# ─────────────────────────────────────────────────────────────────────────────


VERDICT_COMPATIBLE = "compatible"
VERDICT_RISKY = "risky"
VERDICT_INCOMPATIBLE = "incompatible"

VERDICT_ORDER = (VERDICT_COMPATIBLE, VERDICT_RISKY, VERDICT_INCOMPATIBLE)

# Issue severities. We deliberately reuse the verdict labels for
# severity so an operator reading a single line of issue output can
# tell the worst-case impact at a glance.
SEV_INFO = "info"
SEV_RISKY = "risky"
SEV_INCOMPATIBLE = "incompatible"


# ─────────────────────────────────────────────────────────────────────────────
# Druid-specific function tables
# ─────────────────────────────────────────────────────────────────────────────
#
# Names are uppercased for case-insensitive matching. Each entry maps
# the Druid function to (severity, why-it-matters, Pinot equivalent
# if any). Operators can paste these directly into a remediation
# ticket — that's the audience for the ``why`` and ``pinot_equivalent``
# fields.
# ─────────────────────────────────────────────────────────────────────────────


# Wire-incompatible or Druid-only — no clean Pinot port.
DRUID_INCOMPATIBLE_FUNCTIONS: dict[str, tuple[str, str]] = {
    # Sketch reads against Druid's serialized BYTES. Pinot has its own
    # sketch implementations but the wire formats don't match — calling
    # APPROX_COUNT_DISTINCT_DS_HLL on a Pinot column would either fail
    # or return garbage.
    "APPROX_COUNT_DISTINCT_DS_HLL": (
        "Druid sketch wire format is incompatible with Pinot's HLL.",
        "DISTINCTCOUNTHLL(<raw_field>) — re-ingest raw events.",
    ),
    "APPROX_COUNT_DISTINCT_DS_THETA": (
        "Druid theta-sketch wire format is incompatible with Pinot's.",
        "DISTINCTCOUNTTHETASKETCH(<raw_field>) — re-ingest raw events.",
    ),
    "DS_HLL": (
        "Druid sketch byte-serialised; Pinot can't read it.",
        "Re-ingest raw events and use Pinot DISTINCTCOUNTHLL.",
    ),
    "DS_THETA": (
        "Druid sketch byte-serialised; Pinot can't read it.",
        "Re-ingest raw events and use Pinot DISTINCTCOUNTTHETASKETCH.",
    ),
    "DS_QUANTILE": (
        "Druid quantile sketch wire format incompatible with Pinot.",
        "PERCENTILETDIGEST(<raw_field>, <percentile>).",
    ),
    "DS_QUANTILES_SKETCH": (
        "Druid quantile sketch wire format incompatible with Pinot.",
        "PERCENTILETDIGEST(<raw_field>, <percentile>).",
    ),
    "HLL_SKETCH_ESTIMATE": (
        "Reads Druid HLL sketch bytes — wire incompatible.",
        "Re-ingest raw events; query with DISTINCTCOUNTHLL.",
    ),
    "THETA_SKETCH_ESTIMATE": (
        "Reads Druid theta sketch bytes — wire incompatible.",
        "Re-ingest raw events; query with DISTINCTCOUNTTHETASKETCH.",
    ),
    # Druid-only feature with no Pinot port.
    "LOOKUP": (
        "Druid lookups dimension; Pinot has no built-in equivalent.",
        "Pre-join upstream, or use Pinot dimension tables in SQL JOIN.",
    ),
    # MV_* family — different syntax in Pinot's MV column support.
    "MV_CONCAT": (
        "Druid MV_CONCAT has no direct Pinot equivalent.",
        "Use ARRAYTOMV / array functions on the Pinot side.",
    ),
    "MV_OVERLAP": (
        "Druid MV_OVERLAP has no direct Pinot equivalent.",
        "Use Pinot's ARRAY_CONTAINS_ANY or compare via UNNEST.",
    ),
    "MV_CONTAINS": (
        "Druid MV_CONTAINS has no direct Pinot equivalent.",
        "Use Pinot's ARRAY_CONTAINS_ALL or UNNEST + COUNT.",
    ),
    "MV_LENGTH": (
        "Druid MV_LENGTH has no direct Pinot equivalent.",
        "Use Pinot's CARDINALITY(<mv_column>).",
    ),
    "MV_OFFSET": (
        "Druid MV_OFFSET (positional MV access) has no direct Pinot equivalent.",
        "Use Pinot's array index syntax: <mv_column>[<n>].",
    ),
    "MV_FILTER_ONLY": (
        "Druid MV_FILTER_ONLY semantics differ from Pinot's MV filter.",
        "Filter via UNNEST + WHERE in Pinot.",
    ),
    "MV_FILTER_NONE": (
        "Druid MV_FILTER_NONE has no direct Pinot equivalent.",
        "Filter via UNNEST + NOT IN (...) in Pinot.",
    ),
}


# Different name, sometimes-different semantics — manual review needed
# but normally portable.
DRUID_RISKY_FUNCTIONS: dict[str, tuple[str, str]] = {
    "TIME_FLOOR": (
        "Druid TIME_FLOOR semantics map to Pinot DATETRUNC, but the "
        "argument order and granularity tokens differ.",
        "DATETRUNC('hour', <ts>) — verify the granularity string.",
    ),
    "TIME_CEIL": (
        "Druid TIME_CEIL has no exact Pinot equivalent.",
        "DATETRUNC + arithmetic, or rewrite the predicate range.",
    ),
    "TIME_SHIFT": (
        "Druid TIME_SHIFT shifts a timestamp by an ISO period.",
        "DATEADD(<unit>, <n>, <ts>) — compute the shift in the unit Pinot expects.",
    ),
    "TIME_PARSE": (
        "Druid TIME_PARSE maps to Pinot's FROMDATETIME but argument shape differs.",
        "FROMDATETIME(<str>, '<format>') — recheck format string.",
    ),
    "TIME_FORMAT": (
        "Druid TIME_FORMAT maps to Pinot's TODATETIME but argument shape differs.",
        "TODATETIME(<ts>, '<format>') — recheck format string.",
    ),
    "TIME_EXTRACT": (
        "Druid TIME_EXTRACT maps to YEAR / MONTH / HOUR scalar functions in Pinot.",
        "Use the unit-specific scalar (YEAR(<ts>), HOUR(<ts>), …).",
    ),
    "TIMESTAMP_FORMAT": (
        "Druid TIMESTAMP_FORMAT maps to Pinot's TODATETIME.",
        "TODATETIME(<ts>, '<format>').",
    ),
    "TIMESTAMP_TO_MILLIS": (
        "Druid TIMESTAMP_TO_MILLIS maps to a CAST in Pinot.",
        "CAST(<ts> AS LONG).",
    ),
    "MILLIS_TO_TIMESTAMP": (
        "Druid MILLIS_TO_TIMESTAMP maps to a CAST in Pinot.",
        "CAST(<long_ms> AS TIMESTAMP).",
    ),
    "TIMESTAMPADD": (
        "Druid TIMESTAMPADD accepts unit names Pinot's DATEADD doesn't (e.g. 'minute').",
        "DATEADD('<unit>', <n>, <ts>) — confirm the unit string.",
    ),
    "TIMESTAMPDIFF": (
        "Druid TIMESTAMPDIFF accepts unit names Pinot's DATEDIFF doesn't.",
        "DATEDIFF('<unit>', <ts1>, <ts2>) — confirm the unit string.",
    ),
    "BLOOM_FILTER": (
        "Druid BLOOM_FILTER is Druid-specific; Pinot uses bloom indexes implicitly.",
        "Drop the function — Pinot's bloom-filter index is selected at query time.",
    ),
    "ARRAY_TO_STRING": (
        "Druid array → string syntax differs from Pinot's.",
        "ARRAYTOMV / Pinot array functions.",
    ),
    "STRING_FORMAT": (
        "Druid STRING_FORMAT (printf-style) has no direct Pinot equivalent.",
        "CONCAT and CAST chains, or pre-format upstream.",
    ),
    "REGEXP_LIKE": (
        "Druid REGEXP_LIKE is Java regex; Pinot's is Java regex too but flag set differs.",
        "Verify regex flags after migration.",
    ),
    "REGEXP_EXTRACT": (
        "Druid REGEXP_EXTRACT differs slightly from Pinot's REGEXP_EXTRACT in group handling.",
        "Verify capturing-group index and matching behaviour.",
    ),
}


# Druid SQL has these as recognised AST node types (sqlglot parses
# them into specific classes, not Anonymous). Detection is by
# walking for those node types directly.
JSON_EXTRACT_NODES = (exp.JSONExtract, exp.JSONExtractScalar)


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class QueryIssue:
    """One pattern in one query that affects portability."""
    pattern: str           # 'LOOKUP', 'JOIN', 'WINDOW_FUNCTION', 'PARSE_ERROR', …
    severity: str          # SEV_INFO / SEV_RISKY / SEV_INCOMPATIBLE
    detail: str            # Human-readable specifics
    pinot_equivalent: str  # Suggested rewrite pattern; "" when none

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern": self.pattern,
            "severity": self.severity,
            "detail": self.detail,
            "pinot_equivalent": self.pinot_equivalent,
        }


@dataclass
class QueryClassification:
    """Verdict + issues for one query."""
    query_id: str          # Caller-supplied label; usually filename or index
    verdict: str           # VERDICT_COMPATIBLE / RISKY / INCOMPATIBLE
    issues: list[QueryIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "verdict": self.verdict,
            "issues": [i.to_dict() for i in self.issues],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Classification
# ─────────────────────────────────────────────────────────────────────────────


def _verdict_from_issues(issues: list[QueryIssue]) -> str:
    """Roll up per-issue severities to a single query-level verdict.
    Worst severity wins — one INCOMPATIBLE flips the whole query, even
    if 99 other issues are merely RISKY."""
    if any(i.severity == SEV_INCOMPATIBLE for i in issues):
        return VERDICT_INCOMPATIBLE
    if any(i.severity == SEV_RISKY for i in issues):
        return VERDICT_RISKY
    return VERDICT_COMPATIBLE


def _function_name(node: exp.Expression) -> str:
    """Best-effort extract of a function call's name as uppercase.

    sqlglot represents Druid-specific functions as ``Anonymous(this=
    "FUNC_NAME", ...)``. Built-in functions are concrete subclasses
    where the class name is the function — we use ``key`` (the
    canonical lowercase form) and uppercase it for the table lookup.
    """
    if isinstance(node, exp.Anonymous):
        return str(node.this or "").upper()
    return type(node).__name__.upper()


def _detect_function_issues(tree: exp.Expression) -> list[QueryIssue]:
    """Walk the AST and emit one QueryIssue per recognised Druid-only
    or risky function. Duplicate functions emit duplicate issues —
    rolling up duplicates is the aggregator's job, not the
    classifier's."""
    issues: list[QueryIssue] = []
    for node in tree.find_all(exp.Anonymous):
        name = _function_name(node)
        if name in DRUID_INCOMPATIBLE_FUNCTIONS:
            why, eq = DRUID_INCOMPATIBLE_FUNCTIONS[name]
            issues.append(QueryIssue(
                pattern=name,
                severity=SEV_INCOMPATIBLE,
                detail=why,
                pinot_equivalent=eq,
            ))
        elif name in DRUID_RISKY_FUNCTIONS:
            why, eq = DRUID_RISKY_FUNCTIONS[name]
            issues.append(QueryIssue(
                pattern=name,
                severity=SEV_RISKY,
                detail=why,
                pinot_equivalent=eq,
            ))
    return issues


def _detect_structural_issues(tree: exp.Expression) -> list[QueryIssue]:
    """Detect structural patterns (JOIN, window function, subquery,
    JSON path operator) that map differently to Pinot.

    All flagged as RISKY — Pinot supports each in some form; the
    edge cases that matter (correlated subqueries, multi-hop joins,
    LAG/LEAD over partitioned windows) are too query-specific for
    the classifier to render INCOMPATIBLE without false positives.
    """
    issues: list[QueryIssue] = []

    if list(tree.find_all(exp.Join)):
        issues.append(QueryIssue(
            pattern="JOIN",
            severity=SEV_RISKY,
            detail=(
                "Druid supports broadcast joins to lookup tables and "
                "Pinot supports lookups + dimension tables, but multi-"
                "hop joins and correlated joins map differently."
            ),
            pinot_equivalent=(
                "Single-table lookups port directly. For multi-table "
                "joins, configure dimension tables and verify routing."
            ),
        ))

    if list(tree.find_all(exp.Window)):
        issues.append(QueryIssue(
            pattern="WINDOW_FUNCTION",
            severity=SEV_RISKY,
            detail=(
                "Pinot supports window functions in 1.x but feature "
                "coverage trails Druid. RANGE frames, EXCLUDE clauses, "
                "and non-aggregate window functions may not port."
            ),
            pinot_equivalent="Verify on a Pinot 1.x test cluster before deploy.",
        ))

    # Subquery in FROM (derived table) and in WHERE (filter / scalar)
    # both deserve flagging. sqlglot represents them all as Subquery.
    if list(tree.find_all(exp.Subquery)):
        issues.append(QueryIssue(
            pattern="SUBQUERY",
            severity=SEV_RISKY,
            detail=(
                "Pinot supports subqueries with limitations; correlated "
                "subqueries and IN-subqueries on large dimension tables "
                "have different performance / correctness profiles."
            ),
            pinot_equivalent=(
                "Rewrite as a JOIN where possible, or pre-compute the "
                "subquery's output upstream."
            ),
        ))

    if any(isinstance(n, JSON_EXTRACT_NODES) for n in tree.walk()):
        issues.append(QueryIssue(
            pattern="JSON_PATH",
            severity=SEV_RISKY,
            detail=(
                "Druid's '->' / '->>' JSON path operators have direct "
                "Pinot equivalents (JSON_EXTRACT_SCALAR, JSON_EXTRACT_KEY) "
                "but operator-style sugar isn't supported in Pinot SQL."
            ),
            pinot_equivalent=(
                "Rewrite as JSON_EXTRACT_SCALAR(<col>, '<jsonpath>', '<type>')."
            ),
        ))

    return issues


def classify_query(
    sql: str, *, query_id: str = "query",
) -> QueryClassification:
    """Parse a single Druid SQL string and classify it.

    A parse failure is itself an INCOMPATIBLE issue — if sqlglot's
    Druid dialect can't make sense of it, neither will Pinot's.
    Operators occasionally have malformed-but-Druid-accepted SQL in
    their dashboards (string literals with unescaped quotes, missing
    aliases on legacy MSQ output); those should surface in the
    report rather than crash the run.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="druid")
    except Exception as exc:  # noqa: BLE001 — sqlglot raises a wide variety
        return QueryClassification(
            query_id=query_id,
            verdict=VERDICT_INCOMPATIBLE,
            issues=[QueryIssue(
                pattern="PARSE_ERROR",
                severity=SEV_INCOMPATIBLE,
                detail=f"Druid SQL did not parse: {exc}",
                pinot_equivalent=(
                    "Fix the SQL syntax. If it parses in Druid but not "
                    "sqlglot's druid dialect, file an issue with the "
                    "minimum-reproducing query."
                ),
            )],
        )

    if tree is None:
        # Empty / whitespace-only string. Treat as incompatible —
        # there's no valid query to migrate.
        return QueryClassification(
            query_id=query_id,
            verdict=VERDICT_INCOMPATIBLE,
            issues=[QueryIssue(
                pattern="EMPTY_QUERY",
                severity=SEV_INCOMPATIBLE,
                detail="Empty SQL string — nothing to classify.",
                pinot_equivalent="",
            )],
        )

    issues = _detect_function_issues(tree) + _detect_structural_issues(tree)
    return QueryClassification(
        query_id=query_id,
        verdict=_verdict_from_issues(issues),
        issues=issues,
    )
