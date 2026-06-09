"""
Watermark formatting + refinement for hybrid Druid → Pinot cutover.

The watermark is the boundary timestamp Pinot's REALTIME table resumes
from (``auto.offset.reset = <watermark_iso>``). It must be the time up to
which Druid has ingested, so the OFFLINE backfill (< watermark) and the
REALTIME consume (>= watermark) meet with no gap.

Kafka supervisors report a precise ``lastIngestedTimestamp``, so the
captured watermark is exact. Kinesis supervisors report no absolute
timestamp at all, so capture falls back to ``now()`` — which is unsafe
if the supervisor is lagging: Pinot would start consuming at ``now()``
and skip events between Druid's true last-ingested time and ``now()``
that are still sitting in the stream. ``refine_watermark`` replaces such
an estimated watermark with ``MAX(__time)`` of the datasource — the exact
boundary, platform-agnostic. Worst case it leaves a tiny overlap at the
boundary (safe), never a gap (lossy).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from migrator.realtime.models import StreamOffsetMap


def to_pinot_iso(dt: datetime) -> str:
    """Format a UTC datetime as the ISO-8601 string Pinot's TIMESTAMP
    offset criterion accepts on every supported version (Pinot 1.0+):
    ``...Z`` (not ``+00:00``) with exactly millisecond precision, e.g.
    ``"2024-04-25T22:00:00.123Z"``. Pinot 1.0/1.1 reject microsecond
    precision and the ``+00:00`` offset form."""
    return (
        dt.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def parse_epoch_ms(value: Any) -> tuple[int, str] | None:
    """Coerce a Druid timestamp cell — epoch millis (int/float or numeric
    string) or an ISO-8601 string — into ``(epoch_ms, pinot_iso)``.
    Returns None when the value can't be interpreted as a timestamp."""
    if value is None:
        return None
    # Numeric epoch millis (Druid ``CAST(__time AS BIGINT)`` returns this).
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        ms = int(value)
        return ms, to_pinot_iso(datetime.fromtimestamp(ms / 1000, timezone.utc))
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit():
            ms = int(s)
            return ms, to_pinot_iso(datetime.fromtimestamp(ms / 1000, timezone.utc))
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000), to_pinot_iso(dt)
    return None


def refine_watermark(
    offset_map: StreamOffsetMap,
    *,
    druid_sql_query: Callable[[str], list],
) -> StreamOffsetMap:
    """Return ``offset_map`` with a precise watermark when its watermark is
    estimated (a ``now()`` fallback), by querying ``MAX(__time)`` of the
    datasource. Otherwise return it unchanged.

    ``druid_sql_query`` is any callable taking a SQL string and returning a
    list of row dicts (e.g. ``DruidHttpSqlClient.query``). The refinement
    is best-effort: a missing datasource, an empty result, an unparseable
    value, or a query error all leave the original estimated watermark
    intact (and still flagged ``watermark_estimated=True``) rather than
    breaking the cutover — the operator can still override manually.
    """
    if not offset_map.watermark_estimated:
        return offset_map
    ds = offset_map.datasource
    if not ds:
        return offset_map

    # CAST to BIGINT so Druid returns unambiguous epoch milliseconds
    # regardless of the SQL result-format's timestamp rendering.
    sql = f'SELECT CAST(MAX("__time") AS BIGINT) AS wm FROM "{ds}"'
    try:
        rows = druid_sql_query(sql)
    except Exception:  # noqa: BLE001 — best-effort; keep the estimate
        return offset_map
    if not rows:
        return offset_map
    row = rows[0]
    raw = row.get("wm") if isinstance(row, dict) else (row[0] if row else None)
    parsed = parse_epoch_ms(raw)
    if parsed is None:
        return offset_map
    ms, iso = parsed
    return offset_map.model_copy(
        update={
            "watermark_ms": ms,
            "watermark_iso": iso,
            "watermark_estimated": False,
        }
    )
