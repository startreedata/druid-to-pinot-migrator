"""
Thin Druid Overlord HTTP client used by ``dpm extract-offsets``.

Designed for testability:

- Takes an injectable ``requests.Session``-like object so unit tests can
  drop in a mock/replay session without monkey-patching.
- Methods return domain objects (``StreamOffsetMap``), not raw JSON, so the
  CLI / planner don't need to know about Druid wire format.
- All HTTP calls have explicit timeouts and raise typed exceptions.

This module knows about Druid; nothing in the rest of the codebase imports
``requests``, which keeps the migration core dependency-light.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Protocol

from migrator.realtime.models import (
    KafkaPartitionOffset,
    KinesisShardSequence,
    StreamOffsetMap,
    StreamPlatform,
)


# ─────────────────────────────────────────────────────────────────────────────
# Errors
# ─────────────────────────────────────────────────────────────────────────────


class DruidOverlordError(Exception):
    """Raised when the Overlord returns an error or unexpected payload."""


# ─────────────────────────────────────────────────────────────────────────────
# Session protocol — anything ``requests.Session``-shaped works
# ─────────────────────────────────────────────────────────────────────────────


class _Session(Protocol):
    """Just the slice of requests.Session this client actually uses."""

    def get(self, url: str, *, timeout: float | None = ...) -> Any: ...


# ─────────────────────────────────────────────────────────────────────────────
# Client
# ─────────────────────────────────────────────────────────────────────────────


class DruidOverlordClient:
    """
    HTTP client for Druid's Overlord supervisor APIs.

    Parameters
    ----------
    overlord_url
        Base URL of the Druid Overlord (or Router, which proxies it).
    session
        Anything that implements ``.get(url, timeout=...)`` and returns a
        response with ``.status_code``, ``.text``, and ``.json()``. Defaults
        to a fresh ``requests.Session``.
    timeout
        Per-request timeout in seconds.
    """

    def __init__(
        self,
        overlord_url: str,
        session: _Session | None = None,
        *,
        timeout: float = 15.0,
    ) -> None:
        self._url = overlord_url.rstrip("/")
        self._timeout = timeout
        if session is None:
            import requests

            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
        self._session = session

    # ── public API ─────────────────────────────────────────────────────────

    def get_supervisor_status(self, supervisor_id: str) -> dict:
        """Return the raw ``/supervisor/{id}/status`` JSON payload."""
        url = f"{self._url}/druid/indexer/v1/supervisor/{supervisor_id}/status"
        return self._json_get(url)

    def get_supervisor_spec(self, supervisor_id: str) -> dict:
        """Return the raw ``/supervisor/{id}`` JSON payload (the full spec)."""
        url = f"{self._url}/druid/indexer/v1/supervisor/{supervisor_id}"
        return self._json_get(url)

    def list_supervisors(self) -> list[str]:
        """List all active supervisor IDs known to the Overlord."""
        url = f"{self._url}/druid/indexer/v1/supervisor"
        ids = self._json_get(url)
        if not isinstance(ids, list):
            raise DruidOverlordError(
                f"Unexpected /supervisor response shape: {type(ids).__name__}"
            )
        return ids

    def find_supervisor_for_datasource(
        self, datasource: str
    ) -> str | None:
        """
        Return the supervisor ID whose spec ingests into the given
        datasource, or None if no supervisor matches. Iterates the active
        list and inspects each one's spec — N+1 cost, fine for typical
        clusters with a handful of supervisors.
        """
        for sup_id in self.list_supervisors():
            try:
                spec = self.get_supervisor_spec(sup_id)
            except DruidOverlordError:
                continue
            if _safe_get(spec, ["spec", "dataSchema", "dataSource"]) == datasource:
                return sup_id
        return None

    def get_supervisor_offsets(self, supervisor_id: str) -> StreamOffsetMap:
        """
        Build a :class:`StreamOffsetMap` for the given supervisor.

        Works for both Kafka and Kinesis supervisors.

        Druid's ``/supervisor/{id}/status`` payload is produced by the
        shared ``SeekableStreamSupervisorReportPayload`` base class, so
        it is **structurally identical** for Kafka and Kinesis: both
        report per-partition/per-shard positions under ``latestOffsets``
        and the stream identifier under ``stream``. (Kinesis values are
        opaque sequence-number strings; Kafka values are integers — but
        the field name is the same.) The payload therefore cannot tell
        the platforms apart on its own.

        The platform is taken from the supervisor **spec's** top-level
        ``type`` (``kafka`` / ``kinesis``) — the authoritative
        discriminator. The spec is always fetched (one extra request;
        ``extract-offsets`` is a one-shot operation, not a hot path). A
        spec-endpoint failure degrades gracefully via ioConfig shape and
        an offset-value-type heuristic.

        Synthesis logic (common):

        - The watermark timestamp comes from the supervisor status's
          ``lastIngestedTimestamp`` / ``aggregateLag.timestamp`` /
          ``timestamp``, falling back to "now".
        - ``datasource`` comes from the status payload or supervisor id.
        - Positions come from ``latestOffsets`` (``currentOffsets`` as a
          fallback for older Druid).

        Per platform:

        - **Kafka** — per-partition integer offsets → ``offsets``.
        - **Kinesis** — per-shard sequence-number strings →
          ``shard_sequences``.
        """
        status = self.get_supervisor_status(supervisor_id)
        payload = status.get("payload") or {}
        spec = self._try_get_supervisor_spec(supervisor_id)
        datasource = payload.get("dataSource") or supervisor_id
        watermark_ms, watermark_iso = _resolve_watermark(payload)

        positions: dict = (
            payload.get("latestOffsets")
            or payload.get("currentOffsets")
            or {}
        )
        if not isinstance(positions, dict):
            raise DruidOverlordError(
                "Unexpected supervisor status shape: latestOffsets is "
                f"{type(positions).__name__}, expected dict"
            )
        # The supervisor-level ``latestOffsets`` is computed lazily (a
        # periodic stream-head query) and is often absent on a freshly
        # started supervisor — especially for Kinesis, where it can stay
        # null for minutes. Fall back to the positions the tasks have
        # actually consumed, reported under ``activeTasks[].currentOffsets``
        # (and ``publishingTasks`` for a task mid-handoff). These are the
        # more accurate cutover boundary anyway: what Druid has consumed,
        # not the stream head.
        if not positions:
            positions = _positions_from_tasks(payload)

        platform = _detect_platform(spec, payload, positions)

        # Stream / topic identifier — same ``stream`` field for both,
        # with the spec's ioConfig as a fallback.
        stream = (
            payload.get("stream")
            or payload.get("topic")
            or _safe_get(spec, ["spec", "ioConfig", "stream"])
            or _safe_get(spec, ["spec", "ioConfig", "topic"])
        )

        if platform == StreamPlatform.KINESIS:
            if not stream:
                raise DruidOverlordError(
                    f"Could not determine Kinesis stream for supervisor "
                    f"'{supervisor_id}'"
                )
            shard_sequences = [
                KinesisShardSequence(
                    shard_id=str(shard_id),
                    sequence_number=str(seq),
                )
                for shard_id, seq in positions.items()
                if seq is not None and str(seq) != ""
            ]
            return StreamOffsetMap(
                platform=StreamPlatform.KINESIS,
                topic=stream,
                supervisor_id=supervisor_id,
                datasource=datasource,
                watermark_iso=watermark_iso,
                watermark_ms=watermark_ms,
                shard_sequences=sorted(
                    shard_sequences, key=lambda s: s.shard_id
                ),
            )

        # ── Kafka ────────────────────────────────────────────────────────
        if not stream:
            raise DruidOverlordError(
                f"Could not determine Kafka topic for supervisor '{supervisor_id}'"
            )
        partition_offsets = []
        for partition_str, offset in positions.items():
            try:
                partition_offsets.append(
                    KafkaPartitionOffset(
                        partition=int(partition_str),
                        offset=int(offset),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise DruidOverlordError(
                    f"Bad offset entry partition={partition_str!r} "
                    f"offset={offset!r}: {exc}"
                ) from exc

        return StreamOffsetMap(
            platform=StreamPlatform.KAFKA,
            topic=stream,
            supervisor_id=supervisor_id,
            datasource=datasource,
            watermark_iso=watermark_iso,
            watermark_ms=watermark_ms,
            offsets=sorted(partition_offsets, key=lambda po: po.partition),
        )

    def _try_get_supervisor_spec(self, supervisor_id: str) -> dict:
        """Fetch the supervisor spec, returning ``{}`` on any error.

        Used as a *supplementary* signal (platform detection,
        topic/stream fallback) — a spec-endpoint failure must not abort
        an otherwise-complete status snapshot."""
        try:
            return self.get_supervisor_spec(supervisor_id)
        except DruidOverlordError:
            return {}

    # ── internals ──────────────────────────────────────────────────────────

    def _json_get(self, url: str) -> dict:
        resp = self._session.get(url, timeout=self._timeout)
        if resp.status_code != 200:
            raise DruidOverlordError(
                f"GET {url} returned {resp.status_code}: "
                f"{getattr(resp, 'text', '')[:500]}"
            )
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise DruidOverlordError(
                f"GET {url} returned non-JSON body: {exc}"
            ) from exc


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _safe_get(d: dict, path: list[str]) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def _positions_from_tasks(payload: dict) -> dict:
    """Merge per-partition/shard positions from the supervisor's task
    reports, used when the supervisor-level ``latestOffsets`` is absent.

    Each entry of ``activeTasks`` / ``publishingTasks`` is a
    ``TaskReportData`` carrying ``currentOffsets`` (the positions that
    task has consumed) keyed by partition id / shard id. We merge across
    all tasks, preferring the highest value seen for a key so a task
    mid-handoff (in ``publishingTasks``) doesn't regress a position.

    Both maps hold the same value type the supervisor-level map would:
    integers for Kafka, opaque sequence strings for Kinesis. Keys don't
    overlap across tasks in practice (each task owns a disjoint set of
    partitions/shards), so the per-key comparison is only a tie-break
    safeguard for the brief window where a handing-off ``publishingTask``
    and a new ``activeTask`` both report the same partition — there we
    keep the furthest-consumed (largest) position.
    """
    def _gt(a, b) -> bool:
        # Kafka offsets are ints; Kinesis sequence numbers are long
        # all-digit strings — both compare correctly numerically.
        # Lexicographic comparison would be wrong for ints ("100" < "99").
        try:
            return int(a) > int(b)
        except (TypeError, ValueError):
            return str(a) > str(b)

    merged: dict = {}
    for key in ("activeTasks", "publishingTasks"):
        tasks = payload.get(key) or []
        if not isinstance(tasks, list):
            continue
        for task in tasks:
            if not isinstance(task, dict):
                continue
            current = task.get("currentOffsets") or {}
            if not isinstance(current, dict):
                continue
            for pid, val in current.items():
                if val is None or str(val) == "":
                    continue
                existing = merged.get(pid)
                if existing is None or _gt(val, existing):
                    merged[pid] = val
    return merged


def _detect_platform(
    spec: dict, payload: dict, positions: dict | None = None,
) -> StreamPlatform:
    """Detect the streaming platform for a supervisor.

    Druid's status payload is structurally identical for Kafka and
    Kinesis (shared ``SeekableStreamSupervisorReportPayload``: both use
    ``latestOffsets`` + ``stream``), so it cannot discriminate on its
    own. The supervisor **spec** is the source of truth.

    Order of evidence:
      1. Spec top-level ``type`` (``kafka`` / ``kinesis``) — the
         authoritative discriminator Druid stamps on every spec.
      2. ioConfig shape: a ``stream`` key (Kinesis) vs ``topic`` (Kafka).
      3. Status payload ``type`` (some Druid versions echo it).
      4. Last resort when the spec is unavailable: sniff the position
         VALUES — Kinesis sequence numbers are long opaque strings
         (~50+ chars), Kafka offsets are short integers.
    Defaults to Kafka so anything unclassifiable keeps historical
    behaviour.
    """
    sup_type = str(spec.get("type") or "").lower()
    if "kinesis" in sup_type:
        return StreamPlatform.KINESIS
    if "kafka" in sup_type:
        return StreamPlatform.KAFKA

    iocfg = _safe_get(spec, ["spec", "ioConfig"]) or {}
    if isinstance(iocfg, dict):
        if "stream" in iocfg and "topic" not in iocfg:
            return StreamPlatform.KINESIS
        if "topic" in iocfg:
            return StreamPlatform.KAFKA

    payload_type = str(payload.get("type") or "").lower()
    if "kinesis" in payload_type:
        return StreamPlatform.KINESIS
    if "kafka" in payload_type:
        return StreamPlatform.KAFKA

    # Spec unavailable and payload ambiguous: infer from the position
    # value type. A Kafka offset is an int (or short numeric string); a
    # Kinesis sequence number is a long opaque numeric string (~56 chars).
    if positions:
        sample = next(iter(positions.values()), None)
        if isinstance(sample, str) and len(sample) >= 20:
            return StreamPlatform.KINESIS

    return StreamPlatform.KAFKA


def _resolve_watermark(payload: dict) -> tuple[int, str]:
    """Return ``(epoch_ms, iso8601)`` for the watermark."""
    candidates = [
        payload.get("lastIngestedTimestamp"),
        _safe_get(payload, ["aggregateLag", "timestamp"]),
        payload.get("timestamp"),
    ]
    for cand in candidates:
        if cand is None:
            continue
        try:
            if isinstance(cand, (int, float)):
                ts_ms = int(cand)
                return ts_ms, _to_pinot_iso(
                    datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
                )
            if isinstance(cand, str):
                # Druid usually emits ISO-8601 with 'Z'
                dt = datetime.fromisoformat(cand.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
                return ts_ms, _to_pinot_iso(dt.astimezone(timezone.utc))
        except (TypeError, ValueError):
            continue
    # Fallback: now() — operator should override via --watermark-iso
    now = datetime.now(timezone.utc)
    return int(now.timestamp() * 1000), _to_pinot_iso(now)


def _to_pinot_iso(dt: datetime) -> str:
    """
    Format a UTC datetime as an ISO-8601 string Pinot's TIMESTAMP offset
    criterion accepts on every supported version (Pinot 1.0 +).

    Pinot's parser uses Java's ``Instant.parse``, which is strict:
    - Must end in ``Z`` (no ``+00:00`` offset form)
    - Fractional seconds must be 0, 3, 6, or 9 digits — but Pinot 1.0
      and 1.1 are picky about digit count, so we settle on 3 (millis).

    Python's ``datetime.isoformat()`` produces e.g.
    ``"2024-04-25T22:00:00.123456+00:00"``, which Pinot 1.0 rejects with
    ``Unknown initial offset value`` (it falls through to CUSTOM and the
    Kafka stream consumer can't translate it).

    This helper produces ``"2024-04-25T22:00:00.123Z"``.
    """
    return (
        dt.astimezone(timezone.utc)
          .isoformat(timespec="milliseconds")
          .replace("+00:00", "Z")
    )
