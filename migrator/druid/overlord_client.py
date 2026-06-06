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

        Works for both Kafka and Kinesis supervisors. The platform is
        detected from the status payload's shape first (Kinesis exposes
        ``latestSequenceNumbers`` / ``stream``; Kafka exposes
        ``latestOffsets`` / ``topic``) — no extra HTTP call. Only when
        the payload is ambiguous is the supervisor spec consulted (and
        defensively: a spec-endpoint hiccup must not fail a status
        snapshot).

        Synthesis logic (common):

        - The watermark timestamp comes from the supervisor status's
          ``lastIngestedTimestamp`` / ``aggregateLag.timestamp`` /
          ``timestamp``, falling back to "now".
        - ``datasource`` comes from the status payload or supervisor id.

        Per platform:

        - **Kafka** — ``topic`` from the payload (spec as fallback);
          per-partition integer offsets from
          ``status.payload.latestOffsets`` → ``offsets``.
        - **Kinesis** — stream name from the payload (spec as fallback);
          per-shard sequence-number strings from
          ``status.payload.latestSequenceNumbers`` → ``shard_sequences``.
        """
        status = self.get_supervisor_status(supervisor_id)
        payload = status.get("payload") or {}
        datasource = payload.get("dataSource") or supervisor_id
        watermark_ms, watermark_iso = _resolve_watermark(payload)

        # Detect from the payload shape first (no extra call); fall back
        # to a defensive spec fetch only when the payload is ambiguous.
        platform = _detect_platform_from_payload(payload)
        spec: dict | None = None
        if platform is None:
            spec = self._try_get_supervisor_spec(supervisor_id)
            platform = _detect_platform(spec or {}, payload)

        if platform == StreamPlatform.KINESIS:
            stream = payload.get("stream")
            if not stream:
                if spec is None:
                    spec = self._try_get_supervisor_spec(supervisor_id)
                stream = _safe_get(spec or {}, ["spec", "ioConfig", "stream"])
            if not stream:
                raise DruidOverlordError(
                    f"Could not determine Kinesis stream for supervisor "
                    f"'{supervisor_id}'"
                )
            latest_seqs: dict = (
                payload.get("latestSequenceNumbers")
                or payload.get("currentSequenceNumbers")
                or {}
            )
            if not isinstance(latest_seqs, dict):
                raise DruidOverlordError(
                    "Unexpected supervisor status shape: latestSequenceNumbers "
                    f"is {type(latest_seqs).__name__}, expected dict"
                )
            shard_sequences = [
                KinesisShardSequence(
                    shard_id=str(shard_id),
                    sequence_number=str(seq),
                )
                for shard_id, seq in latest_seqs.items()
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

        # ── Kafka (default) ──────────────────────────────────────────────
        topic = payload.get("topic")
        if not topic:
            if spec is None:
                spec = self._try_get_supervisor_spec(supervisor_id)
            topic = _safe_get(spec or {}, ["spec", "ioConfig", "topic"])
        if not topic:
            raise DruidOverlordError(
                f"Could not determine Kafka topic for supervisor '{supervisor_id}'"
            )

        latest_offsets: dict = (
            payload.get("latestOffsets")
            or payload.get("currentOffsets")
            or {}
        )
        if not isinstance(latest_offsets, dict):
            raise DruidOverlordError(
                "Unexpected supervisor status shape: "
                f"latestOffsets is {type(latest_offsets).__name__}, expected dict"
            )

        partition_offsets = []
        for partition_str, offset in latest_offsets.items():
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
            topic=topic,
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


def _detect_platform_from_payload(payload: dict) -> StreamPlatform | None:
    """Detect the platform from the status payload's shape alone, or
    None if the payload carries no discriminating signal.

    Kinesis supervisor status exposes ``latestSequenceNumbers`` /
    ``currentSequenceNumbers`` / ``stream``; Kafka exposes
    ``latestOffsets`` / ``currentOffsets`` / ``topic``. Checking the
    payload avoids a spec fetch for the common case.
    """
    if any(
        k in payload
        for k in ("latestSequenceNumbers", "currentSequenceNumbers", "stream")
    ):
        return StreamPlatform.KINESIS
    if any(
        k in payload for k in ("latestOffsets", "currentOffsets", "topic")
    ):
        return StreamPlatform.KAFKA
    return None


def _detect_platform(spec: dict, payload: dict) -> StreamPlatform:
    """Detect the streaming platform for a supervisor.

    Order of evidence:
      1. Supervisor spec's top-level ``type`` ("kafka" / "kinesis") —
         the authoritative discriminator Druid stamps on every spec.
      2. ioConfig shape: a ``stream`` key (Kinesis) vs ``topic`` (Kafka).
      3. Status payload ``type`` as a last resort.
    Defaults to Kafka so any spec the heuristics can't classify keeps
    the historical behaviour.
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
