"""
Thin Druid Overlord HTTP client used by ``dpm extract-offsets``.

Designed for testability:

- Takes an injectable ``requests.Session``-like object so unit tests can
  drop in a mock/replay session without monkey-patching.
- Methods return domain objects (``KafkaOffsetMap``), not raw JSON, so the
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
    KafkaOffsetMap,
    KafkaPartitionOffset,
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

    def get_supervisor_offsets(self, supervisor_id: str) -> KafkaOffsetMap:
        """
        Build a :class:`KafkaOffsetMap` for the given Kafka supervisor.

        Synthesis logic:

        - Per-partition ``offset`` comes from ``status.payload.latestOffsets``
          (the highest committed offset Druid has seen).
        - The watermark timestamp comes from the supervisor status's
          ``status.payload.aggregateLag.timestamp`` if present, falling back
          to ``status.payload.lastIngestedTimestamp``, falling back to "now".
        - ``topic`` and ``datasource`` come from the supervisor spec.
        """
        status = self.get_supervisor_status(supervisor_id)
        payload = status.get("payload") or {}

        topic = payload.get("topic") or _safe_get(
            self.get_supervisor_spec(supervisor_id),
            ["spec", "ioConfig", "topic"],
        )
        if not topic:
            raise DruidOverlordError(
                f"Could not determine Kafka topic for supervisor '{supervisor_id}'"
            )

        datasource = payload.get("dataSource") or supervisor_id

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

        watermark_ms, watermark_iso = _resolve_watermark(payload)

        return KafkaOffsetMap(
            platform=StreamPlatform.KAFKA,
            topic=topic,
            supervisor_id=supervisor_id,
            datasource=datasource,
            watermark_iso=watermark_iso,
            watermark_ms=watermark_ms,
            offsets=sorted(partition_offsets, key=lambda po: po.partition),
        )

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
                iso = datetime.fromtimestamp(ts_ms / 1000, timezone.utc).isoformat()
                return ts_ms, iso
            if isinstance(cand, str):
                # Druid usually emits ISO-8601 with 'Z'
                dt = datetime.fromisoformat(cand.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
                return ts_ms, dt.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            continue
    # Fallback: now() — operator should override via --watermark-iso
    now = datetime.now(timezone.utc)
    return int(now.timestamp() * 1000), now.isoformat()
