"""
Pure logic that builds a Druid ingestion-spec dict by inspecting a
running Druid cluster.

Two extraction paths:

1. **Stream** — the datasource has an active Kafka/Kinesis supervisor.
   Druid stores the full supervisor spec in ZK metadata; we fetch it
   verbatim and wrap it in the ``{"type": "<stream>", "spec": {...}}``
   shape that ``dpm generate`` / ``dpm plan-hybrid`` expect.

2. **Batch** — no supervisor matches the datasource; we fall back to
   reconstructing what we can from segment metadata. Several fields
   (``ioConfig.inputSource``, ``transformSpec``, parser config) cannot
   be recovered from running-cluster state because they are not stored
   in segments — for those we emit explicit placeholders + warnings.

The functions in this module are **pure**: they take already-fetched
domain objects (a supervisor spec dict, a SegmentMetadata, etc.) and
return a spec dict + list of warnings. The CLI command does the I/O and
calls these functions. Same separation-of-concerns pattern as the rest
of the codebase.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from migrator.druid.coordinator_client import (
    DruidCoordinatorClient,
    SegmentMetadata,
)
from migrator.druid.overlord_client import DruidOverlordClient


# Druid `segmentMetadata` query types → Druid `dimensionsSpec` types
_TYPE_MAP_DIMENSION = {
    "STRING": "string",
    "LONG": "long",
    "FLOAT": "float",
    "DOUBLE": "double",
    "COMPLEX<json>": "string",  # JSON columns get migrated as STRING by default
}


@dataclass
class ExtractedSpec:
    """Result of running ``extract_spec``."""

    spec: dict
    """The Druid ingestion-spec dict, ready for `dpm generate`."""

    source_kind: str
    """Either ``'stream'`` or ``'batch'``."""

    warnings: list[str]
    """
    Fields the extractor could not reconstruct from running-cluster
    state. Each warning is a one-line description; the operator should
    review the spec and fill these in before deploying.
    """

    supervisor_id: str | None = None
    """Set when source_kind == 'stream'; identifies which supervisor was
    used as the spec source."""


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────


def extract_spec(
    datasource: str,
    *,
    coordinator: DruidCoordinatorClient,
    overlord: DruidOverlordClient | None = None,
    prefer: str | None = None,
) -> ExtractedSpec:
    """
    Extract an ingestion spec for ``datasource`` from a running Druid cluster.

    ``prefer`` may be ``"stream"``, ``"batch"``, or ``None`` (auto-detect).
    Auto-detect tries the stream path first (looks for a matching supervisor
    via the Overlord) and falls back to batch if no supervisor exists.

    The Overlord client is optional — pass ``None`` to skip the stream
    path entirely (useful in environments where the Overlord isn't
    reachable but the Coordinator is).
    """
    if not coordinator.datasource_exists(datasource):
        raise ValueError(
            f"Druid datasource '{datasource}' not found via the Coordinator. "
            "Check the datasource name and that the Coordinator URL is correct."
        )

    if prefer == "batch":
        return _from_batch(datasource, coordinator)

    if prefer == "stream":
        if overlord is None:
            raise ValueError(
                "prefer='stream' requires an Overlord client; pass `overlord=...`."
            )
        sup_id = overlord.find_supervisor_for_datasource(datasource)
        if sup_id is None:
            raise ValueError(
                f"No active supervisor ingests into '{datasource}'."
            )
        return _from_supervisor(sup_id, overlord)

    # Auto-detect
    if overlord is not None:
        sup_id = overlord.find_supervisor_for_datasource(datasource)
        if sup_id is not None:
            return _from_supervisor(sup_id, overlord)
    return _from_batch(datasource, coordinator)


# ─────────────────────────────────────────────────────────────────────────────
# Stream extraction (high fidelity)
# ─────────────────────────────────────────────────────────────────────────────


def _from_supervisor(
    supervisor_id: str, overlord: DruidOverlordClient
) -> ExtractedSpec:
    raw = overlord.get_supervisor_spec(supervisor_id)
    sup_type = raw.get("type") or "kafka"
    inner_spec = raw.get("spec")
    if not isinstance(inner_spec, dict):
        raise ValueError(
            f"Supervisor '{supervisor_id}' has no nested .spec block; "
            f"got {raw!r}"
        )

    # Druid supervisors include `spec.dataSchema`, `spec.ioConfig`,
    # `spec.tuningConfig` — the same nested layout `dpm generate`
    # already accepts. Wrap with the supervisor type.
    normalised_inner = _normalise_spec(inner_spec)

    # Druid supervisor specs don't store `ioConfig.type` explicitly — the
    # type is implied by the outer supervisor wrapper. The migrator's
    # parser uses `ioConfig.type` to detect stream sources, so we
    # propagate the supervisor type into ioConfig if not already set.
    iocfg = normalised_inner.setdefault("ioConfig", {})
    iocfg.setdefault("type", sup_type)

    out = {"type": sup_type, "spec": normalised_inner}

    warnings: list[str] = []
    iocfg = inner_spec.get("ioConfig") or {}
    if sup_type == "kinesis":
        warnings.append(
            "Kinesis supervisor extracted; dpm emits a Pinot KinesisConsumerFactory "
            "stream config. Review streamConfigs.region (auto-extracted from the "
            "Druid endpoint when it follows kinesis.<region>.amazonaws.com) and "
            "supply AWS credentials via IAM / env vars in the Pinot deployment."
        )
    cp = iocfg.get("consumerProperties") or {}
    if cp.get("bootstrap.servers", "").startswith("localhost"):
        warnings.append(
            "consumerProperties.bootstrap.servers points at localhost — "
            "this is the in-cluster value; update for the Pinot deployment."
        )

    return ExtractedSpec(
        spec=out,
        source_kind="stream",
        supervisor_id=supervisor_id,
        warnings=warnings,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Batch extraction (best-effort)
# ─────────────────────────────────────────────────────────────────────────────


def _from_batch(
    datasource: str, coordinator: DruidCoordinatorClient
) -> ExtractedSpec:
    meta = coordinator.get_segment_metadata(datasource)
    summary = coordinator.get_datasource_summary(datasource)
    warnings: list[str] = []

    time_field = _detect_time_field(meta)
    dims, mets = _split_dims_metrics(meta, time_field=time_field)

    if not dims:
        warnings.append(
            "No non-time dimension columns inferred from segment metadata; "
            "the generated spec may need manual dimension entries."
        )

    intervals = meta.intervals or _intervals_from_summary(summary)
    if not intervals:
        warnings.append(
            "Could not derive ingestion intervals from segment metadata. "
            "Spec uses an open-ended placeholder interval."
        )

    spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {
                    "column": time_field,
                    "format": "millis",
                },
                "dimensionsSpec": {"dimensions": dims},
                "metricsSpec": mets,
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": _infer_segment_granularity(intervals),
                    "queryGranularity": "NONE",
                    "rollup": bool(mets),
                    "intervals": intervals or ["1970-01-01/3000-01-01"],
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                # NOTE: The actual inputSource cannot be recovered from
                # running-cluster state — segments don't preserve where
                # they were originally ingested from. The user MUST fill
                # this in before deploying.
                "inputSource": {
                    "type": "local",
                    "baseDir": "/path/to/source/data",
                    "filter": "*.json",
                },
                "inputFormat": {"type": "json"},
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxNumConcurrentSubTasks": 1,
            },
        },
    }
    warnings.append(
        "Batch ioConfig.inputSource set to a placeholder local path — "
        "Druid does not retain the original input-source spec at the "
        "segment level. Update before deploying."
    )
    if not mets:
        warnings.append(
            "metricsSpec is empty (no rolled-up metrics detected). If this "
            "datasource originally had aggregator metrics, they cannot be "
            "recovered from segment metadata; add them manually."
        )
    warnings.append(
        "transformSpec / flattenSpec cannot be recovered from running-cluster "
        "metadata. If the original pipeline used either, add them manually."
    )

    return ExtractedSpec(
        spec=spec,
        source_kind="batch",
        warnings=warnings,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _normalise_spec(inner: dict) -> dict:
    """Return inner.copy() — keeps a defensive copy so callers can mutate."""
    return {k: v for k, v in inner.items()}


def _detect_time_field(meta: SegmentMetadata) -> str:
    """Druid uses the magic ``__time`` column; fall back to first LONG column."""
    if "__time" in meta.columns:
        return "__time"
    for name, info in meta.columns.items():
        if (info.get("type") or "").upper() == "LONG":
            return name
    return "__time"


def _split_dims_metrics(
    meta: SegmentMetadata, *, time_field: str
) -> tuple[list, list]:
    dims: list = []
    mets: list = []
    for name, info in sorted(meta.columns.items()):
        if name == time_field:
            continue
        col_type = (info.get("type") or "").upper()
        # Heuristic: numeric columns that aren't the time column are
        # treated as metrics by default — most rolled-up Druid datasources
        # use this convention. Strings are dimensions.
        if col_type in ("LONG", "FLOAT", "DOUBLE"):
            mets.append({
                "type": _aggregator_type_for(col_type),
                "name": name,
                "fieldName": name,
            })
        else:
            druid_type = _TYPE_MAP_DIMENSION.get(col_type, "string")
            if info.get("hasMultipleValues"):
                dims.append({
                    "type": druid_type,
                    "name": name,
                    "multiValueHandling": "SORTED_ARRAY",
                })
            else:
                dims.append(name)
    return dims, mets


def _aggregator_type_for(col_type: str) -> str:
    # Default to a `Sum` aggregator in the matching numeric width.
    return {
        "LONG": "longSum",
        "FLOAT": "floatSum",
        "DOUBLE": "doubleSum",
    }.get(col_type, "doubleSum")


def _intervals_from_summary(summary: dict) -> list[str]:
    """
    Extract ISO-8601 intervals from the Coordinator's datasource summary
    payload (which has shape ``{"segments": {"min..": ..., "max..": ...}}``).
    """
    seg = summary.get("segments") or {}
    mn, mx = seg.get("minTime"), seg.get("maxTime")
    if mn and mx:
        return [f"{mn}/{mx}"]
    return []


_INTERVAL_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)/"
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)$"
)


def _infer_segment_granularity(intervals: list[str]) -> str:
    """
    Best-effort granularity guess from the average interval length.

    Returns one of HOUR / DAY / MONTH / YEAR. Defaults to DAY if we
    can't parse the intervals.
    """
    from datetime import datetime

    spans_sec: list[float] = []
    for iv in intervals or []:
        m = _INTERVAL_RE.match(iv.strip())
        if not m:
            continue
        try:
            a = datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
            b = datetime.fromisoformat(m.group(2).replace("Z", "+00:00"))
            spans_sec.append((b - a).total_seconds())
        except (TypeError, ValueError):
            continue
    if not spans_sec:
        return "DAY"
    avg = sum(spans_sec) / len(spans_sec)
    if avg <= 60 * 60 * 1.5:        # ≤ 1.5 hours
        return "HOUR"
    if avg <= 60 * 60 * 24 * 1.5:    # ≤ 1.5 days
        return "DAY"
    if avg <= 60 * 60 * 24 * 35:     # ≤ ~1 month
        return "MONTH"
    return "YEAR"
