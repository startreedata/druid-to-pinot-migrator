"""
Batch cutover — run ``dpm cutover`` across N Druid datasources with a
single command and a shared progress report.

Real migrations rarely involve a single supervisor; an operator
typically has 5-50 datasources to move at once. Without this module
they shell-loop ``dpm cutover`` per datasource, accumulate per-run
output directories by hand, and lose the aggregate "X of Y succeeded"
view that's needed to decide whether the migration is shippable.

``run_batch_cutover`` reuses the single-datasource orchestrator
(``run_cutover``) verbatim — each entry in the batch gets its own
``out_dir``, its own checkpoint, and its own ``CutoverReport``. The
batch wrapper aggregates them and writes a top-level
``batch-report.json``. Resume semantics carry through transparently:
re-running the batch picks up each datasource where its individual
checkpoint says it left off.

Manifest format
───────────────
YAML or JSON, consumed by ``BatchCutoverManifest.from_path``::

    defaults:
      druid_overlord: http://localhost:8081
      druid_router:   http://localhost:8888
      pinot_controller: http://localhost:9000
      pinot_broker:    http://localhost:8099

    datasources:
      - supervisor_id: events_v1
        datasource: events
        pinot_table: events
        spec: ./events.json
        # Per-DS overrides win over defaults:
        backfill_start_iso: "2024-01-01T00:00:00Z"
      - supervisor_id: pageviews_v1
        datasource: pageviews
        pinot_table: pageviews
        spec: ./pageviews.json
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from migrator.realtime.cutover import (
    CutoverConfig,
    CutoverReport,
    run_cutover,
)
from migrator.utils.io import read_json_or_yaml


# ─────────────────────────────────────────────────────────────────────────────
# Manifest model
# ─────────────────────────────────────────────────────────────────────────────


class BatchCutoverDefaults(BaseModel):
    """Cluster URLs + auth that apply to every datasource in the batch
    unless overridden per entry. Optional — entries can carry the same
    fields explicitly for full per-DS control.
    """
    model_config = ConfigDict(extra="forbid")

    druid_overlord: str | None = None
    druid_router: str | None = None
    pinot_controller: str | None = None
    pinot_broker: str | None = None
    backfill_start_iso: str | None = None
    backfill_page_rows: int | None = None
    # Per-entry settle timeout for the post-backfill Pinot row-count
    # poll. Exposed so test stubs (which never report meaningful
    # counts) can drop it from 300s to a couple of seconds.
    backfill_settle_timeout_s: float | None = None


class BatchCutoverEntry(BaseModel):
    """One datasource's worth of cutover config in the manifest.

    Every field that ``CutoverConfig`` requires must be present here
    (directly, or via the manifest-level ``defaults``). Validation
    happens up-front so an invalid manifest fails before any phase
    side-effects.
    """
    model_config = ConfigDict(extra="forbid")

    # Required identifiers — no sane default exists.
    supervisor_id: str
    datasource: str
    pinot_table: str
    spec: Path

    # Optional per-DS overrides; fall back to defaults if missing.
    backfill_start_iso: str | None = None
    backfill_end_iso: str | None = None
    backfill_page_rows: int | None = None
    backfill_time_column: str | None = None
    backfill_settle_timeout_s: float | None = None
    skip_deploy: bool | None = None
    skip_backfill: bool | None = None
    skip_parity: bool | None = None


class BatchCutoverManifest(BaseModel):
    """Top-level manifest passed to ``dpm cutover-many``."""
    model_config = ConfigDict(extra="forbid")

    defaults: BatchCutoverDefaults = Field(default_factory=BatchCutoverDefaults)
    datasources: list[BatchCutoverEntry]

    @classmethod
    def from_path(cls, path: Path) -> "BatchCutoverManifest":
        """Load a YAML or JSON manifest file."""
        raw = read_json_or_yaml(str(path))
        return cls.model_validate(raw)


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class BatchEntryResult:
    """One datasource's outcome inside a batch."""
    datasource: str
    pinot_table: str
    out_dir: Path
    all_ok: bool
    elapsed_s: float
    error: str | None = None  # set when run_cutover raised before producing a report
    report: CutoverReport | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "datasource": self.datasource,
            "pinot_table": self.pinot_table,
            "out_dir": str(self.out_dir),
            "all_ok": self.all_ok,
            "elapsed_s": round(self.elapsed_s, 2),
            "error": self.error,
            "report": self.report.to_dict() if self.report else None,
        }


@dataclass
class BatchCutoverReport:
    """Aggregate result of ``run_batch_cutover``."""
    out_dir: Path
    started_at: str
    finished_at: str | None = None
    entries: list[BatchEntryResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.entries)

    @property
    def succeeded(self) -> int:
        return sum(1 for e in self.entries if e.all_ok)

    @property
    def failed(self) -> int:
        return sum(1 for e in self.entries if not e.all_ok)

    @property
    def all_ok(self) -> bool:
        return all(e.all_ok for e in self.entries)

    def to_dict(self) -> dict[str, Any]:
        return {
            "out_dir": str(self.out_dir),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "all_ok": self.all_ok,
            "entries": [e.to_dict() for e in self.entries],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────


def _build_cutover_config(
    entry: BatchCutoverEntry,
    defaults: BatchCutoverDefaults,
    *,
    out_root: Path,
    staging_root: Path,
    extra: dict[str, Any] | None = None,
) -> CutoverConfig:
    """Compose a per-entry ``CutoverConfig``: entry-level fields win,
    then manifest defaults fill in the gaps.

    ``extra`` carries operator-supplied flags from the CLI that aren't
    in the manifest schema (resume / restart_from / abort_on_error).
    """
    extra = extra or {}
    # Resolve each optional field with the entry → defaults → built-in
    # default fallback ladder. Keep this conservative — only the
    # narrow set of fields the manifest actually exposes.
    def _pick(name: str, default: Any = None) -> Any:
        v = getattr(entry, name)
        if v is not None:
            return v
        v = getattr(defaults, name, None)
        if v is not None:
            return v
        return default

    cfg = CutoverConfig(
        supervisor_id=entry.supervisor_id,
        datasource=entry.datasource,
        pinot_table=entry.pinot_table,
        spec_path=entry.spec,
        # Each datasource gets its own out + staging subdirectory so
        # checkpoints / artifacts don't collide.
        out_dir=out_root / entry.datasource,
        staging_dir=staging_root / entry.datasource,
        backfill_start_iso=_pick(
            "backfill_start_iso", "1970-01-01T00:00:00.000Z",
        ),
        backfill_end_iso=_pick("backfill_end_iso"),
        backfill_page_rows=_pick("backfill_page_rows", 50_000),
        backfill_time_column=_pick("backfill_time_column", "timestamp"),
        backfill_settle_timeout_s=_pick("backfill_settle_timeout_s", 300.0),
        skip_deploy=bool(_pick("skip_deploy", False)),
        skip_backfill=bool(_pick("skip_backfill", False)),
        skip_parity=bool(_pick("skip_parity", False)),
    )
    # Operator-level flags (resume, restart_from, abort_on_error) are
    # set on every entry uniformly — there's no per-DS knob for these
    # in the manifest because they're about how to RUN the cutover,
    # not what to cut over.
    for k, v in extra.items():
        if v is not None:
            setattr(cfg, k, v)
    return cfg


def run_batch_cutover(
    manifest: BatchCutoverManifest,
    *,
    out_root: Path,
    staging_root: Path,
    client_factory,
    abort_on_first_failure: bool = False,
    resume: bool = True,
    restart_from: str | None = None,
) -> BatchCutoverReport:
    """Run ``run_cutover`` for every entry in the manifest.

    ``client_factory`` is a callable that, given a
    ``BatchCutoverDefaults`` plus a ``BatchCutoverEntry``, returns the
    six clients ``run_cutover`` requires (overlord, deployer, pager,
    sink, druid_sql, pinot_sql). Pulled out as a hook so the CLI can
    plumb in authenticated sessions and the unit tests can drop in
    stubs without monkey-patching.

    ``abort_on_first_failure`` short-circuits the batch when an entry's
    cutover ends with ``all_ok=False``. Default False so a flaky
    parity check on one datasource doesn't block the rest of the
    batch; the operator decides whether to retry per-DS afterwards
    using each entry's checkpoint.
    """
    out_root = Path(out_root)
    staging_root = Path(staging_root)
    out_root.mkdir(parents=True, exist_ok=True)
    staging_root.mkdir(parents=True, exist_ok=True)

    started_iso = _utc_iso_now()
    report = BatchCutoverReport(out_dir=out_root, started_at=started_iso)

    for entry in manifest.datasources:
        cfg = _build_cutover_config(
            entry, manifest.defaults,
            out_root=out_root,
            staging_root=staging_root,
            extra={
                "resume": resume,
                "restart_from": restart_from,
            },
        )
        clients = client_factory(manifest.defaults, entry)
        t0 = time.time()
        try:
            cutover_report = run_cutover(cfg, **clients)
            entry_result = BatchEntryResult(
                datasource=entry.datasource,
                pinot_table=entry.pinot_table,
                out_dir=cfg.out_dir,
                all_ok=cutover_report.all_ok,
                elapsed_s=time.time() - t0,
                report=cutover_report,
            )
        except Exception as exc:  # noqa: BLE001 — keep batch going on infra errors
            entry_result = BatchEntryResult(
                datasource=entry.datasource,
                pinot_table=entry.pinot_table,
                out_dir=cfg.out_dir,
                all_ok=False,
                elapsed_s=time.time() - t0,
                error=f"{type(exc).__name__}: {exc}",
            )
        report.entries.append(entry_result)

        if abort_on_first_failure and not entry_result.all_ok:
            break

    report.finished_at = _utc_iso_now()

    # Write the top-level aggregate report alongside the per-entry
    # output directories so the operator has one file to look at.
    summary_path = out_root / "batch-report.json"
    summary_path.write_text(
        json.dumps(report.to_dict(), indent=2, default=str) + "\n",
    )

    return report


def _utc_iso_now() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
