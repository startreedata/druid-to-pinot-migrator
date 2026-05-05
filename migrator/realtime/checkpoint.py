"""
Cutover checkpoint — phase-level resumability for ``run_cutover``.

Goal: a re-run of ``dpm cutover`` after a partial failure should pick
up where it left off rather than redo every phase. The checkpoint
file records which phases completed successfully and is written to
``<out_dir>/cutover-checkpoint.json`` after each phase. On a re-run,
the orchestrator loads the file and skips phases marked ``"ok"``.

What's NOT in scope yet:
  - Page-level resumability inside the backfill phase. A mid-backfill
    failure replays the whole backfill from page 0; deduplication
    against Pinot is not attempted. Adding that would need either a
    (page → segment) marker file, or a "resume from Pinot row count"
    pass — both viable but heavier than the v0.10.0 phase-level scope.

Config-change detection
───────────────────────
Each checkpoint stores a hash of the fields in ``CutoverConfig`` that
affect a phase's output. If the next run's hash differs (e.g. the
operator changed ``--datasource`` or pointed at a different spec
file), the orchestrator treats the existing checkpoint as stale and
ignores it — better to redo work than silently apply a stale plan to
a different cutover. Operators can opt out by passing ``--no-resume``
when they know the change is intentional.
"""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
CHECKPOINT_FILENAME = "cutover-checkpoint.json"

# The phases the orchestrator runs, in order. Kept here (not derived
# from CutoverConfig) so the checkpoint format is stable across
# orchestrator refactors and so a checkpoint written by an older dpm
# version can still be parsed correctly.
PHASES = (
    "extract_offsets",
    "plan_hybrid",
    "deploy",
    "backfill",
    "parity",
)


@dataclass
class PhaseCheckpoint:
    """Per-phase state written between phase runs.

    ``status`` is the verdict of the most recent attempt:

      - ``"ok"``      — phase succeeded; on resume, skip it.
      - ``"error"``   — phase ran and failed; on resume, retry.
      - ``"pending"`` — phase has not run yet (this run or any prior).

    ``detail`` and ``artifact`` mirror ``CutoverStepResult`` so the
    on-disk checkpoint can reconstruct an equivalent result without
    re-running the phase.
    """
    status: str = "pending"
    completed_at: str | None = None
    detail: str = ""
    artifact: str | None = None


@dataclass
class Checkpoint:
    """Top-level checkpoint persisted to ``cutover-checkpoint.json``.

    ``config_hash`` invalidates the checkpoint when any output-affecting
    config field changes — see ``hash_config``.
    """
    config_hash: str
    started_at: str = field(
        default_factory=lambda: _utc_iso_now()
    )
    schema_version: int = SCHEMA_VERSION
    phases: dict[str, PhaseCheckpoint] = field(default_factory=dict)

    def is_complete(self, phase: str) -> bool:
        ph = self.phases.get(phase)
        return ph is not None and ph.status == "ok"

    def mark_ok(
        self, phase: str, *, detail: str = "", artifact: str | None = None,
    ) -> None:
        self.phases[phase] = PhaseCheckpoint(
            status="ok",
            completed_at=_utc_iso_now(),
            detail=detail,
            artifact=artifact,
        )

    def mark_error(self, phase: str, *, detail: str) -> None:
        self.phases[phase] = PhaseCheckpoint(
            status="error",
            completed_at=_utc_iso_now(),
            detail=detail,
        )

    def discard_from(self, phase: str) -> None:
        """Drop ``phase`` and every phase after it.

        Backs ``--restart-from <phase>``: keeps earlier phases'
        completed status so ``dpm cutover --restart-from parity`` only
        re-runs parity, leaving extract / plan / deploy / backfill
        untouched.
        """
        if phase not in PHASES:
            raise ValueError(f"Unknown phase '{phase}'; valid: {list(PHASES)}")
        idx = PHASES.index(phase)
        for p in PHASES[idx:]:
            self.phases.pop(p, None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "config_hash": self.config_hash,
            "started_at": self.started_at,
            "phases": {
                name: dataclasses.asdict(ph)
                for name, ph in self.phases.items()
            },
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Checkpoint":
        # Older schema versions become breaking changes if we ever bump
        # SCHEMA_VERSION; for now there's only one shape, so a
        # mismatch means "not ours" — treat as missing.
        if d.get("schema_version") != SCHEMA_VERSION:
            raise CheckpointSchemaMismatch(
                f"checkpoint schema version {d.get('schema_version')} "
                f"does not match expected {SCHEMA_VERSION}"
            )
        phases = {
            name: PhaseCheckpoint(**ph)
            for name, ph in d.get("phases", {}).items()
        }
        return cls(
            config_hash=d["config_hash"],
            started_at=d.get("started_at", _utc_iso_now()),
            schema_version=d["schema_version"],
            phases=phases,
        )


class CheckpointSchemaMismatch(Exception):
    """Raised when the on-disk checkpoint's schema is unrecognised."""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _utc_iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def hash_config(cfg: Any) -> str:
    """Stable hash of the cutover-config fields that affect phase output.

    Excludes purely operational knobs (``out_dir``, ``staging_dir``,
    ``abort_on_error``, settle timeouts) so changing them doesn't
    invalidate an in-progress checkpoint. Includes the spec file's
    contents — same path with edited contents must invalidate.
    """
    # Avoid importing CutoverConfig at module-load time (circular: cutover.py
    # imports from this module). Duck-type instead.
    relevant = {
        "supervisor_id": getattr(cfg, "supervisor_id", None),
        "datasource":     getattr(cfg, "datasource", None),
        "pinot_table":    getattr(cfg, "pinot_table", None),
        "backfill_start_iso":  getattr(cfg, "backfill_start_iso", None),
        "backfill_end_iso":    getattr(cfg, "backfill_end_iso", None),
        "backfill_page_rows":  getattr(cfg, "backfill_page_rows", None),
        "backfill_time_column":getattr(cfg, "backfill_time_column", None),
        "skip_deploy":   getattr(cfg, "skip_deploy", None),
        "skip_backfill": getattr(cfg, "skip_backfill", None),
        "skip_parity":   getattr(cfg, "skip_parity", None),
    }
    spec_path = getattr(cfg, "spec_path", None)
    if spec_path is not None:
        try:
            relevant["spec_sha256"] = hashlib.sha256(
                Path(spec_path).read_bytes(),
            ).hexdigest()
        except OSError:
            # Spec missing or unreadable — caller will hit the same
            # error; we don't want to obscure it by hashing a stand-in.
            relevant["spec_sha256"] = None
    serialised = json.dumps(relevant, sort_keys=True, default=str)
    return hashlib.sha256(serialised.encode()).hexdigest()


def checkpoint_path(out_dir: Path) -> Path:
    return Path(out_dir) / CHECKPOINT_FILENAME


def load_checkpoint(out_dir: Path) -> Checkpoint | None:
    """Load the checkpoint from ``out_dir``; return None if missing.

    A schema-version mismatch raises ``CheckpointSchemaMismatch`` so
    the caller can surface the conflict rather than silently start
    fresh (which could bury a real bug in a future format change).
    """
    p = checkpoint_path(out_dir)
    if not p.exists():
        return None
    try:
        raw = json.loads(p.read_text())
    except json.JSONDecodeError as exc:
        raise CheckpointSchemaMismatch(
            f"checkpoint at {p} is not valid JSON: {exc}"
        ) from exc
    return Checkpoint.from_dict(raw)


def save_checkpoint(ckpt: Checkpoint, out_dir: Path) -> Path:
    p = checkpoint_path(out_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    # Atomic-ish write: stage to a sibling tempfile then rename, so a
    # mid-write crash leaves the previous checkpoint intact.
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(ckpt.to_dict(), indent=2) + "\n")
    tmp.replace(p)
    return p
