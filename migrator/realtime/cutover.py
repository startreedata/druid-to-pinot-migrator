"""
End-to-end Druid → Pinot hybrid cutover orchestrator.

Composes the existing building blocks — overlord watermark capture,
hybrid planner, Pinot deployer, batch backfill, parity checker — into
one ``run_cutover()`` call. Each phase writes its artifact under a
single ``out_dir`` so the operator has a complete record of the
cutover after the fact.

The orchestrator itself is pure-ish: it takes already-constructed
clients via dependency injection so unit tests can drop in stubs and
the CLI wrapper can plumb authenticated sessions in. The CLI command
in ``migrator/cli/commands/cutover.py`` is the only place we
instantiate concrete clients with real network endpoints.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from migrator.core.errors import GenerationError
from migrator.core.models import CanonicalMigrationModel
from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.parity.models import ParityResult
from migrator.parity.query_builder import derive_queries_from_canonical
from migrator.parity.runner import run_parity
from migrator.pinot.deployer import (
    DeployArtifacts,
    DeployReport,
    PinotDeployer,
    discover_artifacts,
)
from migrator.realtime.backfill_runner import (
    BackfillResult,
    DruidSqlPager,
    PinotIngestSink,
    run_backfill,
)
from migrator.realtime.checkpoint import (
    PHASES,
    Checkpoint,
    CheckpointSchemaMismatch,
    hash_config,
    load_checkpoint,
    save_checkpoint,
)
from migrator.realtime.hybrid_planner import (
    plan_hybrid_migration,
    write_hybrid_plan,
)
from migrator.realtime.models import StreamOffsetMap
from migrator.realtime.offset_io import load_offset_map, save_offset_map


# ─────────────────────────────────────────────────────────────────────────────
# Config + result types
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class CutoverConfig:
    """All knobs that drive a cutover run.

    ``spec_path`` is the Druid Kafka supervisor JSON used by both
    plan-hybrid (to derive the canonical model) and parity-check
    (to auto-derive queries).
    """
    supervisor_id: str
    datasource: str
    pinot_table: str
    spec_path: Path
    out_dir: Path
    staging_dir: Path
    backfill_start_iso: str = "1970-01-01T00:00:00.000Z"
    backfill_end_iso: str | None = None  # defaults to the captured watermark
    backfill_page_rows: int = 50_000
    backfill_time_column: str = "timestamp"
    # Pinot builds OFFLINE segments asynchronously after /ingestFromFile
    # returns 200; the parity phase queries Pinot, so we have to wait
    # for the segments to become queryable before running it. ``300s``
    # covers the typical 30-300s ingest tail; tests can drop this to
    # avoid hanging on stub clients.
    backfill_settle_timeout_s: float = 300.0
    skip_deploy: bool = False
    skip_backfill: bool = False
    skip_parity: bool = False
    # When False, a step that errors is recorded but the orchestrator
    # keeps going (useful for diagnostic dry-runs). Default True
    # mirrors what an operator wants from a real cutover.
    abort_on_error: bool = True

    # ── Resumability ──────────────────────────────────────────────────────
    # When True, a phase already marked "ok" in the on-disk checkpoint
    # is skipped on this run; downstream phases run as normal. Default
    # True so re-running ``dpm cutover`` after a failure picks up where
    # it left off. Pass ``--no-resume`` (sets this False) to force every
    # phase to run again — useful when the operator wants a clean redo
    # without manually deleting the out_dir.
    resume: bool = True
    # Discard checkpoint state for this phase and every phase after it
    # before the run starts. Lets an operator say "everything earlier
    # is fine, but re-run parity from scratch" without nuking the rest.
    restart_from: str | None = None


@dataclass
class CutoverStepResult:
    step: str
    status: str  # "ok" | "skipped" | "error"
    detail: str = ""
    artifact: str | None = None  # filesystem path for follow-up


@dataclass
class CutoverReport:
    steps: list[CutoverStepResult] = field(default_factory=list)
    out_dir: Path | None = None
    parity: list[ParityResult] = field(default_factory=list)

    @property
    def all_ok(self) -> bool:
        # Skipped phases don't count as failures — only steps that
        # actually ran and reported "error".
        return all(s.status != "error" for s in self.steps)

    def to_dict(self) -> dict[str, Any]:
        return {
            "out_dir": str(self.out_dir) if self.out_dir else None,
            "all_ok": self.all_ok,
            "steps": [
                {"step": s.step, "status": s.status, "detail": s.detail,
                 "artifact": s.artifact}
                for s in self.steps
            ],
            "parity": [r.model_dump() for r in self.parity],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────


def _load_canonical(spec_path: Path) -> CanonicalMigrationModel:
    """Parse the supervisor spec into a canonical model."""
    raw = json.loads(spec_path.read_text())
    parsed = DruidSpecParser().parse(raw)
    norm = DruidNormalizer().normalize(parsed.parsed_spec)
    return norm.canonical


def run_cutover(
    cfg: CutoverConfig,
    *,
    overlord,
    deployer: PinotDeployer | None = None,
    pager: DruidSqlPager | None = None,
    pinot_ingest_sink: PinotIngestSink | None = None,
    druid_sql_client: Any = None,
    pinot_sql_client: Any = None,
) -> CutoverReport:
    """Run all six cutover phases in order.

    Phases (each emits one ``CutoverStepResult``):

      1. extract_offsets   — overlord watermark capture
      2. plan_hybrid       — generate OFFLINE + REALTIME table configs,
                              schema, runbook, etc.
      3. deploy            — push schema + tables to Pinot
      4. backfill          — page Druid SQL → Pinot OFFLINE
      5. parity            — run auto-derived parity queries

    Skipped phases produce ``status="skipped"``. When
    ``cfg.abort_on_error`` is True (default) the first ``"error"``
    short-circuits the rest of the run; the report still contains
    one ``CutoverStepResult`` per phase, with later phases marked
    ``"skipped"``.
    """
    cfg.out_dir = Path(cfg.out_dir)
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    report = CutoverReport(out_dir=cfg.out_dir)

    aborted = False

    # ── Checkpoint setup ──────────────────────────────────────────────────
    # Compute the config-hash once: we use it both to decide whether an
    # existing checkpoint is reusable and to stamp the checkpoint we'll
    # write. ``--no-resume`` (resume=False) discards any prior state.
    new_hash = hash_config(cfg)
    ckpt: Checkpoint | None = None
    if cfg.resume:
        try:
            ckpt = load_checkpoint(cfg.out_dir)
        except CheckpointSchemaMismatch as exc:
            # Don't silently start fresh on an unrecognised file —
            # surface the conflict so the operator can decide.
            raise RuntimeError(
                f"refusing to resume: {exc}. "
                "Pass --no-resume to start over and overwrite the file."
            ) from exc
        if ckpt is not None and ckpt.config_hash != new_hash:
            # Config changed since the previous run — keys like
            # supervisor_id / datasource / spec contents would make any
            # carried-over state nonsensical. Discard rather than risk
            # applying the wrong plan.
            ckpt = None
    if ckpt is None:
        ckpt = Checkpoint(config_hash=new_hash)
    if cfg.restart_from:
        ckpt.discard_from(cfg.restart_from)
    save_checkpoint(ckpt, cfg.out_dir)

    def _step(step: str) -> bool:
        """Append a record for a step we're not running and return
        whether the step should actually execute.

        Three "not running" cases, in priority order:
          1. ``aborted`` after a previous error (status="skipped").
          2. Already complete in the checkpoint (status="ok", with a
             ``resumed`` note in the detail so the report makes the
             skip-with-success state obvious).
          3. None of the above — caller runs the step.
        """
        nonlocal aborted
        if aborted:
            report.steps.append(CutoverStepResult(
                step=step, status="skipped",
                detail="aborted after a previous error",
            ))
            return False
        if ckpt.is_complete(step):
            ph = ckpt.phases[step]
            report.steps.append(CutoverStepResult(
                step=step, status="ok",
                detail=(
                    f"resumed from checkpoint "
                    f"(originally completed at {ph.completed_at}): "
                    f"{ph.detail}"
                ),
                artifact=ph.artifact,
            ))
            return False
        return True

    def _record(step: str, status: str, detail: str = "",
                artifact: str | None = None) -> None:
        nonlocal aborted
        report.steps.append(CutoverStepResult(
            step=step, status=status, detail=detail, artifact=artifact,
        ))
        if status == "ok":
            ckpt.mark_ok(step, detail=detail, artifact=artifact)
        elif status == "error":
            ckpt.mark_error(step, detail=detail)
            if cfg.abort_on_error:
                aborted = True
        save_checkpoint(ckpt, cfg.out_dir)

    # ── 1. Extract watermark ──────────────────────────────────────────────
    offset_map: StreamOffsetMap | None = None
    if _step("extract_offsets"):
        try:
            offset_map = overlord.get_supervisor_offsets(cfg.supervisor_id)
            offsets_path = cfg.out_dir / "offsets.json"
            save_offset_map(offset_map, offsets_path)
            _record(
                "extract_offsets", "ok",
                detail=f"watermark={offset_map.watermark_iso}",
                artifact=str(offsets_path),
            )
        except Exception as exc:  # noqa: BLE001
            _record("extract_offsets", "error", detail=str(exc))
    elif ckpt.is_complete("extract_offsets"):
        # Resume: rehydrate offset_map from the on-disk artifact so
        # downstream phases (plan_hybrid, backfill end-ISO) work
        # without needing the overlord call again.
        offsets_path = cfg.out_dir / "offsets.json"
        if offsets_path.exists():
            offset_map = load_offset_map(offsets_path)

    # ── 2. Plan hybrid ────────────────────────────────────────────────────
    # canonical is needed by parity even if plan_hybrid is resumed-skipped,
    # so always recompute (cheap: one JSON parse + normalize). Saves
    # callers from having to persist + reload the canonical model.
    canonical: CanonicalMigrationModel | None = None
    plan_dir = cfg.out_dir / "hybrid"
    if _step("plan_hybrid"):
        try:
            canonical = _load_canonical(cfg.spec_path)
            if offset_map is None:
                # Defensive — only reachable if the orchestrator was
                # called with skip_extract_offsets in a future revision.
                raise GenerationError("plan_hybrid requires a captured offset map")
            plan = plan_hybrid_migration(canonical, offset_map)
            plan_paths = write_hybrid_plan(plan, plan_dir)
            _record(
                "plan_hybrid", "ok",
                detail=f"wrote {len(plan_paths)} files",
                artifact=str(plan_dir),
            )
        except Exception as exc:  # noqa: BLE001
            _record("plan_hybrid", "error", detail=str(exc))
    elif ckpt.is_complete("plan_hybrid"):
        # Resume: re-derive canonical from the spec (deterministic) for
        # the parity phase to use. The on-disk plan_dir is already
        # populated from the earlier run.
        try:
            canonical = _load_canonical(cfg.spec_path)
        except Exception:  # noqa: BLE001
            # If the spec is now unreadable, downstream phases that
            # need ``canonical`` (parity) will surface the failure
            # with their own error messages.
            canonical = None

    # ── 3. Deploy ─────────────────────────────────────────────────────────
    if cfg.skip_deploy:
        report.steps.append(CutoverStepResult(
            step="deploy", status="skipped", detail="--skip-deploy",
        ))
    elif _step("deploy"):
        if deployer is None:
            _record("deploy", "error",
                    detail="no PinotDeployer wired in")
        else:
            try:
                artifacts = discover_artifacts(plan_dir)
                deploy_report: DeployReport = deployer.deploy(artifacts)
                # Persist a copy of the deploy report for forensics.
                (cfg.out_dir / "deploy-report.json").write_text(
                    json.dumps(
                        [{"artifact": r.artifact, "name": r.name,
                          "status": r.status, "detail": r.detail}
                         for r in deploy_report.results],
                        indent=2,
                    ) + "\n",
                )
                if not deploy_report.all_ok:
                    _record(
                        "deploy", "error",
                        detail=f"{deploy_report.errored} of "
                               f"{len(deploy_report.results)} artifacts failed",
                    )
                else:
                    _record(
                        "deploy", "ok",
                        detail=f"{deploy_report.created} created, "
                               f"{deploy_report.already_exists} already existed",
                    )
            except Exception as exc:  # noqa: BLE001
                _record("deploy", "error", detail=str(exc))

    # ── 4. Backfill ───────────────────────────────────────────────────────
    if cfg.skip_backfill:
        report.steps.append(CutoverStepResult(
            step="backfill", status="skipped", detail="--skip-backfill",
        ))
    elif _step("backfill"):
        if pager is None or pinot_ingest_sink is None:
            _record("backfill", "error",
                    detail="no DruidSqlPager / PinotIngestSink wired in")
        else:
            try:
                end_iso = (cfg.backfill_end_iso
                           or (offset_map.watermark_iso if offset_map else None))
                if end_iso is None:
                    raise RuntimeError(
                        "backfill end-ISO unset; run extract_offsets first "
                        "or pass --backfill-end-iso"
                    )
                staging = Path(cfg.staging_dir)
                t0 = time.time()
                bf: BackfillResult = run_backfill(
                    datasource=cfg.datasource,
                    pinot_table=cfg.pinot_table,
                    start_iso=cfg.backfill_start_iso,
                    end_iso=end_iso,
                    staging_dir=staging,
                    pager=pager,
                    sink=pinot_ingest_sink,
                    page_rows=cfg.backfill_page_rows,
                    time_column=cfg.backfill_time_column,
                )
                _record(
                    "backfill", "ok",
                    detail=f"{bf.rows_dumped} rows in {bf.pages_dumped} "
                           f"pages ({time.time() - t0:.1f}s)",
                    artifact=str(staging),
                )

                # Pinot's /ingestFromFile (and /ingestFromURI) return
                # 200 as soon as the controller queues the segment build,
                # but the OFFLINE segment is asynchronously built and
                # only becomes queryable some seconds-to-minutes later.
                # Without this wait the parity phase queries Pinot too
                # early and reports a (transient) divergence:
                # ``druid=N pinot=0``.
                # We poll until COUNT(*) on the OFFLINE table catches
                # up with the rows the backfill dumped, or time out.
                # Skipped when parity itself is skipped — no point
                # waiting for data nothing will check.
                if not cfg.skip_parity and pinot_sql_client is not None and bf.rows_dumped > 0:
                    expected = bf.rows_dumped
                    deadline = time.time() + cfg.backfill_settle_timeout_s
                    last_seen = -1
                    while time.time() < deadline:
                        try:
                            rows = pinot_sql_client.query(
                                f'SELECT COUNT(*) FROM {cfg.pinot_table}_OFFLINE'
                            )
                            seen = int(rows[0][0]) if rows else 0
                        except Exception:
                            seen = -1
                        if seen >= expected:
                            break
                        last_seen = seen
                        # Don't oversleep past the deadline; capping here
                        # keeps unit tests fast (small timeout) without
                        # changing production behaviour (large timeout).
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            break
                        time.sleep(min(3.0, remaining))
                    # Don't fail the run if we time out — parity-check
                    # will surface the divergence with a more useful
                    # error message than we could here. We just record
                    # what we observed so the cutover-report.json
                    # captures the wait outcome for forensics.
                    if last_seen >= 0 and last_seen < expected:
                        # Append a note onto the backfill step's detail
                        # rather than emit a separate phase — the wait
                        # is part of the backfill semantically.
                        report.steps[-1].detail += (
                            f"; pinot count plateaued at {last_seen}/"
                            f"{expected} after 300s"
                        )
            except Exception as exc:  # noqa: BLE001
                _record("backfill", "error", detail=str(exc))

    # ── 5. Parity ─────────────────────────────────────────────────────────
    if cfg.skip_parity:
        report.steps.append(CutoverStepResult(
            step="parity", status="skipped", detail="--skip-parity",
        ))
    elif _step("parity"):
        if druid_sql_client is None or pinot_sql_client is None or canonical is None:
            _record("parity", "error",
                    detail="no parity SQL clients wired in (or canonical missing)")
        else:
            try:
                queries = derive_queries_from_canonical(
                    canonical, pinot_table=cfg.pinot_table,
                )
                results = run_parity(
                    queries, druid=druid_sql_client, pinot=pinot_sql_client,
                )
                report.parity = results
                # Persist the parity report next to the rest.
                (cfg.out_dir / "parity-report.json").write_text(
                    json.dumps(
                        [r.model_dump() for r in results],
                        indent=2, default=str,
                    ) + "\n",
                )
                failed = [r for r in results if not r.passed]
                if failed:
                    _record(
                        "parity", "error",
                        detail=f"{len(failed)} of {len(results)} parity "
                               f"checks failed",
                    )
                else:
                    _record(
                        "parity", "ok",
                        detail=f"{len(results)}/{len(results)} parity checks passed",
                        artifact=str(cfg.out_dir / "parity-report.json"),
                    )
            except Exception as exc:  # noqa: BLE001
                _record("parity", "error", detail=str(exc))

    # ── 6. Final summary file ─────────────────────────────────────────────
    summary_path = cfg.out_dir / "cutover-report.json"
    summary_path.write_text(json.dumps(report.to_dict(), indent=2, default=str) + "\n")

    return report
