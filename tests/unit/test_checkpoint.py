"""Unit tests for migrator.realtime.checkpoint."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from migrator.realtime.checkpoint import (
    CHECKPOINT_FILENAME,
    PHASES,
    SCHEMA_VERSION,
    Checkpoint,
    CheckpointSchemaMismatch,
    PhaseCheckpoint,
    hash_config,
    load_checkpoint,
    save_checkpoint,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fakes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _FakeCfg:
    """Minimal duck-typed CutoverConfig for hash_config."""
    supervisor_id: str = "sup1"
    datasource: str = "ds"
    pinot_table: str = "ds"
    spec_path: Path | None = None
    backfill_start_iso: str = "1970-01-01T00:00:00.000Z"
    backfill_end_iso: str | None = None
    backfill_page_rows: int = 50_000
    backfill_time_column: str = "timestamp"
    skip_deploy: bool = False
    skip_backfill: bool = False
    skip_parity: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint mutation
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckpointMutation:
    def test_pending_phase_is_not_complete(self):
        c = Checkpoint(config_hash="x")
        assert not c.is_complete("extract_offsets")

    def test_mark_ok_then_is_complete(self):
        c = Checkpoint(config_hash="x")
        c.mark_ok("deploy", detail="2 created", artifact="/tmp/deploy.json")
        assert c.is_complete("deploy")
        ph = c.phases["deploy"]
        assert ph.status == "ok"
        assert ph.detail == "2 created"
        assert ph.artifact == "/tmp/deploy.json"
        assert ph.completed_at is not None

    def test_mark_error_does_not_count_as_complete(self):
        c = Checkpoint(config_hash="x")
        c.mark_error("backfill", detail="boom")
        assert not c.is_complete("backfill")
        assert c.phases["backfill"].status == "error"

    def test_mark_ok_then_mark_error_overwrites(self):
        # Re-running a phase that previously failed must replace the
        # phase state, not silently keep the prior 'ok' if any.
        c = Checkpoint(config_hash="x")
        c.mark_ok("deploy", detail="first")
        c.mark_error("deploy", detail="boom on retry")
        assert c.phases["deploy"].status == "error"
        assert c.phases["deploy"].detail == "boom on retry"

    def test_discard_from_drops_named_and_later(self):
        c = Checkpoint(config_hash="x")
        for p in PHASES:
            c.mark_ok(p, detail="ok")
        c.discard_from("backfill")
        # Earlier phases survive; backfill and parity gone.
        assert c.is_complete("extract_offsets")
        assert c.is_complete("plan_hybrid")
        assert c.is_complete("deploy")
        assert "backfill" not in c.phases
        assert "parity" not in c.phases

    def test_discard_from_unknown_phase_raises(self):
        c = Checkpoint(config_hash="x")
        with pytest.raises(ValueError, match="Unknown phase"):
            c.discard_from("nonexistent")


# ─────────────────────────────────────────────────────────────────────────────
# Serialization round-trip
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckpointSerialization:
    def test_roundtrip_through_disk(self, tmp_path: Path):
        c = Checkpoint(config_hash="abc")
        c.mark_ok("extract_offsets", detail="watermark=...")
        c.mark_ok("plan_hybrid", detail="6 files", artifact="/tmp/plan")
        save_checkpoint(c, tmp_path)

        loaded = load_checkpoint(tmp_path)
        assert loaded is not None
        assert loaded.config_hash == "abc"
        assert loaded.is_complete("extract_offsets")
        assert loaded.is_complete("plan_hybrid")
        assert not loaded.is_complete("deploy")
        # Detail + artifact survive the round-trip.
        assert loaded.phases["plan_hybrid"].artifact == "/tmp/plan"

    def test_load_returns_none_when_missing(self, tmp_path: Path):
        assert load_checkpoint(tmp_path) is None

    def test_save_is_atomic_through_tmp_rename(self, tmp_path: Path):
        # The implementation writes to ``cutover-checkpoint.json.tmp`` and
        # then renames; this test confirms the final file lands at the
        # expected path and the tmp doesn't leak.
        c = Checkpoint(config_hash="x")
        save_checkpoint(c, tmp_path)
        assert (tmp_path / CHECKPOINT_FILENAME).exists()
        assert not (tmp_path / f"{CHECKPOINT_FILENAME}.tmp").exists()

    def test_unknown_schema_version_raises(self, tmp_path: Path):
        # A future-version checkpoint must not be silently accepted —
        # callers should fall back to a fresh run only after explicit
        # operator action (--no-resume).
        bad = {"schema_version": 999, "config_hash": "x", "phases": {}}
        (tmp_path / CHECKPOINT_FILENAME).write_text(json.dumps(bad))
        with pytest.raises(CheckpointSchemaMismatch):
            load_checkpoint(tmp_path)

    def test_corrupt_json_raises_schema_mismatch(self, tmp_path: Path):
        (tmp_path / CHECKPOINT_FILENAME).write_text("{not json")
        with pytest.raises(CheckpointSchemaMismatch):
            load_checkpoint(tmp_path)

    def test_persisted_format_includes_schema_version(self, tmp_path: Path):
        c = Checkpoint(config_hash="x")
        save_checkpoint(c, tmp_path)
        on_disk = json.loads((tmp_path / CHECKPOINT_FILENAME).read_text())
        assert on_disk["schema_version"] == SCHEMA_VERSION


# ─────────────────────────────────────────────────────────────────────────────
# Config hashing
# ─────────────────────────────────────────────────────────────────────────────


class TestHashConfig:
    def test_same_config_hashes_same(self):
        a = _FakeCfg()
        b = _FakeCfg()
        assert hash_config(a) == hash_config(b)

    def test_changing_datasource_changes_hash(self):
        a = _FakeCfg(datasource="ds_v1")
        b = _FakeCfg(datasource="ds_v2")
        assert hash_config(a) != hash_config(b)

    def test_changing_supervisor_changes_hash(self):
        a = _FakeCfg(supervisor_id="sup_old")
        b = _FakeCfg(supervisor_id="sup_new")
        assert hash_config(a) != hash_config(b)

    def test_spec_path_contents_invalidate_hash(self, tmp_path: Path):
        # Same path but the file's bytes changed — hash must differ
        # so the next run discards the prior checkpoint.
        spec = tmp_path / "spec.json"
        spec.write_text('{"a": 1}')
        h1 = hash_config(_FakeCfg(spec_path=spec))
        spec.write_text('{"a": 2}')
        h2 = hash_config(_FakeCfg(spec_path=spec))
        assert h1 != h2

    def test_changing_out_dir_does_not_change_hash(self, tmp_path: Path):
        # Operational fields (where artifacts go) must not invalidate
        # an in-progress run's checkpoint.
        # _FakeCfg doesn't carry out_dir/staging_dir/abort_on_error —
        # which is the point: the hashing function deliberately reads
        # only the output-affecting subset.
        a = _FakeCfg()
        h1 = hash_config(a)
        # Mutate a non-hashed-but-CutoverConfig-real field.
        # (Real CutoverConfig has out_dir; _FakeCfg omits it. Verifying
        # with a getattr-default is enough — adding the attribute
        # shouldn't change the hash.)
        object.__setattr__(a, "out_dir", "/tmp/different")
        assert hash_config(a) == h1

    def test_missing_spec_falls_back_gracefully(self, tmp_path: Path):
        # When the spec file is missing, hash_config records a None
        # marker rather than raising — the orchestrator's own error
        # path will surface the real problem.
        h = hash_config(_FakeCfg(spec_path=tmp_path / "missing.json"))
        assert isinstance(h, str) and len(h) == 64  # sha256 hex
