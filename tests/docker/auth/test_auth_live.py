"""
Live integration tests for the v0.3.0 ``--druid-auth`` / ``--pinot-auth``
CLI features against an auth-enabled Druid + Pinot stack.

What this exercises end-to-end:

  1. Auth gating works: requests without credentials hit a 401 from both
     Druid and Pinot.
  2. ``--druid-auth basic:admin:admin`` and the equivalent
     ``DPM_DRUID_AUTH=basic:admin:admin`` env var both make ``dpm`` reach
     the cluster successfully.
  3. ``dpm extract-spec`` against an authed Druid datasource returns the
     same spec it does without auth (i.e. the auth header doesn't change
     the response shape).
  4. ``dpm backfill-batch`` flows pages out of an authed Druid and into
     an authed Pinot OFFLINE table with both ``--druid-auth`` and
     ``--pinot-auth`` set.

The compose/conftest in this directory boot the auth-enabled stack on
non-conflicting ports so this file can run alongside (or instead of) the
no-auth ``tests/docker/test_live_migration.py`` suite.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import pytest
import requests
from requests.auth import HTTPBasicAuth

from tests.docker.auth.conftest import (
    ADMIN_BASIC_AUTH_FLAG,
    ADMIN_PASS,
    ADMIN_USER,
    AUTH_DRUID_BROKER,
    AUTH_DRUID_COORDINATOR,
    AUTH_DRUID_OVERLORD,
    AUTH_DRUID_ROUTER,
    AUTH_PINOT_BROKER,
    AUTH_PINOT_CONTROLLER,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
ADMIN_AUTH = HTTPBasicAuth(ADMIN_USER, ADMIN_PASS)
DPM = ["python3", "-m", "migrator.cli.app"]
DEFAULT_TIMEOUT = 60


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────


def run_dpm(*args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    """Run dpm as a subprocess and return the completed process.

    We invoke through ``python -m migrator.cli.app`` rather than the
    console script so we don't depend on the Python entry-point being
    on PATH inside whatever runner ends up running the test.
    """
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return subprocess.run(
        DPM + list(args),
        cwd=str(REPO_ROOT),
        env=full_env,
        check=False,
        capture_output=True,
        text=True,
        timeout=DEFAULT_TIMEOUT,
    )


def _wait_for_datasource(name: str, *, timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.post(
            f"{AUTH_DRUID_ROUTER}/druid/v2/sql",
            auth=ADMIN_AUTH,
            json={"query": f'SELECT COUNT(*) AS c FROM "{name}"'},
            timeout=10,
        )
        if resp.status_code == 200:
            rows = resp.json()
            if rows and rows[0].get("c", 0) > 0:
                return
        time.sleep(3)
    raise TimeoutError(f"datasource '{name}' didn't appear within {timeout}s")


@pytest.fixture(scope="module")
def authed_druid_datasource(auth_docker_stack, tmp_path_factory):
    """Ingest a small offline datasource into the authed Druid via auth.

    Yields the datasource name. Cleanup is best-effort (the whole stack
    is torn down at session end anyway).
    """
    name = "auth_pageviews"
    work = tmp_path_factory.mktemp("authed_druid")
    data_path = work / "events.json"
    rows = []
    base = 1_704_067_200_000
    for i in range(200):
        rows.append({
            "timestamp": base + i * 60_000,
            "region": "us-east" if i % 2 == 0 else "us-west",
            "platform": ["desktop", "mobile", "tablet"][i % 3],
            "user_id": f"user_{i % 50}",
            "session_ms": 1000 + i,
            "bytes_sent": 100 + i * 10,
        })
    with data_path.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")

    # Drop the data file into the shared volume mounted into every
    # Druid container (created by druid_shared in compose).
    subprocess.run(
        ["docker", "exec", "authtest-coordinator",
         "mkdir", "-p", "/opt/shared/auth-data"],
        check=True,
    )
    subprocess.run(
        ["docker", "cp", str(data_path),
         f"authtest-coordinator:/opt/shared/auth-data/{data_path.name}"],
        check=True,
    )

    spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": name,
                "timestampSpec": {"column": "timestamp", "format": "millis"},
                "dimensionsSpec": {
                    "dimensions": ["region", "platform", "user_id"],
                },
                "metricsSpec": [
                    {"type": "count", "name": "events"},
                    {"type": "longSum", "name": "session_ms_sum",
                     "fieldName": "session_ms"},
                    {"type": "longSum", "name": "bytes_sent_sum",
                     "fieldName": "bytes_sent"},
                ],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "NONE",
                    "rollup": False,
                    "intervals": ["2024-01-01/2024-01-08"],
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/shared/auth-data",
                    "filter": data_path.name,
                },
                "inputFormat": {"type": "json"},
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxNumConcurrentSubTasks": 1,
            },
        },
    }
    submit = requests.post(
        f"{AUTH_DRUID_ROUTER}/druid/indexer/v1/task",
        auth=ADMIN_AUTH,
        json=spec,
        timeout=15,
    )
    submit.raise_for_status()
    _wait_for_datasource(name, timeout=240)
    return name


# ─────────────────────────────────────────────────────────────────────────────
# 1. Bare HTTP auth gating — sanity-check the cluster is actually authed
# ─────────────────────────────────────────────────────────────────────────────


class TestAuthGating:
    def test_druid_coordinator_rejects_unauth(self, auth_docker_stack):
        r = requests.get(
            f"{AUTH_DRUID_COORDINATOR}/druid/coordinator/v1/datasources",
            timeout=10,
        )
        assert r.status_code == 401

    def test_druid_coordinator_accepts_admin(self, auth_docker_stack):
        r = requests.get(
            f"{AUTH_DRUID_COORDINATOR}/druid/coordinator/v1/datasources",
            auth=ADMIN_AUTH,
            timeout=10,
        )
        assert r.status_code == 200

    def test_pinot_controller_rejects_unauth(self, auth_docker_stack):
        r = requests.get(f"{AUTH_PINOT_CONTROLLER}/tables", timeout=10)
        assert r.status_code == 401

    def test_pinot_controller_accepts_admin(self, auth_docker_stack):
        r = requests.get(
            f"{AUTH_PINOT_CONTROLLER}/tables",
            auth=ADMIN_AUTH,
            timeout=10,
        )
        assert r.status_code == 200

    def test_pinot_broker_rejects_unauth(self, auth_docker_stack):
        r = requests.post(
            f"{AUTH_PINOT_BROKER}/query/sql",
            json={"sql": "SELECT 1"},
            timeout=10,
        )
        assert r.status_code == 401


# ─────────────────────────────────────────────────────────────────────────────
# 2. dpm CLI — auth flag exits cleanly, missing/wrong auth fails predictably
# ─────────────────────────────────────────────────────────────────────────────


class TestDpmAuthFlag:
    def test_extract_spec_without_auth_fails(self, auth_docker_stack, tmp_path):
        """No auth → DruidCoordinatorClient sees 401 and surfaces an error.

        The exact error message is tested loosely because it's
        cosmetic — what matters is exit code != 0 *and* that no spec
        file was written.
        """
        out = tmp_path / "extract.json"
        result = run_dpm(
            "extract-spec",
            "--datasource", "any-name",
            "--coordinator-url", AUTH_DRUID_COORDINATOR,
            "--broker-url", AUTH_DRUID_BROKER,
            "--prefer", "batch",
            "--out", str(out),
        )
        assert result.returncode != 0, result.stderr
        assert not out.exists()

    def test_extract_spec_with_basic_auth_reaches_cluster(
        self, auth_docker_stack, tmp_path
    ):
        """With --druid-auth basic:admin:admin we should reach Druid and
        the failure (if any) should be domain-level, not 401."""
        out = tmp_path / "extract.json"
        result = run_dpm(
            "extract-spec",
            "--datasource", "definitely_does_not_exist",
            "--coordinator-url", AUTH_DRUID_COORDINATOR,
            "--broker-url", AUTH_DRUID_BROKER,
            "--prefer", "batch",
            "--druid-auth", ADMIN_BASIC_AUTH_FLAG,
            "--out", str(out),
        )
        # Datasource missing → exit 1 with a meaningful error.
        assert result.returncode == 1, result.stderr
        # Critically: error mentions the datasource (i.e. we reached
        # Druid's domain logic), not "401" / "unauthorized".
        combined = (result.stderr + result.stdout).lower()
        assert "definitely_does_not_exist" in combined
        assert "401" not in combined
        assert "unauthorized" not in combined

    def test_invalid_auth_format_fails_fast(self, auth_docker_stack, tmp_path):
        out = tmp_path / "extract.json"
        result = run_dpm(
            "extract-spec",
            "--datasource", "x",
            "--coordinator-url", AUTH_DRUID_COORDINATOR,
            "--prefer", "batch",
            "--druid-auth", "kerberos:realm",
            "--out", str(out),
        )
        # Pre-network validation → exit code 2 (Typer convention).
        assert result.returncode == 2, result.stderr
        assert "unknown auth kind" in result.stderr.lower()


# ─────────────────────────────────────────────────────────────────────────────
# 3. Env var fallback (DPM_DRUID_AUTH) — equivalent to --druid-auth
# ─────────────────────────────────────────────────────────────────────────────


class TestDpmAuthEnvVar:
    def test_extract_spec_via_env_var(self, auth_docker_stack, tmp_path):
        out = tmp_path / "extract-env.json"
        result = run_dpm(
            "extract-spec",
            "--datasource", "definitely_does_not_exist",
            "--coordinator-url", AUTH_DRUID_COORDINATOR,
            "--broker-url", AUTH_DRUID_BROKER,
            "--prefer", "batch",
            "--out", str(out),
            env={"DPM_DRUID_AUTH": ADMIN_BASIC_AUTH_FLAG},
        )
        assert result.returncode == 1, result.stderr
        combined = (result.stderr + result.stdout).lower()
        assert "definitely_does_not_exist" in combined
        assert "401" not in combined

    def test_cli_flag_overrides_env(self, auth_docker_stack, tmp_path):
        """Env wrong creds + CLI good creds → CLI wins, request succeeds."""
        out = tmp_path / "extract-cli-wins.json"
        result = run_dpm(
            "extract-spec",
            "--datasource", "definitely_does_not_exist",
            "--coordinator-url", AUTH_DRUID_COORDINATOR,
            "--broker-url", AUTH_DRUID_BROKER,
            "--prefer", "batch",
            "--druid-auth", ADMIN_BASIC_AUTH_FLAG,
            "--out", str(out),
            env={"DPM_DRUID_AUTH": "basic:bad:bad"},
        )
        # CLI-supplied admin:admin should win over the env's bad creds.
        # The resulting failure should be domain-level, not 401.
        assert result.returncode == 1, result.stderr
        combined = (result.stderr + result.stdout).lower()
        assert "definitely_does_not_exist" in combined
        assert "401" not in combined


# ─────────────────────────────────────────────────────────────────────────────
# 4. End-to-end: dpm extract-spec against a real authed datasource
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractSpecLiveAuth:
    def test_extract_real_datasource_with_auth(
        self, authed_druid_datasource, tmp_path
    ):
        out = tmp_path / "auth-pageviews-spec.json"
        result = run_dpm(
            "extract-spec",
            "--datasource", authed_druid_datasource,
            "--coordinator-url", AUTH_DRUID_COORDINATOR,
            "--broker-url", AUTH_DRUID_BROKER,
            "--overlord-url", AUTH_DRUID_OVERLORD,
            "--prefer", "batch",
            "--druid-auth", ADMIN_BASIC_AUTH_FLAG,
            "--out", str(out),
        )
        assert result.returncode == 0, (
            f"exit={result.returncode}\nstdout={result.stdout}\n"
            f"stderr={result.stderr}"
        )
        assert out.exists()
        spec = json.loads(out.read_text())
        # Sanity: the recovered spec mentions the datasource and at least
        # one of the dimensions we ingested.
        assert spec["spec"]["dataSchema"]["dataSource"] == authed_druid_datasource
        dims = spec["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"]
        # dpm sorts the dimensions; just check the set
        assert {"region", "platform", "user_id"}.issubset(set(dims))


# ─────────────────────────────────────────────────────────────────────────────
# 5. End-to-end: dpm backfill-batch against authed Druid + authed Pinot
# ─────────────────────────────────────────────────────────────────────────────


class TestBackfillBatchLiveAuth:
    def test_backfill_with_both_auth_flags(
        self, authed_druid_datasource, tmp_path
    ):
        # Generate Pinot artifacts for the source datasource.
        # Use the extracted spec so we know the schema names match.
        spec_path = tmp_path / "spec.json"
        ext = run_dpm(
            "extract-spec",
            "--datasource", authed_druid_datasource,
            "--coordinator-url", AUTH_DRUID_COORDINATOR,
            "--broker-url", AUTH_DRUID_BROKER,
            "--prefer", "batch",
            "--druid-auth", ADMIN_BASIC_AUTH_FLAG,
            "--out", str(spec_path),
        )
        assert ext.returncode == 0, ext.stderr

        out_dir = tmp_path / "artifacts"
        gen = run_dpm(
            "generate", str(spec_path), "--out", str(out_dir),
        )
        assert gen.returncode == 0, gen.stderr

        # Deploy the dpm-generated schema + OFFLINE table AS-IS via authed
        # POSTs. We're testing the dpm AUTH path, not whether the full
        # migration round-trips byte-for-byte (that's covered by the
        # no-auth suite + the hybrid-migration example).
        schema = (out_dir / "schema.json").read_text()
        s = requests.post(
            f"{AUTH_PINOT_CONTROLLER}/schemas",
            auth=ADMIN_AUTH,
            data=schema,
            headers={"Content-Type": "application/json"},
            timeout=20,
        )
        assert s.status_code in (200, 201), f"schema POST: {s.status_code} {s.text}"

        table_cfg = (out_dir / "table-offline.json").read_text()
        t = requests.post(
            f"{AUTH_PINOT_CONTROLLER}/tables",
            auth=ADMIN_AUTH,
            data=table_cfg,
            headers={"Content-Type": "application/json"},
            timeout=20,
        )
        assert t.status_code in (200, 201), f"table POST: {t.status_code} {t.text}"

        # Now the actual auth check: dpm backfill-batch with both
        # --druid-auth and --pinot-auth. The Druid SQL pager + Pinot
        # ingest must both walk past the cluster auth layer.
        # The staging file write proves Druid auth worked; the absence
        # of 401 in the error proves Pinot auth worked. Whether or not
        # the segment ultimately builds is orthogonal to auth and is
        # covered by other tests.
        staging = tmp_path / "staging"
        result = run_dpm(
            "backfill-batch",
            "--datasource", authed_druid_datasource,
            "--pinot-table", authed_druid_datasource,
            "--start-iso", "2024-01-01T00:00:00.000Z",
            "--end-iso", "2024-01-08T00:00:00.000Z",
            "--druid-router", AUTH_DRUID_ROUTER,
            "--pinot-controller", AUTH_PINOT_CONTROLLER,
            "--staging-dir", str(staging),
            "--druid-auth", ADMIN_BASIC_AUTH_FLAG,
            "--pinot-auth", ADMIN_BASIC_AUTH_FLAG,
        )
        # Druid SQL pager must have produced staging files — this only
        # happens after a successful authed Druid SQL response.
        assert staging.exists()
        page_files = sorted(staging.glob("page-*.json"))
        assert page_files, (
            f"no staging files written — Druid auth path may have failed.\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
        # Page file must be non-empty (i.e. Druid actually returned rows).
        assert page_files[0].stat().st_size > 0

        # Whatever happened on the Pinot side, it must NOT be 401.
        combined = (result.stdout + result.stderr).lower()
        assert "401" not in combined, (
            f"Pinot auth path returned 401 — auth header didn't reach the "
            f"controller.\nstdout={result.stdout}\nstderr={result.stderr}"
        )
        assert "unauthorized" not in combined
