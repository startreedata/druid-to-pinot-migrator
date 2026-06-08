"""
pytest fixtures for the live Docker integration tests.

Lifecycle
─────────
• session-scoped ``docker_stack`` fixture starts and tears down the
  docker-compose stack exactly once per test session.
• session-scoped ``druid`` and ``pinot`` fixtures return pre-warmed clients.
• function-scoped ``tmp_output`` gives each test its own temp directory.
• function-scoped ``ingested_datasource`` factory creates a Druid datasource
  and registers a finalizer to clean it up.

Skip behaviour
──────────────
All tests in this package are skipped unless the environment variable
``LIVE_DOCKER_TESTS=1`` is set.  This prevents CI from running the heavy
stack unless explicitly requested.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Generator

import pytest

from tests.docker.cluster_clients import DruidClient, PinotClient

# ── Directory where docker-compose.yml lives ──────────────────────────────
DOCKER_DIR = Path(__file__).parent

# ── Guard: skip everything unless explicitly enabled ─────────────────────
LIVE = os.environ.get("LIVE_DOCKER_TESTS", "0") == "1"
skip_unless_live = pytest.mark.skipif(
    not LIVE, reason="Set LIVE_DOCKER_TESTS=1 to run live Docker tests"
)


def pytest_collection_modifyitems(items):
    """Automatically apply the live-test skip mark to every test in this dir."""
    for item in items:
        if str(DOCKER_DIR) in str(item.fspath):
            item.add_marker(skip_unless_live)


# ─────────────────────────────────────────────────────────────────────────────
# docker-compose lifecycle
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def docker_stack():
    """Start the docker-compose stack once and tear it down after the session."""
    compose_file = DOCKER_DIR / "docker-compose.yml"

    def run(args: list[str], **kwargs):
        proc = subprocess.run(
            ["docker", "compose", "-f", str(compose_file)] + args,
            cwd=str(DOCKER_DIR),
            check=False,
            capture_output=True,
            text=True,
            **kwargs,
        )
        if proc.returncode != 0:
            # Print whatever docker compose said before raising — otherwise
            # the captured output is silently swallowed and CI logs only
            # show "exit status 1", which is useless for debugging.
            sys.stderr.write(
                f"\n[docker compose {' '.join(args)}] exit={proc.returncode}\n"
                f"--- stdout ---\n{proc.stdout}\n"
                f"--- stderr ---\n{proc.stderr}\n"
            )
            # Capture per-container state and last-100 logs to make the
            # failure self-diagnosing.
            try:
                ps = subprocess.run(
                    ["docker", "ps", "-a", "--filter", "name=migtest-",
                     "--format", "{{.Names}}\t{{.Status}}"],
                    capture_output=True, text=True, check=False,
                )
                sys.stderr.write(f"--- container state ---\n{ps.stdout}\n")
                for line in ps.stdout.splitlines():
                    name = line.split("\t", 1)[0].strip()
                    if not name:
                        continue
                    logs = subprocess.run(
                        ["docker", "logs", "--tail=80", name],
                        capture_output=True, text=True, check=False,
                    )
                    sys.stderr.write(
                        f"--- {name} (last 80 lines) ---\n"
                        f"{logs.stdout}\n{logs.stderr}\n"
                    )
            except Exception as diag_exc:  # noqa: BLE001
                sys.stderr.write(f"(diagnostics failed: {diag_exc})\n")
            sys.stderr.flush()
            raise subprocess.CalledProcessError(
                proc.returncode, proc.args, proc.stdout, proc.stderr
            )
        return proc

    # Bring up the whole stack (images are expected to be present locally)
    run(["up", "-d", "--wait", "--wait-timeout", "420"])

    yield  # tests run here

    # Always tear down, even if tests fail
    run(["down", "-v", "--remove-orphans"])


# ─────────────────────────────────────────────────────────────────────────────
# Cluster clients
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def druid(docker_stack) -> DruidClient:
    client = DruidClient()
    client.wait_healthy(timeout=240)
    return client


@pytest.fixture(scope="session")
def pinot(docker_stack) -> PinotClient:
    client = PinotClient()
    client.wait_healthy(timeout=180)
    return client


@pytest.fixture(scope="session")
def kafka_client(docker_stack):
    """Shared Kafka admin/producer client for any live test that needs it."""
    from tests.docker.cluster_clients import KafkaTestClient

    k = KafkaTestClient()
    k.wait_healthy(timeout=120)
    return k


@pytest.fixture(scope="session")
def supervisor_client(docker_stack):
    """Shared helper for submitting Druid Kafka / Kinesis supervisors."""
    from tests.docker.cluster_clients import DruidSupervisorClient

    return DruidSupervisorClient()


@pytest.fixture(scope="session")
def kinesis_client(docker_stack):
    """Shared boto3 Kinesis client against LocalStack. Skips if boto3 is
    unavailable (it's a dev-only dependency)."""
    pytest.importorskip("boto3")
    from tests.docker.cluster_clients import KinesisTestClient

    k = KinesisTestClient()
    k.wait_healthy(timeout=120)
    return k


# ─────────────────────────────────────────────────────────────────────────────
# Per-test helpers
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_output(tmp_path) -> Path:
    """A fresh temporary directory for each test's generated artifacts."""
    return tmp_path


@pytest.fixture
def druid_datasource_factory(druid: DruidClient):
    """
    Factory fixture that ingests a named datasource into Druid and
    registers a cleanup finalizer.

    Usage::

        def test_foo(druid_datasource_factory):
            ds = druid_datasource_factory(
                name="my_ds",
                records=[...],
                timestamp_col="ts",
                dimensions=["dim1"],
                metrics=[{"type": "count", "name": "cnt"}],
            )
            # ds is the datasource name string
    """
    created: list[str] = []

    def factory(
        name: str,
        records: list[dict],
        timestamp_col: str = "timestamp",
        dimensions: list[str] | None = None,
        metrics: list[dict] | None = None,
        rollup: bool = False,
        segment_granularity: str = "DAY",
        query_granularity: str = "NONE",
        intervals: list[str] | None = None,
    ) -> str:
        # Skip re-ingestion if this factory call already succeeded in this session
        if name in created:
            return name
        druid.ingest_inline(
            datasource=name,
            records=records,
            timestamp_col=timestamp_col,
            timestamp_format="millis",
            dimensions=dimensions,
            metrics=metrics,
            rollup=rollup,
            segment_granularity=segment_granularity,
            query_granularity=query_granularity,
            intervals=intervals,
        )
        druid.wait_for_datasource(name, timeout=180)
        created.append(name)
        return name

    yield factory

    for ds in created:
        druid.drop_datasource(ds)


@pytest.fixture
def pinot_table_factory(pinot: PinotClient):
    """
    Factory fixture that creates a Pinot schema+table and registers cleanup.

    Usage::

        def test_foo(pinot_table_factory):
            pinot_table_factory(schema=..., table_config=...)
    """
    created: list[str] = []

    def factory(schema: dict, table_config: dict) -> str:
        pinot.create_schema(schema)
        pinot.create_table(table_config)
        table_name = table_config["tableName"].removesuffix("_OFFLINE").removesuffix("_REALTIME")
        created.append(table_name)
        return table_name

    yield factory

    for name in created:
        pinot.delete_table(name)
        pinot.delete_schema(name)
