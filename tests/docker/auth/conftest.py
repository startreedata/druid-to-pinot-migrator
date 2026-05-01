"""
pytest fixtures for the live AUTH-enabled integration tests.

Brings up a separate compose stack (``tests/docker/auth/docker-compose.yml``)
on non-conflicting ports so it can run alongside the no-auth stack from
``tests/docker/docker-compose.yml`` without port collisions.

Skip behaviour
──────────────
All tests in this directory are skipped unless ``LIVE_DOCKER_TESTS=1`` is
set, matching the pattern used by the no-auth live tests.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

AUTH_DIR = Path(__file__).parent
LIVE = os.environ.get("LIVE_DOCKER_TESTS", "0") == "1"

# Auth-enabled cluster URLs and credentials. These match the compose file +
# druid.env / pinot-conf in this directory and intentionally diverge from
# the no-auth stack's ports (18081/19000/etc.) so both can run side by side.
AUTH_DRUID_COORDINATOR = "http://localhost:18181"
AUTH_DRUID_BROKER = "http://localhost:18182"
AUTH_DRUID_OVERLORD = "http://localhost:18181"
AUTH_DRUID_ROUTER = "http://localhost:18988"

AUTH_PINOT_CONTROLLER = "http://localhost:19100"
AUTH_PINOT_BROKER = "http://localhost:18199"

AUTH_KAFKA_BOOTSTRAP_HOST = "localhost:19292"

# Single principal — the shared admin/admin used for both Druid and Pinot.
ADMIN_USER = "admin"
ADMIN_PASS = "admin"
ADMIN_BASIC_AUTH_FLAG = f"basic:{ADMIN_USER}:{ADMIN_PASS}"

skip_unless_live = pytest.mark.skipif(
    not LIVE,
    reason="Set LIVE_DOCKER_TESTS=1 to run live auth integration tests",
)


def pytest_collection_modifyitems(items):
    """Apply the live-test skip mark to every test in this auth dir."""
    for item in items:
        if str(AUTH_DIR) in str(item.fspath):
            item.add_marker(skip_unless_live)


@pytest.fixture(scope="session")
def auth_docker_stack():
    """Start the auth-enabled compose stack once and tear it down at end."""
    compose_file = AUTH_DIR / "docker-compose.yml"

    def run(args: list[str]) -> subprocess.CompletedProcess:
        proc = subprocess.run(
            ["docker", "compose", "-f", str(compose_file)] + args,
            cwd=str(AUTH_DIR),
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            sys.stderr.write(
                f"\n[docker compose {' '.join(args)}] exit={proc.returncode}\n"
                f"--- stdout ---\n{proc.stdout}\n"
                f"--- stderr ---\n{proc.stderr}\n"
            )
            ps = subprocess.run(
                ["docker", "ps", "-a", "--filter", "name=authtest-",
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
            sys.stderr.flush()
            raise subprocess.CalledProcessError(
                proc.returncode, proc.args, proc.stdout, proc.stderr
            )
        return proc

    run(["up", "-d", "--wait", "--wait-timeout", "420"])
    yield
    run(["down", "-v", "--remove-orphans"])
