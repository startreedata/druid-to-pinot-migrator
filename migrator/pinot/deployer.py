"""
Apply Pinot schema and table configs to a controller via REST.

Used by the ``dpm deploy`` CLI command. Lifted into its own module
(rather than living inside the CLI command file) so other entry points
— the planned ``dpm cutover`` orchestrator, integration tests, third-
party code embedding dpm — can reuse the same deployment semantics.

The deployer is intentionally idempotent at the controller-status
level: a ``409 Conflict`` (Pinot's response when a schema/table with
the same name already exists) is treated as a soft success. This makes
the command safe to re-run after a partial failure or a manual
dry-run-then-apply workflow.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import requests


@dataclass
class DeployArtifacts:
    """Filesystem locations of the Pinot artifacts to deploy.

    Any field may be ``None`` to skip that step (e.g. an OFFLINE-only
    deployment passes ``realtime_table=None``).
    """
    schema: Path | None = None
    offline_table: Path | None = None
    realtime_table: Path | None = None


@dataclass
class DeployResult:
    """Per-artifact outcome of a deploy call."""
    artifact: str
    name: str
    status: str  # "created", "already_exists", or "error"
    detail: str = ""


@dataclass
class DeployReport:
    """Aggregated results from a single ``deploy()`` call."""
    results: list[DeployResult] = field(default_factory=list)

    @property
    def all_ok(self) -> bool:
        return all(r.status != "error" for r in self.results)

    @property
    def created(self) -> int:
        return sum(1 for r in self.results if r.status == "created")

    @property
    def already_exists(self) -> int:
        return sum(1 for r in self.results if r.status == "already_exists")

    @property
    def errored(self) -> int:
        return sum(1 for r in self.results if r.status == "error")


class PinotDeployer:
    """Thin client around Pinot's controller deploy endpoints.

    ``session`` is optional but recommended — pass an authenticated
    ``requests.Session`` (e.g. one built by
    ``migrator.auth.session_from_env``) to deploy against a Pinot
    controller behind Basic auth, Bearer auth, or a custom header.
    """

    def __init__(
        self,
        controller_url: str,
        *,
        session: requests.Session | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._url = controller_url.rstrip("/")
        self._timeout = timeout
        if session is None:
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
        self._session = session

    # ── public API ─────────────────────────────────────────────────────────

    def deploy(self, artifacts: DeployArtifacts) -> DeployReport:
        """Apply schema + tables in the right order and return a report.

        Order matters: Pinot rejects a table create if its schema
        doesn't exist yet, so schema goes first. OFFLINE then REALTIME
        is the conventional order for hybrid deployments.
        """
        report = DeployReport()

        if artifacts.schema is not None:
            report.results.append(
                self._post_schema(artifacts.schema)
            )

        if artifacts.offline_table is not None:
            report.results.append(
                self._post_table(artifacts.offline_table, kind="offline")
            )

        if artifacts.realtime_table is not None:
            report.results.append(
                self._post_table(artifacts.realtime_table, kind="realtime")
            )

        return report

    # ── per-endpoint helpers ───────────────────────────────────────────────

    def _post_schema(self, path: Path) -> DeployResult:
        body = path.read_text()
        # Best-effort name extraction so the report is informative.
        try:
            name = json.loads(body).get("schemaName", path.stem)
        except json.JSONDecodeError:
            name = path.stem
        resp = self._session.post(
            f"{self._url}/schemas",
            data=body,
            headers={"Content-Type": "application/json"},
            timeout=self._timeout,
        )
        return self._classify(resp, artifact="schema", name=name)

    def _post_table(self, path: Path, *, kind: str) -> DeployResult:
        body = path.read_text()
        try:
            name = json.loads(body).get("tableName", path.stem)
        except json.JSONDecodeError:
            name = path.stem
        resp = self._session.post(
            f"{self._url}/tables",
            data=body,
            headers={"Content-Type": "application/json"},
            timeout=self._timeout,
        )
        return self._classify(resp, artifact=f"table-{kind}", name=name)

    @staticmethod
    def _classify(resp: requests.Response, *, artifact: str, name: str) -> DeployResult:
        if resp.status_code in (200, 201):
            return DeployResult(artifact=artifact, name=name, status="created")
        # 409 from /tables means "table already exists" (Pinot response).
        # /schemas returns 200 when re-posting an identical schema, so
        # the 409 path is mostly relevant for tables; we still treat
        # both as soft-success so re-runs are idempotent.
        if resp.status_code == 409:
            return DeployResult(
                artifact=artifact,
                name=name,
                status="already_exists",
                detail=resp.text[:200],
            )
        return DeployResult(
            artifact=artifact,
            name=name,
            status="error",
            detail=f"HTTP {resp.status_code}: {resp.text[:300]}",
        )


# ─────────────────────────────────────────────────────────────────────────────
# Discovery helper used by the CLI's --artifacts-dir flow
# ─────────────────────────────────────────────────────────────────────────────


def discover_artifacts(directory: Path) -> DeployArtifacts:
    """Look for the standard dpm-generated filenames inside ``directory``.

    A file that doesn't exist yields ``None`` for its slot — the
    deployer skips it. This is the right behaviour for batch-only or
    realtime-only deployments where ``dpm generate`` produced just one
    of the two table configs.
    """
    schema = directory / "schema.json"
    offline = directory / "table-offline.json"
    realtime = directory / "table-realtime.json"
    return DeployArtifacts(
        schema=schema if schema.exists() else None,
        offline_table=offline if offline.exists() else None,
        realtime_table=realtime if realtime.exists() else None,
    )
