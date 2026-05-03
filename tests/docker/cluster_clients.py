"""
Thin HTTP clients for Druid and Pinot during live integration tests.

These are test-only helpers; they do NOT belong in the migrator package.
"""

from __future__ import annotations

import json
import time
from typing import Any

import requests


# ─────────────────────────────────────────────────────────────────────────────
# Constants — match host-side ports declared in docker-compose.yml
# ─────────────────────────────────────────────────────────────────────────────

DRUID_ROUTER_URL = "http://localhost:18888"
DRUID_COORDINATOR_URL = "http://localhost:18081"
PINOT_CONTROLLER_URL = "http://localhost:19000"
PINOT_BROKER_URL = "http://localhost:18099"

# Host-side bootstrap (for tests) and in-network bootstrap (for Druid+Pinot)
KAFKA_BOOTSTRAP_HOST = "localhost:19092"
KAFKA_BOOTSTRAP_INTERNAL = "kafka:9092"


# ─────────────────────────────────────────────────────────────────────────────
# Druid client
# ─────────────────────────────────────────────────────────────────────────────

class DruidClient:
    """Minimal Druid HTTP client used by integration tests."""

    def __init__(self, router_url: str = DRUID_ROUTER_URL,
                 coordinator_url: str = DRUID_COORDINATOR_URL):
        self.router_url = router_url.rstrip("/")
        self.coordinator_url = coordinator_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    # ── health ──────────────────────────────────────────────────────────────

    def is_healthy(self) -> bool:
        try:
            r = self.session.get(f"{self.router_url}/status/health", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def wait_healthy(self, timeout: int = 180, interval: int = 5) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_healthy():
                return
            time.sleep(interval)
        raise TimeoutError(f"Druid router not healthy after {timeout}s")

    # ── ingestion ────────────────────────────────────────────────────────────

    def submit_ingestion(self, spec: dict) -> str:
        """Submit a native batch ingestion spec; return the task ID."""
        url = f"{self.router_url}/druid/indexer/v1/task"
        r = self.session.post(url, data=json.dumps(spec), timeout=30)
        r.raise_for_status()
        return r.json()["task"]

    def wait_for_task(self, task_id: str, timeout: int = 300,
                      interval: int = 5) -> str:
        """Poll until the task reaches a terminal state; return the status."""
        url = f"{self.router_url}/druid/indexer/v1/task/{task_id}/status"
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = self.session.get(url, timeout=10)
            r.raise_for_status()
            status = r.json()["status"]["status"]
            if status in ("SUCCESS", "FAILED"):
                return status
            time.sleep(interval)
        raise TimeoutError(f"Task {task_id} did not complete in {timeout}s")

    def ingest_inline(self, datasource: str, records: list[dict],
                      timestamp_col: str = "timestamp",
                      timestamp_format: str = "millis",
                      dimensions: list[str] | None = None,
                      metrics: list[dict] | None = None,
                      rollup: bool = False,
                      segment_granularity: str = "DAY",
                      query_granularity: str = "NONE",
                      intervals: list[str] | None = None) -> None:
        """
        Submit an index_parallel task with inline JSON data and wait for it
        to complete.  Dimensions default to all non-timestamp, non-metric keys.
        """
        if dimensions is None:
            metric_names = {m["name"] for m in (metrics or [])}
            field_names = set()
            for r in records:
                field_names.update(r.keys())
            dimensions = sorted(
                f for f in field_names
                if f != timestamp_col and f not in metric_names
            )

        spec = {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": datasource,
                    "timestampSpec": {
                        "column": timestamp_col,
                        "format": timestamp_format,
                    },
                    "dimensionsSpec": {
                        "dimensions": dimensions,
                    },
                    "metricsSpec": metrics or [],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": segment_granularity,
                        "queryGranularity": query_granularity,
                        "rollup": rollup,
                        "intervals": intervals or ["2024-01-01/2025-01-01"],
                    },
                },
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": {
                        "type": "inline",
                        "data": "\n".join(json.dumps(r) for r in records),
                    },
                    "inputFormat": {"type": "json"},
                },
                "tuningConfig": {
                    "type": "index_parallel",
                    "maxNumConcurrentSubTasks": 1,
                },
            },
        }
        task_id = self.submit_ingestion(spec)
        status = self.wait_for_task(task_id, timeout=300)
        if status != "SUCCESS":
            raise RuntimeError(
                f"Druid ingestion task {task_id} failed with status '{status}'"
            )

    # ── query ────────────────────────────────────────────────────────────────

    def sql_query(self, sql: str) -> list[dict]:
        """Run a Druid SQL query via the router; return list of row dicts."""
        url = f"{self.router_url}/druid/v2/sql"
        payload = {"query": sql, "resultFormat": "object"}
        r = self.session.post(url, data=json.dumps(payload), timeout=30)
        r.raise_for_status()
        return r.json()

    def wait_for_datasource(self, datasource: str, timeout: int = 120,
                             interval: int = 5) -> None:
        """Wait until the datasource is queryable (segments available)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                rows = self.sql_query(
                    f"SELECT COUNT(*) AS cnt FROM \"{datasource}\""
                )
                if rows and rows[0].get("cnt", 0) > 0:
                    return
            except Exception:
                pass
            time.sleep(interval)
        raise TimeoutError(
            f"Druid datasource '{datasource}' not queryable after {timeout}s"
        )

    def get_datasource_spec(self, datasource: str) -> dict:
        """
        Return a minimal ingestion spec dict constructed from the coordinator
        metadata so we can drive the migration tool.
        """
        # Fetch segment metadata to get column types
        meta_url = (
            f"{self.coordinator_url}/druid/coordinator/v1"
            f"/datasources/{datasource}?full"
        )
        r = self.session.get(meta_url, timeout=15)
        r.raise_for_status()
        return r.json()

    def drop_datasource(self, datasource: str) -> None:
        """Delete all segments for a datasource (best-effort cleanup)."""
        url = (
            f"{self.coordinator_url}/druid/coordinator/v1"
            f"/datasources/{datasource}"
        )
        try:
            self.session.delete(url, timeout=15)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Pinot client
# ─────────────────────────────────────────────────────────────────────────────

class PinotClient:
    """Minimal Apache Pinot HTTP client used by integration tests."""

    def __init__(self, controller_url: str = PINOT_CONTROLLER_URL,
                 broker_url: str = PINOT_BROKER_URL):
        self.controller_url = controller_url.rstrip("/")
        self.broker_url = broker_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    # ── health ──────────────────────────────────────────────────────────────

    def is_healthy(self) -> bool:
        try:
            r = self.session.get(f"{self.controller_url}/health", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def wait_healthy(self, timeout: int = 120, interval: int = 5) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_healthy():
                return
            time.sleep(interval)
        raise TimeoutError(f"Pinot controller not healthy after {timeout}s")

    # ── schema ───────────────────────────────────────────────────────────────

    def create_schema(self, schema: dict) -> None:
        """Create schema; if it already exists, update it."""
        schema_name = schema.get("schemaName", "")
        # Try update first (PUT), then create (POST)
        put_url = f"{self.controller_url}/schemas/{schema_name}"
        r = self.session.put(put_url, data=json.dumps(schema), timeout=30)
        if r.status_code in (200, 201):
            return
        post_url = f"{self.controller_url}/schemas"
        r = self.session.post(post_url, data=json.dumps(schema), timeout=30)
        if r.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create Pinot schema '{schema_name}': {r.status_code} {r.text}"
            )

    def get_schema(self, schema_name: str) -> dict:
        url = f"{self.controller_url}/schemas/{schema_name}"
        r = self.session.get(url, timeout=15)
        r.raise_for_status()
        return r.json()

    def delete_schema(self, schema_name: str) -> None:
        url = f"{self.controller_url}/schemas/{schema_name}"
        try:
            self.session.delete(url, timeout=15)
        except Exception:
            pass

    # ── table ────────────────────────────────────────────────────────────────

    def create_table(self, table_config: dict) -> None:
        """Create table; if it already exists, skip (idempotent)."""
        url = f"{self.controller_url}/tables"
        r = self.session.post(url, data=json.dumps(table_config), timeout=30)
        if r.status_code == 409:
            # Table already exists — acceptable in idempotent test setup
            return
        if r.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create Pinot table: {r.status_code} {r.text}"
            )

    def get_table_config(self, table_name: str) -> dict:
        url = f"{self.controller_url}/tables/{table_name}"
        r = self.session.get(url, timeout=15)
        r.raise_for_status()
        return r.json()

    def delete_table(self, table_name: str) -> None:
        for suffix in ("_OFFLINE", "_REALTIME", ""):
            url = f"{self.controller_url}/tables/{table_name}{suffix}"
            try:
                self.session.delete(url, timeout=15)
            except Exception:
                pass

    # ── ingestion ────────────────────────────────────────────────────────────

    def ingest_from_file(self, table_name: str, file_path: str,
                         schema_name: str | None = None) -> None:
        """
        Use the Pinot controller ingestion endpoint to load a JSON file.
        This calls the /ingestFromFile API (available in Pinot 0.12+).

        Pinot 1.5+ rejects nested JSON objects in ``batchConfigMapStr``
        (the values must be primitives) — see PR #6 / #21 for the
        downstream fixes that surfaced the constraint. The simple
        ``{"inputFormat":"json"}`` form is sufficient: the controller
        resolves the JSON record reader from its built-in registry.
        """
        if schema_name is None:
            schema_name = table_name
        url = (
            f"{self.controller_url}/ingestFromFile"
            f"?tableNameWithType={table_name}_OFFLINE"
            f"&batchConfigMapStr="
            + json.dumps({"inputFormat": "json"})
        )
        with open(file_path, "rb") as fh:
            r = requests.post(
                url,
                files={"file": ("data.json", fh, "application/json")},
                headers={},  # don't send Content-Type: application/json for multipart
                timeout=120,
            )
        if r.status_code not in (200, 201):
            raise RuntimeError(
                f"Pinot ingestFromFile failed: {r.status_code} {r.text}"
            )

    def add_segment(self, table_name: str, records: list[dict],
                    schema: dict, tmp_dir: str) -> None:
        """
        Write records as a NDJSON file and push them via the ingestion job API.
        Uses /ingestFromFile for simplicity in test environments.
        """
        import os
        import tempfile
        data_file = os.path.join(tmp_dir, f"{table_name}_data.json")
        with open(data_file, "w") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")
        self.ingest_from_file(table_name, data_file)

    # ── query ────────────────────────────────────────────────────────────────

    def sql_query(self, sql: str) -> list[dict]:
        """Run a Pinot SQL query via the broker; return list of row dicts."""
        url = f"{self.broker_url}/query/sql"
        payload = {"sql": sql}
        r = self.session.post(url, data=json.dumps(payload), timeout=30)
        r.raise_for_status()
        data = r.json()
        if "exceptions" in data and data["exceptions"]:
            raise RuntimeError(f"Pinot query error: {data['exceptions']}")
        cols = data.get("resultTable", {}).get("dataSchema", {}).get("columnNames", [])
        if not cols:
            # Fallback for older response format
            cols = data.get("columnNames", [])
        rows_raw = data.get("resultTable", {}).get("rows", [])
        return [dict(zip(cols, row)) for row in rows_raw]

    def wait_for_table_queryable(self, table_name: str, timeout: int = 120,
                                  interval: int = 5) -> None:
        """Wait until the Pinot table is reachable and has at least one row."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                rows = self.sql_query(
                    f"SELECT COUNT(*) AS cnt FROM {table_name}"
                )
                if rows and rows[0].get("cnt", 0) > 0:
                    return
            except Exception:
                pass
            time.sleep(interval)
        raise TimeoutError(
            f"Pinot table '{table_name}' not queryable after {timeout}s"
        )

    def list_tables(self) -> list[str]:
        url = f"{self.controller_url}/tables"
        r = self.session.get(url, timeout=15)
        r.raise_for_status()
        return r.json().get("tables", [])

    def get_indexes(self, table_name: str) -> dict:
        """Return the tableIndexConfig section from the table config."""
        cfg = self.get_table_config(table_name)
        # Controller returns {OFFLINE: {...}} or {REALTIME: {...}}
        for key in ("OFFLINE", "REALTIME"):
            if key in cfg:
                return cfg[key].get("tableIndexConfig", {})
        return {}

    def get_segments_count(self, table_name: str) -> int:
        url = f"{self.controller_url}/segments/{table_name}"
        r = self.session.get(url, timeout=15)
        if r.status_code == 404:
            return 0
        r.raise_for_status()
        segments = r.json()
        # Response is a list of {segmentType: [...]}
        total = 0
        for entry in segments:
            for segs in entry.values():
                total += len(segs)
        return total


# ─────────────────────────────────────────────────────────────────────────────
# Kafka test client
# ─────────────────────────────────────────────────────────────────────────────


class KafkaTestClient:
    """Tiny producer/admin wrapper used by realtime live tests."""

    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP_HOST) -> None:
        self.bootstrap = bootstrap

    def wait_healthy(self, timeout: int = 60) -> None:
        from kafka import KafkaAdminClient
        deadline = time.time() + timeout
        last_err: Exception | None = None
        while time.time() < deadline:
            try:
                admin = KafkaAdminClient(bootstrap_servers=self.bootstrap)
                admin.list_topics()
                admin.close()
                return
            except Exception as e:
                last_err = e
                time.sleep(2)
        raise TimeoutError(f"Kafka not healthy: {last_err}")

    def create_topic(self, topic: str, partitions: int = 1) -> None:
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap)
        try:
            admin.create_topics(
                [NewTopic(name=topic, num_partitions=partitions,
                          replication_factor=1)]
            )
        except TopicAlreadyExistsError:
            pass
        finally:
            admin.close()

    def produce_json(
        self,
        topic: str,
        records: list[dict],
        key_field: str | None = None,
    ) -> None:
        """Produce JSON-serialised records, optionally keyed by a field."""
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=(
                (lambda k: str(k).encode("utf-8")) if key_field else None
            ),
            acks="all",
            retries=3,
        )
        for rec in records:
            producer.send(
                topic,
                value=rec,
                key=rec[key_field] if key_field else None,
            )
        producer.flush(timeout=30)
        producer.close()


# ─────────────────────────────────────────────────────────────────────────────
# Druid supervisor helper (Kafka indexing service)
# ─────────────────────────────────────────────────────────────────────────────


class DruidSupervisorClient:
    """Helpers for Druid Kafka supervisors used by realtime live tests."""

    def __init__(self, router_url: str = DRUID_ROUTER_URL):
        self.router_url = router_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def submit_kafka_supervisor(
        self,
        datasource: str,
        topic: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_INTERNAL,
        timestamp_col: str = "timestamp",
        timestamp_format: str = "millis",
        dimensions: list[str] | None = None,
    ) -> str:
        """Submit a Kafka supervisor spec; return supervisor ID."""
        spec = {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": datasource,
                    "timestampSpec": {
                        "column": timestamp_col,
                        "format": timestamp_format,
                    },
                    "dimensionsSpec": {"dimensions": dimensions or []},
                    "metricsSpec": [],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "HOUR",
                        "queryGranularity": "MINUTE",
                        "rollup": False,
                    },
                },
                "ioConfig": {
                    "topic": topic,
                    "consumerProperties": {
                        "bootstrap.servers": bootstrap_servers,
                    },
                    # Modern Druid (≥ 0.17) wants an `inputFormat` declared
                    # at the ioConfig level instead of the legacy `parser`
                    # in dataSchema.
                    "inputFormat": {"type": "json"},
                    "useEarliestOffset": True,
                    "taskCount": 1,
                    "replicas": 1,
                    "taskDuration": "PT300S",
                },
                "tuningConfig": {
                    "type": "kafka",
                    "maxRowsInMemory": 10_000,
                    "maxRowsPerSegment": 100_000,
                },
            },
        }
        r = self.session.post(
            f"{self.router_url}/druid/indexer/v1/supervisor",
            data=json.dumps(spec),
            timeout=30,
        )
        if r.status_code >= 400:
            # Surface Druid's actual error body — much more useful than
            # bare HTTPError("400 Bad Request") for debugging.
            raise RuntimeError(
                f"Druid supervisor submission failed: "
                f"HTTP {r.status_code} {r.reason}\nbody: {r.text[:600]}"
            )
        return r.json()["id"]

    def wait_for_offsets(
        self,
        supervisor_id: str,
        min_total_offset: int,
        timeout: int = 240,
    ) -> dict:
        """
        Poll supervisor status until cumulative offsets across all
        partitions reach ``min_total_offset``. Returns the last status.
        """
        url = f"{self.router_url}/druid/indexer/v1/supervisor/{supervisor_id}/status"
        deadline = time.time() + timeout
        last: dict = {}
        while time.time() < deadline:
            try:
                r = self.session.get(url, timeout=10)
                r.raise_for_status()
                last = r.json()
                offsets = (
                    (last.get("payload") or {}).get("latestOffsets")
                    or (last.get("payload") or {}).get("currentOffsets")
                    or {}
                )
                total = sum(int(v) for v in offsets.values())
                if total >= min_total_offset:
                    return last
            except Exception:
                pass
            time.sleep(3)
        raise TimeoutError(
            f"Supervisor {supervisor_id} did not reach total offset "
            f"{min_total_offset} within {timeout}s. Last={last}"
        )

    def terminate_supervisor(self, supervisor_id: str) -> None:
        url = f"{self.router_url}/druid/indexer/v1/supervisor/{supervisor_id}/terminate"
        try:
            self.session.post(url, timeout=10)
        except Exception:
            pass
