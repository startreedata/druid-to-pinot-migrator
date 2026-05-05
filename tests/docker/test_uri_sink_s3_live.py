"""
Live integration test for ``PinotIngestFromUriSink`` against ``s3://``.

Companion to ``test_uri_sink_live.py``: that one covers the ``file://``
path (where the controller reads from its own filesystem); this one
covers the s3 path (where the controller fetches from a separate
S3-compatible blob store) — the distinction matters because real
deployments rarely have a shared filesystem and rely on object storage.

Stack
─────
The docker-compose adds a ``minio`` service (S3-compatible) and a
one-shot ``minio-init`` job that creates the ``dpm-test`` bucket.
``pinot-controller.conf`` registers the ``S3PinotFS`` factory pointed
at the in-network MinIO endpoint (path-style, ACLs disabled).

What this test covers that the file:// test does not
─────────────────────────────────────────────────────
- The controller's S3 plugin actually loads in this stack.
- The MinIO endpoint override (``pinot.controller.storage.factory.s3.endpoint``)
  reaches the SDK so the S3 client doesn't try real AWS.
- ``PinotIngestFromUriSink`` emits an ``s3://bucket/key`` URI when given
  the ``s3://...`` prefix, and the resulting POST round-trips end-to-end.
- The dpm sink itself stays control-plane-only — boto3 (the test
  uploader) writes the bytes to MinIO, dpm only POSTs the URL.

Skipped unless ``LIVE_DOCKER_TESTS=1``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# boto3 is only needed by this test; gate the import so the rest of the
# tests/docker package collects cleanly when the dev install hasn't been
# refreshed.
boto3 = pytest.importorskip("boto3")

from migrator.realtime.backfill_runner import PinotIngestFromUriSink
from tests.docker.cluster_clients import PINOT_CONTROLLER_URL


# Host-side endpoint (compose maps MinIO 9000 → host 19090).
MINIO_HOST_URL = "http://localhost:19090"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "dpm-test"

# In-network endpoint that the Pinot controller uses (declared in
# pinot-controller.conf). Different from the host URL because the
# controller resolves "minio" via the docker network, not the host port
# mapping.
URI_PREFIX = f"s3://{BUCKET}/uri-sink-s3"


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def s3_client(docker_stack):
    """A boto3 S3 client pointed at the host-mapped MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_HOST_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


@pytest.fixture
def s3_uri_sink_table(pinot_table_factory):
    """Create a Pinot OFFLINE table for the s3 URI-sink test."""
    ds = "uri_sink_s3_live"
    schema = {
        "schemaName": ds,
        "dateTimeFieldSpecs": [
            {"dataType": "LONG", "format": "1:MILLISECONDS:EPOCH",
             "granularity": "1:MILLISECONDS", "name": "timestamp"},
        ],
        "dimensionFieldSpecs": [{"dataType": "STRING", "name": "k"}],
        "metricFieldSpecs": [{"dataType": "LONG", "name": "v"}],
    }
    table_offline = {
        "tableName": f"{ds}_OFFLINE",
        "tableType": "OFFLINE",
        "segmentsConfig": {
            "timeColumnName": "timestamp",
            "timeType": "MILLISECONDS",
            "replication": "1",
            "retentionTimeUnit": "DAYS",
            "retentionTimeValue": "365",
        },
        "tenants": {"broker": "DefaultTenant", "server": "DefaultTenant"},
        "tableIndexConfig": {"loadMode": "MMAP"},
        "metadata": {"customConfigs": {}},
    }
    pinot_table_factory(schema=schema, table_config=table_offline)
    return ds


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestS3UriSinkLive:
    def test_s3_uri_sink_round_trip(
        self, s3_client, s3_uri_sink_table, pinot, tmp_path,
    ):
        """End-to-end: write NDJSON to MinIO, sink emits s3:// URI,
        Pinot controller fetches via S3PinotFS, segment builds, rows
        become queryable.

        The dpm sink stays control-plane-only — boto3 uploads the
        bytes; ``ingest_file`` only POSTs the URL. Same contract as
        the file:// test.
        """
        ds = s3_uri_sink_table

        # 1. Generate a small NDJSON file locally.
        records = [
            {"timestamp": 1_704_067_200_000 + i * 60_000,
             "k": f"k_{i % 5}", "v": i}
            for i in range(40)
        ]
        local_path = tmp_path / "page-000000.json"
        with local_path.open("w") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")

        # 2. Upload to MinIO under the prefix the sink will reference.
        key = f"uri-sink-s3/{local_path.name}"
        s3_client.upload_file(str(local_path), BUCKET, key)

        # 3. Confirm the object made it (catches MinIO-config goofs
        # before chasing them through Pinot's logs).
        head = s3_client.head_object(Bucket=BUCKET, Key=key)
        assert head["ContentLength"] == local_path.stat().st_size

        # 4. Run the sink with the s3 prefix. dpm composes
        # s3://dpm-test/uri-sink-s3/page-000000.json and POSTs that URL
        # to /ingestFromURI; the body is empty.
        sink = PinotIngestFromUriSink(
            PINOT_CONTROLLER_URL, uri_prefix=URI_PREFIX,
        )
        sink.ingest_file(local_path, ds)

        # 5. Wait for Pinot to actually query the rows. The S3 read +
        # segment build is async; same async behaviour as file://, so
        # reuse the existing wait helper. 360s is generous — the
        # bottleneck is the controller cold-starting its S3 client on
        # the first call.
        pinot.wait_for_table_queryable(f"{ds}_OFFLINE", timeout=360)

        rows = pinot.sql_query(f"SELECT COUNT(*) AS c FROM {ds}")
        assert rows[0]["c"] == len(records)

    def test_s3_sink_does_not_send_file_body(
        self, s3_client, s3_uri_sink_table, tmp_path,
    ):
        """Defensive: the s3 sink contract is identical to file:// —
        dpm POSTs URL+query only, no body. Spy on the session to
        confirm. Independent from the round-trip test (different
        object key, smaller payload) so a regression in either
        contract surfaces clearly.
        """
        import requests

        ds = s3_uri_sink_table

        local_path = tmp_path / "page-no-body.json"
        with local_path.open("w") as fh:
            for i in range(10):
                fh.write(json.dumps({
                    "timestamp": 1_704_067_200_000 + i,
                    "k": "x", "v": i,
                }) + "\n")

        key = f"uri-sink-s3/{local_path.name}"
        s3_client.upload_file(str(local_path), BUCKET, key)

        sent_payloads: list[int] = []

        class SpySession:
            def __init__(self):
                self._inner = requests.Session()
                self.headers = self._inner.headers

            def post(self, url, *, timeout=None, **kwargs):
                if "data" in kwargs and kwargs["data"]:
                    sent_payloads.append(len(kwargs["data"]))
                if "files" in kwargs and kwargs["files"]:
                    sent_payloads.append(-1)
                return self._inner.post(url, timeout=timeout, **kwargs)

        sink = PinotIngestFromUriSink(
            PINOT_CONTROLLER_URL,
            uri_prefix=URI_PREFIX,
            session=SpySession(),
        )
        sink.ingest_file(local_path, ds)

        assert sent_payloads == [], (
            f"unexpected request bodies (the s3 URI sink should be "
            f"control-plane-only): {sent_payloads}"
        )
