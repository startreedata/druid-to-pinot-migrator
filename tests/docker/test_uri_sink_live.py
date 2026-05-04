"""
Live integration test for ``PinotIngestFromUriSink`` (v0.7.0).

The unit tests verify the sink emits the right URL/querystring; this
test verifies the **full round-trip**: dpm POSTs a control-plane-only
``/ingestFromURI`` call, Pinot reads the file from the URI, builds an
OFFLINE segment, and the rows become queryable.

## Compose-free design

The integration compose stack doesn't bind-mount the host filesystem
into the pinot-controller container, so a host-side ``file://`` URI
isn't resolvable across the boundary. Rather than tweak the compose
(volumes have to be declared at boot time and a hand-managed shared
mount adds setup), the test ``docker cp``s the NDJSON file into a
known path INSIDE the controller container, then references it via
``file:///pinot-staging/<basename>`` from the sink's ``uri_prefix``.

This exercises the same code path as a real shared-volume deployment
— from the sink's perspective, the URI is a plain ``file://``
reference; from Pinot's perspective, the file is at a regular
filesystem path it can read. Only the *how the file got there* step
is shimmed.

Skipped unless ``LIVE_DOCKER_TESTS=1``.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import requests

from migrator.realtime.backfill_runner import PinotIngestFromUriSink
from tests.docker.cluster_clients import PINOT_CONTROLLER_URL


PINOT_CONTAINER = "migtest-pinot-controller"
CONTAINER_STAGING_DIR = "/pinot-staging"
URI_PREFIX = f"file://{CONTAINER_STAGING_DIR}"


def _docker_exec(cmd: list[str]) -> None:
    """Run a command inside the pinot-controller container."""
    subprocess.run(
        ["docker", "exec", PINOT_CONTAINER] + cmd,
        check=True, capture_output=True, text=True,
    )


def _docker_cp_into_pinot(host_path: Path, container_path: str) -> None:
    """Copy a host-side file into the pinot-controller container."""
    subprocess.run(
        ["docker", "cp", str(host_path), f"{PINOT_CONTAINER}:{container_path}"],
        check=True, capture_output=True, text=True,
    )


@pytest.fixture(scope="module")
def shared_staging_dir(docker_stack):
    """Create the shared staging directory inside pinot-controller.

    Module-scoped: the directory only needs to exist once per session.
    Best-effort cleanup; the container's tmpfs goes away with the
    compose stack anyway.
    """
    _docker_exec(["mkdir", "-p", CONTAINER_STAGING_DIR])
    yield CONTAINER_STAGING_DIR
    # Best-effort cleanup; ignore errors (container may already be down).
    subprocess.run(
        ["docker", "exec", PINOT_CONTAINER, "rm", "-rf", CONTAINER_STAGING_DIR],
        check=False, capture_output=True,
    )


@pytest.fixture
def uri_sink_table(pinot_table_factory):
    """Create a Pinot OFFLINE table for the URI sink test."""
    ds = "uri_sink_live"
    schema = {
        "schemaName": ds,
        "dateTimeFieldSpecs": [
            {"dataType": "LONG", "format": "1:MILLISECONDS:EPOCH",
             "granularity": "1:MILLISECONDS", "name": "timestamp"},
        ],
        "dimensionFieldSpecs": [
            {"dataType": "STRING", "name": "k"},
        ],
        "metricFieldSpecs": [
            {"dataType": "LONG", "name": "v"},
        ],
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


class TestIngestFromUriSinkLive:
    def test_uri_sink_round_trip(
        self, shared_staging_dir, uri_sink_table, pinot, tmp_path,
    ):
        """End-to-end: write NDJSON locally, copy into Pinot's container,
        sink POSTs a file:// URI, Pinot reads + builds segment, rows
        become queryable.

        Asserts the sink's central contract: the data does NOT travel
        through dpm's HTTP request body. The sink only POSTs the URL.
        """
        ds = uri_sink_table

        # 1. Generate a small NDJSON file locally.
        records = [
            {"timestamp": 1_704_067_200_000 + i * 60_000,
             "k": f"k_{i % 5}", "v": i}
            for i in range(50)
        ]
        local_path = tmp_path / "page-000000.json"
        with local_path.open("w") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")

        # 2. Drop it inside pinot-controller at the known path.
        _docker_cp_into_pinot(
            local_path, f"{shared_staging_dir}/{local_path.name}",
        )

        # 3. Run the sink. The sink emits
        #    file:///pinot-staging/page-000000.json — which exists
        #    inside the controller because of step 2. dpm's request
        #    body is empty (the sink contract); only the URL travels.
        sink = PinotIngestFromUriSink(
            PINOT_CONTROLLER_URL, uri_prefix=URI_PREFIX,
        )
        sink.ingest_file(local_path, ds)

        # 4. Wait for Pinot to actually query the rows. ingestFromURI
        #    is async (the controller queues a segment build); the
        #    OFFLINE table only becomes queryable after the build
        #    completes (~30-300s on this stack).
        pinot.wait_for_table_queryable(f"{ds}_OFFLINE", timeout=360)

        # 5. Verify row count matches what we ingested.
        rows = pinot.sql_query(f"SELECT COUNT(*) AS c FROM {ds}")
        assert rows[0]["c"] == len(records)

    def test_uri_sink_does_not_send_file_body(
        self, shared_staging_dir, uri_sink_table, tmp_path,
    ):
        """Defensive contract test: when the URI sink is wired to a
        live Pinot, the data flow must be controller-pulls-from-URI,
        not dpm-uploads-body. We assert this by recording every
        request the session makes and checking the size of the request
        body — for a control-plane-only POST it must be empty (or just
        the empty string), regardless of the underlying file size.
        """
        # Fresh staging file dedicated to this test.
        ds = uri_sink_table
        local_path = tmp_path / "page-000099.json"
        # 1MB of NDJSON — would be a ~1MB body if uploaded.
        with local_path.open("w") as fh:
            for i in range(20_000):
                fh.write(json.dumps({
                    "timestamp": 1_704_067_200_000 + i,
                    "k": "x", "v": i,
                }) + "\n")
        assert local_path.stat().st_size > 100_000, "fixture too small"

        _docker_cp_into_pinot(
            local_path, f"{shared_staging_dir}/{local_path.name}",
        )

        # Wrap the requests session so we can spy on outgoing payloads.
        sent_payloads: list[int] = []  # body byte-lengths

        class SpySession:
            def __init__(self):
                self._inner = requests.Session()
                self._inner.headers.update({"Content-Type": "application/json"})
                self.headers = self._inner.headers

            def post(self, url, *, timeout=None, **kwargs):
                # Sink's contract: no `data=`, no `files=`. Record any
                # body payload we see so the assertion can fail loudly.
                if "data" in kwargs and kwargs["data"]:
                    sent_payloads.append(len(kwargs["data"]))
                if "files" in kwargs and kwargs["files"]:
                    sent_payloads.append(-1)  # multipart marker
                return self._inner.post(url, timeout=timeout, **kwargs)

        sink = PinotIngestFromUriSink(
            PINOT_CONTROLLER_URL,
            uri_prefix=URI_PREFIX,
            session=SpySession(),
        )
        sink.ingest_file(local_path, ds)

        # Critical: no body payloads observed. The 1MB file did not
        # travel through dpm's HTTP request.
        assert sent_payloads == [], (
            f"unexpected request bodies (the URI sink should be "
            f"control-plane-only): {sent_payloads}"
        )
