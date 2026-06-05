"""Unit tests for cloud-storage pinotFSSpecs auto-configuration.

Covers the v0.14-dev work that promotes the batch-job pinotFSSpec from
scheme+className-only to a full entry with a derived/placeholder
``configs`` block, and adds Azure (adl2 / ADLSGen2PinotFS) support.

Principle under test: structural keys (region / projectId / account /
filesystem) are emitted; secrets (access keys, gcpKey, SAS) are NOT —
they come from the Pinot server's ambient credentials.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from migrator.druid.normalizer import DruidNormalizer
from migrator.druid.parser import DruidSpecParser
from migrator.pinot.ingestion_generator import (
    _PINOT_FS,
    _REPLACE_AZURE_ACCOUNT,
    _REPLACE_AZURE_FS,
    _REPLACE_GCP_PROJECT,
    _REPLACE_REGION,
    PinotIngestionGenerator,
    _azure_filesystem_from_uri,
    _pinot_fs_configs,
    _pinot_fs_spec,
    _rewrite_azure_uri,
)


FIXTURES = Path(__file__).parent.parent / "fixtures"


def _canonical(raw: dict):
    parsed = DruidSpecParser().parse(raw)
    return DruidNormalizer().normalize(parsed.parsed_spec).canonical


def _normalize(raw: dict):
    parsed = DruidSpecParser().parse(raw)
    return DruidNormalizer().normalize(parsed.parsed_spec)


def _spec_with_input_source(input_source: dict) -> dict:
    """Build a minimal batch index_parallel spec with the given
    inputSource, reusing the gcs_input fixture's shape."""
    raw = json.loads((FIXTURES / "gcs_input" / "spec.json").read_text())
    (raw.get("spec") or raw)["ioConfig"]["inputSource"] = input_source
    return raw


# ─────────────────────────────────────────────────────────────────────────────
# FS registry: Azure added
# ─────────────────────────────────────────────────────────────────────────────


class TestFsRegistry:
    def test_azure_scheme_maps_to_adls_gen2(self):
        scheme, cls = _PINOT_FS["azure"]
        assert scheme == "adl2"
        assert cls == "org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS"

    def test_adl2_scheme_present(self):
        assert _PINOT_FS["adl2"][0] == "adl2"
        assert "ADLSGen2PinotFS" in _PINOT_FS["adl2"][1]

    def test_abfss_scheme_present(self):
        assert _PINOT_FS["abfss"][0] == "adl2"

    def test_existing_schemes_unchanged(self):
        # Backward compat: the four pre-existing schemes keep their
        # exact class names.
        assert _PINOT_FS["file"][1].endswith("LocalPinotFS")
        assert _PINOT_FS["s3"][1].endswith("S3PinotFS")
        assert _PINOT_FS["gs"][1].endswith("GcsPinotFS")
        assert _PINOT_FS["hdfs"][1].endswith("HadoopPinotFS")


# ─────────────────────────────────────────────────────────────────────────────
# _pinot_fs_configs — per-scheme config blocks
# ─────────────────────────────────────────────────────────────────────────────


class TestPinotFsConfigs:
    def test_local_and_hdfs_have_no_configs(self):
        assert _pinot_fs_configs("file", {}) == {}
        assert _pinot_fs_configs("hdfs", {}) == {}

    def test_s3_emits_region_placeholder_when_absent(self):
        assert _pinot_fs_configs("s3", {}) == {"region": _REPLACE_REGION}

    def test_s3_region_derived_from_top_level(self):
        cfg = _pinot_fs_configs("s3", {"region": "ap-southeast-1"})
        assert cfg == {"region": "ap-southeast-1"}

    def test_s3_region_derived_from_properties(self):
        cfg = _pinot_fs_configs("s3", {"properties": {"region": "eu-west-2"}})
        assert cfg["region"] == "eu-west-2"

    def test_s3_region_derived_from_endpoint_config(self):
        cfg = _pinot_fs_configs("s3", {"endpointConfig": {"region": "us-west-1"}})
        assert cfg["region"] == "us-west-1"

    def test_s3_omits_credentials(self):
        # Even when the Druid spec carries access keys, they must NOT
        # leak into the generated artifact.
        cfg = _pinot_fs_configs("s3", {
            "region": "us-east-1",
            "properties": {"accessKeyId": "AKIA...", "secretAccessKey": "shh"},
        })
        assert cfg == {"region": "us-east-1"}
        assert "accessKeyId" not in cfg
        assert "secretAccessKey" not in cfg

    def test_gcs_emits_project_placeholder_when_absent(self):
        assert _pinot_fs_configs("gs", {}) == {"projectId": _REPLACE_GCP_PROJECT}

    def test_gcs_project_derived_when_present(self):
        cfg = _pinot_fs_configs("gs", {"projectId": "my-proj"})
        assert cfg == {"projectId": "my-proj"}

    def test_gcs_omits_gcp_key(self):
        cfg = _pinot_fs_configs("gs", {"projectId": "p", "gcpKey": "/secret.json"})
        assert "gcpKey" not in cfg

    def test_adl2_placeholders_when_absent(self):
        cfg = _pinot_fs_configs("adl2", {})
        assert cfg == {
            "accountName": _REPLACE_AZURE_ACCOUNT,
            "fileSystemName": _REPLACE_AZURE_FS,
        }

    def test_adl2_account_derived(self):
        cfg = _pinot_fs_configs("adl2", {"account": "mystore"})
        assert cfg["accountName"] == "mystore"

    def test_adl2_filesystem_from_uri(self):
        cfg = _pinot_fs_configs("adl2", {
            "uris": ["azure://events-fs/path/data.json"],
        })
        assert cfg["fileSystemName"] == "events-fs"

    def test_adl2_omits_access_key(self):
        cfg = _pinot_fs_configs("adl2", {
            "account": "s", "accessKey": "shh", "fileSystemName": "fs",
        })
        assert "accessKey" not in cfg


# ─────────────────────────────────────────────────────────────────────────────
# Azure URI helpers
# ─────────────────────────────────────────────────────────────────────────────


class TestAzureUriHelpers:
    def test_filesystem_from_azure_uri(self):
        assert _azure_filesystem_from_uri("azure://mycontainer/a/b") == "mycontainer"

    def test_filesystem_from_abfss_with_account_suffix(self):
        fs = _azure_filesystem_from_uri(
            "abfss://fs@acct.dfs.core.windows.net/path"
        )
        assert fs == "fs"

    def test_filesystem_none_for_non_azure(self):
        assert _azure_filesystem_from_uri("s3://bucket/key") is None
        assert _azure_filesystem_from_uri("/local/path") is None

    def test_rewrite_azure_scheme(self):
        assert _rewrite_azure_uri("azure://c/p") == "adl2://c/p"

    def test_rewrite_leaves_adl2_untouched(self):
        assert _rewrite_azure_uri("adl2://c/p") == "adl2://c/p"

    def test_rewrite_leaves_non_azure_untouched(self):
        assert _rewrite_azure_uri("gs://b/k") == "gs://b/k"


# ─────────────────────────────────────────────────────────────────────────────
# _pinot_fs_spec — scheme dispatch + configs attachment
# ─────────────────────────────────────────────────────────────────────────────


class TestPinotFsSpec:
    def test_azure_uri_dispatches_to_adls(self):
        spec = _pinot_fs_spec("adl2://fs/path")
        assert spec["scheme"] == "adl2"
        assert "ADLSGen2PinotFS" in spec["className"]

    def test_raw_azure_uri_dispatches_to_adls(self):
        spec = _pinot_fs_spec("azure://fs/path")
        assert spec["scheme"] == "adl2"

    def test_s3_spec_has_configs_block(self):
        spec = _pinot_fs_spec("s3://bucket/key")
        assert "configs" in spec
        assert "region" in spec["configs"]

    def test_local_spec_has_no_configs_key(self):
        # Backward compat: local artifacts stay byte-identical.
        spec = _pinot_fs_spec("/data/local")
        assert "configs" not in spec

    def test_hdfs_spec_has_no_configs_key(self):
        spec = _pinot_fs_spec("hdfs://nn/path")
        assert "configs" not in spec

    def test_configs_derived_from_input_source(self):
        spec = _pinot_fs_spec(
            "s3://bucket/key", {"region": "ca-central-1"},
        )
        assert spec["configs"]["region"] == "ca-central-1"


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end via fixtures
# ─────────────────────────────────────────────────────────────────────────────


class TestEndToEnd:
    def test_gcs_fixture_batch_job_has_project_config(self):
        raw = json.loads((FIXTURES / "gcs_input" / "spec.json").read_text())
        job = PinotIngestionGenerator().generate_batch_job(_canonical(raw))
        fs = job["pinotFSSpecs"][0]
        assert fs["scheme"] == "gs"
        assert fs["configs"]["projectId"] == _REPLACE_GCP_PROJECT

    def test_orc_fixture_batch_job_has_project_config(self):
        raw = json.loads((FIXTURES / "orc_input" / "spec.json").read_text())
        job = PinotIngestionGenerator().generate_batch_job(_canonical(raw))
        fs = job["pinotFSSpecs"][0]
        assert fs["scheme"] == "gs"
        assert "projectId" in fs["configs"]

    def test_azure_spec_rewrites_uri_and_derives_filesystem(self):
        raw = _spec_with_input_source({
            "type": "azure",
            "uris": ["azure://events-container/dt=2024-01-01/data.json"],
        })
        job = PinotIngestionGenerator().generate_batch_job(_canonical(raw))
        assert job["inputDirURI"].startswith("adl2://events-container/")
        fs = job["pinotFSSpecs"][0]
        assert fs["scheme"] == "adl2"
        assert "ADLSGen2PinotFS" in fs["className"]
        assert fs["configs"]["fileSystemName"] == "events-container"
        assert fs["configs"]["accountName"] == _REPLACE_AZURE_ACCOUNT

    def test_local_fixture_batch_job_unchanged(self):
        raw = json.loads((FIXTURES / "raw_batch" / "spec.json").read_text())
        job = PinotIngestionGenerator().generate_batch_job(_canonical(raw))
        fs = job["pinotFSSpecs"][0]
        assert fs["scheme"] == "file"
        assert "configs" not in fs


# ─────────────────────────────────────────────────────────────────────────────
# Normalizer warnings
# ─────────────────────────────────────────────────────────────────────────────


class TestNormalizerWarnings:
    def test_gcs_warning_names_project_id(self):
        raw = _spec_with_input_source({
            "type": "google", "uris": ["gs://b/k"],
        })
        result = _normalize(raw)
        assert any(
            "projectId" in w and "GcsPinotFS" in w for w in result.warnings
        ), result.warnings

    def test_azure_warning_names_adls_and_account(self):
        raw = _spec_with_input_source({
            "type": "azure", "uris": ["azure://c/p"],
        })
        result = _normalize(raw)
        assert any(
            "ADLSGen2PinotFS" in w and "accountName" in w
            for w in result.warnings
        ), result.warnings

    def test_http_warning_says_no_pinotfs_plugin(self):
        raw = _spec_with_input_source({
            "type": "http", "uris": ["https://api.example.com/data.json"],
        })
        result = _normalize(raw)
        assert any(
            "no HTTP PinotFS plugin" in w for w in result.warnings
        ), result.warnings
