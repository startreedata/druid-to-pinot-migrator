"""Unit tests for migrator.utils.io — focused on the format-detection paths."""

from __future__ import annotations

from pathlib import Path

import pytest

from migrator.utils.io import read_json_or_yaml, write_json


class TestReadJsonOrYaml:
    def test_reads_yaml_extension(self, tmp_path: Path):
        p = tmp_path / "x.yaml"
        p.write_text("a: 1\nb: 2\n")
        assert read_json_or_yaml(p) == {"a": 1, "b": 2}

    def test_reads_yml_extension(self, tmp_path: Path):
        p = tmp_path / "x.yml"
        p.write_text("a: 1\n")
        assert read_json_or_yaml(p) == {"a": 1}

    def test_reads_json_default(self, tmp_path: Path):
        p = tmp_path / "x.json"
        p.write_text('{"a": 1}')
        assert read_json_or_yaml(p) == {"a": 1}

    def test_falls_back_to_yaml_when_extension_lies(self, tmp_path: Path):
        # ".json" extension but contents are YAML — the fallback path
        # is the resilience knob that lets dpm tolerate operator typos.
        p = tmp_path / "looks-like.json"
        p.write_text("a: 1\nb: 2\n")
        assert read_json_or_yaml(p) == {"a": 1, "b": 2}

    def test_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            read_json_or_yaml(tmp_path / "nope.json")

    def test_non_dict_root_raises(self, tmp_path: Path):
        # A bare list at the top level is rejected — the migrator only
        # ever consumes spec dicts, and silently accepting [...] would
        # mask programmer errors downstream.
        p = tmp_path / "list.json"
        p.write_text("[1, 2, 3]")
        with pytest.raises(ValueError, match="dict"):
            read_json_or_yaml(p)


class TestWriteJson:
    def test_creates_parent_dirs(self, tmp_path: Path):
        out = tmp_path / "deep" / "nested" / "f.json"
        write_json(out, {"a": 1})
        assert out.exists()

    def test_sorts_keys_for_determinism(self, tmp_path: Path):
        out = tmp_path / "f.json"
        write_json(out, {"z": 1, "a": 1})
        # Sorted-keys means 'a' precedes 'z' textually.
        text = out.read_text()
        assert text.index('"a"') < text.index('"z"')
