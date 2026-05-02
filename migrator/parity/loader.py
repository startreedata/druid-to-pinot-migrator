"""Load a queries-file into ``ParityQueryFile`` (YAML or JSON, by suffix)."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from migrator.parity.models import ParityQueryFile


def load_queries(path: Path) -> ParityQueryFile:
    text = Path(path).read_text()
    if str(path).lower().endswith((".yml", ".yaml")):
        raw = yaml.safe_load(text)
    else:
        # Default to JSON. Files without an extension or with .json get
        # this branch.
        raw = json.loads(text)
    if raw is None:
        raise ValueError(f"queries file {path} is empty")
    return ParityQueryFile.model_validate(raw)
