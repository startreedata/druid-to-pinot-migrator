from __future__ import annotations

import json
from pathlib import Path

import yaml


def read_json_or_yaml(path: str | Path) -> dict:
    """Read either a JSON or YAML file and return its contents as a dict."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix in (".yaml", ".yml"):
        result = yaml.safe_load(text)
    else:
        # Default to JSON; also handles .json files
        try:
            result = json.loads(text)
        except json.JSONDecodeError:
            # Fallback to YAML for ambiguous files
            result = yaml.safe_load(text)
    if not isinstance(result, dict):
        raise ValueError(f"Expected a dict at root level, got {type(result).__name__}")
    return result


def write_json(path: str | Path, data: dict, indent: int = 2) -> None:
    """Write deterministic JSON (sorted keys) to a file."""
    path = Path(path)
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, sort_keys=True, indent=indent, ensure_ascii=False), encoding="utf-8")


def ensure_dir(path: str | Path) -> None:
    """Create directory and all parents if they do not exist."""
    Path(path).mkdir(parents=True, exist_ok=True)
