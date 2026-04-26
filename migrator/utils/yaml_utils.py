from __future__ import annotations

from typing import Any

import yaml


def dump_yaml(data: Any) -> str:
    """Return a YAML string representation of data."""
    return yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=True)
