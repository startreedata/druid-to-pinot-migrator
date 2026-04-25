"""JSON serialisation for offset maps."""

from __future__ import annotations

import json
from pathlib import Path

from migrator.realtime.models import KafkaOffsetMap


def save_offset_map(offset_map: KafkaOffsetMap, path: str | Path) -> Path:
    """Write an offset-map snapshot to disk as pretty-printed JSON."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(offset_map.model_dump(mode="json"), indent=2) + "\n")
    return p


def load_offset_map(path: str | Path) -> KafkaOffsetMap:
    """Load an offset-map snapshot from disk."""
    return KafkaOffsetMap.model_validate_json(Path(path).read_text())
