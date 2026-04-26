from __future__ import annotations

import json
from typing import Any


def to_ordered_jsonable(obj: Any) -> Any:
    """Recursively convert Pydantic models/dicts/lists to JSON-serializable ordered dicts."""
    # Handle Pydantic v2 models
    if hasattr(obj, "model_dump"):
        return to_ordered_jsonable(obj.model_dump())
    # Handle Pydantic v1 models
    if hasattr(obj, "dict") and callable(obj.dict):
        try:
            return to_ordered_jsonable(obj.dict())
        except Exception:
            pass
    if isinstance(obj, dict):
        return {k: to_ordered_jsonable(v) for k, v in sorted(obj.items())}
    if isinstance(obj, (list, tuple)):
        return [to_ordered_jsonable(item) for item in obj]
    # Primitive types
    return obj


def dump_json(data: Any, indent: int = 2) -> str:
    """Return a deterministic JSON string with sorted keys."""
    return json.dumps(data, sort_keys=True, indent=indent, ensure_ascii=False)
