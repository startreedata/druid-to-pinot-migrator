from __future__ import annotations

import re


def sanitize_name(name: str) -> str:
    """Replace invalid characters with underscores and lowercase the result."""
    # Replace anything that is not alphanumeric or underscore with '_'
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    return sanitized.lower()


def resolve_field_collision(names: list[str]) -> list[str]:
    """Deduplicate field names by appending a numeric suffix to duplicates."""
    seen: dict[str, int] = {}
    result: list[str] = []
    for name in names:
        if name not in seen:
            seen[name] = 0
            result.append(name)
        else:
            seen[name] += 1
            new_name = f"{name}_{seen[name]}"
            result.append(new_name)
    return result
