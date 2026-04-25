from __future__ import annotations

from migrator.core.models import TimeField

# Druid time format strings -> Pinot dateTimeFieldSpec format dicts
_FORMAT_MAP = {
    "iso": {
        "format": "SIMPLE_DATE_FORMAT",
        "pattern": "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "timeUnit": "MILLISECONDS",
        "timeZone": "UTC",
    },
    "millis": {
        "format": "EPOCH",
        "timeUnit": "MILLISECONDS",
    },
    "seconds": {
        "format": "EPOCH",
        "timeUnit": "SECONDS",
    },
    "posix": {
        "format": "EPOCH",
        "timeUnit": "SECONDS",
    },
    "micro": {
        "format": "EPOCH",
        "timeUnit": "MICROSECONDS",
    },
    "nano": {
        "format": "EPOCH",
        "timeUnit": "NANOSECONDS",
    },
    "auto": {
        "format": "EPOCH",
        "timeUnit": "MILLISECONDS",
    },
}


def time_field_to_pinot_spec(time_field: TimeField) -> dict:
    """Convert a TimeField into a Pinot dateTimeFieldSpec dict."""
    fmt = (time_field.format or "auto").lower()
    format_info = _FORMAT_MAP.get(fmt, _FORMAT_MAP["auto"])

    spec: dict = {"name": time_field.column_name, "dataType": "LONG"}
    if format_info.get("format") == "EPOCH":
        unit = format_info["timeUnit"]
        spec["format"] = f"1:{unit}:EPOCH"
        spec["granularity"] = f"1:{unit}"
    else:
        spec["format"] = f"1:MILLISECONDS:SIMPLE_DATE_FORMAT:{format_info['pattern']}"
        spec["granularity"] = "1:MILLISECONDS"
    return spec
