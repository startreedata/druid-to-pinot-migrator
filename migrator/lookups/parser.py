"""Parse a Druid lookup config dict into ``CanonicalLookup`` instances.

The Druid Coordinator returns a tier → lookup-name → spec nested map:

    {
      "__default": {
        "country": {
          "version": "v1",
          "lookupExtractorFactory": {
            "type": "cachedNamespace",
            "extractionNamespace": {
              "type": "staticMap",
              "map": {"us": "United States", "ca": "Canada"}
            },
            ...
          }
        },
        ...
      },
      "high-priority-tier": { ... }
    }

This parser flattens the nested structure and translates each
recognised lookup into a ``CanonicalLookup``. Unknown shapes
(JDBC, kafka, polling, etc.) raise ``UnsupportedLookupError`` with
a clear message — explicit refusal beats a silent partial migration.
"""

from __future__ import annotations

from typing import Any

from migrator.lookups.models import CanonicalLookup, StaticMapEntry


class LookupParseError(ValueError):
    """Raised when the lookup config is malformed."""


class UnsupportedLookupError(ValueError):
    """Raised for lookup shapes the migrator doesn't yet translate."""


# The Druid extension types the migrator currently understands.
# Anything else surfaces as UnsupportedLookupError with a pointer to
# this list — operators can either pre-flatten the lookup themselves
# or file an issue.
_SUPPORTED_EXTRACTION_TYPES: dict[str, str] = {
    "staticMap": "static_map",
    "uri":       "uri",
}


def parse_lookups_config(
    raw: dict[str, Any] | dict[str, dict[str, Any]],
    *,
    source_file: str | None = None,
) -> list[CanonicalLookup]:
    """Parse a Druid lookups-config dict.

    Accepts either:
      - the tier-keyed form returned by the Coordinator (``{tier:
        {name: spec}}``), or
      - the flat ``{name: spec}`` form an operator might pass via a
        local file. The flat form is autodetected.

    Returns one ``CanonicalLookup`` per recognised lookup. The order
    is stable: tiers in dict-iteration order, then names in
    dict-iteration order within each tier.
    """
    if not isinstance(raw, dict):
        raise LookupParseError(
            f"expected a dict at top level; got {type(raw).__name__}"
        )

    # Heuristic: if every value is itself a dict containing
    # 'lookupExtractorFactory' or 'version', treat the top level as
    # FLAT (name → spec). Otherwise it's the tier-keyed form.
    is_flat = bool(raw) and all(
        isinstance(v, dict)
        and ("lookupExtractorFactory" in v or "version" in v)
        for v in raw.values()
    )

    out: list[CanonicalLookup] = []
    if is_flat:
        for name, spec in raw.items():
            out.append(_parse_one(name, spec, source_file=source_file))
    else:
        for tier_name, tier_lookups in raw.items():
            if not isinstance(tier_lookups, dict):
                raise LookupParseError(
                    f"tier {tier_name!r}: expected dict of "
                    f"name → spec; got {type(tier_lookups).__name__}"
                )
            for name, spec in tier_lookups.items():
                out.append(_parse_one(name, spec, source_file=source_file))
    return out


def _parse_one(
    name: str, spec: dict, *, source_file: str | None,
) -> CanonicalLookup:
    if not isinstance(spec, dict):
        raise LookupParseError(
            f"lookup {name!r}: spec must be a dict, got "
            f"{type(spec).__name__}"
        )

    factory = spec.get("lookupExtractorFactory")
    if not isinstance(factory, dict):
        raise LookupParseError(
            f"lookup {name!r}: missing or non-object "
            f"'lookupExtractorFactory'"
        )

    factory_type = factory.get("type", "")
    if factory_type != "cachedNamespace":
        raise UnsupportedLookupError(
            f"lookup {name!r}: only 'cachedNamespace' factories are "
            f"supported (got {factory_type!r}). Polling lookups, "
            f"kafkaLookup, and inline `map` factories are out of "
            f"scope for v0.8.0."
        )

    namespace = factory.get("extractionNamespace")
    if not isinstance(namespace, dict):
        raise LookupParseError(
            f"lookup {name!r}: cachedNamespace requires an "
            f"'extractionNamespace' object"
        )

    ns_type = namespace.get("type", "")
    if ns_type not in _SUPPORTED_EXTRACTION_TYPES:
        supported = ", ".join(sorted(_SUPPORTED_EXTRACTION_TYPES))
        raise UnsupportedLookupError(
            f"lookup {name!r}: extractionNamespace type {ns_type!r} "
            f"is not supported. Supported: {supported}. JDBC/kafka "
            f"lookups need their source data to be exported to a "
            f"file or static map first."
        )

    if ns_type == "staticMap":
        return _parse_static_map(name, namespace, source_file=source_file)
    if ns_type == "uri":
        return _parse_uri(name, namespace, source_file=source_file)

    # Defensive — _SUPPORTED_EXTRACTION_TYPES guards above.
    raise UnsupportedLookupError(  # pragma: no cover
        f"lookup {name!r}: namespace type {ns_type!r} fell through "
        f"the dispatch — this is a bug in lookups.parser"
    )


def _parse_static_map(
    name: str, namespace: dict, *, source_file: str | None,
) -> CanonicalLookup:
    raw_map = namespace.get("map")
    if not isinstance(raw_map, dict):
        raise LookupParseError(
            f"lookup {name!r}: staticMap.map must be a dict, got "
            f"{type(raw_map).__name__}"
        )
    entries = [
        StaticMapEntry(key=str(k), value=str(v))
        for k, v in raw_map.items()
    ]
    return CanonicalLookup(
        name=name,
        source_kind="static_map",
        entries=entries,
        source_file=source_file,
    )


def _parse_uri(
    name: str, namespace: dict, *, source_file: str | None,
) -> CanonicalLookup:
    uri = namespace.get("uri")
    if not isinstance(uri, str) or not uri:
        raise LookupParseError(
            f"lookup {name!r}: uri-extraction namespace requires a "
            f"non-empty 'uri' string"
        )
    parse_spec = namespace.get("namespaceParseSpec", {})
    fmt = (parse_spec.get("format") if isinstance(parse_spec, dict)
           else "").lower()
    if fmt == "csv":
        source_kind = "uri_csv"
        cols = (parse_spec.get("columns") or []) if isinstance(parse_spec, dict) else []
        if len(cols) != 2:
            raise UnsupportedLookupError(
                f"lookup {name!r}: only 2-column CSV lookups are "
                f"supported (got {len(cols)} columns: {cols!r}). "
                f"Multi-column lookups would need an explicit "
                f"key/value column choice — file an issue with the "
                f"shape you have in mind."
            )
        key_column, value_column = cols[0], cols[1]
    elif fmt in ("simplejson", "json"):
        source_kind = "uri_json"
        key_column, value_column = "key", "value"
    else:
        raise UnsupportedLookupError(
            f"lookup {name!r}: namespaceParseSpec.format {fmt!r} is "
            f"not supported. Use 'csv' or 'simpleJson'."
        )
    return CanonicalLookup(
        name=name,
        source_kind=source_kind,
        uri=uri,
        key_column=key_column,
        value_column=value_column,
        source_file=source_file,
    )
