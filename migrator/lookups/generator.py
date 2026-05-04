"""Generate Pinot dim-table artifacts from a ``CanonicalLookup``.

A Pinot dim-table is just an OFFLINE table config with two extra
properties:

  - ``isDimTable: true`` in the table-level config — tells Pinot to
    replicate every segment to every server, so JOINs don't shuffle.
  - ``"dimTableConfig": {"disablePreload": false}`` in the broader
    config — preload at startup for fast lookups.

Schema-wise: a key dimension column + a value dimension column +
the standard dpm-style timestamp column (Pinot requires every table
to have one — for dim tables it's just a placeholder).

This module does NOT emit query-rewrite suggestions (``LOOKUP(dim,
"foo")`` → ``JOIN dim_foo USING(key)``). That's intentionally out of
scope: it's a per-query concern that depends on the query's shape,
and forcing operators to inspect their queries by hand is the
correct contract here. The runbook the CLI emits points at the
relevant Pinot docs.
"""

from __future__ import annotations

from migrator.lookups.models import CanonicalLookup, LookupArtifacts


# Pinot wants a time column on every table. For a static dim-table
# the time column is meaningless — a constant millisecond timestamp
# is fine.
_PLACEHOLDER_TIMESTAMP_MS = 0
_PLACEHOLDER_TIMESTAMP_COL = "ingest_timestamp"


def generate_lookup_artifacts(
    lookup: CanonicalLookup,
    *,
    table_name_prefix: str = "lookup_",
) -> LookupArtifacts:
    """Build the Pinot schema + table config for a single lookup.

    The Pinot table is named ``<prefix><lookup.name>``. Default
    prefix ``"lookup_"`` keeps the namespace clear; pass an empty
    string to use the bare lookup name (callers who already prefix
    upstream).
    """
    table_name = f"{table_name_prefix}{lookup.name}"

    # Schema: key + value + placeholder timestamp.
    schema = {
        "schemaName": table_name,
        "dateTimeFieldSpecs": [
            {
                "name": _PLACEHOLDER_TIMESTAMP_COL,
                "dataType": "LONG",
                "format": "1:MILLISECONDS:EPOCH",
                "granularity": "1:MILLISECONDS",
            },
        ],
        "dimensionFieldSpecs": [
            {"name": lookup.key_column,   "dataType": "STRING"},
            {"name": lookup.value_column, "dataType": "STRING"},
        ],
        "metricFieldSpecs": [],
        # Pinot dim-table optimisation: setting the primary key here
        # lets the LOOKUP() UDF use a hash index instead of a scan.
        "primaryKeyColumns": [lookup.key_column],
    }

    # Table: OFFLINE + isDimTable.
    table = {
        "tableName": f"{table_name}_OFFLINE",
        "tableType": "OFFLINE",
        "segmentsConfig": {
            "timeColumnName": _PLACEHOLDER_TIMESTAMP_COL,
            "timeType": "MILLISECONDS",
            "replication": "1",
            # Dim tables are immutable in practice; long retention.
            "retentionTimeUnit": "DAYS",
            "retentionTimeValue": "36500",
        },
        "tenants": {
            "broker": "DefaultTenant",
            "server": "DefaultTenant",
        },
        "tableIndexConfig": {
            "loadMode": "MMAP",
        },
        "isDimTable": True,
        "dimTableConfig": {
            "disablePreload": False,
        },
        "metadata": {
            "customConfigs": {
                "druid_source_lookup_name": lookup.name,
                "druid_source_kind": lookup.source_kind,
            },
        },
    }

    notes: list[str] = []
    inline_data: list[dict] | None = None

    if lookup.source_kind == "static_map":
        # Materialise the inline map as a list of NDJSON-shaped rows.
        # Operators ingest with the standard dpm batch path or the
        # /ingestFromFile endpoint.
        inline_data = [
            {
                lookup.key_column:           e.key,
                lookup.value_column:         e.value,
                _PLACEHOLDER_TIMESTAMP_COL:  _PLACEHOLDER_TIMESTAMP_MS,
            }
            for e in lookup.entries
        ]
        notes.append(
            f"static_map: {len(lookup.entries)} entries materialised "
            f"as inline NDJSON. Ingest via dpm deploy + /ingestFromFile "
            f"once the schema/table exist."
        )
    elif lookup.source_kind == "uri_csv":
        notes.append(
            f"uri_csv: source URI {lookup.uri!r}. Convert to NDJSON "
            f"or rely on Pinot's CSV ingestion plugin; the dpm "
            f"batch path expects NDJSON by default."
        )
    elif lookup.source_kind == "uri_json":
        notes.append(
            f"uri_json: source URI {lookup.uri!r}. The Druid "
            f"simpleJson format is `{{\"key\": \"value\"}}` per line; "
            f"convert to NDJSON `{{\"key\": ..., \"value\": ...}}` "
            f"before Pinot ingestion."
        )

    notes.append(
        f"Pinot query rewrite: every Druid `LOOKUP({lookup.key_column}, "
        f"\"{lookup.name}\")` call should become a Pinot LOOKUP() UDF "
        f"or a JOIN against {table_name}. See Pinot's dim-table docs."
    )

    return LookupArtifacts(
        table_name=table_name,
        schema=schema,
        table=table,
        inline_data=inline_data,
        notes=notes,
    )
