from __future__ import annotations

from migrator.core.models import CanonicalMigrationModel
from migrator.pinot.heuristics import time_field_to_pinot_spec


class PinotSchemaGenerator:
    """Generate a Pinot schema dict from a CanonicalMigrationModel."""

    def generate(self, canonical: CanonicalMigrationModel) -> dict:
        warnings: list[str] = []

        # ------------------------------------------------------------------ #
        # dateTimeFieldSpecs — time column first
        # ------------------------------------------------------------------ #
        date_time_field_specs: list[dict] = []
        if canonical.time_field is not None:
            date_time_field_specs.append(time_field_to_pinot_spec(canonical.time_field))

        # ------------------------------------------------------------------ #
        # dimensionFieldSpecs — sorted alphabetically by name
        # ------------------------------------------------------------------ #
        dimension_field_specs: list[dict] = []
        for dim in sorted(canonical.dimensions, key=lambda d: d.name):
            spec: dict = {"dataType": dim.pinot_type, "name": dim.name}
            if dim.multi_value:
                spec["singleValueField"] = False
            dimension_field_specs.append(spec)

        # ------------------------------------------------------------------ #
        # metricFieldSpecs — sorted alphabetically by name
        # ------------------------------------------------------------------ #
        metric_field_specs: list[dict] = []
        for met in sorted(canonical.metrics, key=lambda m: m.name):
            if met.pinot_type == "BYTES":
                warnings.append(
                    f"Metric '{met.name}' has BYTES type (sketch/complex); "
                    "manual migration required for this field"
                )
            metric_field_specs.append({"dataType": met.pinot_type, "name": met.name})

        schema: dict = {
            "schemaName": canonical.datasource_name,
            "dimensionFieldSpecs": dimension_field_specs,
            "metricFieldSpecs": metric_field_specs,
            "dateTimeFieldSpecs": date_time_field_specs,
        }
        # Upsert tables need ``primaryKeyColumns`` declared at the
        # schema level (Pinot 1.0+). Without this the upsertConfig on
        # the table side has nothing to dedupe against and ingestion
        # fails at table-creation time.
        if canonical.upsert.enabled and canonical.upsert.primary_key:
            schema["primaryKeyColumns"] = list(canonical.upsert.primary_key)
        return schema

    def generate_with_warnings(self, canonical: CanonicalMigrationModel) -> tuple[dict, list[str]]:
        """Generate schema and also return any warnings produced."""
        warnings: list[str] = []

        date_time_field_specs: list[dict] = []
        if canonical.time_field is not None:
            date_time_field_specs.append(time_field_to_pinot_spec(canonical.time_field))

        dimension_field_specs: list[dict] = []
        for dim in sorted(canonical.dimensions, key=lambda d: d.name):
            spec: dict = {"dataType": dim.pinot_type, "name": dim.name}
            if dim.multi_value:
                spec["singleValueField"] = False
            dimension_field_specs.append(spec)

        metric_field_specs: list[dict] = []
        for met in sorted(canonical.metrics, key=lambda m: m.name):
            if met.pinot_type == "BYTES":
                warnings.append(
                    f"Metric '{met.name}' has BYTES type (sketch/complex); "
                    "manual migration required for this field"
                )
            metric_field_specs.append({"dataType": met.pinot_type, "name": met.name})

        schema = {
            "schemaName": canonical.datasource_name,
            "dimensionFieldSpecs": dimension_field_specs,
            "metricFieldSpecs": metric_field_specs,
            "dateTimeFieldSpecs": date_time_field_specs,
        }
        if canonical.upsert.enabled and canonical.upsert.primary_key:
            schema["primaryKeyColumns"] = list(canonical.upsert.primary_key)
        return schema, warnings
