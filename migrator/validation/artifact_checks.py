from __future__ import annotations

from migrator.core.enums import ValidationStatus
from migrator.core.models import ValidationCheck
from migrator.validation.base import BaseValidator

_VALID_TABLE_TYPES = frozenset(["OFFLINE", "REALTIME"])


class ArtifactValidator(BaseValidator):
    """Validate generated Pinot schema and table config artifacts."""

    def validate(self, target: dict) -> list[ValidationCheck]:
        """Validate artifacts dict with keys 'schema' and 'table'."""
        schema: dict = target.get("schema", {})
        table: dict = target.get("table", {})
        checks: list[ValidationCheck] = []

        # ------------------------------------------------------------------ #
        # 1. Schema has at least one dateTimeFieldSpec
        # ------------------------------------------------------------------ #
        date_time_specs = schema.get("dateTimeFieldSpecs", [])
        if date_time_specs:
            checks.append(
                ValidationCheck(
                    check_id="artifact.schema_has_datetime",
                    status=ValidationStatus.PASS.value,
                    message=f"Schema has {len(date_time_specs)} dateTimeFieldSpec(s).",
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    check_id="artifact.schema_has_datetime",
                    status=ValidationStatus.FAIL.value,
                    message="Schema has no dateTimeFieldSpecs.",
                )
            )

        # ------------------------------------------------------------------ #
        # 2. No duplicate field names in schema
        # ------------------------------------------------------------------ #
        all_field_names: list[str] = (
            [f.get("name", "") for f in schema.get("dimensionFieldSpecs", [])]
            + [f.get("name", "") for f in schema.get("metricFieldSpecs", [])]
            + [f.get("name", "") for f in schema.get("dateTimeFieldSpecs", [])]
        )
        duplicates = {n for n in all_field_names if all_field_names.count(n) > 1 and n}
        if duplicates:
            checks.append(
                ValidationCheck(
                    check_id="artifact.schema_no_duplicate_fields",
                    status=ValidationStatus.FAIL.value,
                    message=f"Duplicate field names in schema: {', '.join(sorted(duplicates))}",
                    details={"duplicates": sorted(duplicates)},
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    check_id="artifact.schema_no_duplicate_fields",
                    status=ValidationStatus.PASS.value,
                    message="No duplicate field names in schema.",
                )
            )

        # ------------------------------------------------------------------ #
        # 3. Table type is valid
        # ------------------------------------------------------------------ #
        table_type = table.get("tableType", "")
        if table_type in _VALID_TABLE_TYPES:
            checks.append(
                ValidationCheck(
                    check_id="artifact.table_type_valid",
                    status=ValidationStatus.PASS.value,
                    message=f"Table type '{table_type}' is valid.",
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    check_id="artifact.table_type_valid",
                    status=ValidationStatus.FAIL.value,
                    message=f"Table type '{table_type}' is not valid. Expected OFFLINE or REALTIME.",
                )
            )

        # ------------------------------------------------------------------ #
        # 4. Schema and table time column names match
        # ------------------------------------------------------------------ #
        schema_time_col = (
            date_time_specs[0].get("name", "") if date_time_specs else ""
        )
        table_time_col = table.get("segmentsConfig", {}).get("timeColumnName", "")
        if schema_time_col and table_time_col:
            if schema_time_col == table_time_col:
                checks.append(
                    ValidationCheck(
                        check_id="artifact.time_column_match",
                        status=ValidationStatus.PASS.value,
                        message=f"Time column '{schema_time_col}' matches between schema and table.",
                    )
                )
            else:
                checks.append(
                    ValidationCheck(
                        check_id="artifact.time_column_match",
                        status=ValidationStatus.FAIL.value,
                        message=(
                            f"Time column mismatch: schema has '{schema_time_col}', "
                            f"table has '{table_time_col}'."
                        ),
                        details={
                            "schema_time_column": schema_time_col,
                            "table_time_column": table_time_col,
                        },
                    )
                )
        else:
            checks.append(
                ValidationCheck(
                    check_id="artifact.time_column_match",
                    status=ValidationStatus.WARN.value,
                    message="Cannot verify time column match: one or both configs missing time column.",
                    details={
                        "schema_time_column": schema_time_col,
                        "table_time_column": table_time_col,
                    },
                )
            )

        # ------------------------------------------------------------------ #
        # 5. Realtime table has streamConfigs
        # ------------------------------------------------------------------ #
        if table_type == "REALTIME":
            stream_configs = table.get("tableIndexConfig", {}).get("streamConfigs", {})
            if stream_configs:
                checks.append(
                    ValidationCheck(
                        check_id="artifact.realtime_has_stream_configs",
                        status=ValidationStatus.PASS.value,
                        message="REALTIME table has streamConfigs.",
                    )
                )
            else:
                checks.append(
                    ValidationCheck(
                        check_id="artifact.realtime_has_stream_configs",
                        status=ValidationStatus.FAIL.value,
                        message="REALTIME table is missing streamConfigs.",
                    )
                )

        return checks
