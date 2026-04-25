from __future__ import annotations

from migrator.core.enums import ValidationStatus
from migrator.core.models import CanonicalMigrationModel, ValidationCheck
from migrator.validation.base import BaseValidator


class StaticSpecValidator(BaseValidator):
    """Validate a CanonicalMigrationModel for basic structural correctness."""

    def validate(self, target: CanonicalMigrationModel) -> list[ValidationCheck]:
        checks: list[ValidationCheck] = []

        # ------------------------------------------------------------------ #
        # 1. Datasource name not empty
        # ------------------------------------------------------------------ #
        if target.datasource_name:
            checks.append(
                ValidationCheck(
                    check_id="static.datasource_name_present",
                    status=ValidationStatus.PASS.value,
                    message="Datasource name is present.",
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    check_id="static.datasource_name_present",
                    status=ValidationStatus.FAIL.value,
                    message="Datasource name is empty.",
                )
            )

        # ------------------------------------------------------------------ #
        # 2. Time field present
        # ------------------------------------------------------------------ #
        if target.time_field is not None:
            checks.append(
                ValidationCheck(
                    check_id="static.time_field_present",
                    status=ValidationStatus.PASS.value,
                    message=f"Time field '{target.time_field.column_name}' is present.",
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    check_id="static.time_field_present",
                    status=ValidationStatus.FAIL.value,
                    message="No time field defined.",
                )
            )

        # ------------------------------------------------------------------ #
        # 3. Field names unique across dimensions + metrics
        # ------------------------------------------------------------------ #
        all_names: list[str] = (
            [d.name for d in target.dimensions] + [m.name for m in target.metrics]
        )
        duplicates = {n for n in all_names if all_names.count(n) > 1}
        if duplicates:
            checks.append(
                ValidationCheck(
                    check_id="static.field_names_unique",
                    status=ValidationStatus.FAIL.value,
                    message=f"Duplicate field names found: {', '.join(sorted(duplicates))}",
                    details={"duplicates": sorted(duplicates)},
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    check_id="static.field_names_unique",
                    status=ValidationStatus.PASS.value,
                    message="All field names are unique.",
                )
            )

        # ------------------------------------------------------------------ #
        # 4. Metric names match output names (name == output name in druid)
        # ------------------------------------------------------------------ #
        bad_metrics = [m.name for m in target.metrics if not m.name]
        if bad_metrics:
            checks.append(
                ValidationCheck(
                    check_id="static.metric_names_valid",
                    status=ValidationStatus.FAIL.value,
                    message="Some metrics have empty names.",
                    details={"bad_metrics": bad_metrics},
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    check_id="static.metric_names_valid",
                    status=ValidationStatus.PASS.value,
                    message="All metric names are valid.",
                )
            )

        # ------------------------------------------------------------------ #
        # 5. Classification assigned
        # ------------------------------------------------------------------ #
        if target.classification and target.classification != "unknown":
            checks.append(
                ValidationCheck(
                    check_id="static.classification_assigned",
                    status=ValidationStatus.PASS.value,
                    message=f"Classification is '{target.classification}'.",
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    check_id="static.classification_assigned",
                    status=ValidationStatus.WARN.value,
                    message="Classification is 'unknown'.",
                )
            )

        return checks
