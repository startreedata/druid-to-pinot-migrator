from __future__ import annotations

from migrator.core.enums import RiskConfidence, RiskSeverity
from migrator.core.models import CanonicalMigrationModel, RiskAnnotation
from migrator.core.result_types import AnalyzeResult
from migrator.druid.feature_flags import (
    detect_complex_aggregators,
    detect_multivalue_ambiguity,
    detect_transform_portability_risk,
)
from migrator.risks.taxonomy import (
    APPROX_AGGREGATOR_MISMATCH,
    CUSTOM_TIMESTAMP_FORMAT,
    FLATTEN_SPEC_NOT_PORTABLE,
    INGESTION_BEHAVIOR_MISMATCH,
    MULTIVALUE_AMBIGUITY,
    PARTITIONING_CONFIG_REQUIRED,
    RISK_DESCRIPTIONS,
    BATCH_AGGREGATION_NOT_REPLAYED,
    ROLLUP_SEMANTIC_MISMATCH,
    STREAM_SOURCE_MISMATCH,
    TIME_SEMANTICS_MISMATCH,
    TRANSFORM_PORTABILITY_RISK,
    UNSUPPORTED_COMPLEX_FIELD,
)

_NON_STANDARD_TIME_FORMATS = frozenset(["posix", "auto", "custom", "ruby"])
# Timestamp formats that are known non-standard patterns (Java SimpleDateFormat)
_KNOWN_EPOCH_OR_ISO_FORMATS = frozenset([
    "auto", "iso", "millis", "posix", "seconds", "micro", "nano", "milliseconds",
])


class RiskAnalyzer:
    """Analyze a CanonicalMigrationModel and produce risk annotations."""

    def analyze(self, canonical: CanonicalMigrationModel) -> AnalyzeResult:
        risks: list[RiskAnnotation] = []
        warnings: list[str] = []

        # ------------------------------------------------------------------ #
        # ROLLUP_SEMANTIC_MISMATCH
        # ------------------------------------------------------------------ #
        if canonical.granularity.rollup:
            risks.append(
                RiskAnnotation(
                    risk_id=ROLLUP_SEMANTIC_MISMATCH,
                    severity=RiskSeverity.HIGH.value,
                    confidence=RiskConfidence.CERTAIN.value,
                    description=RISK_DESCRIPTIONS[ROLLUP_SEMANTIC_MISMATCH],
                    evidence=[
                        f"rollup=True in granularitySpec",
                        f"queryGranularity={canonical.granularity.query_granularity}",
                    ],
                    remediation=(
                        "After migration, validate that COUNT(*) and SUM() results match "
                        "expected values against a reference dataset."
                    ),
                )
            )

            # Batch + rollup + metrics is the specific case where the
            # Pinot side genuinely cannot replay the aggregation (the
            # generic ROLLUP_SEMANTIC_MISMATCH covers the broader
            # semantics gap; this one is the actionable "your numbers
            # WILL be wrong unless you do one of these three things"
            # warning).
            if (
                canonical.source_kind == "batch"
                and len(canonical.metrics) > 0
            ):
                risks.append(
                    RiskAnnotation(
                        risk_id=BATCH_AGGREGATION_NOT_REPLAYED,
                        severity=RiskSeverity.HIGH.value,
                        confidence=RiskConfidence.CERTAIN.value,
                        description=RISK_DESCRIPTIONS[BATCH_AGGREGATION_NOT_REPLAYED],
                        evidence=[
                            f"source_kind=batch",
                            f"rollup=True",
                            f"{len(canonical.metrics)} metric(s) declared",
                        ],
                        remediation=(
                            "Pre-aggregate upstream (Spark / Trino / a Druid MSQ "
                            "into a separate output location), OR add a star-tree "
                            "index to the generated table config (run "
                            "``dpm recommend <spec>`` for the suggested "
                            "starTreeIndexConfigs), OR accept query-time aggregation."
                        ),
                    )
                )

        # ------------------------------------------------------------------ #
        # APPROX_AGGREGATOR_MISMATCH (BLOCKING)
        # ------------------------------------------------------------------ #
        complex_aggs = detect_complex_aggregators(canonical.metrics)
        if complex_aggs:
            risks.append(
                RiskAnnotation(
                    risk_id=APPROX_AGGREGATOR_MISMATCH,
                    severity=RiskSeverity.BLOCKING.value,
                    confidence=RiskConfidence.CERTAIN.value,
                    description=RISK_DESCRIPTIONS[APPROX_AGGREGATOR_MISMATCH],
                    evidence=[f"Complex aggregators found: {', '.join(complex_aggs)}"],
                    remediation=(
                        "Re-ingest raw events into Pinot and define "
                        "DISTINCTCOUNTHLL or DISTINCTCOUNTTHETASKETCH aggregations "
                        "on the raw field values."
                    ),
                )
            )

        # ------------------------------------------------------------------ #
        # UNSUPPORTED_COMPLEX_FIELD
        # ------------------------------------------------------------------ #
        bytes_metrics = [m.name for m in canonical.metrics if m.pinot_type == "BYTES"]
        if bytes_metrics:
            risks.append(
                RiskAnnotation(
                    risk_id=UNSUPPORTED_COMPLEX_FIELD,
                    severity=RiskSeverity.HIGH.value,
                    confidence=RiskConfidence.CERTAIN.value,
                    description=RISK_DESCRIPTIONS[UNSUPPORTED_COMPLEX_FIELD],
                    evidence=[f"BYTES-type fields: {', '.join(bytes_metrics)}"],
                    remediation="Map these fields to appropriate Pinot types or exclude them from the schema.",
                )
            )

        # ------------------------------------------------------------------ #
        # TRANSFORM_PORTABILITY_RISK
        # ------------------------------------------------------------------ #
        risky_transforms = detect_transform_portability_risk(canonical.transforms)
        if risky_transforms:
            risks.append(
                RiskAnnotation(
                    risk_id=TRANSFORM_PORTABILITY_RISK,
                    severity=RiskSeverity.MEDIUM.value,
                    confidence=RiskConfidence.LIKELY.value,
                    description=RISK_DESCRIPTIONS[TRANSFORM_PORTABILITY_RISK],
                    evidence=[f"Non-trivial transforms: {', '.join(risky_transforms)}"],
                    remediation=(
                        "Re-implement transform logic upstream in the ETL pipeline "
                        "or use Pinot's ingest transform functions if supported."
                    ),
                )
            )
        elif canonical.transforms:
            # Even simple transforms carry some risk
            transform_names = [t.name for t in canonical.transforms]
            warnings.append(
                f"Transforms present ({', '.join(transform_names)}); "
                "verify expression compatibility with Pinot."
            )

        # ------------------------------------------------------------------ #
        # MULTIVALUE_AMBIGUITY
        # ------------------------------------------------------------------ #
        mv_dims = detect_multivalue_ambiguity(canonical.dimensions)
        if mv_dims:
            risks.append(
                RiskAnnotation(
                    risk_id=MULTIVALUE_AMBIGUITY,
                    severity=RiskSeverity.MEDIUM.value,
                    confidence=RiskConfidence.LIKELY.value,
                    description=RISK_DESCRIPTIONS[MULTIVALUE_AMBIGUITY],
                    evidence=[f"Multi-value dimensions: {', '.join(mv_dims)}"],
                    remediation=(
                        "Set singleValueField=false in Pinot schema for MV columns "
                        "and validate GROUP BY / COUNT DISTINCT query results."
                    ),
                )
            )

        # ------------------------------------------------------------------ #
        # TIME_SEMANTICS_MISMATCH
        # ------------------------------------------------------------------ #
        if canonical.time_field is not None:
            tf_format = (canonical.time_field.format or "auto").lower()
            if tf_format in _NON_STANDARD_TIME_FORMATS:
                risks.append(
                    RiskAnnotation(
                        risk_id=TIME_SEMANTICS_MISMATCH,
                        severity=RiskSeverity.LOW.value,
                        confidence=RiskConfidence.POSSIBLE.value,
                        description=RISK_DESCRIPTIONS[TIME_SEMANTICS_MISMATCH],
                        evidence=[
                            f"Time column '{canonical.time_field.column_name}' "
                            f"uses format '{tf_format}'"
                        ],
                        remediation=(
                            "Verify the dateTimeFieldSpec format in the generated schema "
                            "matches the actual timestamp format in the data."
                        ),
                    )
                )

        # ------------------------------------------------------------------ #
        # INGESTION_BEHAVIOR_MISMATCH — advisory for appendToExisting
        # ------------------------------------------------------------------ #
        if canonical.raw_io_config.get("appendToExisting"):
            risks.append(
                RiskAnnotation(
                    risk_id=INGESTION_BEHAVIOR_MISMATCH,
                    severity=RiskSeverity.INFO.value,
                    confidence=RiskConfidence.CERTAIN.value,
                    description=RISK_DESCRIPTIONS[INGESTION_BEHAVIOR_MISMATCH],
                    evidence=["appendToExisting=true in ioConfig"],
                    remediation=(
                        "Review Pinot segment compaction and upsert documentation "
                        "to replicate the desired behavior."
                    ),
                )
            )

        # ------------------------------------------------------------------ #
        # PARTITIONING_CONFIG_REQUIRED — partitionsSpec present
        # ------------------------------------------------------------------ #
        partitions_features = [
            f for f in canonical.unsupported_features
            if f.feature.startswith("partitionsSpec:")
        ]
        if partitions_features:
            p_type = partitions_features[0].feature.split(":")[1]
            risks.append(
                RiskAnnotation(
                    risk_id=PARTITIONING_CONFIG_REQUIRED,
                    severity=RiskSeverity.MEDIUM.value,
                    confidence=RiskConfidence.CERTAIN.value,
                    description=RISK_DESCRIPTIONS[PARTITIONING_CONFIG_REQUIRED],
                    evidence=[f"Druid partitionsSpec type='{p_type}' detected in tuningConfig"],
                    remediation=(
                        "Configure tableIndexConfig.segmentPartitionConfig in the Pinot "
                        "table config with equivalent columnPartitionMap entries."
                    ),
                )
            )

        # ------------------------------------------------------------------ #
        # FLATTEN_SPEC_NOT_PORTABLE
        # ------------------------------------------------------------------ #
        flatten_features = [
            f for f in canonical.unsupported_features
            if f.feature == "flattenSpec"
        ]
        if flatten_features:
            risks.append(
                RiskAnnotation(
                    risk_id=FLATTEN_SPEC_NOT_PORTABLE,
                    severity=RiskSeverity.HIGH.value,
                    confidence=RiskConfidence.CERTAIN.value,
                    description=RISK_DESCRIPTIONS[FLATTEN_SPEC_NOT_PORTABLE],
                    evidence=["flattenSpec with path expressions detected in inputFormat"],
                    remediation=(
                        "Implement field extraction via Pinot ingestion transformations "
                        "or pre-flatten the JSON upstream before ingest."
                    ),
                )
            )

        # ------------------------------------------------------------------ #
        # CUSTOM_TIMESTAMP_FORMAT
        # ------------------------------------------------------------------ #
        custom_ts_features = [
            f for f in canonical.unsupported_features
            if f.feature.startswith("custom_timestamp_format:")
        ]
        if custom_ts_features:
            col = custom_ts_features[0].feature.split(":")[1]
            fmt = canonical.time_field.format if canonical.time_field else "unknown"
            risks.append(
                RiskAnnotation(
                    risk_id=CUSTOM_TIMESTAMP_FORMAT,
                    severity=RiskSeverity.MEDIUM.value,
                    confidence=RiskConfidence.LIKELY.value,
                    description=RISK_DESCRIPTIONS[CUSTOM_TIMESTAMP_FORMAT],
                    evidence=[f"Column '{col}' uses format '{fmt}'"],
                    remediation=(
                        "Update the dateTimeFieldSpec.format in schema.json to use "
                        "the correct Pinot SIMPLE_DATE_FORMAT pattern."
                    ),
                )
            )

        # ------------------------------------------------------------------ #
        # STREAM_SOURCE_MISMATCH — Kinesis source generating Kafka config
        # ------------------------------------------------------------------ #
        io_type = (canonical.raw_io_config.get("type") or "").lower()
        if io_type == "kinesis":
            risks.append(
                RiskAnnotation(
                    risk_id=STREAM_SOURCE_MISMATCH,
                    severity=RiskSeverity.HIGH.value,
                    confidence=RiskConfidence.CERTAIN.value,
                    description=RISK_DESCRIPTIONS[STREAM_SOURCE_MISMATCH],
                    evidence=["ioConfig.type=kinesis; REALTIME table generated with Kafka defaults"],
                    remediation=(
                        "Replace streamConfigs with Kinesis consumer factory settings or "
                        "bridge Kinesis to Kafka before Pinot ingestion."
                    ),
                )
            )

        return AnalyzeResult(risks=risks, warnings=warnings)
