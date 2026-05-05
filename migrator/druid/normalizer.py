from __future__ import annotations

from migrator.core.enums import SourceKind
from migrator.core.models import (
    CanonicalMigrationModel,
    DimensionField,
    GranularityInfo,
    MetricField,
    TimeField,
    TransformField,
    UnsupportedFeature,
)
from migrator.core.result_types import NormalizeResult
from migrator.druid.models import DruidParsedSpec
from migrator.translators.rules import DRUID_TO_PINOT_TYPE, map_metric_to_pinot

# Aggregation types that produce sketch/complex/unsupported Pinot output
_COMPLEX_METRIC_TYPES = frozenset(
    [
        "thetaSketch",
        "HLLSketchBuild",
        "HLLSketchMerge",
        "hyperUnique",
        "quantilesDoublesSketch",
        "momentSketch",
        "fixedBucketsHistogram",
    ]
)

_STREAM_IO_TYPES = frozenset(["kafka", "kinesis"])

# Druid inputFormat.type → canonical input_format string. Pinot's
# matching RecordReader plugin is selected from this in the ingestion
# generator. Names kept lowercase so case differences in operator-
# written specs don't cause a fallback to JSON.
_KNOWN_INPUT_FORMATS = frozenset([
    "json", "parquet", "avro", "orc", "csv", "tsv", "protobuf",
])

# Standard timestamp formats the tool handles natively
_KNOWN_TIMESTAMP_FORMATS = frozenset([
    "auto", "iso", "millis", "posix", "seconds", "micro", "nano",
    "millis", "milliseconds",
])


class DruidNormalizer:
    """Convert a DruidParsedSpec into a CanonicalMigrationModel."""

    def normalize(self, parsed: DruidParsedSpec) -> NormalizeResult:
        warnings: list[str] = []
        errors: list[str] = []
        unsupported: list[UnsupportedFeature] = []

        # ------------------------------------------------------------------
        # Time field
        # ------------------------------------------------------------------
        ts = parsed.timestamp_spec
        ts_format = ts.format or "auto"
        # Detect custom (non-standard) timestamp formats
        if ts_format.lower() not in _KNOWN_TIMESTAMP_FORMATS:
            warnings.append(
                f"Custom timestamp format '{ts_format}' detected on column "
                f"'{ts.column}'; verify the dateTimeFieldSpec format string "
                "in the generated schema matches the actual data format."
            )
            unsupported.append(UnsupportedFeature(
                feature=f"custom_timestamp_format:{ts.column}",
                reason=f"Format '{ts_format}' requires manual dateTimeFieldSpec configuration",
                severity="medium",
            ))
        time_field = TimeField(
            column_name=ts.column,
            format=ts_format,
            timezone="UTC",
        )

        # ------------------------------------------------------------------
        # Dimensions
        # ------------------------------------------------------------------
        dimensions: list[DimensionField] = []
        for dim_dict in parsed.dimensions_spec.dimensions:
            name = dim_dict.get("name", "")
            if not name:
                warnings.append(f"Dimension entry missing 'name': {dim_dict}")
                continue
            druid_type = dim_dict.get("type", "string")
            # Treat "mv_enum", "multi-value", or explicit multiValueHandling as multi-value
            multi_value = dim_dict.get("multiValueHandling") is not None or druid_type in (
                "mv_enum",
            )
            pinot_type = DRUID_TO_PINOT_TYPE.get(druid_type.lower(), "STRING")
            if pinot_type == "BYTES":
                warnings.append(
                    f"Dimension '{name}' has complex type '{druid_type}' which maps to BYTES in Pinot"
                )
                unsupported.append(
                    UnsupportedFeature(
                        feature=f"complex_dimension:{name}",
                        reason=f"Druid type '{druid_type}' has no direct Pinot equivalent; mapped to BYTES",
                        severity="high",
                    )
                )
            dimensions.append(
                DimensionField(
                    name=name,
                    druid_type=druid_type,
                    pinot_type=pinot_type,
                    multi_value=multi_value,
                )
            )

        # ------------------------------------------------------------------
        # Metrics
        # ------------------------------------------------------------------
        metrics: list[MetricField] = []
        for m in parsed.metrics_spec:
            pinot_type, aggregation = map_metric_to_pinot(m.type)
            notes = ""
            if m.type in _COMPLEX_METRIC_TYPES:
                notes = f"Druid '{m.type}' is a sketch/complex aggregator; Pinot equivalent is approximate"
                warnings.append(
                    f"Metric '{m.name}' uses complex aggregator '{m.type}'; maps to BYTES in Pinot"
                )
                unsupported.append(
                    UnsupportedFeature(
                        feature=f"complex_metric:{m.name}",
                        reason=f"Druid aggregator '{m.type}' has no direct Pinot equivalent",
                        severity="high",
                    )
                )
            metrics.append(
                MetricField(
                    name=m.name,
                    druid_type=m.type,
                    field_name=m.fieldName,
                    pinot_type=pinot_type,
                    aggregation=aggregation,
                    notes=notes,
                )
            )

        # ------------------------------------------------------------------
        # Transforms
        # ------------------------------------------------------------------
        transforms: list[TransformField] = []
        for t in parsed.transform_spec.transforms:
            t_name = t.get("name", "")
            t_expr = t.get("expression", "")
            if not t_name:
                warnings.append(f"Transform entry missing 'name': {t}")
                continue
            transforms.append(
                TransformField(
                    name=t_name,
                    expression=t_expr,
                    output_type=t.get("outputType", "string"),
                )
            )

        # ------------------------------------------------------------------
        # Granularity
        # ------------------------------------------------------------------
        g = parsed.granularity_spec
        granularity = GranularityInfo(
            segment_granularity=g.segmentGranularity,
            query_granularity=g.queryGranularity,
            rollup=g.rollup,
            intervals=list(g.intervals),
        )

        # ------------------------------------------------------------------
        # Source kind
        # ------------------------------------------------------------------
        io_type = (parsed.io_config.type or "").lower()
        if any(s in io_type for s in _STREAM_IO_TYPES):
            source_kind = SourceKind.STREAM.value
        else:
            source_kind = SourceKind.BATCH.value

        # ------------------------------------------------------------------
        # Input format (json / parquet / avro / orc / csv / protobuf)
        # ------------------------------------------------------------------
        # Druid stores it under ioConfig.inputFormat.type. We normalise
        # the case and fall back to JSON for unknown / missing values
        # (Druid's own behaviour mirrors this). ``tsv`` is treated as a
        # csv variant — Pinot's CSVRecordReader handles both via the
        # delimiter config knob. Avro has two Druid sub-types
        # (``avro_ocf`` for Object Container Files, ``avro_stream`` for
        # Kafka with a bytes decoder) — both collapse to canonical
        # ``avro``; the downstream generator dispatches on the source
        # kind to pick the right Pinot reader vs decoder.
        raw_format = (
            parsed.io_config.inputFormat or {}
        ).get("type", "")
        input_format = (raw_format or "json").lower()
        if input_format == "tsv":
            input_format = "csv"
        if input_format in ("avro_ocf", "avro_stream"):
            input_format = "avro"
        if input_format not in _KNOWN_INPUT_FORMATS:
            warnings.append(
                f"unknown inputFormat.type '{raw_format}' — "
                "defaulting to JSON. Generated Pinot batch-job "
                "RecordReader may need manual adjustment."
            )
            input_format = "json"

        # Parquet binaryAsString=true is a known compatibility footgun —
        # Druid silently coerces binary columns to strings while Pinot's
        # ParquetRecordReader respects the original schema. Surface it
        # so the operator notices before discovering data drift.
        if input_format == "parquet":
            parquet_cfg = parsed.io_config.inputFormat or {}
            if parquet_cfg.get("binaryAsString"):
                warnings.append(
                    "Parquet inputFormat has binaryAsString=true; "
                    "Pinot's ParquetRecordReader does NOT honour this "
                    "Druid-only flag. Binary columns will round-trip "
                    "as bytes — re-encode upstream or add a transform."
                )

        # Avro stream specs ride or die on the schema-registry config —
        # without a URL Pinot's Confluent decoder can't deserialise the
        # bytes off the wire. Surface this loudly because the failure
        # mode is silent (table just stays empty).
        if input_format == "avro" and raw_format.lower() == "avro_stream":
            avro_decoder = (
                parsed.io_config.inputFormat or {}
            ).get("avroBytesDecoder", {})
            decoder_type = (avro_decoder.get("type") or "").lower()
            if decoder_type == "schema_registry":
                if not avro_decoder.get("url"):
                    warnings.append(
                        "avro_stream specifies type=schema_registry but no "
                        "``url`` — Pinot's KafkaConfluentSchemaRegistry"
                        "AvroMessageDecoder will need the URL added to the "
                        "generated table config before deploy."
                    )
            elif decoder_type == "schema_inline":
                if not avro_decoder.get("schema"):
                    warnings.append(
                        "avro_stream type=schema_inline missing ``schema`` — "
                        "fill in the inline schema in the generated table "
                        "config's stream.kafka.decoder.prop.schema entry."
                    )
            else:
                warnings.append(
                    f"avro_stream avroBytesDecoder.type='{decoder_type}' "
                    "is unrecognised; defaulting to schema_registry — verify "
                    "before deploy."
                )

        # ------------------------------------------------------------------
        # partitionsSpec — surface as unsupported feature note
        # ------------------------------------------------------------------
        tuning = parsed.raw_sections.get("tuningConfig", {})
        partitions_spec = tuning.get("partitionsSpec", {})
        if partitions_spec:
            p_type = partitions_spec.get("type", "dynamic")
            unsupported.append(UnsupportedFeature(
                feature=f"partitionsSpec:{p_type}",
                reason=(
                    f"Druid {p_type} partitioning has no automatic Pinot equivalent; "
                    "configure tableIndexConfig.segmentPartitionConfig manually"
                ),
                severity="medium",
            ))

        # ------------------------------------------------------------------
        # flattenSpec — surface as unsupported feature note
        # ------------------------------------------------------------------
        if parsed.raw_sections.get("flattenSpec"):
            unsupported.append(UnsupportedFeature(
                feature="flattenSpec",
                reason=(
                    "JSON flattenSpec path expressions require manual mapping "
                    "to Pinot's ingest transform functions or upstream ETL"
                ),
                severity="medium",
            ))

        # ------------------------------------------------------------------
        # appendToExisting — note in warnings
        # ------------------------------------------------------------------
        if parsed.io_config.appendToExisting:
            warnings.append(
                "appendToExisting=true detected; review Pinot segment append "
                "vs. replace semantics for the target table"
            )

        # ------------------------------------------------------------------
        # Input source type — note non-S3/local sources
        # ------------------------------------------------------------------
        input_source_type = (parsed.io_config.inputSource or {}).get("type", "")
        if input_source_type == "google":
            warnings.append(
                "GCS (google) inputSource detected; "
                "Pinot batch job config requires GCS URI and appropriate auth"
            )
        elif input_source_type == "azure":
            warnings.append(
                "Azure inputSource detected; "
                "Pinot batch job config requires Azure blob storage URI and auth"
            )
        elif input_source_type == "http":
            warnings.append(
                "HTTP inputSource detected; "
                "Pinot batch job supports HTTP input via the HTTP pinotFS plugin"
            )

        # ------------------------------------------------------------------
        # Assemble canonical model
        # ------------------------------------------------------------------
        canonical = CanonicalMigrationModel(
            datasource_name=parsed.datasource_name,
            source_kind=source_kind,
            classification="unknown",  # will be set by classifier
            time_field=time_field,
            dimensions=dimensions,
            metrics=metrics,
            transforms=transforms,
            granularity=granularity,
            unsupported_features=unsupported,
            input_format=input_format,
            raw_io_config=parsed.raw_io_config if parsed.raw_io_config else parsed.io_config.model_dump(),
            notes=warnings,
        )
        return NormalizeResult(
            success=True,
            canonical=canonical,
            errors=errors,
            warnings=warnings,
        )
