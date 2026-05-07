from __future__ import annotations

from migrator.core.errors import ParseError
from migrator.core.result_types import ParseResult
from migrator.druid.models import (
    DruidDimensionsSpec,
    DruidGranularitySpec,
    DruidIoConfig,
    DruidMetricSpec,
    DruidParsedSpec,
    DruidTimestampSpec,
    DruidTransformSpec,
)
from migrator.druid.msq_parser import looks_like_msq, parse_msq_spec


class DruidSpecParser:
    """Parse a raw Druid ingestion spec dict into a DruidParsedSpec."""

    def parse(self, raw: dict) -> ParseResult:
        errors: list[str] = []
        warnings: list[str] = []

        # MSQ specs (``{"query": "INSERT INTO ... SELECT ..."}``) take
        # a different path — the SQL string is parsed via sqlglot and
        # translated into the same ``DruidParsedSpec`` shape so the
        # rest of the pipeline (normalize → generate → validate) is
        # untouched.
        if looks_like_msq(raw):
            try:
                parsed_spec, msq_warnings = parse_msq_spec(raw)
            except ParseError as exc:
                return ParseResult(
                    success=False, parsed_spec=None,
                    errors=[str(exc)], warnings=warnings,
                )
            return ParseResult(
                success=True,
                parsed_spec=parsed_spec,
                errors=[],
                warnings=msq_warnings,
            )

        try:
            # Locate dataSchema — supports nested spec.dataSchema or top-level dataSchema
            data_schema = self._extract_data_schema(raw)
            if data_schema is None:
                raise ParseError("No 'dataSchema' found at top-level or under 'spec'")

            # Datasource name
            datasource_name = data_schema.get("dataSource") or data_schema.get("datasource", "")
            if not datasource_name:
                errors.append("Missing 'dataSource' in dataSchema")
                datasource_name = ""

            # timestampSpec
            ts_raw = data_schema.get("timestampSpec", {})
            timestamp_spec = DruidTimestampSpec(**ts_raw) if ts_raw else DruidTimestampSpec()

            # dimensionsSpec
            dim_raw = data_schema.get("dimensionsSpec", {})
            if dim_raw:
                # Normalise the dimensions list: convert plain strings to dicts
                raw_dims = dim_raw.get("dimensions", [])
                normalized_dims: list[dict] = []
                for d in raw_dims:
                    if isinstance(d, str):
                        normalized_dims.append({"type": "string", "name": d})
                    elif isinstance(d, dict):
                        normalized_dims.append(d)
                    else:
                        warnings.append(f"Unexpected dimension entry type: {type(d).__name__}")
                dim_raw = dict(dim_raw)
                dim_raw["dimensions"] = normalized_dims
                dimensions_spec = DruidDimensionsSpec(**dim_raw)
            else:
                warnings.append("No 'dimensionsSpec' found; assuming empty dimensions")
                dimensions_spec = DruidDimensionsSpec()

            # metricsSpec
            metrics_raw = data_schema.get("metricsSpec", [])
            metrics_spec: list[DruidMetricSpec] = []
            for m in metrics_raw:
                if isinstance(m, dict):
                    known_keys = {"type", "name", "fieldName"}
                    extra = {k: v for k, v in m.items() if k not in known_keys}
                    metrics_spec.append(
                        DruidMetricSpec(
                            type=m.get("type", ""),
                            name=m.get("name", ""),
                            fieldName=m.get("fieldName", ""),
                            extra=extra,
                        )
                    )
                else:
                    warnings.append(f"Unexpected metricsSpec entry type: {type(m).__name__}")

            # granularitySpec
            gran_raw = data_schema.get("granularitySpec", {})
            granularity_spec = DruidGranularitySpec(**gran_raw) if gran_raw else DruidGranularitySpec()

            # transformSpec
            transform_raw = data_schema.get("transformSpec", {})
            transform_spec = DruidTransformSpec(**transform_raw) if transform_raw else DruidTransformSpec()

            # ioConfig — may be at spec.ioConfig or top-level ioConfig
            io_raw = self._extract_io_config(raw)
            if io_raw:
                # Kinesis uses "stream" instead of inputSource
                input_source = io_raw.get("inputSource", {})
                if not input_source and io_raw.get("stream"):
                    input_source = {"type": "kinesis", "stream": io_raw["stream"]}
                # Build a clean dict for DruidIoConfig, only using known fields.
                # Druid itself accepts a Kafka/Kinesis supervisor spec without
                # an explicit ``ioConfig.type`` — it infers from the top-level
                # task ``type`` (``"kafka"`` / ``"kinesis"`` / etc.). Mirror
                # that inference here so dpm classifies the same set of specs
                # as ``stream`` that Druid does. ioConfig.type still wins
                # when present.
                inferred_type = io_raw.get("type") or raw.get("type") or "index"
                io_known = {
                    "type": inferred_type,
                    "inputSource": input_source,
                    "inputFormat": io_raw.get("inputFormat", {}),
                    "appendToExisting": io_raw.get("appendToExisting", False),
                }
                io_config = DruidIoConfig(**io_known)
            else:
                warnings.append("No 'ioConfig' found; using defaults")
                io_config = DruidIoConfig()

            # flattenSpec — captured from inputFormat if present
            flatten_spec = (io_raw or {}).get("inputFormat", {}).get("flattenSpec")
            if flatten_spec:
                warnings.append(
                    "flattenSpec detected in inputFormat; nested-field extraction "
                    "requires manual mapping in Pinot ingest config"
                )

            # Collect any unknown/extra sections
            known_top_keys = {"type", "spec", "dataSchema", "ioConfig", "tuningConfig"}
            raw_sections: dict = {}
            for k, v in raw.items():
                if k not in known_top_keys:
                    raw_sections[k] = v
            # Also capture tuningConfig if present (partitionsSpec lives here)
            tuning = raw.get("tuningConfig") or (raw.get("spec") or {}).get("tuningConfig")
            if tuning:
                raw_sections["tuningConfig"] = tuning
                partitions = tuning.get("partitionsSpec", {})
                if partitions:
                    p_type = partitions.get("type", "dynamic")
                    warnings.append(
                        f"partitionsSpec type='{p_type}' detected; "
                        "configure equivalent Pinot segment partitioning manually"
                    )
            if flatten_spec:
                raw_sections["flattenSpec"] = flatten_spec

            parsed = DruidParsedSpec(
                datasource_name=datasource_name,
                timestamp_spec=timestamp_spec,
                dimensions_spec=dimensions_spec,
                metrics_spec=metrics_spec,
                granularity_spec=granularity_spec,
                transform_spec=transform_spec,
                io_config=io_config,
                raw_io_config=io_raw or {},
                raw_sections=raw_sections,
            )
            return ParseResult(success=True, parsed_spec=parsed, errors=errors, warnings=warnings)

        except ParseError:
            raise
        except Exception as exc:
            raise ParseError(f"Unexpected error parsing Druid spec: {exc}") from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_data_schema(self, raw: dict) -> dict | None:
        """Return the dataSchema dict, searching common locations."""
        # Nested: spec.dataSchema
        spec = raw.get("spec")
        if isinstance(spec, dict):
            ds = spec.get("dataSchema")
            if isinstance(ds, dict):
                return ds
        # Top-level: dataSchema
        ds = raw.get("dataSchema")
        if isinstance(ds, dict):
            return ds
        return None

    def _extract_io_config(self, raw: dict) -> dict | None:
        """Return the ioConfig dict, searching common locations."""
        spec = raw.get("spec")
        if isinstance(spec, dict):
            io = spec.get("ioConfig")
            if isinstance(io, dict):
                return io
        io = raw.get("ioConfig")
        if isinstance(io, dict):
            return io
        return None
