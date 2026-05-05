from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ColumnType(BaseModel):
    druid_type: str
    pinot_type: str
    notes: str = ""


class TimeField(BaseModel):
    column_name: str
    format: str = "auto"
    timezone: str = "UTC"
    notes: str = ""


class DimensionField(BaseModel):
    name: str
    druid_type: str = "string"
    pinot_type: str = "STRING"
    multi_value: bool = False
    notes: str = ""


class MetricField(BaseModel):
    name: str
    druid_type: str
    field_name: str = ""
    pinot_type: str
    aggregation: str
    notes: str = ""


class TransformField(BaseModel):
    name: str
    expression: str
    output_type: str = "string"
    notes: str = ""


class GranularityInfo(BaseModel):
    segment_granularity: str = "DAY"
    query_granularity: str = "NONE"
    rollup: bool = False
    intervals: list[str] = Field(default_factory=list)


class RetentionHint(BaseModel):
    period: str = ""
    notes: str = ""


class UnsupportedFeature(BaseModel):
    feature: str
    reason: str
    severity: str = "high"


class RiskAnnotation(BaseModel):
    risk_id: str
    severity: str
    confidence: str
    description: str
    evidence: list[str] = Field(default_factory=list)
    remediation: str = ""


class CanonicalMigrationModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    datasource_name: str = ""
    source_kind: str = "unknown"
    classification: str = "unknown"
    time_field: TimeField | None = None
    dimensions: list[DimensionField] = Field(default_factory=list)
    metrics: list[MetricField] = Field(default_factory=list)
    transforms: list[TransformField] = Field(default_factory=list)
    granularity: GranularityInfo = Field(default_factory=GranularityInfo)
    retention_hint: RetentionHint = Field(default_factory=RetentionHint)
    unsupported_features: list[UnsupportedFeature] = Field(default_factory=list)
    risk_annotations: list[RiskAnnotation] = Field(default_factory=list)
    # The wire-format Druid was reading. dpm uses this to pick the
    # right Pinot RecordReader (JSON / Parquet / Avro / ORC / CSV /
    # Protobuf). Default ``json`` matches the Druid + Pinot common case
    # and preserves backward compatibility with specs that pre-date this
    # field. Unknown formats fall back to ``json`` with a warning.
    input_format: str = "json"
    raw_io_config: dict = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


class ArtifactBundleManifest(BaseModel):
    datasource_name: str
    files: list[str] = Field(default_factory=list)
    generated_at: str = ""


class ValidationCheck(BaseModel):
    check_id: str
    status: str
    message: str
    details: dict = Field(default_factory=dict)


class ValidationReport(BaseModel):
    datasource_name: str
    checks: list[ValidationCheck]
    confidence_score: float
    overall_status: str
