from __future__ import annotations

from pydantic import BaseModel, Field


class DruidTimestampSpec(BaseModel):
    column: str = "__time"
    format: str = "auto"
    missingValue: str | None = None


class DruidDimensionsSpec(BaseModel):
    dimensions: list[dict] = Field(default_factory=list)
    dimensionExclusions: list[str] = Field(default_factory=list)
    spatialDimensions: list[dict] = Field(default_factory=list)


class DruidMetricSpec(BaseModel):
    type: str
    name: str
    fieldName: str = ""
    extra: dict = Field(default_factory=dict)


class DruidGranularitySpec(BaseModel):
    type: str = "uniform"
    segmentGranularity: str = "DAY"
    queryGranularity: str = "NONE"
    rollup: bool = False
    intervals: list[str] = Field(default_factory=list)


class DruidTransformSpec(BaseModel):
    transforms: list[dict] = Field(default_factory=list)
    filter: dict | None = None


class DruidIoConfig(BaseModel):
    type: str = "index"
    inputSource: dict = Field(default_factory=dict)
    inputFormat: dict = Field(default_factory=dict)
    appendToExisting: bool = False


class DruidParsedSpec(BaseModel):
    datasource_name: str
    timestamp_spec: DruidTimestampSpec = Field(default_factory=DruidTimestampSpec)
    dimensions_spec: DruidDimensionsSpec = Field(default_factory=DruidDimensionsSpec)
    metrics_spec: list[DruidMetricSpec] = Field(default_factory=list)
    granularity_spec: DruidGranularitySpec = Field(default_factory=DruidGranularitySpec)
    transform_spec: DruidTransformSpec = Field(default_factory=DruidTransformSpec)
    io_config: DruidIoConfig = Field(default_factory=DruidIoConfig)
    raw_io_config: dict = Field(default_factory=dict)
    raw_sections: dict = Field(default_factory=dict)
