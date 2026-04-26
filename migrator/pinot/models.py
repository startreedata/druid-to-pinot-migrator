from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PinotSchemaFieldSpec(BaseModel):
    name: str
    dataType: str
    notNull: bool = False


class PinotSchema(BaseModel):
    schemaName: str
    dimensionFieldSpecs: list[dict] = Field(default_factory=list)
    metricFieldSpecs: list[dict] = Field(default_factory=list)
    dateTimeFieldSpecs: list[dict] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaName": self.schemaName,
            "dimensionFieldSpecs": self.dimensionFieldSpecs,
            "metricFieldSpecs": self.metricFieldSpecs,
            "dateTimeFieldSpecs": self.dateTimeFieldSpecs,
        }


class PinotTableConfig(BaseModel):
    """Minimal typed wrapper around a Pinot table config dict."""

    tableName: str
    tableType: str
    segmentsConfig: dict = Field(default_factory=dict)
    tenants: dict = Field(default_factory=dict)
    tableIndexConfig: dict = Field(default_factory=dict)
    ingestionConfig: dict = Field(default_factory=dict)
    metadata: dict = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "tableName": self.tableName,
            "tableType": self.tableType,
            "segmentsConfig": self.segmentsConfig,
            "tenants": self.tenants,
            "tableIndexConfig": self.tableIndexConfig,
        }
        if self.ingestionConfig:
            d["ingestionConfig"] = self.ingestionConfig
        if self.metadata:
            d["metadata"] = self.metadata
        return d
