from enum import Enum


class SourceKind(str, Enum):
    BATCH = "batch"
    STREAM = "stream"
    HYBRID = "hybrid"
    UNKNOWN = "unknown"


class DatasourceClassification(str, Enum):
    RAW_EVENT = "raw_event"
    ROLLED_UP_ADDITIVE = "rolled_up_additive"
    COMPLEX_AGGREGATED = "complex_aggregated"
    UNKNOWN = "unknown"


class RiskSeverity(str, Enum):
    BLOCKING = "blocking"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class RiskConfidence(str, Enum):
    CERTAIN = "certain"
    LIKELY = "likely"
    POSSIBLE = "possible"


class ValidationStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


class OutputFormat(str, Enum):
    JSON = "json"
    YAML = "yaml"
    MARKDOWN = "markdown"
