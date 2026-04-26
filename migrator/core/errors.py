class MigrationError(Exception):
    def __init__(self, message: str, code: str = "MIGRATION_ERROR", context: dict = None):
        super().__init__(message)
        self.code = code
        self.context = context or {}


class ParseError(MigrationError):
    def __init__(self, message: str, context: dict = None):
        super().__init__(message, "PARSE_ERROR", context)


class NormalizationError(MigrationError):
    def __init__(self, message: str, context: dict = None):
        super().__init__(message, "NORMALIZATION_ERROR", context)


class GenerationError(MigrationError):
    def __init__(self, message: str, context: dict = None):
        super().__init__(message, "GENERATION_ERROR", context)


class ValidationError(MigrationError):
    def __init__(self, message: str, context: dict = None):
        super().__init__(message, "VALIDATION_ERROR", context)
