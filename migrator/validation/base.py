from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from migrator.core.models import ValidationCheck


class BaseValidator(ABC):
    """Abstract base class for all validators."""

    @abstractmethod
    def validate(self, target: Any) -> list[ValidationCheck]:
        """Validate the target and return a list of ValidationCheck results."""
        ...
