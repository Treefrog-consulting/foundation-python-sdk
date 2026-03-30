"""Unit of measure reference data model"""

from typing import Optional
from .base import BaseModel


class UnitOfMeasure(BaseModel):
    """Unit of measure reference data model."""

    @property
    def code(self) -> Optional[str]:
        """Unit code."""
        return self._data.get("code")

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the unit is active."""
        return self._data.get("activeFlag", False)
