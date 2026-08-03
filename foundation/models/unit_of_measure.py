"""Unit of measure reference data model"""

from .base import BaseModel


class UnitOfMeasure(BaseModel):
    """Unit of measure reference data model."""

    @property
    def code(self) -> str | None:
        """Unit code."""
        return self._data.get("code")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the unit is active."""
        return self._data.get("activeFlag")
