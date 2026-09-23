"""Unit of measure reference data model"""

from .base import BaseModel


class UnitOfMeasure(BaseModel):
    """Unit of measure reference data model."""

    @property
    def code(self) -> str | None:
        """Unit code."""
        return self._data.get("code")
