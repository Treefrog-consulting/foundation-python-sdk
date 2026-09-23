"""Custom field definition model"""

from .base import BaseModel


class CustomField(BaseModel):
    """Custom field definition model."""

    @property
    def level(self) -> int | None:
        """Level (1=Program, 2=Layer)."""
        return self._data.get("level")

    @property
    def data_type(self) -> str | None:
        """Data type."""
        return self._data.get("dataType")
