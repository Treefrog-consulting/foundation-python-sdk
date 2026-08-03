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

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the custom field is active."""
        return self._data.get("activeFlag")
