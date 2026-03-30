"""Custom field definition model"""

from typing import Optional
from .base import BaseModel


class CustomField(BaseModel):
    """Custom field definition model."""

    @property
    def level(self) -> Optional[int]:
        """Level (1=Program, 2=Layer)."""
        return self._data.get("level")

    @property
    def data_type(self) -> Optional[str]:
        """Data type."""
        return self._data.get("dataType")

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the custom field is active."""
        return self._data.get("activeFlag", False)
