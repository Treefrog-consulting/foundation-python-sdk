"""Category reference data model"""

from .base import BaseModel


class Category(BaseModel):
    """Category reference data model."""

    @property
    def code(self) -> str | None:
        """Category code."""
        return self._data.get("code")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the category is active."""
        return self._data.get("activeFlag")
