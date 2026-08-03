"""Category detail reference data model"""

from .base import BaseModel


class CategoryDetail(BaseModel):
    """Category detail reference data model."""

    @property
    def category_id(self) -> int | None:
        """Category ID."""
        return self._data.get("categoryId")

    @property
    def code(self) -> str | None:
        """Category detail code."""
        return self._data.get("code")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the category detail is active."""
        return self._data.get("activeFlag")
