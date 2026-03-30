"""Category detail reference data model"""

from typing import Optional
from .base import BaseModel


class CategoryDetail(BaseModel):
    """Category detail reference data model."""

    @property
    def category_id(self) -> Optional[int]:
        """Category ID."""
        return self._data.get("categoryId")

    @property
    def code(self) -> Optional[str]:
        """Category detail code."""
        return self._data.get("code")

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the category detail is active."""
        return self._data.get("activeFlag", False)
