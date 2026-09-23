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
