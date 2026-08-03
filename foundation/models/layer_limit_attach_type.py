"""Layer limit/attachment type reference data model"""

from .base import BaseModel


class LayerLimitAttachType(BaseModel):
    """Layer limit/attachment type reference data model."""

    @property
    def code(self) -> str | None:
        """Type code."""
        return self._data.get("code")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the type is active."""
        return self._data.get("activeFlag")
