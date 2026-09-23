"""Layer limit/attachment type reference data model"""

from .base import BaseModel


class LayerLimitAttachType(BaseModel):
    """Layer limit/attachment type reference data model."""

    @property
    def code(self) -> str | None:
        """Type code."""
        return self._data.get("code")
