"""Layer premium type reference data model"""

from typing import Optional
from .base import BaseModel


class LayerPremiumType(BaseModel):
    """Layer premium type reference data model."""

    @property
    def code(self) -> Optional[str]:
        """Type code."""
        return self._data.get("code")

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the type is active."""
        return self._data.get("activeFlag", False)
