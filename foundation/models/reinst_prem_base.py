"""Reinstatement premium base reference data model"""

from typing import Optional
from .base import BaseModel


class ReinstPremBase(BaseModel):
    """Reinstatement premium base reference data model."""

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the reinstatement premium base is active."""
        return self._data.get("activeFlag", False)

    @property
    def default_flag(self) -> bool:
        """Whether this is the default reinstatement premium base."""
        return self._data.get("defaultFlag", False)
