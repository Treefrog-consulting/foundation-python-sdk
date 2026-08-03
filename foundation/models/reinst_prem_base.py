"""Reinstatement premium base reference data model"""

from .base import BaseModel


class ReinstPremBase(BaseModel):
    """Reinstatement premium base reference data model."""

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the reinstatement premium base is active."""
        return self._data.get("activeFlag")

    @property
    def default_flag(self) -> bool | None:
        """Whether this is the default reinstatement premium base."""
        return self._data.get("defaultFlag")
