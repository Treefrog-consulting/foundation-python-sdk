"""Reinstatement premium base reference data model"""

from .base import BaseModel


class ReinstPremBase(BaseModel):
    """Reinstatement premium base reference data model."""

    @property
    def default_flag(self) -> bool | None:
        """Whether this is the default reinstatement premium base."""
        return self._data.get("defaultFlag")
