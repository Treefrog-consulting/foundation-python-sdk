"""Participant reference data model"""

from .base import BaseModel


class Participant(BaseModel):
    """Participant reference data model."""

    @property
    def external_ref(self) -> str | None:
        """External reference."""
        return self._data.get("externalRef")
