"""Broker reference data model"""

from .base import BaseModel


class Broker(BaseModel):
    """Broker reference data model."""

    @property
    def external_ref(self) -> str | None:
        """External reference."""
        return self._data.get("externalRef")
