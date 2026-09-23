"""Client reference data model"""

from .base import BaseModel


class Client(BaseModel):
    """Client reference data model."""

    @property
    def external_ref(self) -> str | None:
        """External reference."""
        return self._data.get("externalRef")
