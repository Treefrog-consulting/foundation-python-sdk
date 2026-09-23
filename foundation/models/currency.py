"""Currency reference data model"""

from .base import BaseModel


class Currency(BaseModel):
    """Currency reference data model."""

    @property
    def code(self) -> str | None:
        """Currency code (e.g., USD, EUR)."""
        return self._data.get("code")

    @property
    def symbol(self) -> str | None:
        """Currency symbol (e.g., $, €)."""
        return self._data.get("symbol")
