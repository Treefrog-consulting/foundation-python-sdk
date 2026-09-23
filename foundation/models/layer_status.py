"""Layer status reference data model"""

from .base import BaseModel


class LayerStatus(BaseModel):
    """Layer status reference data model."""

    @property
    def code(self) -> str | None:
        """Status code."""
        return self._data.get("code")

    @property
    def description(self) -> str | None:
        """Status description."""
        return self._data.get("description")

    @property
    def seniority(self) -> int | None:
        """Status seniority."""
        return self._data.get("seniority")
