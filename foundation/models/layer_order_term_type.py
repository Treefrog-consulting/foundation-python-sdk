"""Layer order term type reference data model"""

from .base import BaseModel


class LayerOrderTermType(BaseModel):
    """Layer order term type, defining up to three configurable terms."""

    @property
    def term1_status(self) -> int | None:
        """Status of the first term."""
        return self._data.get("term1Status")

    @property
    def term2_status(self) -> int | None:
        """Status of the second term."""
        return self._data.get("term2Status")

    @property
    def term3_status(self) -> int | None:
        """Status of the third term."""
        return self._data.get("term3Status")

    @property
    def term1_format(self) -> str | None:
        """Display format of the first term."""
        return self._data.get("term1Format")

    @property
    def term2_format(self) -> str | None:
        """Display format of the second term."""
        return self._data.get("term2Format")

    @property
    def term3_format(self) -> str | None:
        """Display format of the third term."""
        return self._data.get("term3Format")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the type is active."""
        return self._data.get("activeFlag")
