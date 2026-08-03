"""Vendor reference data model"""

from .base import BaseModel


class Vendor(BaseModel):
    """Loss module vendor (RMS, AIR, etc.).

    Note: SequenceStartValue is not exposed. The Loss module's /api/ref
    response never populates it, so it would always be None here.
    """

    @property
    def description(self) -> str | None:
        """Vendor description."""
        return self._data.get("description")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the vendor is active."""
        return self._data.get("activeFlag")

    @property
    def default_flag(self) -> bool | None:
        """Whether this is the default vendor. None when the server sends null."""
        return self._data.get("defaultFlag")
