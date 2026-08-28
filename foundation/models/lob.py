"""Line of business reference data model"""

from .base import BaseModel


class Lob(BaseModel):
    """Line of business reference data model."""

    @property
    def description(self) -> str | None:
        """Longer description of the line of business."""
        return self._data.get("description")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the line of business is active."""
        return self._data.get("activeFlag")
