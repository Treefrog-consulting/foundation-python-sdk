"""Line of business reference data model"""

from .base import BaseModel


class Lob(BaseModel):
    """Line of business reference data model."""

    @property
    def description(self) -> str | None:
        """Longer description of the line of business."""
        return self._data.get("description")
