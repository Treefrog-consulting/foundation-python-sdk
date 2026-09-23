"""Peril reference data model"""

from .base import BaseModel


class Peril(BaseModel):
    """Peril reference data model."""

    @property
    def code(self) -> str | None:
        """Short code describing this Peril."""
        return self._data.get("code")

    @property
    def external_ref(self) -> str | None:
        """Optional external system reference."""
        return self._data.get("externalRef")

    @property
    def unknown_flag(self) -> bool | None:
        """Whether this is an unknown peril."""
        return self._data.get("unknownFlag")
