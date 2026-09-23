"""External reference type reference data model"""

from .base import BaseModel


class ExternalRefType(BaseModel):
    """Type of an external system reference held against a deal."""

    @property
    def display_level(self) -> int | None:
        """Level at which the reference is displayed."""
        return self._data.get("displayLevel")

    @property
    def include_in_program_search_flag(self) -> bool | None:
        """Whether references of this type are searchable."""
        return self._data.get("includeInProgramSearchFlag")
