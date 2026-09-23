"""Category reference data model"""

from .base import BaseModel


class Category(BaseModel):
    """Category reference data model."""

    @property
    def code(self) -> str | None:
        """Category code."""
        return self._data.get("code")

    @property
    def multi_select_flag(self) -> bool | None:
        """Whether a deal may assign this category more than one value.

        ``program.categories`` reports whatever the deal carries, so two values
        read the same way whether the category permits them or not; this is what
        tells the two apart.
        """
        return self._data.get("multiSelectFlag")
