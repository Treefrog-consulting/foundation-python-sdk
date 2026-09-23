"""Custom table definition reference data model"""

from .base import BaseModel


class CustomTableDefinition(BaseModel):
    """Definition of a custom table.

    This is the definition only. For the rows of a module-level custom table
    use ``client.get_custom_table(name)``; for a deal- or layer-level table use
    ``program.get_custom_table(name)`` / ``layer.get_custom_table(name)``.
    """

    @property
    def description(self) -> str | None:
        """Table description."""
        return self._data.get("description")

    @property
    def data_level_id(self) -> int | None:
        """Level the table belongs to (1 = module level)."""
        return self._data.get("dataLevelId")

    @property
    def page_id(self) -> int | None:
        """UI page the table is displayed on."""
        return self._data.get("pageId")
