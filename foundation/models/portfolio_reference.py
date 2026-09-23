"""The Portfolio module reference data templates are built from, loaded on first use."""

from functools import cached_property
from typing import TYPE_CHECKING, Any

from .base import BaseModel
from .formula_converter import FormulaConverter
from .reference_table import ReferenceTable

if TYPE_CHECKING:
    from .foundation_client import FoundationClient

#: The name the Portfolio UI gives model 0, which a model view can include.
UNKNOWN_MODEL = "Unknown"


class PortfolioReference:
    """Resolves the names a template takes: dimensions, models and model
    views, event split views and template folders."""

    def __init__(self, client: "FoundationClient"):
        self._client = client

    @cached_property
    def formulas(self) -> FormulaConverter:
        return FormulaConverter(self._client)

    @cached_property
    def _dimension_rows(self) -> list[dict[str, Any]]:
        return self._client._fetch("portfolio", "/api/Dimensions")

    @cached_property
    def _active_dimensions(self) -> ReferenceTable:
        return ReferenceTable(self._dimension_rows, BaseModel, "dimensions")

    def dimension_name(self, dimension_id: int) -> str:
        """Any dimension's name, active or not."""
        found = next((d for d in self._dimension_rows if d["id"] == dimension_id), None)
        return found["name"] if found else str(dimension_id)

    def dimension(self, value: int | str) -> BaseModel:
        """An active dimension by ID or name."""
        found = _find(self._active_dimensions, value)
        if found is None:
            names = ", ".join(sorted(d.name for d in self._active_dimensions))
            raise ValueError(
                f"No active dimension is {value!r}. Active dimensions: {names}."
            )
        return found

    def model_id(self, value: int | str) -> int:
        """A model's ID from its ID or name, or 0 for "Unknown".

        An ID stands for any model the platform knows, active or not: a
        template keeps the models it was built with after they are
        deactivated, so its own IDs must be accepted back. A name picks among
        active models only, as ``client.models`` lists them.
        """
        if value in (0, UNKNOWN_MODEL):
            return 0
        if isinstance(value, int):
            if value in self._client._cache()._by_id("models"):
                return value
            raise ValueError(f"No model has ID {value}.")
        found = _find(self._client.models, value)
        if found is None:
            raise ValueError(f"No model is {value!r}; client.models lists them.")
        return found.id

    def model_view(self, value: int | str) -> "BaseModel":
        """An active model view by ID or exact name."""
        views = self._client.model_views
        found = _find(views, value)
        if found is None:
            raise ValueError(
                f"No active model view is {value!r}; client.model_views lists them."
            )
        return found

    def model_view_models(self, view: "BaseModel") -> list[int]:
        """The models a view stands for, as the Portfolio UI expands it: model 0
        first when the view includes unknown models, then the view's models."""
        models = list(self._client.get_model_ids_by_view_id(view.id))
        if view._data.get("includeUnknownModelFlag"):
            models.insert(0, 0)
        return models

    def event_split_view(self, value: int | str) -> "BaseModel | None":
        return _find(self._client.event_split_views, value)

    def lob(self, value: int | str) -> "BaseModel | None":
        """A line of business by ID, name or code."""
        lobs = self._client.lobs
        return _find(lobs, value) or (
            lobs.get_by_code(value) if isinstance(value, str) else None
        )

    def folder_id(self, folder: int | str | None) -> int | None:
        """A template folder's ID from its ID or unique name; None for the top level."""
        if folder is None:
            return None
        folders = [
            row
            for row in self._client._fetch("portfolio", "/api/TemplateDirectories")
            if row.get("templateId") is None
        ]
        if isinstance(folder, int):
            matches = [row for row in folders if row["id"] == folder]
        else:
            matches = [
                row
                for row in folders
                if (row.get("name") or "").casefold() == folder.casefold()
            ]
        if len(matches) != 1:
            state = "no" if not matches else "more than one"
            raise ValueError(
                f"There is {state} template folder {folder!r}; pass a folder ID."
            )
        return matches[0]["id"]


def _find(table: ReferenceTable, value: int | str) -> Any:
    """A reference table's row by ID, or by name in any case."""
    return (
        table.get_by_id(value) if isinstance(value, int) else table.get_by_name(value)
    )
