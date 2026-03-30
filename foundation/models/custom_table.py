"""Custom table wrapper that provides pandas integration."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Sequence, Union, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:  # pragma: no cover - for type checking only
    from .program import Program
    from .layer import Layer
    from ..ref_data import ReferenceDataCache


class CustomTable:
    """Wrapper around custom table data with lazy pandas DataFrame support."""

    def __init__(
        self,
        owner: Union["Program", "Layer"],
        ref_cache: "ReferenceDataCache",
        table_id: Optional[int],
        snake_name: str,
        original_name: str,
        column_names: Sequence[str],
        initial_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self._owner = owner
        self._ref_cache = ref_cache
        self._table_id = table_id
        self._snake_name = snake_name
        self._original_name = original_name
        self._column_names = list(column_names)
        self._rows: List[Dict[str, Any]] = copy.deepcopy(initial_rows or [])
        self._frame: Optional[pd.DataFrame] = None

    @property
    def name(self) -> str:
        """Original table name as defined in Foundation."""
        return self._original_name

    @property
    def snake_case_name(self) -> str:
        """Snake_case name for attribute access."""
        return self._snake_name

    @property
    def columns(self) -> List[str]:
        """Column names defined for the table."""
        return list(self._column_names)

    @property
    def row_count(self) -> int:
        """Number of rows currently stored."""
        if self._frame is not None:
            return len(self._frame)
        return len(self._rows)

    def __bool__(self) -> bool:
        """Truthiness reflects whether the table has any rows."""
        return self.row_count > 0

    def as_pandas(self) -> pd.DataFrame:
        """
        Return the table as a pandas DataFrame.

        The DataFrame is created once and cached. Subsequent calls return the
        same DataFrame instance, so in-place edits persist.
        """
        if self._frame is None:
            frame = pd.DataFrame(self._rows)
            if self._column_names:
                if frame.empty:
                    frame = pd.DataFrame(columns=self._column_names)
                else:
                    frame = frame.reindex(columns=self._column_names)
            self._frame = frame
        self._owner._mark_custom_table_modified(self._snake_name)
        return self._frame

    def set_data(self, value: Union[pd.DataFrame, List[Dict[str, Any]]]) -> None:
        """
        Replace table contents with the provided value.

        Args:
            value: DataFrame or list of dictionaries containing the new rows.
        """
        if isinstance(value, pd.DataFrame):
            frame = value.copy(deep=False)
        else:
            frame = pd.DataFrame(value or [])

        frame = self._align_frame(frame)
        self._frame = frame
        self._rows = self._frame_to_rows(frame)
        self._owner._mark_custom_table_modified(self._snake_name)

    def load_initial_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Populate the table with initial rows (used during construction)."""
        if not rows:
            return
        self._rows.extend(copy.deepcopy(rows))
        # Reset cached frame if present so it reflects new data.
        self._frame = None

    def to_records(self) -> List[Dict[str, Any]]:
        """Return table data as a list of dictionaries."""
        if self._frame is not None:
            self._rows = self._frame_to_rows(self._frame)
        return copy.deepcopy(self._rows)

    def _align_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Align columns to match the table definition."""
        if not self._column_names:
            return frame

        if frame.empty:
            return pd.DataFrame(columns=self._column_names)

        return frame.reindex(columns=self._column_names)

    def _frame_to_rows(self, frame: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert a DataFrame into list-of-dict records with None for missing values."""
        if frame.empty:
            return []
        sanitized = frame.where(pd.notna(frame), None)
        return sanitized.to_dict(orient="records")

    def __dir__(self):
        """Return list of available attributes for autocomplete."""
        attrs = set(super().__dir__())
        attrs.update(
            [
                "name",
                "snake_case_name",
                "columns",
                "row_count",
                "as_pandas",
                "set_data",
                "to_records",
                "describe",
            ]
        )
        return sorted(attrs)

    def describe(self) -> None:
        """Display a summary of this custom table.

        Example:
            >>> program.claims_history.describe()
        """
        print("=" * 60)
        print(f"CUSTOM TABLE: {self._original_name}")
        print("=" * 60)
        print(f"  Snake name:  {self._snake_name}")
        print(f"  Rows:        {self.row_count}")
        print(f"  Columns:     {self._column_names}")
        print()
        print("METHODS:")
        print("  as_pandas()    -> pandas.DataFrame")
        print("  set_data(data) -> None")
        print("  to_records()   -> list[dict]")
        print("=" * 60)
        print()

    def __repr__(self) -> str:
        return (
            f"CustomTable(name={self._original_name!r}, "
            f"rows={self.row_count}, columns={self._column_names})"
        )
