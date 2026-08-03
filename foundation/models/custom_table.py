"""Custom table wrapper that provides pandas integration."""

from __future__ import annotations

import copy
from collections.abc import Sequence
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from ..utils import require

if TYPE_CHECKING:
    import pandas as pd  # pragma: no cover - for type checking only
    import polars as pl

    from ..ref_data import ReferenceDataCache
    from .layer import Layer
    from .program import Program


class CustomTable:
    """Wrapper around custom table data with lazy pandas DataFrame support."""

    def __init__(
        self,
        owner: Program | Layer,
        ref_cache: ReferenceDataCache,
        table_id: int | None,
        snake_name: str,
        original_name: str,
        column_names: Sequence[str],
        initial_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self._owner = owner
        self._ref_cache = ref_cache
        self._table_id = table_id
        self._snake_name = snake_name
        self._original_name = original_name
        self._column_names = list(column_names)
        self._rows: list[dict[str, Any]] = copy.deepcopy(initial_rows or [])
        self._frame: pd.DataFrame | None = None

    @property
    def name(self) -> str:
        """Original table name as defined in Foundation."""
        return self._original_name

    @property
    def snake_case_name(self) -> str:
        """Snake_case name for attribute access."""
        return self._snake_name

    @property
    def columns(self) -> list[str]:
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

    def _owner_is_versioned(self) -> bool:
        """Whether the owning program or layer is a past-version snapshot."""
        return self._owner.is_versioned

    def as_pandas(self) -> pd.DataFrame:
        """
        Return the table as a pandas DataFrame.

        The DataFrame is created once and cached. Subsequent calls return the
        same DataFrame instance, so in-place edits persist.

        On a program or layer read at a past version there is nothing to edit
        into, so each call returns a fresh copy instead.
        """
        pd = require("pandas", "as_pandas")
        if self._frame is None:
            frame = pd.DataFrame(self._rows)
            if self._column_names:
                if frame.empty:
                    frame = pd.DataFrame(columns=self._column_names)
                else:
                    frame = frame.reindex(columns=self._column_names)
            self._frame = frame
        if self._owner_is_versioned():
            return self._frame.copy()
        self._owner._mark_custom_table_modified(self._snake_name)
        return self._frame

    def as_polars(self) -> pl.DataFrame:
        """Return the table as a Polars DataFrame.

        Columns are the table's declared columns, in declared order. A new
        DataFrame is built on every call, so edits never leak between callers.
        Once as_pandas() has been called its cached frame becomes the source, so
        in-place pandas edits are reflected here.

        Date columns come back as datetimes: a column can hold both date and
        datetime values, which polars cannot mix.

        Raises:
            ImportError: If polars is not installed.
        """
        pl = require("polars", "as_polars")

        rows = (
            self._frame_to_rows(self._frame)
            if self._frame is not None
            else (self._rows or [])
        )
        if not rows:
            if self._column_names:
                return pl.DataFrame(schema={c: pl.Null for c in self._column_names})
            return pl.DataFrame()

        # datetime is a subclass of date, so compare the exact type.
        normalized = [
            {
                # A date-only cell widened so polars can hold one column
                # type; naive on purpose, the server stored no time.
                key: datetime(value.year, value.month, value.day)  # noqa: DTZ001
                if type(value) is date
                else value
                for key, value in row.items()
            }
            for row in rows
        ]
        frame = pl.DataFrame(normalized, infer_schema_length=None)

        # A column whose valueType the SDK cannot map is absent from the rows
        # but still declared, so add it back as null rather than raising.
        if self._column_names:
            frame = frame.select(
                [
                    pl.col(c) if c in frame.columns else pl.lit(None).alias(c)
                    for c in self._column_names
                ]
            )
        return frame

    def set_data(self, value: pd.DataFrame | list[dict[str, Any]]) -> None:
        """
        Replace table contents with the provided value.

        Args:
            value: DataFrame or list of dictionaries containing the new rows.
        """
        self._owner._require_writable(f"replace custom table '{self._original_name}'")
        pd = require("pandas", "set_data")
        if isinstance(value, pd.DataFrame):
            frame = value.copy(deep=False)
        else:
            frame = pd.DataFrame(value or [])

        frame = self._align_frame(frame)
        self._frame = frame
        self._rows = self._frame_to_rows(frame)
        self._owner._mark_custom_table_modified(self._snake_name)

    def load_initial_rows(self, rows: list[dict[str, Any]]) -> None:
        """Populate the table with initial rows (used during construction)."""
        if not rows:
            return
        self._owner._require_writable(
            f"add rows to custom table '{self._original_name}'"
        )
        self._rows.extend(copy.deepcopy(rows))
        # Reset cached frame if present so it reflects new data.
        self._frame = None

    def to_records(self) -> list[dict[str, Any]]:
        """Return table data as a list of dictionaries."""
        if self._frame is not None:
            self._rows = self._frame_to_rows(self._frame)
        return copy.deepcopy(self._rows)

    def _align_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Align columns to match the table definition."""
        if not self._column_names:
            return frame

        if frame.empty:
            pd = require("pandas", "this table")
            return pd.DataFrame(columns=self._column_names)

        return frame.reindex(columns=self._column_names)

    def _frame_to_rows(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert a DataFrame into list-of-dict records with None for missing values."""
        if frame.empty:
            return []
        pd = require("pandas", "this table")
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
