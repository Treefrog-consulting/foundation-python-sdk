"""Module-level custom table wrapper with on-demand loading."""

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
    from .foundation_client import FoundationClient


class ModuleCustomTable:
    """
    Wrapper for module-level custom tables with lazy loading and pandas integration.

    Module-level custom tables (dataLevelId=1) are not returned as part of program data.
    They are loaded on-demand when first accessed and cached for later use.

    Note: Module-level custom tables are mutable on the client side, but changes
    won't be saved on the backend and won't affect the analysis.
    """

    def __init__(
        self,
        client: FoundationClient,
        ref_cache: ReferenceDataCache,
        table_id: int,
        snake_name: str,
        original_name: str,
        column_names: Sequence[str],
    ) -> None:
        self._client = client
        self._ref_cache = ref_cache
        self._table_id = table_id
        self._snake_name = snake_name
        self._original_name = original_name
        self._column_names = list(column_names)
        self._rows: list[dict[str, Any]] | None = None
        self._frame: pd.DataFrame | None = None
        self._loaded = False

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
        self._ensure_loaded()
        if self._frame is not None:
            return len(self._frame)
        return len(self._rows) if self._rows else 0

    def __bool__(self) -> bool:
        """Truthiness reflects whether the table has any rows."""
        return self.row_count > 0

    def _ensure_loaded(self) -> None:
        """Load table data from API if not already loaded."""
        if self._loaded:
            return

        raw_rows = self._client._fetch(
            "deal",
            f"/api/customTables/getByCustomTableDefinition/{self._table_id}",
        )

        # Process rows using reference cache to build proper column names
        self._rows = []
        for raw_row in raw_rows:
            processed_row = self._ref_cache.build_custom_table_row(
                raw_row, self._table_id
            )
            self._rows.append(processed_row)

        self._loaded = True

    def as_pandas(self) -> pd.DataFrame:
        """
        Return the table as a pandas DataFrame.

        The DataFrame is created once and cached. Subsequent calls return the
        same DataFrame instance, so in-place edits persist.

        Note: Changes to the DataFrame are local only and won't be saved to the backend.
        """
        self._ensure_loaded()

        pd = require("pandas", "as_pandas")
        if self._frame is None:
            frame = pd.DataFrame(self._rows)
            if self._column_names:
                if frame.empty:
                    frame = pd.DataFrame(columns=self._column_names)
                else:
                    frame = frame.reindex(columns=self._column_names)
            self._frame = frame

        return self._frame

    def as_polars(self) -> pl.DataFrame:
        """
        Return the table as a polars DataFrame.

        Columns are the table's declared columns, in declared order. A new
        DataFrame is built on every call, so edits never leak between callers.
        Once as_pandas() has been called its cached frame becomes the source, so
        in-place pandas edits are reflected here.

        Date columns come back as datetimes: a column can hold both date and
        datetime values, which polars cannot mix.

        Note: Changes to the DataFrame are local only and won't be saved to the backend.
        """
        pl = require("polars", "as_polars")

        self._ensure_loaded()
        # to_records() deep-copies so its callers can mutate; this builds fresh
        # dicts below anyway, so read the live rows directly.
        rows = (
            self._frame_to_rows(self._frame)
            if self._frame is not None
            else (self._rows or [])
        )

        if not rows:
            return pl.DataFrame(schema={c: pl.Null for c in self._column_names})

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

    def to_records(self) -> list[dict[str, Any]]:
        """Return table data as a list of dictionaries."""
        self._ensure_loaded()

        if self._frame is not None:
            return self._frame_to_rows(self._frame)
        return copy.deepcopy(self._rows) if self._rows else []

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
                "as_polars",
                "to_records",
                "describe",
            ]
        )
        return sorted(attrs)

    def describe(self) -> None:
        """Display a summary of this module custom table.

        Example:
            >>> client.some_module_table.describe()
        """
        status = "loaded" if self._loaded else "not loaded"
        row_info = str(self.row_count) if self._loaded else "?"
        print("=" * 60)
        print(f"MODULE CUSTOM TABLE: {self._original_name}")
        print("=" * 60)
        print(f"  Snake name:  {self._snake_name}")
        print(f"  Status:      {status}")
        print(f"  Rows:        {row_info}")
        print(f"  Columns:     {self._column_names}")
        print()
        print("METHODS:")
        print("  as_pandas()    -> pandas.DataFrame")
        print("  as_polars()    -> polars.DataFrame")
        print("  to_records()   -> list[dict]")
        print("=" * 60)
        print()

    def __repr__(self) -> str:
        status = "loaded" if self._loaded else "not loaded"
        row_info = f"rows={self.row_count}" if self._loaded else "rows=?"
        return (
            f"ModuleCustomTable(name={self._original_name!r}, "
            f"{row_info}, columns={self._column_names}, status={status})"
        )
