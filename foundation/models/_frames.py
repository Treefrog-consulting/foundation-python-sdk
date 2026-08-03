"""Shared pandas/polars accessors for the record-backed collections."""

from typing import TYPE_CHECKING, Any

from ..utils import require

if TYPE_CHECKING:  # pragma: no cover - for type checking only
    import pandas as pd
    import polars as pl


class RecordFrames:
    """Gives a collection ``as_pandas()`` and ``as_polars()`` over its records.

    A collection mixes this in and defines :meth:`_to_records`. The columns of
    both frames are the keys that method produces, so each collection documents
    its own columns in its class docstring.

    Empty is not special-cased: ``DataFrame([])`` and ``DataFrame()`` build the
    same empty frame in both libraries, across the whole supported range
    (pandas >= 2.2.2, polars >= 1.0.0), so one code path serves both.
    """

    def _to_records(self) -> list[dict[str, Any]]:
        """One dict per row, keyed by column name. Supplied by the collection."""
        raise NotImplementedError

    def as_pandas(self) -> "pd.DataFrame":
        """Convert the collection to a pandas DataFrame.

        Returns:
            pandas DataFrame with one row per item, empty when the collection
            is. Columns are named in the class docstring.

        Raises:
            ImportError: If pandas is not installed.
        """
        pd = require("pandas", "as_pandas")
        return pd.DataFrame(self._to_records())

    def as_polars(self) -> "pl.DataFrame":
        """Convert the collection to a Polars DataFrame.

        Same columns as :meth:`as_pandas`.

        Raises:
            ImportError: If polars is not installed.
        """
        pl = require("polars", "as_polars")
        # Mixed and all-None columns have no inferable type from a short
        # prefix, so let Polars scan every row before deciding.
        return pl.DataFrame(self._to_records(), infer_schema_length=None)
