"""Rows: a list of plain records with DataFrame output."""

from typing import Any

from ._frames import RecordFrames


class Rows(RecordFrames, list[dict[str, Any]]):
    """A list of dicts, one per row, that also converts to a DataFrame.

    Each owner documents its columns. ``as_pandas()`` and ``as_polars()`` take
    the dict keys as columns, in the order the rows give them.

    Example:
        >>> rows = portfolio.metric_results
        >>> rows[0]["metric_name"]
        >>> rows.as_polars()
    """

    def __getitem__(self, key):
        # A slice stays Rows, so it keeps the frame accessors.
        item = super().__getitem__(key)
        return type(self)(item) if isinstance(key, slice) else item

    def _to_records(self) -> list[dict[str, Any]]:
        return list(self)
