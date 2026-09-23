"""Reference table collection with id/name/code indexes and DataFrame output."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from requests.structures import CaseInsensitiveDict

from ..utils import resolve_snake_case_collisions
from ._frames import RecordFrames


class ReferenceTable(RecordFrames, list):
    """A reference table: model rows plus id/name/code indexes and DataFrames.

    Subclasses ``list``, so iteration, ``len()``, ``in``, ``bool()``, positional
    indexing and slicing behave exactly as they do for a list. Note that slicing
    returns a plain ``list``, not a ``ReferenceTable``.

    Only active rows are included: a row whose ``activeFlag`` is present and
    falsy is dropped, and a row without the key is kept.

    The DataFrames are derived from the list, so mutating it is reflected in
    them. The ``by_id`` / ``by_name`` / ``by_code`` indexes are built once at
    construction and do NOT track later mutation.

    Example:
        >>> client.brokers[0]
        >>> client.brokers.by_id[46]
        >>> client.brokers.by_name["Aon"]
        >>> client.brokers.as_polars()
    """

    def __init__(
        self,
        rows: Sequence[dict[str, Any]],
        item_factory: Callable[[dict[str, Any]], Any],
        label: str,
    ) -> None:
        # A row carrying a falsy activeFlag is dropped; one without the key kept.
        active = [row for row in rows if row.get("activeFlag", True)]
        super().__init__(item_factory(row) for row in active)

        self._label = label
        self.by_id: dict[Any, Any] = {}
        # Case-insensitive: by_name["Aon"] and by_name["aon"] are the same row,
        # matching the contract of the singular get_* accessors.
        self.by_name: CaseInsensitiveDict[Any] = CaseInsensitiveDict()
        self.by_code: CaseInsensitiveDict[Any] = CaseInsensitiveDict()

        for raw, item in zip(active, self, strict=True):
            row_id = raw.get("id")
            if row_id is not None:
                self.by_id[row_id] = item
            # The model's name, not the raw key: a user record carries no
            # "name" field at all and assembles one from its first and last.
            name = getattr(item, "name", None)
            if name is None:
                name = raw.get("name")
            if name is not None:
                self.by_name[name] = item
            code = raw.get("code")
            if code is not None:
                self.by_code[code] = item

    def get_by_id(self, item_id: Any) -> Any | None:
        """Return the row with this id, or None.

        Same contract as every other get_by_* in the SDK. ``by_id[item_id]``
        raises KeyError instead, as any mapping lookup does.
        """
        return self.by_id.get(item_id)

    def get_by_name(self, name: str) -> Any | None:
        """Return the row with this name, case-insensitively, or None."""
        return self.by_name.get(name)

    def get_by_code(self, code: str) -> Any | None:
        """Return the row with this code, case-insensitively, or None."""
        return self.by_code.get(code)

    def _raw_rows(self) -> list[dict[str, Any]]:
        """Return the payload row behind each model currently in the list.

        Derived from the list itself rather than a stored copy, so a caller who
        mutates the table (append, remove, sort, clear) sees the same rows in
        len(), in iteration and in the DataFrames.
        """
        return [getattr(item, "_data", None) or {} for item in self]

    def _source_keys(self, rows: Sequence[dict[str, Any]]) -> list[str]:
        """Return the payload keys that become columns, in first-seen order.

        Keys whose value is a list or dict in any row are dropped: those are EF
        navigation collections, and polars cannot write nested data to CSV.
        """
        nested = {
            key
            for row in rows
            for key, value in row.items()
            if isinstance(value, (list, dict))
        }
        seen = dict.fromkeys(key for row in rows for key in row)
        return [key for key in seen if key not in nested]

    def to_records(self) -> list[dict[str, Any]]:
        """Return the table as a list of dicts with snake_case keys.

        Values are the raw server values: a date column arrives as an ISO
        string, where the model property would give a ``date`` object.
        ``as_polars()`` and ``as_pandas()`` build their frames from these
        records, a new one on every call, so edits never leak between callers.
        """
        rows = self._raw_rows()
        if not rows:
            return []

        keys = self._source_keys(rows)
        column_names = resolve_snake_case_collisions(keys)
        return [{column_names[key]: row.get(key) for key in keys} for row in rows]

    # What RecordFrames turns into the pandas and polars frames.
    _to_records = to_records

    def describe(self) -> None:
        """Display a summary of the table.

        Example:
            >>> client.brokers.describe()
        """
        # Column names only - no need to materialise every row.
        keys = self._source_keys(self._raw_rows())
        columns = list(resolve_snake_case_collisions(keys).values())

        print("=" * 60)
        print(f"REFERENCE TABLE: {self._label}")
        print("=" * 60)
        print(f"  Rows:     {len(self)}")
        print(f"  Columns:  {columns}")
        print()

        if not self:
            print("  (empty)")
        else:
            for item in self[:10]:
                print(
                    f"  id={getattr(item, 'id', None)} name={getattr(item, 'name', None)!r}"
                )
            if len(self) > 10:
                print(f"  ... and {len(self) - 10} more")

        print()
        print("METHODS:")
        print("  by_id[id]      -> item")
        print("  by_name[name]  -> item (case-insensitive)")
        print("  by_code[code]  -> item (case-insensitive)")
        print("  as_polars()    -> polars.DataFrame")
        print("  as_pandas()    -> pandas.DataFrame")
        print("  to_records()   -> list[dict]")
        print("=" * 60)
        print()

    def __repr__(self) -> str:
        return f"ReferenceTable({self._label!r}, {len(self)} rows)"
