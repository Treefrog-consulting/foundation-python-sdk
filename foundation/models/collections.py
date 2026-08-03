"""Collection classes"""

from typing import (
    Any,
    overload,
)

from ._frames import RecordFrames


class NamedCollection[T](RecordFrames, list):
    """A list that supports indexing by name attribute.

    ``as_pandas()`` / ``as_polars()`` introspect the contained objects' public
    properties to build a tabular representation, so they work with any
    contained type that has properties (Layer, ProgramBroker, LayerParticipant,
    Reinstatement, etc.). One row per item; the columns are the items' public
    property names, private and internal attributes excluded.

    Example:
        >>> df = program.layers.as_pandas()
        >>> df = program.brokers.as_pandas()
        >>> df = layer.participants.as_pandas()
        >>> df = layer.reinstatements.as_pandas()
        >>> df = program.layers.as_polars()
    """

    @overload
    def __getitem__(self, key: int) -> T: ...

    @overload
    def __getitem__(self, key: str) -> T: ...

    @overload
    def __getitem__(self, key: slice) -> "NamedCollection[T]": ...

    def __getitem__(self, key: int | str | slice) -> "T | NamedCollection[T]":
        """Get an item by index or name, or a sub-collection by slice."""
        if isinstance(key, slice):
            # A slice used to reach the TypeError below, so program.layers[:3]
            # failed on a type that subclasses list. Return the same class so
            # the slice keeps name lookup and the frame accessors.
            return type(self)(super().__getitem__(key))
        if isinstance(key, int):
            return super().__getitem__(key)
        if isinstance(key, str):
            # Search for item with matching name
            for item in self:
                if getattr(item, "name", None) == key:
                    return item
            raise KeyError(f"No item found with name '{key}'")
        raise TypeError(
            f"Indices must be integers, strings or slices, not {type(key).__name__}"
        )

    def __contains__(self, item: object) -> bool:
        """Membership by name as well as by identity.

        ``coll["Layer A"]`` resolved a name while ``"Layer A" in coll`` was
        False, because list compares the string against the objects.
        """
        if super().__contains__(item):
            return True
        return isinstance(item, str) and any(
            getattr(i, "name", None) == item for i in self
        )

    def get_by_id(self, item_id: int) -> T | None:
        """
        Get item by its ID attribute.

        Args:
            item_id: The ID of the item to find

        Returns:
            The item with matching ID, or None if there is none. Every other
            get_by_* in the SDK returns None on a miss; this one raised
            KeyError until 0.12.

        Example:
            >>> layer = program.layers.get_by_id(1234)
        """
        for item in self:
            if getattr(item, "id", None) == item_id:
                return item
        return None

    def get_by_name(self, name: str) -> T | None:
        """
        Get item by its name attribute, case-insensitively.

        Args:
            name: The name of the item to find

        Returns:
            The matching item, or None if there is none. Use ``coll[name]`` to
            raise KeyError instead.

        Example:
            >>> layer = program.layers.get_by_name("Layer 1")
        """
        folded = name.casefold()
        for item in self:
            item_name = getattr(item, "name", None)
            if item_name is not None and item_name.casefold() == folded:
                return item
        return None

    def _to_records(self) -> list[dict[str, Any]]:
        """One dict per item, keyed by the items' public property names."""
        if not self:
            return []

        # Extract property names from the first item's class
        first = self[0]
        item_type = type(first)

        # Collect property names (defined via @property on the class)
        prop_names = []
        for name in dir(item_type):
            if name.startswith("_"):
                continue
            if isinstance(getattr(item_type, name, None), property):
                prop_names.append(name)

        # Also include plain instance attributes that aren't private
        for name in vars(first):
            if not name.startswith("_") and name not in prop_names:
                prop_names.append(name)

        # Sort for consistent column ordering, but put 'id' and 'name' first
        priority = {"id": 0, "name": 1}
        prop_names.sort(key=lambda n: (priority.get(n, 2), n))

        records: list[dict[str, Any]] = []
        for item in self:
            row: dict[str, Any] = {}
            for prop in prop_names:
                try:
                    val = getattr(item, prop)
                    if callable(val):
                        continue
                    # A model object (Broker, Participant, ...) is not a scalar
                    # a frame can hold; no builtin scalar carries __dict__.
                    row[prop] = str(val) if hasattr(val, "__dict__") else val
                except Exception:  # noqa: BLE001 - reflection over arbitrary properties
                    row[prop] = None
            records.append(row)

        return records

    def __dir__(self):
        """Autocomplete over the real methods plus the items' names.

        super() already lists everything defined on the class, so naming the
        methods again only creates a second list to keep in step.
        """
        return sorted(
            set(super().__dir__()) | {i.name for i in self if getattr(i, "name", None)}
        )

    def describe(self) -> None:
        """Display a summary of collection contents.

        Example:
            >>> program.layers.describe()
            >>> program.brokers.describe()
        """
        item_type_name = type(self[0]).__name__ if self else "Item"
        print("=" * 60)
        print(f"COLLECTION: {len(self)} {item_type_name}(s)")
        print("=" * 60)

        if not self:
            print("  (empty)")
        else:
            # Show name and id if available
            for i, item in enumerate(self):
                name = getattr(item, "name", None)
                item_id = getattr(item, "id", None)
                parts = [f"  [{i}]"]
                if item_id is not None:
                    parts.append(f"id={item_id}")
                if name is not None:
                    parts.append(f"name='{name}'")
                if not item_id and not name:
                    parts.append(repr(item))
                print(" ".join(parts))

        print()
        print("METHODS:")
        print("  get_by_id(id)    -> item | None")
        print("  get_by_name(nm)  -> item | None")
        print("  as_pandas()      -> pandas.DataFrame")
        print("  as_polars()      -> polars.DataFrame")
        print("  [index]        -> item by index")
        print("  ['name']       -> item by name")
        print("=" * 60)
        print()
