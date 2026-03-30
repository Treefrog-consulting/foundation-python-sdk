"""Collection classes"""

from typing import (
    Union,
    TypeVar,
    Generic,
    overload,
    Optional,
    TYPE_CHECKING,
    Any,
    Dict,
    List,
)

T = TypeVar("T")

if TYPE_CHECKING:
    import pandas as pd


class NamedCollection(list, Generic[T]):
    """A list that supports indexing by name attribute."""

    @overload
    def __getitem__(self, key: int) -> T: ...

    @overload
    def __getitem__(self, key: str) -> T: ...

    def __getitem__(self, key: Union[int, str]) -> T:
        """Get item by index or by name."""
        if isinstance(key, int):
            return super().__getitem__(key)
        elif isinstance(key, str):
            # Search for item with matching name
            for item in self:
                if hasattr(item, "name") and item.name == key:
                    return item
            raise KeyError(f"No item found with name '{key}'")
        else:
            raise TypeError(
                f"Indices must be integers or strings, not {type(key).__name__}"
            )

    def get_by_id(self, item_id: int) -> T:
        """
        Get item by its ID attribute.

        Args:
            item_id: The ID of the item to find

        Returns:
            The item with matching ID

        Raises:
            KeyError: If no item with the specified ID is found

        Example:
            >>> layer = program.layers.get_by_id(1234)
        """
        for item in self:
            if hasattr(item, "id") and item.id == item_id:
                return item
        raise KeyError(f"No item found with id={item_id}")

    def as_pandas(self) -> "pd.DataFrame":
        """Convert collection items to a pandas DataFrame.

        Introspects the contained objects' public properties and creates
        a tabular representation. Works with any contained type that has
        properties (Layer, ProgramBroker, LayerParticipant, Reinstatement, etc.).

        Returns:
            pandas DataFrame with one row per item. Columns are derived from
            the items' public properties (excluding private/internal attributes).

        Raises:
            ImportError: If pandas is not installed.

        Example:
            >>> df = program.layers.as_pandas()
            >>> df = program.brokers.as_pandas()
            >>> df = layer.participants.as_pandas()
            >>> df = layer.reinstatements.as_pandas()
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required to use as_pandas(). "
                "Install it with: pip install pandas"
            )

        if not self:
            return pd.DataFrame()

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

        records: List[Dict[str, Any]] = []
        for item in self:
            row: Dict[str, Any] = {}
            for prop in prop_names:
                try:
                    val = getattr(item, prop)
                    # Skip callable methods and complex objects
                    if callable(val) and not isinstance(
                        getattr(item_type, prop, None), property
                    ):
                        continue
                    # For reference objects (Broker, Participant, etc.), use string representation
                    if hasattr(val, "__dict__") and not isinstance(
                        val, (str, int, float, bool, type(None))
                    ):
                        row[prop] = str(val)
                    else:
                        row[prop] = val
                except Exception:
                    row[prop] = None
            records.append(row)

        return pd.DataFrame(records)

    def __dir__(self):
        """Return list of available attributes for autocomplete."""
        attrs = set(super().__dir__())
        attrs.update(["get_by_id", "as_pandas", "describe"])
        # Add item names for tab-completion awareness
        for item in self:
            if hasattr(item, "name") and item.name:
                attrs.add(item.name)
        return sorted(attrs)

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
        print("  get_by_id(id)  -> item")
        print("  as_pandas()    -> pandas.DataFrame")
        print("  [index]        -> item by index")
        print("  ['name']       -> item by name")
        print("=" * 60)
        print()
