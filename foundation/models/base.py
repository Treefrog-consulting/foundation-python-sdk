"""Base model classes"""

from typing import Dict, Any, Optional
from ..utils import parse_datetime_string


class BaseModel:
    """Base class for all models with common functionality."""

    def __init__(self, data: Dict[str, Any]):
        """Initialize with dictionary data."""
        self._data = data

    @property
    def id(self) -> Optional[int]:
        """Object ID."""
        return self._data.get("id")

    @property
    def name(self) -> Optional[str]:
        """Object name."""
        return self._data.get("name")

    def __str__(self) -> str:
        """Return the name if available."""
        return str(self.name) if self.name else f"{self.__class__.__name__}(id={self.id})"

    def __repr__(self) -> str:
        """Return detailed representation."""
        return f"{self.__class__.__name__}(id={self.id}, name={self.name!r})"

    def _get_value(self, key: str) -> Any:
        """Get value from data, parsing datetime strings automatically."""
        value = self._data.get(key)
        return parse_datetime_string(value)

    def describe(self) -> None:
        """
        Display a comprehensive overview of the object.
        
        Shows all available properties and their current values.
        
        Example:
            >>> obj.describe()
        """
        class_name = self.__class__.__name__
        print("=" * 80)
        print(f"{class_name.upper()}: {self.name or 'Untitled'} (ID: {self.id})")
        print("=" * 80)
        
        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-'*30} {'-'*40}")
        
        # Get all properties (not methods or private attributes)
        properties = []
        for attr_name in dir(self):
            if not attr_name.startswith('_') and attr_name not in ['describe']:
                try:
                    attr = getattr(type(self), attr_name, None)
                    if isinstance(attr, property):
                        value = getattr(self, attr_name)
                        properties.append((attr_name, value))
                except:
                    pass
        
        # Sort and display properties
        for prop_name, value in sorted(properties):
            value_str = str(value) if value is not None else "None"
            value_str = value_str[:38] if len(value_str) <= 38 else value_str[:35] + "..."
            print(f"  {prop_name:<30} {value_str:<40}")
        
        print("\n" + "=" * 80 + "\n")
