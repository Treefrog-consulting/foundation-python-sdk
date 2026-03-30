"""Custom table row wrapper for trackable modifications"""

from typing import Dict, Any, Callable


class CustomTableRow(dict):
    """
    A dictionary-like class that tracks modifications to custom table rows.
    
    Allows direct modification like:
        row["Breakout"] = 2000
        
    And automatically marks the table as modified in the parent program.
    """
    
    def __init__(self, data: Dict[str, Any], on_modify: Callable):
        """
        Initialize a custom table row.
        
        Args:
            data: Initial row data (column names as keys)
            on_modify: Callback function to call when row is modified
        """
        super().__init__(data)
        self._on_modify = on_modify
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Set item and trigger modification callback."""
        super().__setitem__(key, value)
        self._on_modify()
    
    def update(self, *args, **kwargs) -> None:
        """Update multiple items and trigger modification callback."""
        super().update(*args, **kwargs)
        self._on_modify()
    
    def pop(self, *args) -> Any:
        """Pop item and trigger modification callback."""
        result = super().pop(*args)
        self._on_modify()
        return result
    
    def popitem(self) -> tuple:
        """Pop item and trigger modification callback."""
        result = super().popitem()
        self._on_modify()
        return result
    
    def clear(self) -> None:
        """Clear all items and trigger modification callback."""
        super().clear()
        self._on_modify()
    
    def setdefault(self, key: str, default: Any = None) -> Any:
        """Set default and trigger modification callback if key doesn't exist."""
        had_key = key in self
        result = super().setdefault(key, default)
        if not had_key:
            self._on_modify()
        return result
