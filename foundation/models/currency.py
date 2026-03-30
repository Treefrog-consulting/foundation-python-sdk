"""Currency reference data model"""

from typing import Optional
from .base import BaseModel


class Currency(BaseModel):
    """Currency reference data model."""

    @property
    def code(self) -> Optional[str]:
        """Currency code (e.g., USD, EUR)."""
        return self._data.get("code")

    @property
    def symbol(self) -> Optional[str]:
        """Currency symbol (e.g., $, €)."""
        return self._data.get("symbol")

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the currency is active."""
        return self._data.get("activeFlag", False)

    def describe(self) -> None:
        """
        Display a comprehensive overview of the currency.
        
        Shows currency details including code, symbol, and flags.
        
        Example:
            >>> currency.describe()
        """
        print("=" * 80)
        print(f"CURRENCY: {self.name or 'Untitled'} (ID: {self.id})")
        print("=" * 80)
        
        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-'*30} {'-'*40}")
        print(f"  {'id':<30} {str(self.id):<40}")
        print(f"  {'name':<30} {str(self.name or 'None')[:38]:<40}")
        print(f"  {'code':<30} {str(self.code or 'None'):<40}")
        print(f"  {'symbol':<30} {str(self.symbol or 'None'):<40}")
        print(f"  {'sort_order':<30} {str(self.sort_order or 'None'):<40}")
        print(f"  {'active_flag':<30} {str(self.active_flag):<40}")
        
        print("\n" + "=" * 80 + "\n")
