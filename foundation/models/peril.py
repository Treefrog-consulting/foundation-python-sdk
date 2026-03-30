"""Peril reference data model"""

from typing import Optional
from .base import BaseModel


class Peril(BaseModel):
    """Peril reference data model."""

    @property
    def code(self) -> Optional[str]:
        """Short code describing this Peril."""
        return self._data.get("code")

    @property
    def external_ref(self) -> Optional[str]:
        """Optional external system reference."""
        return self._data.get("externalRef")

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the peril is active."""
        return self._data.get("activeFlag", False)

    @property
    def unknown_flag(self) -> bool:
        """Whether this is an unknown peril."""
        return self._data.get("unknownFlag", False)

    def describe(self) -> None:
        """
        Display a comprehensive overview of the peril.
        
        Shows peril details including code, name, and flags.
        
        Example:
            >>> peril.describe()
        """
        print("=" * 80)
        print(f"PERIL: {self.name or 'Untitled'} (ID: {self.id})")
        print("=" * 80)
        
        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-'*30} {'-'*40}")
        print(f"  {'id':<30} {str(self.id):<40}")
        print(f"  {'name':<30} {str(self.name or 'None')[:38]:<40}")
        print(f"  {'code':<30} {str(self.code or 'None'):<40}")
        print(f"  {'external_ref':<30} {str(self.external_ref or 'None')[:38]:<40}")
        print(f"  {'sort_order':<30} {str(self.sort_order or 'None'):<40}")
        print(f"  {'active_flag':<30} {str(self.active_flag):<40}")
        print(f"  {'unknown_flag':<30} {str(self.unknown_flag):<40}")
        
        print("\n" + "=" * 80 + "\n")
