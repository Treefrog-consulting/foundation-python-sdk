"""Client reference data model"""

from typing import Optional
from .base import BaseModel


class Client(BaseModel):
    """Client reference data model."""

    @property
    def external_ref(self) -> Optional[str]:
        """External reference."""
        return self._data.get("externalRef")

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the client is active."""
        return self._data.get("activeFlag", False)

    def describe(self) -> None:
        """
        Display a comprehensive overview of the client.
        
        Shows client details including name, external reference, and flags.
        
        Example:
            >>> client.describe()
        """
        print("=" * 80)
        print(f"CLIENT: {self.name or 'Untitled'} (ID: {self.id})")
        print("=" * 80)
        
        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-'*30} {'-'*40}")
        print(f"  {'id':<30} {str(self.id):<40}")
        print(f"  {'name':<30} {str(self.name or 'None')[:38]:<40}")
        print(f"  {'external_ref':<30} {str(self.external_ref or 'None')[:38]:<40}")
        print(f"  {'sort_order':<30} {str(self.sort_order or 'None'):<40}")
        print(f"  {'active_flag':<30} {str(self.active_flag):<40}")
        
        print("\n" + "=" * 80 + "\n")
