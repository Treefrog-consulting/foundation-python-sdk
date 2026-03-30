"""Reinstatement model"""

from typing import Dict, Any, Optional


class Reinstatement:
    """Represents a layer reinstatement."""

    def __init__(self, reinst_data: Dict[str, Any]):
        self._data = reinst_data

    @property
    def reinst_num(self) -> Optional[int]:
        """Reinstatement number."""
        return self._data.get("reinstNum")

    @property
    def reinst_order(self) -> Optional[int]:
        """Reinstatement order."""
        return self._data.get("reinstOrder")

    @property
    def reinst_percent(self) -> Optional[float]:
        """Reinstatement percentage."""
        return self._data.get("reinstPercent")

    @property
    def brokerage_percent(self) -> Optional[float]:
        """Brokerage percentage."""
        return self._data.get("brokeragePercent")

    def describe(self) -> None:
        """
        Display a comprehensive overview of the reinstatement.
        
        Shows reinstatement details including number, order, and percentages.
        
        Example:
            >>> layer.reinstatements[0].describe()
        """
        print("=" * 80)
        print(f"REINSTATEMENT #{self.reinst_num or 'Unknown'}")
        print("=" * 80)
        
        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-'*30} {'-'*40}")
        print(f"  {'reinst_num':<30} {str(self.reinst_num or 'None'):<40}")
        print(f"  {'reinst_order':<30} {str(self.reinst_order or 'None'):<40}")
        print(f"  {'reinst_percent':<30} {str(self.reinst_percent or 'None'):<40}")
        print(f"  {'brokerage_percent':<30} {str(self.brokerage_percent or 'None'):<40}")
        
        print("\n" + "=" * 80 + "\n")
