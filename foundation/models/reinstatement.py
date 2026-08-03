"""Reinstatement model"""

from typing import Any


class Reinstatement:
    """Represents a layer reinstatement."""

    def __init__(self, reinst_data: dict[str, Any]):
        self._data = reinst_data

    @property
    def reinst_num(self) -> int | None:
        """Reinstatement number."""
        return self._data.get("reinstNum")

    @property
    def reinst_order(self) -> int | None:
        """Reinstatement order."""
        return self._data.get("reinstOrder")

    @property
    def reinst_percent(self) -> float | None:
        """Reinstatement percentage."""
        return self._data.get("reinstPercent")

    @property
    def brokerage_percent(self) -> float | None:
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
        print(f"  {'-' * 30} {'-' * 40}")
        print(f"  {'reinst_num':<30} {self.reinst_num or 'None'!s:<40}")
        print(f"  {'reinst_order':<30} {self.reinst_order or 'None'!s:<40}")
        print(f"  {'reinst_percent':<30} {self.reinst_percent or 'None'!s:<40}")
        print(f"  {'brokerage_percent':<30} {self.brokerage_percent or 'None'!s:<40}")

        print("\n" + "=" * 80 + "\n")
