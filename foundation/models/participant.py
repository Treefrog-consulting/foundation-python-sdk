"""Participant reference data model"""

from .base import BaseModel


class Participant(BaseModel):
    """Participant reference data model."""

    @property
    def external_ref(self) -> str | None:
        """External reference."""
        return self._data.get("externalRef")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the participant is active."""
        return self._data.get("activeFlag")

    def describe(self) -> None:
        """
        Display a comprehensive overview of the participant.

        Shows participant details including name, external reference, and flags.

        Example:
            >>> participant.describe()
        """
        print("=" * 80)
        print(f"PARTICIPANT: {self.name or 'Untitled'} (ID: {self.id})")
        print("=" * 80)

        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-' * 30} {'-' * 40}")
        print(f"  {'id':<30} {self.id!s:<40}")
        print(f"  {'name':<30} {str(self.name or 'None')[:38]:<40}")
        print(f"  {'external_ref':<30} {str(self.external_ref or 'None')[:38]:<40}")
        print(f"  {'sort_order':<30} {self.sort_order or 'None'!s:<40}")
        print(f"  {'active_flag':<30} {self.active_flag!s:<40}")

        print("\n" + "=" * 80 + "\n")
