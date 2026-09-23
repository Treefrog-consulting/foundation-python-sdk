"""Region reference data model"""

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from ..ref_data import ReferenceDataCache

from .base import BaseModel


class Region(BaseModel):
    """Region reference data model with hierarchical support."""

    def __init__(
        self, data: dict[str, Any], cache: Optional["ReferenceDataCache"] = None
    ):
        """
        Initialize with data and optional cache reference.

        Args:
            data: Dictionary containing region data
            cache: Optional ReferenceDataCache for lazy loading relationships
        """
        super().__init__(data)
        self._cache = cache

    @property
    def code(self) -> str | None:
        """Short code describing this Region."""
        return self._data.get("code")

    @property
    def external_ref(self) -> str | None:
        """Optional external system reference."""
        return self._data.get("externalRef")

    @property
    def parent_id(self) -> int | None:
        """Parent region ID if this is a subset of another region."""
        return self._data.get("parentId")

    @property
    def unknown_flag(self) -> bool | None:
        """Whether this is an unknown region."""
        return self._data.get("unknownFlag")

    @property
    def parent(self) -> Optional["Region"]:
        """
        Get the parent region if it exists.

        Returns:
            Parent Region instance or None if this is a top-level region
        """
        if self.parent_id is None or self._cache is None:
            return None
        return self._cache.get_region(self.parent_id)

    @property
    def children(self) -> list["Region"]:
        """
        Get direct child regions.

        Returns:
            List of direct child Region instances (empty list if no children)
        """
        if self._cache is None:
            return []

        all_regions = self._cache.get_all_regions()
        return [region for region in all_regions if region.parent_id == self.id]

    def all_descendants(self) -> list["Region"]:
        """
        Get all descendant regions at all levels.

        Returns:
            List of all descendant Region instances (empty list if no descendants)
        """
        descendants = []
        for child in self.children:
            descendants.append(child)
            descendants.extend(child.all_descendants())
        return descendants

    def describe(self) -> None:
        """
        Display a comprehensive overview of the region.

        Shows region details including hierarchical relationships.

        Example:
            >>> region.describe()
        """
        print("=" * 80)
        print(f"REGION: {self.name or 'Untitled'} (ID: {self.id})")
        print("=" * 80)

        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-' * 30} {'-' * 40}")
        print(f"  {'id':<30} {self.id!s:<40}")
        print(f"  {'name':<30} {str(self.name or 'None')[:38]:<40}")
        print(f"  {'code':<30} {self.code or 'None'!s:<40}")
        print(f"  {'external_ref':<30} {str(self.external_ref or 'None')[:38]:<40}")
        print(f"  {'parent_id':<30} {self.parent_id or 'None'!s:<40}")
        print(f"  {'sort_order':<30} {self.sort_order or 'None'!s:<40}")
        print(f"  {'active_flag':<30} {self.active_flag!s:<40}")
        print(f"  {'unknown_flag':<30} {self.unknown_flag!s:<40}")

        print("\nHIERARCHY:")
        if self.parent:
            print(f"  Parent: {self.parent.name} (ID: {self.parent.id})")
        else:
            print("  Parent: (top-level region)")

        children = self.children
        print(f"  Direct children: {len(children)}")
        if children:
            for child in children[:5]:
                print(f"    - {child.code}: {child.name}")
            if len(children) > 5:
                print(f"    ... and {len(children) - 5} more")

        descendants = self.all_descendants()
        print(f"  Total descendants (all levels): {len(descendants)}")

        print("\nAVAILABLE METHODS:")
        print("  • parent - Get parent region")
        print("  • children - Get direct child regions")
        print("  • all_descendants() - Get all descendant regions")

        print("\n" + "=" * 80 + "\n")
