"""Program External References - Access to program external system references"""

from typing import Dict, List, Any, Optional, TYPE_CHECKING
from ..utils import camel_to_snake

if TYPE_CHECKING:
    from ..ref_data import ReferenceDataCache


class ExternalRef:
    """Represents a single external reference."""

    def __init__(self, ref_data: Dict[str, Any], ref_cache: "ReferenceDataCache"):
        """
        Initialize external reference.

        Args:
            ref_data: External reference data
            ref_cache: Reference data cache
        """
        self._data = ref_data
        self._ref_cache = ref_cache

    @property
    def id(self) -> Optional[int]:
        """External reference ID."""
        return self._data.get("id")

    @property
    def type_id(self) -> Optional[int]:
        """External reference type ID."""
        return self._data.get("externalRefTypeId")

    @property
    def type_name(self) -> Optional[str]:
        """External reference type name (enriched from reference data)."""
        type_id = self.type_id
        if type_id:
            ref_type = self._ref_cache.get_external_ref_type(type_id)
            if ref_type:
                return ref_type.get("name")
        return None

    @property
    def value(self) -> Optional[str]:
        """External reference value."""
        return self._data.get("value")

    def __repr__(self) -> str:
        """String representation."""
        return f"<ExternalRef: {self.type_name}={self.value}>"


class ProgramExternalRefs:
    """
    Provides access to program external references.
    
    Example:
        >>> # Access all external refs
        >>> for ref in program.external_refs:
        ...     print(f"{ref.type_name}: {ref.value}")
        
        >>> # Access by type name
        >>> program.external_refs.get_by_type("CRM Pro")
        'KGYALF'
        
        >>> # Dot-style access (snake_case)
        >>> program.external_refs.crm_pro
        'KGYALF'
        >>> program.external_refs.microsoft_dynamics_ax
        '934-646327'
    """

    def __init__(self, external_refs: List[Dict[str, Any]], ref_cache: "ReferenceDataCache"):
        """
        Initialize program external references.

        Args:
            external_refs: List of programExternalRefs from program data
            ref_cache: Reference data cache
        """
        self._ref_cache = ref_cache
        self._refs = [ExternalRef(ref, ref_cache) for ref in external_refs]
        
        # Build a map of snake_case type names to values for dot-style access
        self._ref_map: Dict[str, Optional[str]] = {}
        for ref in self._refs:
            if ref.type_name:
                snake_name = camel_to_snake(ref.type_name)
                self._ref_map[snake_name] = ref.value

    def get_by_type(self, type_name: str) -> Optional[str]:
        """
        Get external reference value by type name.

        Args:
            type_name: External reference type name (e.g., "CRM Pro")

        Returns:
            External reference value, or None if not found
        """
        for ref in self._refs:
            if ref.type_name == type_name:
                return ref.value
        return None

    def get_by_type_id(self, type_id: int) -> Optional[str]:
        """
        Get external reference value by type ID.

        Args:
            type_id: External reference type ID

        Returns:
            External reference value, or None if not found
        """
        for ref in self._refs:
            if ref.type_id == type_id:
                return ref.value
        return None

    def __iter__(self):
        """Iterate over external references."""
        return iter(self._refs)

    def __len__(self) -> int:
        """Number of external references."""
        return len(self._refs)

    def __getattr__(self, name: str) -> Optional[str]:
        """
        Dynamic attribute access for external references.

        Args:
            name: External ref type name in snake_case

        Returns:
            External reference value, or None if not found

        Raises:
            AttributeError: If external reference not found
        """
        if name.startswith("_"):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        
        if name in self._ref_map:
            return self._ref_map[name]
        
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def __dir__(self) -> List[str]:
        """
        Return list of available attributes for autocomplete.

        Returns:
            List of external ref type names (snake_case)
        """
        attrs = set(super().__dir__())
        attrs.update(self._ref_map.keys())
        return sorted(list(attrs))

    def __repr__(self) -> str:
        """String representation."""
        types = [ref.type_name for ref in self._refs]
        return f"<ProgramExternalRefs: {types}>"

    def describe(self) -> None:
        """
        Display a comprehensive overview of external references.
        
        Shows all external system references with their types and values.
        
        Example:
            >>> program.external_refs.describe()
        """
        print("=" * 80)
        print("EXTERNAL REFERENCES")
        print("=" * 80)
        
        if not self._refs:
            print("\n(no external references)")
            print("\n" + "=" * 80 + "\n")
            return
        
        print(f"\n{'System Type':<35} {'Snake Case Access':<25} {'Value':<20}")
        print(f"{'-'*35} {'-'*25} {'-'*20}")
        
        for ref in self._refs:
            type_name = ref.type_name or "(unknown)"
            snake_name = camel_to_snake(ref.type_name) if ref.type_name else ""
            value = ref.value or ""
            value_str = value[:18] if len(value) <= 18 else value[:15] + "..."
            print(f"{type_name:<35} {snake_name:<25} {value_str:<20}")
        
        print("\nACCESS METHODS:")
        print("  • Dot notation: external_refs.<snake_case_name>")
        print("  • By type name: external_refs.get_by_type('Type Name')")
        print("  • By type ID: external_refs.get_by_type_id(id)")
        print("  • Iterate: for ref in external_refs: ...")
        
        print("\n" + "=" * 80 + "\n")
