"""Program Categories - Dynamic category access for programs"""

from typing import Dict, List, Any, Optional, TYPE_CHECKING
from ..utils import camel_to_snake

if TYPE_CHECKING:
    from ..ref_data import ReferenceDataCache


class ProgramCategories:
    """
    Provides dynamic attribute access to program categories.
    
    Example:
        >>> program.categories.modelling_status
        'Valuation'
    """

    def __init__(self, program_categories: List[Dict[str, Any]], ref_cache: "ReferenceDataCache"):
        """
        Initialize program categories.

        Args:
            program_categories: List of programCategories from program data
            ref_cache: Reference data cache
        """
        self._ref_cache = ref_cache
        self._category_map: Dict[str, Optional[str]] = {}
        
        # Build a map of category_name (snake_case) -> category_detail_name
        for prog_cat in program_categories:
            category_id = prog_cat.get("categoryId")
            category_detail_id = prog_cat.get("categoryDetailId")
            
            if category_id:
                category = ref_cache.get_category(category_id)
                if category:
                    category_name = camel_to_snake(category.get("name", ""))
                    
                    if category_detail_id:
                        detail = ref_cache.get_category_detail(category_detail_id)
                        detail_name = detail.get("name") if detail else None
                    else:
                        detail_name = None
                    
                    self._category_map[category_name] = detail_name

    def __getattr__(self, name: str) -> Optional[str]:
        """
        Dynamic attribute access for categories.

        Args:
            name: Category name in snake_case

        Returns:
            Category detail name, or None if not found

        Raises:
            AttributeError: If category not found
        """
        if name.startswith("_"):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        
        if name in self._category_map:
            return self._category_map[name]
        
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def __dir__(self) -> List[str]:
        """
        Return list of available attributes for autocomplete.

        Returns:
            List of category names (snake_case)
        """
        return sorted(list(self._category_map.keys()))

    def __repr__(self) -> str:
        """String representation."""
        return f"<ProgramCategories: {list(self._category_map.keys())}>"

    def describe(self) -> None:
        """
        Display a comprehensive overview of program categories.
        
        Shows all assigned categories with their values.
        
        Example:
            >>> program.categories.describe()
        """
        print("=" * 80)
        print("PROGRAM CATEGORIES")
        print("=" * 80)
        
        if not self._category_map:
            print("\n(no categories assigned)")
            print("\n" + "=" * 80 + "\n")
            return
        
        print(f"\n{'Category':<40} {'Value':<40}")
        print(f"{'-'*40} {'-'*40}")
        
        for category_name, detail_name in sorted(self._category_map.items()):
            value_str = detail_name if detail_name else "(not set)"
            value_str = value_str[:38] if len(value_str) <= 38 else value_str[:35] + "..."
            print(f"{category_name:<40} {value_str:<40}")
        
        print("\nACCESS METHOD:")
        print("  • Dot notation: categories.<category_name>")
        
        print("\n" + "=" * 80 + "\n")
