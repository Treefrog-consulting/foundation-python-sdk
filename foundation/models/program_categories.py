"""Program Categories - Dynamic category access for programs"""

from typing import TYPE_CHECKING, Any

from ._init_helpers import raise_if_private, raise_read_only

if TYPE_CHECKING:
    from ..ref_data import ReferenceDataCache


class ProgramCategories:
    """
    Provides dynamic attribute access to program categories.

    Every active category is present, whether or not this deal assigns it, so a
    category the deal has left blank reads as None rather than raising. That
    mirrors the platform's UI, which lists every active category and leaves the
    unset ones empty.

    A category may hold more than one value -- "Modelling Status" is the usual
    one -- and dot access joins those with ", " exactly as the UI displays them.
    Use :meth:`get_values` for the values as a list.

    Example:
        >>> program.categories.modelling_status
        'Standard, Valuation'
        >>> program.categories.get_values("modelling_status")
        ['Standard', 'Valuation']
        >>> program.categories.coverage_type      # active, but unset on this deal
        None
    """

    def __init__(
        self, program_categories: list[dict[str, Any]], ref_cache: "ReferenceDataCache"
    ):
        """
        Initialize program categories.

        Args:
            program_categories: List of programCategories from program data
            ref_cache: Reference data cache
        """
        # Seed every active category, so one the deal leaves unset is still
        # reachable -- an AttributeError there is indistinguishable from a
        # category that does not exist, which is what made this cost a bug
        # report. The cache names them, keyed by ID, so two categories sharing a
        # display name stay distinct.
        #: snake_case category name -> its values, in payload order. A category
        #: with no value on this deal maps to an empty list.
        self._category_map: dict[str, list[str]] = {
            ref_cache.category_snake_name(category): []
            for category in ref_cache.active_categories()
        }

        for prog_cat in program_categories:
            category_id = prog_cat.get("categoryId")
            category_detail_id = prog_cat.get("categoryDetailId")

            if not category_id:
                continue
            category = ref_cache.get_category(category_id)
            if not category:
                continue

            # A category the deal assigns but the platform has retired is not in
            # the seed, so the cache names it here rather than leaving it
            # unreachable -- and gives it its own name, not an active one's.
            values = self._category_map.setdefault(
                ref_cache.category_snake_name(category), []
            )

            if not category_detail_id:
                continue
            detail = ref_cache.get_category_detail(category_detail_id)
            if detail and detail.name:
                # Append: a multi-select category carries one row per value, and
                # assigning here dropped every value but the last until 0.13.
                values.append(detail.name)

    def get_values(self, name: str) -> list[str]:
        """
        Every value assigned to a category, as a list.

        Args:
            name: Category name in snake_case

        Returns:
            The assigned values, in the order the deal document carried them.
            Empty when the category exists but this deal leaves it unset.

        Raises:
            AttributeError: If the category is not known at all

        Example:
            >>> program.categories.get_values("modelling_status")
            ['Standard', 'Valuation']
        """
        if name not in self._category_map:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )
        return list(self._category_map[name])

    def __getattr__(self, name: str) -> str | None:
        """
        Dynamic attribute access for categories.

        Args:
            name: Category name in snake_case

        Returns:
            The category's value, or None when this deal leaves it unset.
            Multiple values are joined with ", ", as the UI shows them; use
            get_values() for them as a list.

        Raises:
            AttributeError: If category not found
        """
        raise_if_private(self, name)

        if name in self._category_map:
            values = self._category_map[name]
            return ", ".join(values) if values else None

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def __setattr__(self, name: str, value: Any) -> None:
        """Refuse a write; categories are read-only through the SDK."""
        raise_read_only(self, name, value, "categories")

    def __dir__(self) -> list[str]:
        """
        Return list of available attributes for autocomplete.

        Returns:
            List of category names (snake_case)
        """
        return sorted(self._category_map)

    def __repr__(self) -> str:
        """String representation."""
        return f"<ProgramCategories: {sorted(self._category_map)}>"

    def describe(self) -> None:
        """
        Display a comprehensive overview of program categories.

        Shows every category this deal can carry, with its values.

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
        print(f"{'-' * 40} {'-' * 40}")

        for category_name in sorted(self._category_map):
            values = self._category_map[category_name]
            value_str = ", ".join(values) if values else "(not set)"
            value_str = (
                value_str[:38] if len(value_str) <= 38 else value_str[:35] + "..."
            )
            print(f"{category_name:<40} {value_str:<40}")

        print("\nACCESS METHODS:")
        print("  • Dot notation: categories.<category_name>")
        print("  • Every value:  categories.get_values('<category_name>')")

        print("\n" + "=" * 80 + "\n")
