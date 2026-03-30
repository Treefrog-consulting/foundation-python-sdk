"""Collection for reference runs from the Deal Module."""

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from .reference_run import ReferenceRun

if TYPE_CHECKING:
    import pandas as pd

    from ..ref_data import ReferenceDataCache


class ReferenceRuns:
    """Collection of reference runs from the Deal Module.

    Reference runs are pre-stored run configurations that are loaded once
    when the client authenticates and cached for the session. Only active
    reference runs (activeFlag=True) are included.

    Example:
        >>> # Access all reference runs
        >>> for ref_run in client.reference_runs:
        ...     print(f"{ref_run.name}: {ref_run.sim_years} years")

        >>> # Get specific reference run by name
        >>> ref_run = client.reference_runs.get_by_name("Standard Analysis")
        >>> if ref_run:
        ...     config = ref_run.to_run_configuration()
        ...     losses = layer.layered_losses.as_polars(config)

        >>> # Get by ID
        >>> ref_run = client.reference_runs.get_by_id(1)
    """

    def __init__(
        self,
        reference_runs_data: List[Dict[str, Any]],
        ref_cache: Optional["ReferenceDataCache"] = None,
    ):
        """Initialize collection from API response.

        Only active reference runs (activeFlag=True) are included.

        Args:
            reference_runs_data: List of reference run dictionaries from API.
            ref_cache: Reference data cache for resolving model IDs.
        """
        self._ref_cache = ref_cache

        # Filter to only active reference runs and create ReferenceRun objects
        self._reference_runs = [
            ReferenceRun.from_dict(data, ref_cache)
            for data in reference_runs_data
            if data.get("activeFlag", False)
        ]
        # Sort by preference group and preference order
        self._reference_runs.sort(
            key=lambda r: (r.preference_group, r.preference_order)
        )
        # Index by ID for quick lookup
        self._by_id = {r.id: r for r in self._reference_runs}
        # Index by name (case-insensitive) for quick lookup
        self._by_name = {r.name.lower(): r for r in self._reference_runs}

    def get_by_name(self, name: str) -> Optional[ReferenceRun]:
        """Get a reference run by its name.

        Args:
            name: The name to search for (case-insensitive).

        Returns:
            ReferenceRun instance if found, None otherwise.

        Example:
            >>> ref_run = client.reference_runs.get_by_name("Standard Analysis")
            >>> if ref_run:
            ...     print(f"Found: {ref_run.name}")
            ...     config = ref_run.to_run_configuration()
        """
        return self._by_name.get(name.lower())

    def get_by_id(self, id: int) -> Optional[ReferenceRun]:
        """Get a reference run by its ID.

        Args:
            id: The ID to search for.

        Returns:
            ReferenceRun instance if found, None otherwise.

        Example:
            >>> ref_run = client.reference_runs.get_by_id(1)
            >>> if ref_run:
            ...     print(f"Found: {ref_run.name}")
        """
        return self._by_id.get(id)

    def __iter__(self):
        """Iterate over all reference runs."""
        return iter(self._reference_runs)

    def __len__(self) -> int:
        """Get the number of reference runs."""
        return len(self._reference_runs)

    def __getitem__(self, index: int) -> ReferenceRun:
        """Get a reference run by index.

        Args:
            index: Index of the reference run to retrieve.

        Returns:
            ReferenceRun instance at the given index.
        """
        return self._reference_runs[index]

    def as_pandas(self) -> "pd.DataFrame":
        """Get reference runs as a pandas DataFrame.

        Returns a tabular view of all active reference runs with their
        key configuration parameters.

        Returns:
            pandas DataFrame with columns: id, name, description,
                vendor_id, variant_id, perspective_id, sim_years,
                event_set_id, model_view_id, preference_group,
                preference_order

        Raises:
            ImportError: If pandas is not installed.

        Example:
            >>> df = client.reference_runs.as_pandas()
            >>> print(df)
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required to use as_pandas(). "
                "Install it with: pip install pandas"
            )

        records = []
        for r in self._reference_runs:
            records.append(
                {
                    "id": r.id,
                    "name": r.name,
                    "description": r.description,
                    "vendor_id": r.vendor_id,
                    "variant_id": r.variant_id,
                    "perspective_id": r.perspective_id,
                    "sim_years": r.sim_years,
                    "event_set_id": r.event_set_id,
                    "model_view_id": r.model_view_id,
                    "preference_group": r.preference_group,
                    "preference_order": r.preference_order,
                }
            )
        return pd.DataFrame(records)

    def __dir__(self):
        """Return list of available attributes for autocomplete."""
        attrs = set(super().__dir__())
        attrs.update(
            [
                "get_by_name",
                "get_by_id",
                "as_pandas",
                "describe",
            ]
        )
        # Add reference run names for tab-completion awareness
        for r in self._reference_runs:
            attrs.add(r.name)
        return sorted(attrs)

    def describe(self) -> None:
        """Display a summary of available reference runs.

        Example:
            >>> client.reference_runs.describe()
        """
        print("=" * 80)
        print(f"REFERENCE RUNS ({len(self._reference_runs)} active)")
        print("=" * 80)
        print()
        print(
            f"  {'ID':<6} {'Name':<30} {'Sim Years':<12} {'Vendor':<10} {'Group':<8} {'Order':<8}"
        )
        print(f"  {'-' * 6} {'-' * 30} {'-' * 12} {'-' * 10} {'-' * 8} {'-' * 8}")
        for r in self._reference_runs:
            print(
                f"  {str(r.id):<6} {str(r.name)[:28]:<30} "
                f"{str(r.sim_years):<12} {str(r.vendor_id):<10} "
                f"{str(r.preference_group):<8} {str(r.preference_order):<8}"
            )
        print()
        print("METHODS:")
        print("  get_by_name(name)  -> ReferenceRun")
        print("  get_by_id(id)      -> ReferenceRun")
        print("  as_pandas()        -> pandas.DataFrame")
        print("=" * 80)
        print()

    def __repr__(self) -> str:
        """String representation of the collection."""
        names = [r.name for r in self._reference_runs]
        return f"ReferenceRuns({len(self._reference_runs)} active runs: {names})"
