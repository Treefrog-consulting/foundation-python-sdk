"""Collection for reference runs from the Deal Module."""

from typing import TYPE_CHECKING, Any, Optional

from ._frames import RecordFrames
from .reference_run import ReferenceRun

if TYPE_CHECKING:
    from ..ref_data import ReferenceDataCache


class ReferenceRuns(RecordFrames, list[ReferenceRun]):
    """Collection of reference runs from the Deal Module.

    Reference runs are pre-stored run configurations that are loaded once
    when the client authenticates and cached for the session. Only active
    reference runs (activeFlag=True) are included, ordered by preference
    group then preference order.

    Subclasses ``list``, so iteration, ``len()``, ``in``, ``bool()``,
    positional indexing and slicing behave exactly as they do for a list.

    ``as_pandas()`` / ``as_polars()`` give a tabular view of all active
    reference runs with their key configuration parameters, in columns: id,
    name, description, vendor_id, variant_id, perspective_id, sim_years,
    event_set_id, model_view_id, preference_group, preference_order.

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

        >>> # Tabular view of every active reference run
        >>> df = client.reference_runs.as_pandas()
        >>> print(df)
        >>> df = client.reference_runs.as_polars()
    """

    def __init__(
        self,
        reference_runs_data: list[dict[str, Any]],
        ref_cache: Optional["ReferenceDataCache"] = None,
    ):
        """Initialize collection from API response.

        Only active reference runs (activeFlag=True) are included.

        Args:
            reference_runs_data: List of reference run dictionaries from API.
            ref_cache: Reference data cache for resolving model IDs.
        """
        # Filter to only active reference runs and create ReferenceRun objects
        runs = [
            ReferenceRun.from_dict(data, ref_cache)
            for data in reference_runs_data
            if data.get("activeFlag")
        ]
        # Sort by preference group and preference order
        runs.sort(key=lambda r: (r.preference_group, r.preference_order))
        super().__init__(runs)
        # Index by ID for quick lookup
        self._by_id = {r.id: r for r in runs}
        # Index by name (case-insensitive) for quick lookup
        self._by_name = {r.name.lower(): r for r in runs}

    def get_by_name(self, name: str) -> ReferenceRun | None:
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

    def get_by_id(self, id: int) -> ReferenceRun | None:
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

    def _to_records(self) -> list[dict]:
        return [
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
            for r in self
        ]

    def __dir__(self):
        """Autocomplete over the real methods plus the runs' names.

        super() already lists everything defined on the class, so naming the
        methods again only creates a second list to keep in step.
        """
        # Add reference run names for tab-completion awareness
        return sorted(set(super().__dir__()) | {r.name for r in self})

    def describe(self) -> None:
        """Display a summary of available reference runs.

        Example:
            >>> client.reference_runs.describe()
        """
        print("=" * 80)
        print(f"REFERENCE RUNS ({len(self)} active)")
        print("=" * 80)
        print()
        print(
            f"  {'ID':<6} {'Name':<30} {'Sim Years':<12} {'Vendor':<10} {'Group':<8} {'Order':<8}"
        )
        print(f"  {'-' * 6} {'-' * 30} {'-' * 12} {'-' * 10} {'-' * 8} {'-' * 8}")
        for r in self:
            print(
                f"  {r.id!s:<6} {str(r.name)[:28]:<30} "
                f"{r.sim_years!s:<12} {r.vendor_id!s:<10} "
                f"{r.preference_group!s:<8} {r.preference_order!s:<8}"
            )
        print()
        print("METHODS:")
        print("  get_by_name(name)  -> ReferenceRun")
        print("  get_by_id(id)      -> ReferenceRun")
        print("  as_pandas()        -> pandas.DataFrame")
        print("  as_polars()        -> polars.DataFrame")
        print("=" * 80)
        print()

    def __repr__(self) -> str:
        """String representation of the collection."""
        names = [r.name for r in self]
        return f"ReferenceRuns({len(self)} active runs: {names})"
