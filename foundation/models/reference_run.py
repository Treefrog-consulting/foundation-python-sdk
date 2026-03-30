"""Reference run configuration for loss analysis.

Reference runs are pre-stored run configurations available from the Deal Module.
They provide a convenient way to run loss analysis without manually specifying all parameters.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from ..ref_data import ReferenceDataCache


class ReferenceRun:
    """Pre-stored run configuration from the Deal Module.

    Reference runs are maintained in the Deal Module and provide pre-configured
    analysis parameters. They are cached once when the client is authenticated
    and can be used directly for loss analysis.

    Attributes:
        id: Unique identifier for the reference run.
        name: User-defined name for this run.
        description: Optional description of this run.
        vendor_id: Vendor ID (e.g., 1 for AIR).
        variant_id: Variant identifier.
        perspective_id: Perspective identifier.
        sim_years: Number of simulation years.
        event_set_id: Event set identifier.
        model_view_id: Model view identifier.
        preference_group: Preference group for organizing reference runs.
        preference_order: Order within preference group (lower values have priority).
        active_flag: Whether this reference run is active.

    Example:
        >>> # Get reference runs from client
        >>> ref_runs = client.reference_runs
        >>> print(f"Available: {len(ref_runs)} reference runs")
        >>>
        >>> # Get by name and convert to RunConfiguration
        >>> ref_run = ref_runs.get_by_name("Standard Analysis")
        >>> if ref_run:
        ...     config = ref_run.to_run_configuration()
        ...     losses = layer.layered_losses.as_polars(config)
    """

    def __init__(
        self,
        id: int,
        name: str,
        description: Optional[str],
        vendor_id: int,
        variant_id: int,
        perspective_id: int,
        sim_years: int,
        event_set_id: int,
        model_view_id: int,
        preference_group: int,
        preference_order: int,
        active_flag: bool,
        ref_cache: Optional["ReferenceDataCache"] = None,
    ):
        """Initialize a reference run.

        Args:
            id: Unique identifier for the reference run.
            name: User-defined name for this run.
            description: Optional description of this run.
            vendor_id: Vendor ID (e.g., 1 for AIR).
            variant_id: Variant identifier.
            perspective_id: Perspective identifier.
            sim_years: Number of simulation years.
            event_set_id: Event set identifier.
            model_view_id: Model view identifier.
            preference_group: Preference group for organizing reference runs.
            preference_order: Order within preference group.
            active_flag: Whether this reference run is active.
            ref_cache: Reference data cache for resolving model IDs.
        """
        self.id = id
        self.name = name
        self.description = description
        self.vendor_id = vendor_id
        self.variant_id = variant_id
        self.perspective_id = perspective_id
        self.sim_years = sim_years
        self.event_set_id = event_set_id
        self.model_view_id = model_view_id
        self.preference_group = preference_group
        self.preference_order = preference_order
        self.active_flag = active_flag
        self._ref_cache = ref_cache

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], ref_cache: Optional["ReferenceDataCache"] = None
    ) -> "ReferenceRun":
        """Create a ReferenceRun from API response data.

        Args:
            data: Dictionary from /api/referenceRuns API response.
            ref_cache: Reference data cache for resolving model IDs.

        Returns:
            ReferenceRun instance.
        """
        return cls(
            id=data.get("id"),
            name=data.get("name"),
            description=data.get("description"),
            vendor_id=data.get("vendorId"),
            variant_id=data.get("variantId"),
            perspective_id=data.get("perspectiveId"),
            sim_years=data.get("simYears"),
            event_set_id=data.get("eventSetId"),
            model_view_id=data.get("modelViewId"),
            preference_group=data.get("preferenceGroup"),
            preference_order=data.get("preferenceOrder"),
            active_flag=data.get("activeFlag", False),
            ref_cache=ref_cache,
        )

    def to_run_configuration(
        self,
        currency_code: str = "USD",
        participant_id: Optional[int] = None,
        model_ids: Optional[List[int]] = None,
        analysis_type: int = 1,
        event_split_view_id: Optional[int] = None,
        apply_loss_splits_flag: Optional[bool] = None,
        as_of_date: Optional[datetime] = None,
        claim_amount: Optional[str] = None,
        historical_flag: Optional[bool] = None,
        event_list: Optional[List[int]] = None,
        claims_history_values: Optional[Dict[str, Any]] = None,
    ) -> "RunConfiguration":
        """Convert this reference run to a RunConfiguration.

        Creates a RunConfiguration object using this reference run's stored
        parameters, plus any additional parameters you provide.

        Model IDs are automatically resolved from the reference run's model_view_id
        using cached reference data.

        Args:
            currency_code: Currency code (e.g., "USD"). Defaults to "USD".
            participant_id: Participant to filter by (None for all participants).
            model_ids: List of model IDs to include. If not provided, will be
                automatically resolved from model_view_id.
            analysis_type: Analysis type (1=Stochastic, 2=Blended, 3=Claims only).
            event_split_view_id: Optional event split view identifier.
            apply_loss_splits_flag: Optional flag for whether to apply loss splits.
            as_of_date: Optional as-of date for analysis.
            claim_amount: Optional claim amount setting.
            historical_flag: Optional flag to include historical events.
            event_list: Optional list of specific events to include.
            claims_history_values: Optional claims history configuration.

        Returns:
            RunConfiguration instance ready for loss analysis.

        Example:
            >>> ref_run = client.reference_runs.get_by_name("Standard Analysis")
            >>> config = ref_run.to_run_configuration()
            >>> losses = layer.layered_losses.as_polars(config)
        """
        from .run_configuration import RunConfiguration

        # Resolve model IDs from model_view_id if not explicitly provided
        resolved_model_ids = model_ids
        if resolved_model_ids is None and self._ref_cache is not None:
            resolved_model_ids = self._ref_cache.get_model_ids_by_view_id(
                self.model_view_id
            )
        if resolved_model_ids is None:
            resolved_model_ids = []

        return RunConfiguration(
            sim_years=self.sim_years,
            vendor_id=self.vendor_id,
            currency_code=currency_code,
            variant_id=self.variant_id,
            event_set_id=self.event_set_id,
            perspective_id=self.perspective_id,
            participant_id=participant_id,
            model_ids=resolved_model_ids,
            analysis_type=analysis_type,
            event_split_view_id=event_split_view_id,
            apply_loss_splits_flag=apply_loss_splits_flag,
            as_of_date=as_of_date,
            claim_amount=claim_amount,
            historical_flag=historical_flag,
            event_list=event_list,
            claims_history_values=claims_history_values,
            label=self.name,
        )

    def __dir__(self):
        """Return list of available attributes for autocomplete."""
        attrs = set(super().__dir__())
        attrs.update(
            [
                "id",
                "name",
                "description",
                "vendor_id",
                "variant_id",
                "perspective_id",
                "sim_years",
                "event_set_id",
                "model_view_id",
                "preference_group",
                "preference_order",
                "active_flag",
                "to_run_configuration",
                "describe",
            ]
        )
        return sorted(attrs)

    def describe(self) -> None:
        """Display a comprehensive overview of this reference run.

        Example:
            >>> ref_run = client.reference_runs.get_by_name("Standard Analysis")
            >>> ref_run.describe()
        """
        print("=" * 60)
        print(f"REFERENCE RUN: {self.name}")
        print("=" * 60)
        print()
        print(f"  {'Property':<25} {'Value':<30}")
        print(f"  {'-' * 25} {'-' * 30}")
        print(f"  {'id':<25} {self.id}")
        print(f"  {'name':<25} {self.name}")
        print(f"  {'description':<25} {self.description or 'None'}")
        print(f"  {'sim_years':<25} {self.sim_years}")
        print(f"  {'vendor_id':<25} {self.vendor_id}")
        print(f"  {'variant_id':<25} {self.variant_id}")
        print(f"  {'perspective_id':<25} {self.perspective_id}")
        print(f"  {'event_set_id':<25} {self.event_set_id}")
        print(f"  {'model_view_id':<25} {self.model_view_id}")
        print(f"  {'preference_group':<25} {self.preference_group}")
        print(f"  {'preference_order':<25} {self.preference_order}")
        print(f"  {'active_flag':<25} {self.active_flag}")
        print()
        print("METHODS:")
        print("  to_run_configuration() -> RunConfiguration")
        print("=" * 60)
        print()

    def __repr__(self) -> str:
        """String representation of the reference run."""
        return (
            f"ReferenceRun(id={self.id}, name='{self.name}', "
            f"sim_years={self.sim_years}, vendor_id={self.vendor_id})"
        )
