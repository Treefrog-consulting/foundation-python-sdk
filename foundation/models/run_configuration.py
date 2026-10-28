"""Run configuration for loss export."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _freeze(value: Any) -> Any:
    """Return a hashable stand-in for an arbitrarily nested payload.

    claims_history_values is a free-form dict, and the previous one-level
    conversion raised "unhashable type: dict" on a nested one -- from inside
    the export cache key, so passing a documented argument broke every
    layered_losses, ylt and metric_results read.
    """
    if isinstance(value, dict):
        # Ordered by a stable string form: claims_history_values is free-form,
        # and comparing an int key against a str one raises.
        return tuple(
            sorted(
                ((k, _freeze(v)) for k, v in value.items()),
                key=lambda kv: repr(kv[0]),
            )
        )
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value


@dataclass(kw_only=True, eq=False, repr=False)
class RunConfiguration:
    """Configuration for running loss analysis.

    This class encapsulates all the parameters needed to run a loss analysis
    and export layered losses and YLT data.

    Every argument is keyword-only, and the nine through ``analysis_type`` are
    required: six of them are bare ints, so a positional call could transpose
    two and export a different analysis without any error.

    Args:
        sim_years: Number of simulation years (required).
        vendor_id: Vendor ID (e.g., 1 for AIR) (required).
        currency_code: Currency code (e.g., "USD") (required).
        variant_id: Variant identifier (required).
        event_set_id: Event set identifier (required).
        perspective_id: Perspective identifier (required).
        participant_id: Participant to filter by (required, can be None for all participants).
        model_ids: List of model IDs to include (required, can be empty list for all models).
        analysis_type: Analysis type (required).
        event_split_view_id: Optional event split view identifier.
        apply_loss_splits_flag: Optional flag for whether to apply loss splits.
        as_of_date: Optional as-of date for analysis.
        claim_amount: Optional claim amount setting.
        historical_flag: Optional flag to include historical events.
        distribute_claims: Optional flag to draw each claim's reserves with secondary uncertainty (Blended/Claims-only analyses).
        event_list: Optional list of specific events to include.
        claims_history_values: Optional claims history configuration. The
            platform reads the claims it distributes from the deal, not from
            here.
        label: Optional label for this configuration (e.g., "Default", "LabelB").

    Example:
        >>> config = RunConfiguration(
        ...     sim_years=10000,
        ...     vendor_id=1,
        ...     currency_code="USD",
        ...     variant_id=1,
        ...     event_set_id=5,
        ...     perspective_id=1,
        ...     participant_id=None,   # None means every participant
        ...     model_ids=[],          # empty means every model
        ...     analysis_type=1,       # 1=Stochastic, 2=Blended, 3=Claims only
        ... )
        >>> losses_df = layer.layered_losses.as_polars(config)

    Prefer a stored configuration where one exists:

        >>> config = program.run_configurations.get_by_label("Default")
    """

    sim_years: int
    vendor_id: int
    currency_code: str
    variant_id: int | None
    event_set_id: int | None
    perspective_id: int | None
    participant_id: int | None
    model_ids: list[int]
    analysis_type: int | None
    event_split_view_id: int | None = None
    apply_loss_splits_flag: bool | None = None
    as_of_date: datetime | None = None
    claim_amount: str | None = None
    historical_flag: bool | None = None
    distribute_claims: bool | None = None
    event_list: list[int] | None = None
    claims_history_values: dict[str, Any] | None = None
    label: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunConfiguration":
        """Create a RunConfiguration from API response data.

        Args:
            data: Dictionary from analysesByProgram API response.

        Returns:
            RunConfiguration instance.

        Example:
            >>> analysis_data = {...}  # From API response
            >>> config = RunConfiguration.from_dict(analysis_data)
            >>> losses = layer.layered_losses.as_polars(config)
        """
        return cls(
            sim_years=data.get("simYears"),
            vendor_id=data.get("vendorId"),
            currency_code=data.get("currencyCode"),
            variant_id=data.get("variantId"),
            event_set_id=data.get("eventSetId"),
            perspective_id=data.get("perspectiveId"),
            participant_id=data.get("participantId"),
            model_ids=data.get("modelIds", []),
            analysis_type=data.get("analysisType"),
            event_split_view_id=data.get("eventSplitViewId"),
            apply_loss_splits_flag=data.get("applyLossSplitsFlag"),
            distribute_claims=data.get("distributeClaims"),
            label=data.get("label"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary for API request.

        Returns:
            Dictionary representation suitable for API request.
        """
        result = {
            "simYears": self.sim_years,
            "vendorId": self.vendor_id,
            "currencyCode": self.currency_code,
            "variantId": self.variant_id,
            "eventSetId": self.event_set_id,
            "perspectiveId": self.perspective_id,
            "participantId": self.participant_id,
            "modelIds": self.model_ids,
            "analysisType": self.analysis_type,
            "asOfDate": self.as_of_date.isoformat() if self.as_of_date else None,
            "claimAmount": self.claim_amount,
            "historicalFlag": self.historical_flag,
            "distributeClaims": self.distribute_claims,
            "eventList": self.event_list,
            "claimsHistoryValues": self.claims_history_values,
            "eventSplitViewId": self.event_split_view_id,
            "applyLossSplitsFlag": self.apply_loss_splits_flag,
        }
        return result

    def __eq__(self, other: object) -> bool:
        """Check equality with another RunConfiguration.

        Args:
            other: Another object to compare with.

        Returns:
            True if configurations are equal, False otherwise.
        """
        if not isinstance(other, RunConfiguration):
            return False
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        """Generate hash for use in dictionaries and sets.

        Returns:
            Hash value based on configuration parameters.
        """
        return hash(_freeze(self.to_dict()))

    def describe(self) -> None:
        """Display a comprehensive overview of this run configuration.

        Example:
            >>> config = program.run_configurations.get_by_label("Default")
            >>> config.describe()
        """
        label_display = self.label or "(unlabeled)"
        print("=" * 60)
        print(f"RUN CONFIGURATION: {label_display}")
        print("=" * 60)
        print()
        print(f"  {'Property':<25} {'Value':<30}")
        print(f"  {'-' * 25} {'-' * 30}")
        print(f"  {'label':<25} {label_display}")
        print(f"  {'sim_years':<25} {self.sim_years}")
        print(f"  {'vendor_id':<25} {self.vendor_id}")
        print(f"  {'currency_code':<25} {self.currency_code}")
        print(f"  {'variant_id':<25} {self.variant_id}")
        print(f"  {'event_set_id':<25} {self.event_set_id}")
        print(f"  {'perspective_id':<25} {self.perspective_id}")
        print(f"  {'participant_id':<25} {self.participant_id}")
        print(f"  {'model_ids':<25} {self.model_ids}")
        print(f"  {'analysis_type':<25} {self.analysis_type}")
        if self.event_split_view_id is not None:
            print(f"  {'event_split_view_id':<25} {self.event_split_view_id}")
        if self.apply_loss_splits_flag is not None:
            print(f"  {'apply_loss_splits_flag':<25} {self.apply_loss_splits_flag}")
        if self.as_of_date is not None:
            print(f"  {'as_of_date':<25} {self.as_of_date}")
        if self.claim_amount is not None:
            print(f"  {'claim_amount':<25} {self.claim_amount}")
        if self.historical_flag is not None:
            print(f"  {'historical_flag':<25} {self.historical_flag}")
        if self.distribute_claims is not None:
            print(f"  {'distribute_claims':<25} {self.distribute_claims}")
        if self.event_list is not None:
            print(f"  {'event_list':<25} {self.event_list}")
        if self.claims_history_values is not None:
            print(f"  {'claims_history_values':<25} {self.claims_history_values}")
        print()
        print("METHODS:")
        print("  to_dict() -> dict")
        print("=" * 60)
        print()

    def __repr__(self) -> str:
        """String representation of the configuration.

        Returns:
            String representation.
        """
        label_str = f", label='{self.label}'" if self.label else ""
        return (
            f"RunConfiguration(sim_years={self.sim_years}, "
            f"vendor_id={self.vendor_id}, "
            f"currency_code='{self.currency_code}', "
            f"variant_id={self.variant_id}, "
            f"analysis_type={self.analysis_type}{label_str})"
        )
