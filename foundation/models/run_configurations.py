"""Collection for stored run configurations."""

from typing import Any

from ._frames import RecordFrames
from .run_configuration import RunConfiguration


class RunConfigurations(RecordFrames):
    """Collection of run configurations for a program.

    Provides access to pre-configured analyses that have been saved in
    the Foundation Platform. These are loaded automatically when retrieving
    a program and can be accessed by label.

    ``as_pandas()`` / ``as_polars()`` give a tabular view of all stored run
    configurations with their key parameters, in columns: label, sim_years,
    vendor_id, currency_code, variant_id, event_set_id, perspective_id,
    participant_id, analysis_type.

    Example:
        >>> # Access all configurations
        >>> for config in program.run_configurations:
        ...     print(f"{config.label}: {config.sim_years} years")

        >>> # Get specific configuration by label
        >>> config = program.run_configurations.get_by_label("Default")
        >>> if config:
        ...     losses = layer.layered_losses.as_polars(config)

        >>> # Tabular view of every configuration
        >>> df = program.run_configurations.as_pandas()
        >>> print(df)
        >>> df = program.run_configurations.as_polars()
    """

    def __init__(self, analyses_data: list[dict[str, Any]]):
        """Initialize collection from API response.

        Args:
            analyses_data: List of analysis dictionaries from analysesByProgram API.
        """
        self._configurations = [
            RunConfiguration.from_dict(data) for data in analyses_data
        ]

    def get_by_label(self, label: str) -> RunConfiguration | None:
        """Get a run configuration by its label.

        Args:
            label: The label to search for (case-sensitive).

        Returns:
            RunConfiguration instance if found, None otherwise.

        Example:
            >>> config = program.run_configurations.get_by_label("Default")
            >>> if config:
            ...     print(f"Using configuration: {config.label}")
            ...     losses = layer.layered_losses.as_polars(config)
        """
        for config in self._configurations:
            if config.label == label:
                return config
        return None

    def __iter__(self):
        """Iterate over all run configurations."""
        return iter(self._configurations)

    def __len__(self) -> int:
        """Get the number of run configurations."""
        return len(self._configurations)

    def __getitem__(self, index: int) -> RunConfiguration:
        """Get a run configuration by index.

        Args:
            index: Index of the configuration to retrieve.

        Returns:
            RunConfiguration instance at the given index.
        """
        return self._configurations[index]

    def _to_records(self) -> list[dict]:
        return [
            {
                "label": c.label,
                "sim_years": c.sim_years,
                "vendor_id": c.vendor_id,
                "currency_code": c.currency_code,
                "variant_id": c.variant_id,
                "event_set_id": c.event_set_id,
                "perspective_id": c.perspective_id,
                "participant_id": c.participant_id,
                "analysis_type": c.analysis_type,
            }
            for c in self._configurations
        ]

    def describe(self) -> None:
        """Display a summary of available run configurations.

        Example:
            >>> program.run_configurations.describe()
        """
        print("=" * 80)
        print(f"RUN CONFIGURATIONS ({len(self._configurations)} configurations)")
        print("=" * 80)
        print()
        print(
            f"  {'Label':<25} {'Sim Years':<12} {'Vendor':<10} {'Currency':<10} {'Analysis':<10}"
        )
        print(f"  {'-' * 25} {'-' * 12} {'-' * 10} {'-' * 10} {'-' * 10}")
        for c in self._configurations:
            label = c.label or "(unlabeled)"
            print(
                f"  {str(label)[:23]:<25} {c.sim_years!s:<12} "
                f"{c.vendor_id!s:<10} {c.currency_code!s:<10} "
                f"{c.analysis_type!s:<10}"
            )
        print()
        print("METHODS:")
        print("  get_by_label(label) -> RunConfiguration")
        print("  as_pandas()         -> pandas.DataFrame")
        print("  as_polars()         -> polars.DataFrame")
        print("=" * 80)
        print()

    def __repr__(self) -> str:
        """String representation of the collection."""
        labels = [c.label or "(unlabeled)" for c in self._configurations]
        return (
            f"RunConfigurations({len(self._configurations)} configurations: {labels})"
        )
