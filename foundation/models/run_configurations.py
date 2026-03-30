"""Collection for stored run configurations."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from .run_configuration import RunConfiguration

if TYPE_CHECKING:
    import pandas as pd


class RunConfigurations:
    """Collection of run configurations for a program.

    Provides access to pre-configured analyses that have been saved in
    the Foundation Platform. These are loaded automatically when retrieving
    a program and can be accessed by label.

    Example:
        >>> # Access all configurations
        >>> for config in program.run_configurations:
        ...     print(f"{config.label}: {config.sim_years} years")

        >>> # Get specific configuration by label
        >>> config = program.run_configurations.get_by_label("Default")
        >>> if config:
        ...     losses = layer.layered_losses.as_polars(config)
    """

    def __init__(self, analyses_data: List[Dict[str, Any]]):
        """Initialize collection from API response.

        Args:
            analyses_data: List of analysis dictionaries from analysesByProgram API.
        """
        self._configurations = [
            RunConfiguration.from_dict(data) for data in analyses_data
        ]

    def get_by_label(self, label: str) -> Optional[RunConfiguration]:
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

    def as_pandas(self) -> "pd.DataFrame":
        """Get run configurations as a pandas DataFrame.

        Returns a tabular view of all stored run configurations with
        their key parameters.

        Returns:
            pandas DataFrame with columns: label, sim_years, vendor_id,
                currency_code, variant_id, event_set_id, perspective_id,
                participant_id, analysis_type

        Raises:
            ImportError: If pandas is not installed.

        Example:
            >>> df = program.run_configurations.as_pandas()
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
        for c in self._configurations:
            records.append(
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
            )
        return pd.DataFrame(records)

    def __dir__(self):
        """Return list of available attributes for autocomplete."""
        attrs = set(super().__dir__())
        attrs.update(
            [
                "get_by_label",
                "as_pandas",
                "describe",
            ]
        )
        return sorted(attrs)

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
                f"  {str(label)[:23]:<25} {str(c.sim_years):<12} "
                f"{str(c.vendor_id):<10} {str(c.currency_code):<10} "
                f"{str(c.analysis_type):<10}"
            )
        print()
        print("METHODS:")
        print("  get_by_label(label) -> RunConfiguration")
        print("  as_pandas()         -> pandas.DataFrame")
        print("=" * 80)
        print()

    def __repr__(self) -> str:
        """String representation of the collection."""
        labels = [c.label or "(unlabeled)" for c in self._configurations]
        return (
            f"RunConfigurations({len(self._configurations)} configurations: {labels})"
        )
