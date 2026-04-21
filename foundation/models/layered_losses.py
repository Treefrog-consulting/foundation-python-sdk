"""Layered losses accessor for layers."""

from typing import TYPE_CHECKING, Any

from ._loss_export import _LossExportAccessor

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

    from .run_configuration import RunConfiguration


_LAYERED_LOSSES_SELECT = (
    "SELECT year, sequence, event_id, region_id, peril_id, model_id, "
    "loss, rip_amt, rip_pct, rip_brok_amt, rip_brok_pct "
    "FROM layered_losses WHERE layer_id = ?"
)


class LayeredLosses(_LossExportAccessor):
    """Accessor for layered loss data.

    This class provides access to layered loss data for a specific layer.
    It handles caching of the DuckDB file and lazy loading of data.

    Calling the accessor directly returns a Polars DataFrame by default:

        >>> config = RunConfiguration(sim_years=10000, vendor_id=1, currency_code="USD")
        >>> df = layer.layered_losses(config)  # returns Polars DataFrame
        >>> print(df.head())

    You can also explicitly request a specific format:

        >>> df_polars = layer.layered_losses.as_polars(config)
        >>> df_pandas = layer.layered_losses.as_pandas(config)
    """

    _noun = "losses"

    def as_polars(self, run_config: "RunConfiguration") -> "pl.DataFrame":
        """Get layered losses as a Polars DataFrame.

        Returns:
            Polars DataFrame with layered losses for this layer.
            Columns: year, sequence, event_id, region_id, peril_id, model_id,
                    loss, rip_amt, rip_pct, rip_brok_amt, rip_brok_pct.

        Raises:
            ImportError: If polars is not installed.
            RuntimeError: If the API call fails or data cannot be loaded.
        """
        return super().as_polars(run_config)

    def as_pandas(self, run_config: "RunConfiguration") -> "pd.DataFrame":
        """Get layered losses as a pandas DataFrame.

        Returns:
            pandas DataFrame with layered losses for this layer.

        Raises:
            ImportError: If pandas is not installed.
            RuntimeError: If the API call fails or data cannot be loaded.
        """
        return super().as_pandas(run_config)

    def _read_frame(self, con: Any, format_method: str) -> Any:
        query = con.execute(_LAYERED_LOSSES_SELECT, [self._layer.id])
        return getattr(query, format_method)()

    def __dir__(self):
        attrs = set(super().__dir__())
        attrs.update(["as_polars", "as_pandas", "describe", "__call__"])
        return sorted(attrs)

    def describe(self) -> None:
        """Display a summary of this LayeredLosses accessor.

        Example:
            >>> layer.layered_losses.describe()
        """
        print("=" * 60)
        print("LAYERED LOSSES")
        print("=" * 60)
        print(f"  Layer: {self._layer.name} (id={self._layer.id})")
        print(f"  Cached configs: {len(self._cache)}")
        print()
        print("USAGE:")
        print("  layer.layered_losses(run_config)  -> polars.DataFrame (default)")
        print()
        print("METHODS:")
        print("  as_polars(run_config) -> polars.DataFrame")
        print("  as_pandas(run_config) -> pandas.DataFrame")
        print("=" * 60)
        print()
