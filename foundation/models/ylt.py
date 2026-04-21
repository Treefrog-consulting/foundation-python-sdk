"""YLT (Year Loss Table) accessor for layers."""

from typing import TYPE_CHECKING, Any

from ._loss_export import _LossExportAccessor

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

    from .run_configuration import RunConfiguration


class Ylt(_LossExportAccessor):
    """Accessor for YLT (Year Loss Table) data.

    This class provides access to YLT custom field data for a specific layer.
    It handles caching of the DuckDB file and lazy loading of data.

    Calling the accessor directly returns a Polars DataFrame by default:

        >>> config = RunConfiguration(sim_years=10000, vendor_id=1, currency_code="USD")
        >>> df = layer.ylt(config)  # returns Polars DataFrame
        >>> print(df.head())

    You can also explicitly request a specific format:

        >>> df_polars = layer.ylt.as_polars(config)
        >>> df_pandas = layer.ylt.as_pandas(config)
    """

    _noun = "YLT data"

    def as_polars(self, run_config: "RunConfiguration") -> "pl.DataFrame":
        """Get YLT data as a Polars DataFrame.

        Returns:
            Polars DataFrame with YLT data for this layer.
            Columns: year, loss, rip_amt, rip_pct, rip_brok_amt, rip_brok_pct,
                    [custom_field_1], [custom_field_2], ...

        Raises:
            ImportError: If polars is not installed.
            RuntimeError: If the API call fails, data cannot be loaded,
                         or YLT table doesn't exist.
        """
        return super().as_polars(run_config)

    def as_pandas(self, run_config: "RunConfiguration") -> "pd.DataFrame":
        """Get YLT data as a pandas DataFrame.

        Returns:
            pandas DataFrame with YLT data for this layer.

        Raises:
            ImportError: If pandas is not installed.
            RuntimeError: If the API call fails, data cannot be loaded,
                         or YLT table doesn't exist.
        """
        return super().as_pandas(run_config)

    def _read_frame(self, con: Any, format_method: str) -> Any:
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'ylt_losses'"
        ).fetchall()
        if not tables:
            raise RuntimeError(
                "No YLT data available for this program. "
                "The DuckDB file does not contain a ylt_losses table."
            )

        columns = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'ylt_losses' ORDER BY ordinal_position"
        ).fetchall()
        select_clause = ", ".join(col[0] for col in columns if col[0] != "layer_id")

        query = con.execute(
            f"SELECT {select_clause} FROM ylt_losses WHERE layer_id = ?",
            [self._layer.id],
        )
        return getattr(query, format_method)()

    def __dir__(self):
        attrs = set(super().__dir__())
        attrs.update(["as_polars", "as_pandas", "describe", "__call__"])
        return sorted(attrs)

    def describe(self) -> None:
        """Display a summary of this YLT accessor.

        Example:
            >>> layer.ylt.describe()
        """
        print("=" * 60)
        print("YLT (Year Loss Table)")
        print("=" * 60)
        print(f"  Layer: {self._layer.name} (id={self._layer.id})")
        print(f"  Cached configs: {len(self._cache)}")
        print()
        print("USAGE:")
        print("  layer.ylt(run_config)  -> polars.DataFrame (default)")
        print()
        print("METHODS:")
        print("  as_polars(run_config) -> polars.DataFrame")
        print("  as_pandas(run_config) -> pandas.DataFrame")
        print("=" * 60)
        print()
