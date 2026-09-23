"""YLT (Year Loss Table) accessor for layers."""

from typing import Any

from ._loss_export import _LossExportAccessor


class Ylt(_LossExportAccessor):
    """Accessor for YLT (Year Loss Table) data.

    This class provides access to YLT custom field data for a specific layer.
    It handles caching of the DuckDB file and lazy loading of data.

    Columns: year, loss, rip_amt, rip_pct, rip_brok_amt, rip_brok_pct, followed by
    one column per YLT custom field on the layer.

    Calling the accessor directly returns a Polars DataFrame by default:

        >>> config = RunConfiguration(sim_years=10000, vendor_id=1, currency_code="USD")
        >>> df = layer.ylt(config)  # returns Polars DataFrame
        >>> print(df.head())

    You can also explicitly request a specific format:

        >>> df_polars = layer.ylt.as_polars(config)
        >>> df_pandas = layer.ylt.as_pandas(config)
    """

    _noun = "YLT data"

    def _read_frame(self, con: Any, format_method: str) -> Any:
        self._require_table(con, "ylt_losses")

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

    def describe(self) -> None:
        """Display a summary of this YLT accessor.

        Example:
            >>> layer.ylt.describe()
        """
        print("=" * 60)
        print("YLT (Year Loss Table)")
        print("=" * 60)
        print(f"  Layer: {self._layer.name} (id={self._layer.id})")
        print(f"  Cached exports (deal-wide): {self._cached_export_count()}")
        print()
        print("USAGE:")
        print("  layer.ylt(run_config)  -> polars.DataFrame (default)")
        print()
        print("METHODS:")
        print("  as_polars(run_config) -> polars.DataFrame")
        print("  as_pandas(run_config) -> pandas.DataFrame")
        print("=" * 60)
        print()
