"""Layered losses accessor for layers."""

from typing import Any

from ._loss_export import _LossExportAccessor

_LAYERED_LOSSES_COLUMNS = (
    "year, sequence, event_id, region_id, peril_id, model_id, "
    "loss, rip_amt, rip_pct, rip_brok_amt, rip_brok_pct"
)

_LAYERED_LOSSES_SELECT = (
    f"SELECT {_LAYERED_LOSSES_COLUMNS}, lob FROM layered_losses WHERE layer_id = ?"
)

# A DuckDB exported by a pre-#3642 server has no lob column; selecting a typed
# NULL keeps the frame shape identical, so old exports simply return lob=None.
_LAYERED_LOSSES_SELECT_NO_LOB = (
    f"SELECT {_LAYERED_LOSSES_COLUMNS}, CAST(NULL AS INTEGER) AS lob "
    "FROM layered_losses WHERE layer_id = ?"
)


class LayeredLosses(_LossExportAccessor):
    """Accessor for layered loss data.

    This class provides access to layered loss data for a specific layer.
    It handles caching of the DuckDB file and lazy loading of data.

    Columns: year, sequence, event_id, region_id, peril_id, model_id, loss,
    rip_amt, rip_pct, rip_brok_amt, rip_brok_pct, lob. lob is null for losses
    without a line of business.

    Calling the accessor directly returns a Polars DataFrame by default:

        >>> config = RunConfiguration(sim_years=10000, vendor_id=1, currency_code="USD")
        >>> df = layer.layered_losses(config)  # returns Polars DataFrame
        >>> print(df.head())

    You can also explicitly request a specific format:

        >>> df_polars = layer.layered_losses.as_polars(config)
        >>> df_pandas = layer.layered_losses.as_pandas(config)
    """

    _noun = "losses"

    def _read_frame(self, con: Any, format_method: str) -> Any:
        select = (
            _LAYERED_LOSSES_SELECT
            if self._has_lob_column(con)
            else _LAYERED_LOSSES_SELECT_NO_LOB
        )
        query = con.execute(select, [self._layer.id])
        return getattr(query, format_method)()

    @staticmethod
    def _has_lob_column(con: Any) -> bool:
        rows = con.execute(
            "SELECT 1 FROM pragma_table_info('layered_losses') WHERE name = 'lob'"
        ).fetchall()
        return bool(rows)

    def describe(self) -> None:
        """Display a summary of this LayeredLosses accessor.

        Example:
            >>> layer.layered_losses.describe()
        """
        print("=" * 60)
        print("LAYERED LOSSES")
        print("=" * 60)
        print(f"  Layer: {self._layer.name} (id={self._layer.id})")
        print(f"  Cached exports (deal-wide): {self._cached_export_count()}")
        print()
        print("USAGE:")
        print("  layer.layered_losses(run_config)  -> polars.DataFrame (default)")
        print()
        print("METHODS:")
        print("  as_polars(run_config) -> polars.DataFrame")
        print("  as_pandas(run_config) -> pandas.DataFrame")
        print("=" * 60)
        print()
