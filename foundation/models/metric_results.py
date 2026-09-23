"""Metric results accessor for layers."""

from typing import TYPE_CHECKING, Any

from ._loss_export import _LossExportAccessor

if TYPE_CHECKING:
    from .run_configuration import RunConfiguration


_METRIC_RESULTS_SELECT = (
    "SELECT metric_id, metric_name, source_name, value "
    "FROM metric_results WHERE layer_id = ?"
)


class MetricResults(_LossExportAccessor):
    """Accessor for metric results data.

    Provides the values shown on the Metrics grid of the deal's Loss Analysis tab
    for a specific layer -- Share EL, ROL, Combined Ratio and so on.

    Columns: metric_id, metric_name, source_name, value.

    Calling the accessor directly returns a Polars DataFrame by default:

        >>> config = program.run_configurations.get_by_label("Default")
        >>> df = layer.metric_results(config)  # returns Polars DataFrame
        >>> print(df.head())

    You can also explicitly request a specific format:

        >>> df_polars = layer.metric_results.as_polars(config)
        >>> df_pandas = layer.metric_results.as_pandas(config)

    A single metric is retrievable by its Deal Module name, its Deal Module id, or
    the source name that links it to the Metrics Module:

        >>> layer.metric_results.get_by_name(config, "Combined Ratio")
        >>> layer.metric_results.get_by_id(config, 24)
        >>> layer.metric_results.get_by_source_name(config, "mmCombinedRatio")

    Each of those re-reads the export, so read the frame once when you want more
    than a couple of metrics rather than looping over the lookups.

    Every value is the string the Foundation Platform stores for that metric.
    Multi-line metrics hold their lines joined by "|". Nothing is parsed or split,
    so the caller decides how to interpret each value. A metric that failed to
    calculate stores its error text in the same way, so a value is not necessarily
    a number even when the metric normally returns one.
    """

    _noun = "metric results"

    def get_by_name(self, run_config: "RunConfiguration", name: str) -> str | None:
        """Get one metric's result by its Deal Module name.

        Args:
            run_config: The run configuration to calculate results for.
            name: Deal Module metric name, matched in full and case-sensitively.
                A multi-line metric's name is a single string holding one line per
                result row, so passing the label of one row will not match it --
                use :meth:`get_by_id` or :meth:`get_by_source_name` for those.

        Returns:
            The metric's value, or None when this layer has no result for it or
            the stored value is null. The two are not distinguishable; read the
            frame if you need to tell them apart.

        Example:
            >>> layer.metric_results.get_by_name(config, "Combined Ratio")
            '0.7569'
        """
        return self._lookup(run_config, "metric_name", name)

    def get_by_id(self, run_config: "RunConfiguration", metric_id: int) -> str | None:
        """Get one metric's result by its Deal Module id.

        Args:
            run_config: The run configuration to calculate results for.
            metric_id: Deal Module metric id.

        Returns:
            The metric's value, or None when this layer has no result for it or
            the stored value is null.

        Example:
            >>> layer.metric_results.get_by_id(config, 16)
            '0.4311'
        """
        return self._lookup(run_config, "metric_id", metric_id)

    def get_by_source_name(
        self, run_config: "RunConfiguration", source_name: str
    ) -> str | None:
        """Get one metric's result by its Metrics Module source name.

        Args:
            run_config: The run configuration to calculate results for.
            source_name: The name this metric is stored under in the Metrics
                Module (e.g. "mmROL"). Case-sensitive.

        Returns:
            The metric's value, or None when this layer has no result for it or
            the stored value is null.

        Example:
            >>> layer.metric_results.get_by_source_name(config, "mmROL")
            '0.4311'
        """
        return self._lookup(run_config, "source_name", source_name)

    def _lookup(
        self, run_config: "RunConfiguration", column: str, key: Any
    ) -> str | None:
        """Return a single metric value, or None when it is absent for this layer."""
        with self._open(run_config) as con:
            self._require_table(con, "metric_results")
            # The column is one of three literals chosen by the calling method, never
            # caller-supplied, so it is safe to interpolate; the values are bound.
            row = con.execute(
                f"SELECT value FROM metric_results WHERE layer_id = ? AND {column} = ?",
                [self._layer.id, key],
            ).fetchone()
        return None if row is None else row[0]

    def _read_frame(self, con: Any, format_method: str) -> Any:
        self._require_table(con, "metric_results")
        query = con.execute(_METRIC_RESULTS_SELECT, [self._layer.id])
        return getattr(query, format_method)()

    def describe(self) -> None:
        """Display a summary of this MetricResults accessor.

        Example:
            >>> layer.metric_results.describe()
        """
        print("=" * 60)
        print("METRIC RESULTS")
        print("=" * 60)
        print(f"  Layer: {self._layer.name} (id={self._layer.id})")
        print(f"  Cached exports (deal-wide): {self._cached_export_count()}")
        print()
        print("USAGE:")
        print("  layer.metric_results(run_config)  -> polars.DataFrame (default)")
        print()
        print("METHODS:")
        print("  as_polars(run_config) -> polars.DataFrame")
        print("  as_pandas(run_config) -> pandas.DataFrame")
        print("  get_by_name(run_config, name) -> str | None")
        print("  get_by_id(run_config, metric_id) -> str | None")
        print("  get_by_source_name(run_config, source_name) -> str | None")
        print("=" * 60)
        print()
