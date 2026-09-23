"""Portfolio model: a rollup's result, with its metric results and loss files."""

from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any

from ..exceptions import PermissionDeniedError
from ..utils import parse_utc_timestamp
from .portfolio_layered_losses import ROOT_SLICE, PortfolioLayeredLosses
from .rows import Rows

if TYPE_CHECKING:
    from .foundation_client import FoundationClient

#: Multi-line metrics separate their line labels with a literal backslash-n in
#: the metric name, and their values with real line breaks.
_LINE_LABEL_SEPARATOR = "\\n"


class Portfolio:
    """A portfolio: the result of one rollup of a template.

    ``metric_results`` and ``slices`` are read on first access and kept, since a
    finished portfolio's results never change. ``layered_losses`` reads a loss
    file on every call.

    Example:
        >>> p = client.get_portfolio(9181)
        >>> p.metric_results.as_polars()
        >>> p.layered_losses(LossFileType.AEP)
        >>> p.layered_losses(LossFileType.EVENT, slice="Europe / Windstorm")
    """

    def __init__(self, data: dict[str, Any], client: "FoundationClient"):
        """Initialize from a Portfolio module ``api/Portfolios`` row."""
        self._data = data
        self._client = client

    @property
    def id(self) -> int:
        """Portfolio ID."""
        return self._data["id"]

    @property
    def name(self) -> str:
        """Portfolio name: the template's name, plus the scenario's for a scenario portfolio."""
        return self._data["name"]

    @property
    def template_id(self) -> int | None:
        """The template the portfolio was created from; None for a run of an unsaved template."""
        return self._data.get("templateId")

    @property
    def active_flag(self) -> bool:
        """Whether this is one of the template's current portfolios."""
        return bool(self._data.get("activeFlag"))

    @property
    def pricing_flag(self) -> bool:
        """Whether deals may be priced against this portfolio."""
        return bool(self._data.get("pricingFlag"))

    @property
    def date_created(self) -> datetime | None:
        """When the portfolio was created, in UTC."""
        return parse_utc_timestamp(self._data.get("dateCreated"))

    @property
    def as_of_date(self) -> datetime | None:
        """The point in time the deals were read at; None for their current state."""
        return parse_utc_timestamp(self._data.get("asOfDate"))

    @property
    def auto_deleted_on(self) -> datetime | None:
        """When the portfolio's results were deleted; its loss files are gone after that."""
        return parse_utc_timestamp(self._data.get("autoDeletedOn"))

    @property
    def metric_results(self) -> Rows:
        """Every metric for the whole portfolio and for each dimension slice.

        Columns: portfolio_id, metric_id, metric_version, metric_name, line,
        dimension, slice, slice_key, value, is_error.

        - ``slice`` is "Full Portfolio" for the whole portfolio, otherwise the
          slice's labels joined by " / ", e.g. "Europe / Windstorm";
          ``dimension`` is None for the whole portfolio.
        - ``slice_key`` addresses the slice's loss files; None where the
          platform cannot locate them (see ``slices``).
        - A multi-line metric gives one row per line, labelled in ``line``;
          ``line`` is None for every other metric.
        - ``value`` is the string the platform stores, or the error text when
          ``is_error`` is True.

        Rows are sorted by metric ID, then in the order the Portfolio UI shows
        the slices.

        Raises:
            PermissionDeniedError: If the user cannot access every deal in the
                portfolio.
        """
        return self._results[0]

    @cached_property
    def _results(self) -> tuple[Rows, Rows]:
        """The metric results and the slices, derived together so that editing
        one list leaves the other intact."""
        try:
            blocks = self._client._fetch(
                "portfolio",
                f"/api/PortfolioAnalysisMetrics/PortfolioAnalysisMetricsByPortfolioId/{self.id}",
                method="POST",
                json=0,
            )
        except PermissionDeniedError as exc:
            raise PermissionDeniedError(
                f"Portfolio {self.id}'s results are visible only to users who can "
                f"access every deal in it. {exc}",
                response=exc.response,
                request=exc.request,
            ) from exc
        rows = Rows(
            row
            for block in sorted(blocks, key=lambda b: b["metricId"])
            for row in _metric_block_rows(self.id, block)
        )
        return rows, _slices(rows)

    @property
    def slices(self) -> Rows:
        """The slices whose loss files ``layered_losses`` can read.

        Columns: dimension, slice, slice_key. The whole portfolio comes first,
        when it has results, then each dimension's slices, parents before their
        children. A slice is missing when the platform cannot locate its files:
        the portfolio's results were deleted, the slice's labels give the same
        key as a sibling's, or the slice was saved without its parent.
        """
        return self._results[1]

    @cached_property
    def layered_losses(self) -> PortfolioLayeredLosses:
        """The portfolio's AEP, OEP and event loss files, for the whole
        portfolio or a slice.

        Example:
            >>> p.layered_losses(LossFileType.AEP)
            >>> p.layered_losses.to_csv(LossFileType.OEP, "oep.csv")
        """
        return PortfolioLayeredLosses(self)

    def describe(self) -> None:
        """Display a summary of this portfolio.

        Example:
            >>> portfolio.describe()
        """
        print("=" * 60)
        print(f"PORTFOLIO: {self.name}")
        print("=" * 60)
        print(f"  ID:          {self.id}")
        print(f"  Template:    {self.template_id or '(unsaved template)'}")
        print(f"  Created:     {self.date_created}")
        print(f"  As of:       {self.as_of_date or '(current deals)'}")
        print(f"  Active:      {self.active_flag}")
        print(f"  Pricing:     {self.pricing_flag}")
        if self.auto_deleted_on:
            print(f"  Deleted on:  {self.auto_deleted_on} (no loss files)")
        print()
        print("PROPERTIES:")
        print("  metric_results  -> Rows")
        print("  slices          -> Rows")
        print()
        print("METHODS:")
        print("  layered_losses(kind, slice=None)          -> polars.DataFrame")
        print("  layered_losses.as_pandas(kind, slice)     -> pandas.DataFrame")
        print("  layered_losses.to_csv(kind, path, slice)  -> pathlib.Path")
        print("=" * 60)
        print()

    def __repr__(self) -> str:
        """String representation of the portfolio."""
        return f"Portfolio(id={self.id}, name={self.name!r}, template_id={self.template_id})"


def _metric_block_rows(
    portfolio_id: int, block: dict[str, Any]
) -> list[dict[str, Any]]:
    """Flatten one metric's tree: its whole-portfolio node, then every slice
    beneath each dimension header, depth first."""
    name = block["name"]
    labels = [label.strip() for label in name.split(_LINE_LABEL_SEPARATOR)]
    # A metric without a whole-portfolio result has a root node with no ID.
    nodes = [(None, ROOT_SLICE, block)] if block.get("id") is not None else []
    for dimension in block.get("children") or []:
        nodes += [
            (dimension["name"], " / ".join(path), node)
            for path, node in _walk(dimension.get("children") or [], ())
        ]

    rows = []
    for dimension, slice_name, node in nodes:
        is_error = bool(node.get("isError"))
        for line, value in _lines(labels, node.get("result"), is_error):
            rows.append(
                {
                    "portfolio_id": portfolio_id,
                    "metric_id": block["metricId"],
                    "metric_version": node.get("metricVersion"),
                    "metric_name": name,
                    "line": line,
                    "dimension": dimension,
                    "slice": slice_name,
                    # Absent on a platform older than v49.0.
                    "slice_key": node.get("s3Key"),
                    "value": value,
                    "is_error": is_error,
                }
            )
    return rows


def _slices(rows: Rows) -> Rows:
    """Each keyed slice once: the root first, then by dimension in the order
    the dimensions appear, and by key, which puts children after their parent."""
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row["slice_key"]
        if key is not None and key not in by_key:
            by_key[key] = {
                "dimension": row["dimension"],
                "slice": row["slice"],
                "slice_key": key,
            }
    dimensions = list(dict.fromkeys(row["dimension"] for row in by_key.values()))
    return Rows(
        sorted(
            by_key.values(),
            key=lambda row: (
                row["dimension"] is not None,
                dimensions.index(row["dimension"]),
                row["slice_key"],
            ),
        )
    )


def _walk(nodes: list[dict[str, Any]], path: tuple[str, ...]):
    """Yield (label path, node) for every slice, parents before children."""
    for node in nodes:
        here = (*path, node["name"])
        yield here, node
        yield from _walk(node.get("children") or [], here)


def _lines(
    labels: list[str], value: str | None, is_error: bool
) -> list[tuple[str | None, str | None]]:
    """Split a multi-line metric's value into (line label, value) pairs.

    Only when the labels and the values agree in number: anything else, an
    error message included, stays one row holding the stored value.
    """
    values = value.splitlines() if isinstance(value, str) else []
    if not is_error and len(labels) > 1 and len(labels) == len(values):
        return list(zip(labels, values, strict=True))
    return [(None, value)]
