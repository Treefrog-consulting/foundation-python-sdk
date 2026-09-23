"""LossAdjustment: scale a scenario's losses."""

from dataclasses import dataclass, field

from .loss_scale import LossScale


@dataclass
class LossAdjustment:
    """The Portfolio UI's Loss Adjustment Filter, scaling all of a scenario's
    losses.

    Each loss is multiplied by the weight of the region and peril row that
    matches it, else of the region row, else of the peril row, else by
    ``weight``; a matching model or line-of-business row then multiplies it
    again. The platform applies only these weights to a scenario's losses,
    so there is no include, exclude or loss-set filter here.

    Example:
        >>> LossAdjustment(weight=0.9, scales=[LossScale(region="Europe", peril="Windstorm", weight=0.5)])
    """

    weight: float = 1.0
    scales: list[LossScale] = field(default_factory=list)
    # The stored row an adjustment was read from, so that edits update that row.
    _row_id: int | None = field(default=None, compare=False, repr=False)
