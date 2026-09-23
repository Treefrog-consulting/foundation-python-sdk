"""LossScale: a multiplier for the losses in a region, peril, model or line of business."""

from dataclasses import dataclass


@dataclass
class LossScale:
    """Multiplies the matching losses by ``weight``.

    Give a region, a peril or both, optionally narrowed to a line of
    business; or a model or a line of business alone, which multiplies on top
    of the region and peril weights. Each is an ID or a name, and a region
    includes its sub-regions. A model cannot be combined with a region or a
    peril: the platform would ignore it.
    """

    region: int | str | None = None
    peril: int | str | None = None
    model: int | str | None = None
    lob: int | str | None = None
    weight: float = 1.0
