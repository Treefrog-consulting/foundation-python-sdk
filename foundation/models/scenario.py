"""Scenario: a named set of adjustments a template's scenario portfolio applies."""

from dataclasses import dataclass, field

from .field_adjustment import FieldAdjustment
from .loss_adjustment import LossAdjustment


@dataclass
class Scenario:
    """A scenario of a template, the Portfolio UI's Proforma Adjustments.

    Each run of the template creates one extra portfolio per scenario, with
    the scenario's adjustments applied in list order.

    Example:
        >>> Scenario("Premium up", [FieldAdjustment("Gross Premium", "Gross Premium * 1.1")])
    """

    name: str
    adjustments: list[FieldAdjustment | LossAdjustment] = field(default_factory=list)
    # The stored row a scenario was read from, so that edits update that row.
    _row_id: int | None = field(default=None, compare=False, repr=False)
