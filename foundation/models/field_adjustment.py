"""FieldAdjustment: set a deal or layer field for the layers that match."""

import dataclasses


@dataclasses.dataclass
class FieldAdjustment:
    """Sets a field to a new value for every layer matching all ``where``
    criteria, or every layer when ``where`` is empty.

    ``field`` is a formula builder field name, as ``client.criteria_fields``
    lists them; ``formula`` and each criterion are written in the builder's
    format. A new value for a lookup field may be its quoted label.

    Example:
        >>> FieldAdjustment("Gross Premium", "Gross Premium * 1.1", where=['Client == "Acme Re"'])
    """

    field: str
    formula: str
    where: list[str] = dataclasses.field(default_factory=list)
    # The stored row an adjustment was read from, so that edits update that row.
    _row_id: int | None = dataclasses.field(default=None, compare=False, repr=False)
