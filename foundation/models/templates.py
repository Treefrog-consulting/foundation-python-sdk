"""The collection get_templates() returns."""

from typing import Any

from .collections import NamedCollection
from .template import Template

#: Frame columns: the fields every template header carries, so a frame of
#: every template costs no request per template.
_COLUMNS = (
    "id",
    "name",
    "description",
    "active_flag",
    "pricing_flag",
    "currency_code",
    "sim_years",
    "as_of_date",
    "vendor_id",
    "variant_id",
    "perspective_id",
    "event_set_id",
    "participant_id",
    "event_split_view_id",
    "generate_scenarios_only",
)


class Templates(NamedCollection[Template]):
    """Every template, found by index, name or ID. A template's groups, models,
    dimensions and scenarios load when first read.

    ``as_pandas()`` / ``as_polars()`` columns: id, name, description,
    active_flag, pricing_flag, currency_code, sim_years, as_of_date,
    vendor_id, variant_id, perspective_id, event_set_id, participant_id,
    event_split_view_id, generate_scenarios_only.
    """

    def _to_records(self) -> list[dict[str, Any]]:
        return [{column: getattr(t, column) for column in _COLUMNS} for t in self]
