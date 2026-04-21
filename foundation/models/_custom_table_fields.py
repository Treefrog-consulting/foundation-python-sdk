"""Shared constants and helpers for the server's custom-table row shape.

Custom table rows come back from Foundation with parallel arrays of typed
columns ``numValue1..N``, ``textValue1..N``, etc. The column definition
(``valueType`` + ``valueFieldNum``) picks which slot a given column occupies.
This module centralizes that mapping so it isn't re-declared in every consumer.
"""

from typing import Any, Dict, Optional

# valueType id -> raw-row field prefix. Matches the server's CustomField enum.
FIELD_TYPE_PREFIXES: Dict[int, str] = {
    1: "numValue",
    2: "dateValue",
    3: "bitValue",
    4: "textValue",
    5: "lookupValue",
}

LOOKUP_VALUE_TYPE = 5

# Server caps each custom table at 30 columns per type (see CustomTable schema).
MAX_CUSTOM_FIELDS_PER_TYPE = 30

# Tuple for startswith() checks against raw-row keys.
FIELD_PREFIXES_TUPLE = tuple(FIELD_TYPE_PREFIXES.values())


def field_name_for_column(col: Dict[str, Any]) -> Optional[str]:
    """Return the raw-row field name (e.g. ``numValue1``) for a column def, or None."""
    prefix = FIELD_TYPE_PREFIXES.get(col["valueType"])
    if prefix is None:
        return None
    return f"{prefix}{col['valueFieldNum']}"


def initialize_blank_fields(raw_row: Dict[str, Any]) -> None:
    """Populate ``raw_row`` with None for every typed slot the server expects."""
    for prefix in FIELD_TYPE_PREFIXES.values():
        for i in range(1, MAX_CUSTOM_FIELDS_PER_TYPE + 1):
            raw_row[f"{prefix}{i}"] = None
