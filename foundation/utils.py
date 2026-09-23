"""Utility functions for the Foundation Platform SDK"""

import re
from datetime import UTC, datetime
from typing import Any


def ref_values(payload: Any, key: str) -> list[dict[str, Any]]:
    """
    Return the rows for a reference key in a /api/ref payload, tolerating nulls.

    The server declares both the entity envelope and its Values as nullable, so
    ``{"peril": null}`` and ``{"peril": {"values": null}}`` are equally legal
    and must not raise. A null payload is tolerated too.

    Args:
        payload: A module's /api/ref response, or None
        key: The entity key, e.g. "peril"

    Returns:
        List of raw row dicts, empty if anything along the path is absent or null
    """
    if not payload:
        return []
    return (payload.get(key) or {}).get("values") or []


#: Which extra installs each optional dependency. polars output also needs
#: pyarrow, because duckdb's .pl() converts through Arrow.
_EXTRA_FOR_MODULE = {
    "pandas": "pandas",
    "polars": "polars",
    "pyarrow": "polars",
    "duckdb": "duckdb",
}


def require(module_name: str, method_name: str) -> Any:
    """
    Import an optional-at-runtime dependency, with an actionable error.

    Args:
        module_name: Module to import, e.g. "polars"
        method_name: SDK method needing it, used in the error message

    Returns:
        The imported module

    Raises:
        ImportError: If the module is not installed
    """
    try:
        return __import__(module_name)
    except ImportError as exc:
        # .get, not [...]: a KeyError raised while handling this ImportError
        # would replace the real cause with a lookup failure.
        extra = _EXTRA_FOR_MODULE.get(module_name)
        remedy = (
            f"pip install 'foundation-platform-sdk[{extra}]'"
            if extra
            else f"pip install {module_name}"
        )
        # Name the underlying error: an installed-but-broken dependency reports
        # an ImportError too, and "install it" would be misleading advice.
        raise ImportError(
            f"{module_name} is required to use {method_name}() and could not be "
            f"imported ({exc}). If it is not installed: {remedy}"
        ) from exc


def camel_to_snake(name: str) -> str:
    """
    Convert camelCase to snake_case.

    Args:
        name: camelCase string

    Returns:
        snake_case string

    Examples:
        >>> camel_to_snake("uwYear")
        'uw_year'
        >>> camel_to_snake("primaryFlag")
        'primary_flag'
        >>> camel_to_snake("DCF")
        'dcf'
        >>> camel_to_snake("Portfolio Tag")
        'portfolio_tag'
        >>> camel_to_snake("Test-Field_Name")
        'test_field_name'
    """
    # Replace all non-alphanumeric characters with underscores
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    # Handle acronyms at the start
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Handle transitions from lowercase to uppercase
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    # Remove multiple consecutive underscores
    name = re.sub(r"_+", "_", name)
    # Remove leading/trailing underscores
    name = name.strip("_")
    return name.lower()


def claim_unique_name(snake: str, claimed: set[str]) -> str:
    """
    Claim a free numeric-suffixed variant of a snake_case name.

    The suffix has to be searched, not counted: a name that already ends in
    "_2" occupies the slot the second occurrence of its unsuffixed form would
    be given, and handing the same name out twice loses one of the two.

    Args:
        snake: The snake_case name to claim
        claimed: Names handed out so far, updated in place

    Returns:
        A name not present in `claimed`

    Examples:
        >>> claimed = set()
        >>> claim_unique_name("coverage_type_2", claimed)
        'coverage_type_2'
        >>> claim_unique_name("coverage_type", claimed)
        'coverage_type'
        >>> claim_unique_name("coverage_type", claimed)
        'coverage_type_3'
    """
    resolved, suffix = snake, 1
    while resolved in claimed:
        suffix += 1
        resolved = f"{snake}_{suffix}"

    claimed.add(resolved)
    return resolved


def resolve_snake_case_collisions(names: list[str]) -> dict[str, str]:
    """
    Resolve snake_case collisions by appending numeric suffixes.

    When multiple original names convert to the same snake_case name,
    this function appends _2, _3, etc. to subsequent occurrences.

    Args:
        names: List of original names (e.g., ['PC', 'PC_', 'Portfolio Tag'])

    Returns:
        Dictionary mapping original names to unique snake_case names

    Examples:
        >>> resolve_snake_case_collisions(['PC', 'PC_', 'Portfolio Tag'])
        {'PC': 'pc', 'PC_': 'pc_2', 'Portfolio Tag': 'portfolio_tag'}
        >>> resolve_snake_case_collisions(['Test', 'test', 'TEST'])
        {'Test': 'test', 'test': 'test_2', 'TEST': 'test_3'}
        >>> resolve_snake_case_collisions(['Coverage Type 2', 'Coverage type'])
        {'Coverage Type 2': 'coverage_type_2', 'Coverage type': 'coverage_type'}
    """
    claimed: set[str] = set()
    return {
        original_name: claim_unique_name(camel_to_snake(original_name), claimed)
        for original_name in names
    }


def snake_to_camel(name: str) -> str:
    """
    Convert snake_case to camelCase.

    Args:
        name: snake_case string

    Returns:
        camelCase string

    Examples:
        >>> snake_to_camel("uw_year")
        'uwYear'
        >>> snake_to_camel("primary_flag")
        'primaryFlag'
    """
    components = name.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def parse_utc_timestamp(value: Any) -> datetime | None:
    """
    Parse a UTC timestamp returned by the API into an aware datetime.

    The platform serializes UTC timestamps with a trailing ``Z`` and up to
    seven fractional-second digits, one more than ``datetime`` can represent,
    so the fraction is truncated to microseconds. Callers that need the exact
    instant the server recorded must keep the original string.

    Args:
        value: Timestamp string, or None

    Returns:
        Timezone-aware datetime in UTC, or None if the value is not parseable

    Examples:
        >>> parse_utc_timestamp("2024-01-15T10:42:08.1234567Z")
        datetime.datetime(2024, 1, 15, 10, 42, 8, 123456, tzinfo=datetime.timezone.utc)
    """
    if not isinstance(value, str) or not value:
        return None

    try:
        # fromisoformat reads the trailing Z and truncates the fraction to the
        # six digits datetime can hold.
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None

    # Timestamps from the platform are always UTC, tagged or not.
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_datetime_string(value: Any) -> datetime | Any:
    """
    Parse an ISO 8601 timestamp into a timezone-aware UTC datetime.

    Always a datetime, never a date. Until 0.12 the return type was chosen from
    the value -- midnight gave a ``date`` and any other time a ``datetime`` --
    so one field had two types across a deal's layers, and
    ``expiry_date - incept_date`` raised TypeError on some of them. Always
    aware, too: the platform stores UTC, and mixing tagged and untagged values
    made comparing two of the SDK's own timestamps raise.

    Args:
        value: Value to parse (typically a string)

    Returns:
        An aware UTC datetime, or the original value if it is not a timestamp

    Examples:
        >>> parse_datetime_string("2020-01-01T00:00:00")
        datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        >>> parse_datetime_string("2020-01-01T14:30:00")
        datetime.datetime(2020, 1, 1, 14, 30, tzinfo=datetime.timezone.utc)
        >>> parse_datetime_string("not a date")
        'not a date'
    """
    if not isinstance(value, str):
        return value

    # Match ISO 8601 datetime format
    # YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SS.ffffff with optional Z
    pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z)?$"

    if not re.match(pattern, value):
        return value

    try:
        dt = datetime.fromisoformat(value)
    except (ValueError, AttributeError):
        # If parsing fails, return the original value
        return value

    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def to_wire_datetime(value: datetime) -> str:
    """Serialize a datetime the way the platform sent it: naive, no offset.

    The SDK normalizes every timestamp it hands out to aware UTC, but the deal
    document must go back in the shape the server issued. Its models bind these
    to ``DateTime?``, which System.Text.Json converts to the receiving process's
    local time when the string carries an offset -- shifting the instant, and
    the calendar day, on any host not running UTC.
    """
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.isoformat()
