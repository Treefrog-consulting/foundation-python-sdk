"""Utility functions for the Foundation Platform SDK"""

import re
from datetime import datetime, date
from typing import Any, Union, List, Dict


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
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    # Handle acronyms at the start
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Handle transitions from lowercase to uppercase
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    # Remove multiple consecutive underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    return name.lower()


def resolve_snake_case_collisions(names: List[str]) -> Dict[str, str]:
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
    """
    mapping = {}
    snake_counts = {}
    
    for original_name in names:
        snake_name = camel_to_snake(original_name)
        
        # Track how many times we've seen this snake_case name
        if snake_name not in snake_counts:
            snake_counts[snake_name] = 0
        
        snake_counts[snake_name] += 1
        
        # First occurrence gets the plain name, subsequent get _2, _3, etc.
        if snake_counts[snake_name] == 1:
            mapping[original_name] = snake_name
        else:
            mapping[original_name] = f"{snake_name}_{snake_counts[snake_name]}"
    
    return mapping


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
    components = name.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def parse_datetime_string(value: Any) -> Union[date, datetime, Any]:
    """
    Parse ISO 8601 datetime strings to date or datetime objects.

    If the string matches ISO 8601 format (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SS.ffffff):
    - Returns a date object if time is 00:00:00
    - Returns a datetime object if time is non-zero

    Args:
        value: Value to parse (typically a string)

    Returns:
        date, datetime, or the original value if not a datetime string

    Examples:
        >>> parse_datetime_string("2020-01-01T00:00:00")
        date(2020, 1, 1)
        >>> parse_datetime_string("2020-01-01T14:30:00")
        datetime(2020, 1, 1, 14, 30)
        >>> parse_datetime_string("not a date")
        'not a date'
    """
    if not isinstance(value, str):
        return value

    # Match ISO 8601 datetime format
    # YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SS.ffffff with optional Z
    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z)?$'

    if not re.match(pattern, value):
        return value

    try:
        # Remove trailing Z if present (UTC indicator)
        clean_value = value.rstrip('Z')

        # Parse the datetime
        dt = datetime.fromisoformat(clean_value)

        # If the time is midnight, return a date object
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
            return dt.date()

        # Otherwise return datetime object
        return dt
    except (ValueError, AttributeError):
        # If parsing fails, return the original value
        return value
