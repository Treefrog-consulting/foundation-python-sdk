"""Shared init helpers for Program/Layer custom field + custom table processing.

Program and Layer have identical shapes for their custom-field arrays
(``programCustomFields`` / ``layerCustomFields``) and custom-table arrays
(``customTables``). Both classes need to:

- resolve ``customFieldId`` -> display name via the ref cache,
- apply snake-case collision handling, and
- bucket table rows by snake-cased table name while preserving the raw row
  payload for later round-trip saves.

These helpers centralize that logic so the two call sites stay in sync.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Tuple

from ..ref_data import ReferenceDataCache
from ..utils import camel_to_snake, resolve_snake_case_collisions

if TYPE_CHECKING:
    from .custom_table import CustomTable


def build_custom_field_values(
    custom_fields: List[Dict[str, Any]],
    ref_cache: ReferenceDataCache,
) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """Resolve a ``*CustomFields`` array into a (name_mapping, values) pair.

    Args:
        custom_fields: Raw array from server, each entry has
            ``customFieldId`` + ``value``.
        ref_cache: Reference cache used to look up field definitions.

    Returns:
        A tuple of:
            - ``name_mapping``: original field name -> snake_case name, with
              collisions resolved so names stay unique.
            - ``values``: snake_case name -> value.
    """
    original_names: List[str] = []
    for cf in custom_fields:
        field_def = ref_cache.get_custom_field(cf.get("customFieldId"))
        if field_def:
            original_names.append(field_def["name"])

    name_mapping = resolve_snake_case_collisions(original_names)

    values: Dict[str, Any] = {}
    for cf in custom_fields:
        field_def = ref_cache.get_custom_field(cf.get("customFieldId"))
        if not field_def:
            continue
        original_name = field_def["name"]
        field_name = name_mapping.get(original_name, camel_to_snake(original_name))
        values[field_name] = cf.get("value")

    return name_mapping, values


def resolve_or_create_custom_table(
    table_name: str,
    *,
    owner: Any,
    ref_cache: ReferenceDataCache,
    custom_tables: Dict[str, "CustomTable"],
    custom_tables_info: List[Dict[str, Any]],
    custom_table_original_names: Dict[str, str],
    data_level_ids: Tuple[int, ...],
) -> "CustomTable":
    """Return an existing custom table by name, or create an empty one from ref data.

    The lookup chain (in order): already-materialized table dict, then the info
    list (handles an on-the-fly rename), then the ref-cache definition filtered
    by ``data_level_ids`` (program tables live at 2/4, layer tables at 3).
    Falls through to an empty placeholder so callers can always dot-access the
    table even if no rows exist yet.
    """
    from .custom_table import CustomTable

    snake_name = camel_to_snake(table_name)
    table = custom_tables.get(snake_name)
    if table:
        return table

    for info in custom_tables_info:
        if info["name"] == table_name or info["snake_case_name"] == snake_name:
            return custom_tables[info["snake_case_name"]]

    target_id = None
    column_names: List[str] = []
    resolved_name = table_name
    for table_id, table_def in ref_cache._custom_tables.items():
        if table_def.get("dataLevelId") not in data_level_ids:
            continue
        current_snake = camel_to_snake(table_def["name"])
        if table_def["name"] == table_name or current_snake == snake_name:
            target_id = table_id
            resolved_name = table_def["name"]
            snake_name = current_snake
            columns = ref_cache.get_custom_table_columns(table_id)
            column_names = [col["name"] for col in columns]
            break

    new_table = CustomTable(
        owner=owner,
        ref_cache=ref_cache,
        table_id=target_id,
        snake_name=snake_name,
        original_name=resolved_name,
        column_names=column_names,
        initial_rows=[],
    )
    custom_tables[snake_name] = new_table
    custom_table_original_names.setdefault(snake_name, resolved_name)

    if not any(info["snake_case_name"] == snake_name for info in custom_tables_info):
        custom_tables_info.append(
            {
                "id": target_id,
                "name": resolved_name,
                "snake_case_name": snake_name,
                "column_count": len(column_names),
                "columns": column_names,
            }
        )
        custom_tables_info.sort(key=lambda x: x["name"])

    return new_table
