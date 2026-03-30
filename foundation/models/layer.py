"""Layer model"""

import copy
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from ..ref_data import ReferenceDataCache
from ..utils import (
    camel_to_snake,
    parse_datetime_string,
    resolve_snake_case_collisions,
    snake_to_camel,
)
from .custom_table import CustomTable
from .layer_participant import LayerParticipant
from .layered_losses import LayeredLosses
from .reinstatement import Reinstatement
from .ylt import Ylt

if TYPE_CHECKING:
    import pandas as pd

    from .currency import Currency
    from .layer_status import LayerStatus
    from .program import Program
    from .reinst_prem_base import ReinstPremBase
    from .unit_of_measure import UnitOfMeasure


class Layer:
    """Represents a program layer with enriched data."""

    def __init__(
        self,
        layer_data: Dict[str, Any],
        ref_cache: ReferenceDataCache,
        program: Optional["Program"] = None,
    ):
        self._original_data = copy.deepcopy(layer_data)
        self._data = layer_data
        self._ref_cache = ref_cache
        self._program = program
        self._modifications = {}

        # Initialize loss accessors
        self._layered_losses = LayeredLosses(self)
        self._ylt = Ylt(self)

        # Build collision-aware mapping for custom fields
        layer_custom_field_names = []
        for cf in layer_data.get("layerCustomFields", []):
            field_def = ref_cache.get_custom_field(cf.get("customFieldId"))
            if field_def:
                layer_custom_field_names.append(field_def["name"])

        self._custom_field_name_mapping = resolve_snake_case_collisions(
            layer_custom_field_names
        )

        # Process custom fields with collision-aware names
        self._custom_field_values = {}
        for cf in layer_data.get("layerCustomFields", []):
            field_def = ref_cache.get_custom_field(cf.get("customFieldId"))
            if field_def:
                original_name = field_def["name"]
                field_name = self._custom_field_name_mapping.get(
                    original_name, camel_to_snake(original_name)
                )
                self._custom_field_values[field_name] = cf.get("value")

        # Process custom tables
        raw_table_rows: Dict[str, List[Dict[str, Any]]] = {}
        self._custom_table_original_names: Dict[str, str] = {}
        self._custom_table_original_rows: Dict[str, List[Dict[str, Any]]] = {}
        for ct in layer_data.get("customTables", []):
            table_id = ct.get("customTableDefinitionId") or ct.get("customTableId")
            table_def = ref_cache.get_custom_table(table_id)
            if table_def:
                table_name = camel_to_snake(table_def["name"])
                rows_payload = ct.get("rows")
                if isinstance(rows_payload, list):
                    normalized_rows = [
                        self._normalize_table_row(table_id, row) for row in rows_payload
                    ]
                    # Store original rows to preserve metadata
                    self._custom_table_original_rows.setdefault(table_name, []).extend(
                        rows_payload
                    )
                else:
                    normalized_rows = [ref_cache.build_custom_table_row(ct, table_id)]
                    # Store original row to preserve metadata
                    self._custom_table_original_rows.setdefault(table_name, []).append(
                        ct
                    )
                raw_table_rows.setdefault(table_name, []).extend(normalized_rows)
                self._custom_table_original_names[table_name] = table_def["name"]
        self._custom_tables: Dict[str, CustomTable] = {}
        self._custom_table_dirty: Set[str] = set()

        # Process limits and attachments
        self._limits_attachments = {}
        for la in layer_data.get("layerLimitAttach", []):
            la_type = ref_cache.get_layer_limit_attach_type(
                la.get("layerLimitAttachTypeId")
            )
            if la_type:
                type_name = camel_to_snake(la_type.name)
                self._limits_attachments[f"{type_name}_limit"] = la.get("limit")
                self._limits_attachments[f"{type_name}_attachment"] = la.get(
                    "attachment"
                )
                self._limits_attachments[f"{type_name}_limit_unlimited"] = la.get(
                    "limitUnlimitedFlag"
                )
                self._limits_attachments[f"{type_name}_limit_binary"] = la.get(
                    "limitBinaryFlag"
                )
                self._limits_attachments[f"{type_name}_attachment_franchise"] = la.get(
                    "attachmentFranchiseFlag"
                )

        # Process premiums
        self._premiums = {}
        for prem in layer_data.get("layerPremiums", []):
            prem_type = ref_cache.get_layer_premium_type(prem.get("premiumTypeId"))
            if prem_type:
                type_name = camel_to_snake(prem_type.name)
                self._premiums[f"{type_name}_premium"] = prem.get("amount")

        # Build custom fields info (all available layer-level custom fields)
        # First collect all layer-level field names to detect collisions
        all_layer_field_names = []
        for field_id, field_def in ref_cache._custom_fields.items():
            if field_def.get("level") == 2:  # Layer level
                all_layer_field_names.append(field_def["name"])

        # Create collision-aware mapping for all layer fields
        all_layer_field_mapping = resolve_snake_case_collisions(all_layer_field_names)

        self._custom_fields_info = []
        for field_id, field_def in ref_cache._custom_fields.items():
            if field_def.get("level") == 2:  # Layer level
                original_name = field_def["name"]
                self._custom_fields_info.append(
                    {
                        "id": field_id,
                        "name": original_name,
                        "snake_case_name": all_layer_field_mapping.get(
                            original_name, camel_to_snake(original_name)
                        ),
                        "value_type": field_def.get("valueType"),
                    }
                )
        self._custom_fields_info.sort(key=lambda x: x["name"])

        # Build custom tables info (all available layer-level custom tables)
        self._custom_tables_info = []
        for table_id, table_def in ref_cache._custom_tables.items():
            if table_def.get("dataLevelId") == 3:  # Layer level
                columns = ref_cache.get_custom_table_columns(table_id)
                column_names = [col["name"] for col in columns]
                snake_name = camel_to_snake(table_def["name"])

                # Only create CustomTable instances for tables that have data
                if snake_name in raw_table_rows:
                    table = CustomTable(
                        owner=self,
                        ref_cache=ref_cache,
                        table_id=table_id,
                        snake_name=snake_name,
                        original_name=table_def["name"],
                        column_names=column_names,
                        initial_rows=raw_table_rows.get(snake_name, []),
                    )
                    self._custom_tables[snake_name] = table
                    self._custom_table_original_names.setdefault(
                        snake_name, table_def["name"]
                    )

                # Add to info list regardless of whether data exists
                self._custom_tables_info.append(
                    {
                        "id": table_id,
                        "name": table_def["name"],
                        "snake_case_name": snake_name,
                        "column_count": len(column_names),
                        "columns": column_names,
                    }
                )
        self._custom_tables_info.sort(key=lambda x: x["name"])

    @property
    def layered_losses(self) -> LayeredLosses:
        """Access layered loss data for this layer.

        Returns:
            LayeredLosses accessor that can export data as Polars DataFrame.

        Example:
            >>> config = RunConfiguration(sim_years=10000, vendor_id=1, currency_code="USD")
            >>> df = layer.layered_losses.as_polars(config)
        """
        return self._layered_losses

    @property
    def ylt(self) -> Ylt:
        """Access YLT (Year Loss Table) data for this layer.

        Returns:
            Ylt accessor that can export data as Polars DataFrame.

        Example:
            >>> config = RunConfiguration(sim_years=10000, vendor_id=1, currency_code="USD")
            >>> df = layer.ylt.as_polars(config)
        """
        return self._ylt

    # Standard layer properties with full type hints
    @property
    def id(self) -> int:
        """Layer ID (read-only)."""
        return self._data["id"]

    @property
    def name(self) -> str:
        """Layer name."""
        if "name" in self._modifications:
            return self._modifications["name"]
        return self._data.get("name", "")

    @name.setter
    def name(self, value: str) -> None:
        """Set layer name."""
        self._modifications["name"] = value

    @property
    def number(self) -> Optional[int]:
        """Layer number."""
        if "number" in self._modifications:
            return self._modifications["number"]
        return self._data.get("number")

    @number.setter
    def number(self, value: Optional[int]) -> None:
        """Set layer number."""
        self._modifications["number"] = value

    @property
    def incept_date(self) -> Union[date, datetime, str, None]:
        """Inception date."""
        if "incept_date" in self._modifications:
            return self._modifications["incept_date"]
        value = self._data.get("inceptDate")
        return parse_datetime_string(value)

    @incept_date.setter
    def incept_date(self, value: Union[date, datetime, str, None]) -> None:
        """Set inception date."""
        parsed_value = parse_datetime_string(value)
        self._modifications["incept_date"] = parsed_value

    @property
    def expiry_date(self) -> Union[date, datetime, str, None]:
        """Expiry date."""
        if "expiry_date" in self._modifications:
            return self._modifications["expiry_date"]
        value = self._data.get("expiryDate")
        return parse_datetime_string(value)

    @expiry_date.setter
    def expiry_date(self, value: Union[date, datetime, str, None]) -> None:
        """Set expiry date."""
        parsed_value = parse_datetime_string(value)
        self._modifications["expiry_date"] = parsed_value

    @property
    def note(self) -> Optional[str]:
        """Layer note."""
        if "note" in self._modifications:
            return self._modifications["note"]
        return self._data.get("note")

    @note.setter
    def note(self, value: Optional[str]) -> None:
        """Set layer note."""
        self._modifications["note"] = value

    @property
    def limit_percent(self) -> Optional[float]:
        """Limit percentage."""
        if "limit_percent" in self._modifications:
            return self._modifications["limit_percent"]
        return self._data.get("limitPercent")

    @limit_percent.setter
    def limit_percent(self, value: Optional[float]) -> None:
        """Set limit percentage."""
        self._modifications["limit_percent"] = value

    @property
    def structure_only_flag(self) -> bool:
        """Whether this is a structure-only layer."""
        if "structure_only_flag" in self._modifications:
            return self._modifications["structure_only_flag"]
        return self._data.get("structureOnlyFlag", False)

    @structure_only_flag.setter
    def structure_only_flag(self, value: bool) -> None:
        """Set structure-only flag."""
        self._modifications["structure_only_flag"] = value

    @property
    def unlimited_reinst_flag(self) -> bool:
        """Whether reinstatements are unlimited."""
        if "unlimited_reinst_flag" in self._modifications:
            return self._modifications["unlimited_reinst_flag"]
        return self._data.get("unlimitedReinstFlag", False)

    @unlimited_reinst_flag.setter
    def unlimited_reinst_flag(self, value: bool) -> None:
        """Set unlimited reinstatement flag."""
        self._modifications["unlimited_reinst_flag"] = value

    # Reference data properties (read-only, enriched)
    @property
    def status(self) -> Optional["LayerStatus"]:
        """Layer status (read-only, enriched from reference data)."""
        status_id = self._data.get("layerStatusId")
        if status_id:
            return self._ref_cache.get_layer_status(status_id)
        return None

    @property
    def currency(self) -> Optional["Currency"]:
        """Currency (read-only, enriched from reference data)."""
        currency_id = self._data.get("currencyId")
        if currency_id:
            return self._ref_cache.get_currency(currency_id)
        return None

    @property
    def unit_of_measure(self) -> Optional["UnitOfMeasure"]:
        """Unit of measure (read-only, enriched from reference data)."""
        unit_id = self._data.get("unitOfMeasureId")
        if unit_id:
            return self._ref_cache.get_unit_of_measure(unit_id)
        return None

    @property
    def reinst_prem_base(self) -> Optional["ReinstPremBase"]:
        """Reinstatement premium base (read-only, enriched from reference data)."""
        reinst_prem_base_id = self._data.get("reinstPremBaseId")
        if reinst_prem_base_id:
            return self._ref_cache.get_reinst_prem_base(reinst_prem_base_id)
        return None

    @property
    def participants(self) -> List[LayerParticipant]:
        """List of layer participants (read-only)."""
        return [
            LayerParticipant(p, self._ref_cache)
            for p in self._data.get("layerParticipants", [])
        ]

    @property
    def reinstatements(self) -> List[Reinstatement]:
        """List of layer reinstatements (read-only)."""
        return [Reinstatement(r) for r in self._data.get("layerReinst", [])]

    @property
    def custom_fields_info(self) -> List[Dict[str, Any]]:
        """
        List of all available custom fields for layers (read-only).

        Returns metadata about all layer-level custom fields including:
        - id: Custom field ID
        - name: Display name (as in Foundation)
        - snake_case_name: Python-friendly attribute name
        - value_type: Data type of the field

        Example:
            >>> for field in layer.custom_fields_info:
            ...     print(f"{field['name']} -> {field['snake_case_name']}")
        """
        return self._custom_fields_info

    @property
    def custom_fields_names(self) -> List[str]:
        """
        List of all available custom field names for layers (read-only).

        Returns a list of strings, where each string is the snake_case_name of a custom field.
        Names are sorted alphabetically.

        Example:
            >>> layer.custom_fields_names
            ['layer_type_tag', 'rofr']
        """
        return sorted([field["snake_case_name"] for field in self._custom_fields_info])

    @property
    def custom_tables_info(self) -> List[Dict[str, Any]]:
        """
        List of all available custom tables for layers (read-only).

        Returns metadata about all layer-level custom tables including:
        - id: Custom table ID
        - name: Display name (as in Foundation)
        - snake_case_name: Python-friendly attribute name
        - column_count: Number of columns
        - columns: List of column names

        Example:
            >>> for table in layer.custom_tables_info:
            ...     print(f"{table['name']}: {table['columns']}")
        """
        return self._custom_tables_info

    @property
    def custom_tables_names(self) -> List[str]:
        """
        List of all available custom table names for layers (read-only).

        Returns a list of strings, where each string is the snake_case_name of a custom table.
        Names are sorted alphabetically.

        Example:
            >>> layer.custom_tables_names
            ['buffer_layer_1', 'exposure_data']
        """
        return sorted([table["snake_case_name"] for table in self._custom_tables_info])

    @property
    def program(self) -> Optional["Program"]:
        """Parent program (read-only)."""
        return self._program

    # Limit and attachment methods
    def get_limit(self, limit_type: str) -> Optional[float]:
        """
        Get a limit value by type (snake_case).

        Args:
            limit_type: Limit type name in snake_case (e.g., 'occurence_limit')

        Returns:
            Limit value, or None if not found

        Example:
            >>> layer.get_limit('occurence_limit')
            1000000.0
        """
        return self._limits_attachments.get(limit_type)

    def get_attachment(self, attachment_type: str) -> Optional[float]:
        """
        Get an attachment value by type (snake_case).

        Args:
            attachment_type: Attachment type name in snake_case (e.g., 'aggregate_attachment')

        Returns:
            Attachment value, or None if not found

        Example:
            >>> layer.get_attachment('aggregate_attachment')
            500000.0
        """
        return self._limits_attachments.get(attachment_type)

    # Premium methods
    def get_premium(self, premium_type: str) -> Optional[float]:
        """
        Get a premium value by type (snake_case).

        Args:
            premium_type: Premium type name in snake_case (e.g., 'gross_premium')

        Returns:
            Premium value, or None if not found

        Example:
            >>> layer.get_premium('gross_premium')
            50000.0
        """
        return self._premiums.get(premium_type)

    # Custom field methods
    def get_custom_field(self, field_name: str) -> Any:
        """
        Get a custom field value by name (original name as defined in Foundation).

        Args:
            field_name: Custom field name as it appears in Foundation (e.g., 'ROFR')

        Returns:
            Custom field value, or None if not found

        Example:
            >>> layer.get_custom_field('ROFR')
            'Yes'
        """
        # Use the collision-aware mapping
        snake_name = self._custom_field_name_mapping.get(field_name)
        if snake_name:
            value = self._custom_field_values.get(snake_name)
            return parse_datetime_string(value)

        # Not found
        return None

    def set_custom_field(self, field_name: str, value: Any) -> None:
        """
        Set a custom field value by name (snake_case).

        Args:
            field_name: Custom field name in snake_case (e.g., 'rofr')
            value: Value to set

        Example:
            >>> layer.set_custom_field('rofr', 'Yes')
        """
        parsed_value = parse_datetime_string(value)
        self._modifications[field_name] = parsed_value

    def get_custom_table(self, table_name: str) -> CustomTable:
        """
        Get a custom table wrapper by name (original name as defined in Foundation).

        Args:
            table_name: Custom table name as it appears in Foundation (e.g., 'Buffer Layer 1')

        Returns:
            CustomTable instance for the requested table.

        Example:
            >>> buffer_layer = layer.get_custom_table('Buffer Layer 1')
            >>> buffer_layer.as_pandas().head()
        """
        snake_name = camel_to_snake(table_name)
        table = self._custom_tables.get(snake_name)
        if table:
            return table

        for info in self._custom_tables_info:
            if info["name"] == table_name or info["snake_case_name"] == snake_name:
                return self._custom_tables[info["snake_case_name"]]

        target_id = None
        column_names: List[str] = []
        resolved_name = table_name
        for table_id, table_def in self._ref_cache._custom_tables.items():
            if table_def.get("dataLevelId") == 3:
                current_snake = camel_to_snake(table_def["name"])
                if table_def["name"] == table_name or current_snake == snake_name:
                    target_id = table_id
                    resolved_name = table_def["name"]
                    snake_name = current_snake
                    columns = self._ref_cache.get_custom_table_columns(table_id)
                    column_names = [col["name"] for col in columns]
                    break

        new_table = CustomTable(
            owner=self,
            ref_cache=self._ref_cache,
            table_id=target_id,
            snake_name=snake_name,
            original_name=resolved_name,
            column_names=column_names,
            initial_rows=[],
        )
        self._custom_tables[snake_name] = new_table
        self._custom_table_original_names.setdefault(snake_name, resolved_name)

        if not any(
            info["snake_case_name"] == snake_name for info in self._custom_tables_info
        ):
            self._custom_tables_info.append(
                {
                    "id": target_id,
                    "name": resolved_name,
                    "snake_case_name": snake_name,
                    "column_count": len(column_names),
                    "columns": column_names,
                }
            )
            self._custom_tables_info.sort(key=lambda x: x["name"])

        return new_table

    def _mark_custom_table_modified(self, table_name: str) -> None:
        """Mark a custom table as modified to include it in JSON output."""
        self._custom_table_dirty.add(table_name)

    def set_custom_table(
        self, table_name: str, rows: Union[List[Dict[str, Any]], "pd.DataFrame"]
    ) -> None:
        """
        Set a custom table by name (snake_case).

        Args:
            table_name: Custom table name in snake_case (e.g., 'buffer_layer_1')
            rows: DataFrame or list of table rows (dicts)

        Example:
            >>> layer.set_custom_table('buffer_layer_1', [{'Text 1': 'Value'}])
        """
        snake_name = camel_to_snake(table_name)
        if snake_name in self._modifications:
            del self._modifications[snake_name]
        table = self.get_custom_table(table_name)
        table.set_data(rows)

    def get_json(self) -> Dict[str, Any]:
        """
        Get the layer data as JSON, applying any modifications.

        Returns:
            Dictionary representation of the layer with modifications applied
        """
        result = copy.deepcopy(self._original_data)

        # Apply modifications
        for key, value in list(self._modifications.items()):
            # Check if this is a custom field
            if key in self._custom_field_values or self._is_custom_field_name(key):
                # Update the custom field in layerCustomFields array
                self._update_custom_field_in_json(result, key, value)
            # Check if this is a limit/attachment
            elif key in self._limits_attachments:
                self._update_limit_attachment_in_json(result, key, value)
            # Check if this is a premium
            elif key in self._premiums:
                self._update_premium_in_json(result, key, value)
            else:
                # Regular field - convert to camelCase and serialize dates
                camel_key = snake_to_camel(key)
                # Convert date/datetime objects to ISO strings for JSON serialization
                if isinstance(value, datetime):
                    result[camel_key] = value.isoformat()
                elif isinstance(value, date):
                    result[camel_key] = f"{value}T00:00:00"
                else:
                    result[camel_key] = value

        # Always update custom tables if they have been modified
        for table_name in self._custom_table_dirty:
            table = self._custom_tables.get(table_name)
            if table is None:
                continue
            rows = table.to_records()
            self._update_custom_table_in_json(result, table_name, rows)
        self._custom_table_dirty.clear()

        return result

    def _normalize_table_row(
        self, table_id: Optional[int], row: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normalize a custom table row to user-facing column names.

        Args:
            table_id: Custom table definition ID.
            row: Row data from the API payload.

        Returns:
            Dictionary keyed by configured column names.
        """
        if table_id is None:
            return copy.deepcopy(row)

        raw_keys = row.keys()
        if any(
            key.startswith(
                ("numValue", "textValue", "bitValue", "dateValue", "lookupValue")
            )
            for key in raw_keys
        ):
            return self._ref_cache.build_custom_table_row(row, table_id)
        return copy.deepcopy(row)

    def _is_custom_field_name(self, name: str) -> bool:
        """Check if a name matches any custom field."""
        # Check if the name is in our custom field values (which uses collision-aware names)
        return name in self._custom_field_values

    def _is_custom_table_name(self, name: str) -> bool:
        """Check if a name matches any custom table."""
        if name in self._custom_tables:
            return True
        return any(info["snake_case_name"] == name for info in self._custom_tables_info)

    def _update_custom_field_in_json(
        self, result: Dict[str, Any], field_name: str, value: Any
    ) -> None:
        """Update a custom field value in the layerCustomFields array."""
        # Find the custom field ID using the collision-aware mapping
        field_id = None
        for original_name, snake_name in self._custom_field_name_mapping.items():
            if snake_name == field_name:
                # Found the original name, now get the field ID
                for fid, field_def in self._ref_cache._custom_fields.items():
                    if (
                        field_def.get("level") == 2
                        and field_def["name"] == original_name
                    ):
                        field_id = fid
                        break
                break

        if field_id is None:
            return

        # Update or add the custom field in the array
        custom_fields = result.get("layerCustomFields", [])
        found = False
        for cf in custom_fields:
            if cf.get("customFieldId") == field_id:
                cf["value"] = value
                found = True
                break

        if not found:
            # Add new custom field entry
            custom_fields.append(
                {
                    "id": None,  # Will be assigned by API
                    "layerId": result.get("id"),
                    "layer": None,
                    "customFieldId": field_id,
                    "customField": None,
                    "value": value,
                }
            )
            result["layerCustomFields"] = custom_fields

    def _update_limit_attachment_in_json(
        self, result: Dict[str, Any], field_name: str, value: Any
    ) -> None:
        """Update a limit or attachment value in the layerLimitAttach array."""
        # Parse the field name to determine type and field
        # Format: {type_name}_limit, {type_name}_attachment, etc.
        if field_name.endswith("_limit"):
            type_name = field_name[: -len("_limit")]
            field_type = "limit"
        elif field_name.endswith("_attachment"):
            type_name = field_name[: -len("_attachment")]
            field_type = "attachment"
        elif field_name.endswith("_limit_unlimited"):
            type_name = field_name[: -len("_limit_unlimited")]
            field_type = "limitUnlimitedFlag"
        elif field_name.endswith("_limit_binary"):
            type_name = field_name[: -len("_limit_binary")]
            field_type = "limitBinaryFlag"
        elif field_name.endswith("_attachment_franchise"):
            type_name = field_name[: -len("_attachment_franchise")]
            field_type = "attachmentFranchiseFlag"
        else:
            return

        # Find the layer limit attach type ID
        type_id = None
        for tid, type_dict in self._ref_cache._layer_limit_attach_types.items():
            type_obj = self._ref_cache.get_layer_limit_attach_type(tid)
            if type_obj and camel_to_snake(type_obj.name) == type_name:
                type_id = tid
                break

        if type_id is None:
            return

        # Update or add the limit/attachment in the array
        limit_attach_list = result.get("layerLimitAttach", [])
        found = False
        for la in limit_attach_list:
            if la.get("layerLimitAttachTypeId") == type_id:
                # Update the appropriate field
                if field_type == "limit":
                    la["limit"] = value
                elif field_type == "attachment":
                    la["attachment"] = value
                elif field_type == "limitUnlimitedFlag":
                    la["limitUnlimitedFlag"] = value
                elif field_type == "limitBinaryFlag":
                    la["limitBinaryFlag"] = value
                elif field_type == "attachmentFranchiseFlag":
                    la["attachmentFranchiseFlag"] = value
                found = True
                break

        if not found:
            # Add new limit/attachment entry
            new_entry = {
                "id": None,  # Will be assigned by API
                "layerId": result.get("id"),
                "layer": None,
                "layerLimitAttachTypeId": type_id,
                "layerLimitAttachType": None,
                "limit": None,
                "attachment": None,
                "limitUnlimitedFlag": False,
                "limitBinaryFlag": False,
                "attachmentFranchiseFlag": False,
            }
            # Set the appropriate field
            if field_type == "limit":
                new_entry["limit"] = value
            elif field_type == "attachment":
                new_entry["attachment"] = value
            elif field_type == "limitUnlimitedFlag":
                new_entry["limitUnlimitedFlag"] = value
            elif field_type == "limitBinaryFlag":
                new_entry["limitBinaryFlag"] = value
            elif field_type == "attachmentFranchiseFlag":
                new_entry["attachmentFranchiseFlag"] = value

            limit_attach_list.append(new_entry)
            result["layerLimitAttach"] = limit_attach_list

    def _update_premium_in_json(
        self, result: Dict[str, Any], field_name: str, value: Any
    ) -> None:
        """Update a premium value in the layerPremiums array."""
        # Parse the field name to determine type
        # Format: {type_name}_premium
        if not field_name.endswith("_premium"):
            return

        type_name = field_name[: -len("_premium")]

        # Find the layer premium type ID
        type_id = None
        for tid, type_dict in self._ref_cache._layer_premium_types.items():
            type_obj = self._ref_cache.get_layer_premium_type(tid)
            if type_obj and camel_to_snake(type_obj.name) == type_name:
                type_id = tid
                break

        if type_id is None:
            return

        # Update or add the premium in the array
        premiums_list = result.get("layerPremiums", [])
        found = False
        for prem in premiums_list:
            if prem.get("premiumTypeId") == type_id:
                prem["amount"] = value
                found = True
                break

        if not found:
            # Add new premium entry
            premiums_list.append(
                {
                    "id": None,  # Will be assigned by API
                    "layerId": result.get("id"),
                    "layer": None,
                    "premiumTypeId": type_id,
                    "premiumType": None,
                    "amount": value,
                }
            )
            result["layerPremiums"] = premiums_list

    def _update_custom_table_in_json(
        self, result: Dict[str, Any], table_name: str, value: List[Dict[str, Any]]
    ) -> None:
        """Update a custom table in the customTables array."""
        # Find the custom table ID
        table_id = None
        for tid, table_def in self._ref_cache._custom_tables.items():
            if table_def.get("level") == 2:
                if camel_to_snake(table_def["name"]) == table_name:
                    table_id = tid
                    break

        if table_id is None:
            return

        # Get column definitions to map column names to internal fields
        columns = self._ref_cache.get_custom_table_columns(table_id)

        # Build mapping from column name to internal field
        column_to_field = {}
        # Track lookup columns for reverse resolution (display label -> raw ID)
        lookup_columns = {}  # col_name -> lookupObjectId
        for col in columns:
            col_name = col["name"]
            value_type = col["valueType"]
            field_num = col["valueFieldNum"]

            field_map = {
                1: f"numValue{field_num}",
                2: f"dateValue{field_num}",
                3: f"bitValue{field_num}",
                4: f"textValue{field_num}",
                5: f"lookupValue{field_num}",
            }

            field_name = field_map.get(value_type)
            if field_name:
                column_to_field[col_name] = field_name
                # Track lookup columns for reverse resolution
                if value_type == 5:
                    lookup_object_id = col.get("lookupObjectId")
                    if lookup_object_id is not None:
                        lookup_columns[col_name] = lookup_object_id

        # Get original rows for this table to preserve metadata
        original_rows = self._custom_table_original_rows.get(table_name, [])

        # Build new rows with preserved metadata
        new_rows = []
        for sort_order, row_data in enumerate(value):
            # Start with original row if available to preserve id and other metadata
            if sort_order < len(original_rows):
                raw_row = copy.deepcopy(original_rows[sort_order])
                # Update sortOrder in case it changed
                raw_row["sortOrder"] = sort_order
            else:
                # New row - create from scratch
                raw_row = {
                    "id": None,  # Will be assigned by API
                    "customTableDefinitionId": table_id,
                    "customTableDefinition": None,
                    "programId": None,
                    "layerId": result.get("id"),
                    "layer": None,
                    "sortOrder": sort_order,
                }

                # Initialize all possible value fields to None
                for i in range(1, 31):  # Assuming max 30 fields of each type
                    raw_row[f"numValue{i}"] = None
                    raw_row[f"textValue{i}"] = None
                    raw_row[f"bitValue{i}"] = None
                    raw_row[f"dateValue{i}"] = None
                    raw_row[f"lookupValue{i}"] = None

            # Map user values to internal fields
            for col_name, user_value in row_data.items():
                field_name = column_to_field.get(col_name)
                if field_name:
                    # Reverse-resolve lookup display labels back to raw IDs
                    if col_name in lookup_columns and user_value is not None:
                        user_value = self._ref_cache.reverse_lookup_value(
                            lookup_columns[col_name], user_value
                        )
                    # Convert date/datetime objects to ISO strings
                    if isinstance(user_value, (date, datetime)):
                        user_value = (
                            user_value.isoformat()
                            if isinstance(user_value, datetime)
                            else f"{user_value}T00:00:00"
                        )
                    raw_row[field_name] = user_value

            new_rows.append(raw_row)

        # Remove existing rows for this table
        custom_tables = result.get("customTables", [])
        custom_tables = [
            ct
            for ct in custom_tables
            if ct.get("customTableDefinitionId") != table_id
            or ct.get("layerId") != result.get("id")
        ]

        # Add all new rows
        custom_tables.extend(new_rows)
        result["customTables"] = custom_tables

    def __getattr__(self, name: str) -> Any:
        """
        Dynamic attribute access for custom fields, limits, attachments, and premiums.

        This enables dot notation access like:
        - layer.occurence_limit (instead of layer.get_limit('occurence_limit'))
        - layer.gross_premium (instead of layer.get_premium('gross_premium'))
        - layer.custom_field_name (instead of layer.get_custom_field('custom_field_name'))

        Args:
            name: Attribute name in snake_case

        Returns:
            Attribute value

        Raises:
            AttributeError: If attribute not found
        """
        # Check limits and attachments
        if name in self._limits_attachments:
            return self._limits_attachments[name]

        # Check premiums
        if name in self._premiums:
            return self._premiums[name]

        # Check custom fields (check modifications first)
        if name in self._modifications:
            return self._modifications[name]

        if name in self._custom_field_values:
            return self._custom_field_values[name]

        # Check custom tables
        if name in self._custom_tables:
            return self._custom_tables[name]

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Dynamic attribute setting for custom fields, limits, attachments, and premiums.

        This enables dot notation assignment like:
        - layer.custom_field_name = value
        - layer.occurence_limit = 1000000
        - layer.gross_premium = 50000

        Args:
            name: Attribute name in snake_case
            value: Value to set
        """
        # Internal attributes (prefixed with _) are set directly
        if name.startswith("_"):
            super().__setattr__(name, value)
            return

        # Check if it's a defined property with a setter
        prop = getattr(type(self), name, None)
        if isinstance(prop, property) and prop.fset is not None:
            prop.fset(self, value)
            # Property setter already handles modifications
            return

        # Check if it's a custom table (only if initialization is complete)
        if hasattr(self, "_custom_tables_info") and self._is_custom_table_name(name):
            self.set_custom_table(name, value)
            return

        # Check if it's a limit/attachment (only if initialization is complete)
        if hasattr(self, "_limits_attachments") and name in self._limits_attachments:
            self._limits_attachments[name] = value
            self._modifications[name] = value
            return

        # Check if it's a premium (only if initialization is complete)
        if hasattr(self, "_premiums") and name in self._premiums:
            self._premiums[name] = value
            self._modifications[name] = value
            return

        # Otherwise, treat it as a custom field or modification
        parsed_value = parse_datetime_string(value)
        self._modifications[name] = parsed_value

    def describe(self) -> None:
        """
        Display a comprehensive overview of the layer object's available features.

        Shows standard properties, custom fields, custom tables, limits/attachments,
        premiums, and available methods for easy exploration and discovery.

        Example:
            >>> layer.describe()
        """
        print("=" * 80)
        print(f"LAYER: {self.name or 'Untitled'} (ID: {self.id})")
        print("=" * 80)

        # Standard Properties
        print("\nSTANDARD PROPERTIES:")
        print(f"  {'Property':<30} {'Access':<10} {'Current Value':<30}")
        print(f"  {'-' * 30} {'-' * 10} {'-' * 30}")

        standard_props = [
            ("id", "read-only", self.id),
            ("name", "read-write", self.name),
            ("number", "read-write", self.number),
            ("incept_date", "read-write", self.incept_date),
            ("expiry_date", "read-write", self.expiry_date),
            ("note", "read-write", self.note),
            ("limit_percent", "read-write", self.limit_percent),
            ("structure_only_flag", "read-write", self.structure_only_flag),
            ("unlimited_reinst_flag", "read-write", self.unlimited_reinst_flag),
        ]

        for prop_name, access, value in standard_props:
            value_str = str(value)[:28] if value is not None else "None"
            print(f"  {prop_name:<30} {access:<10} {value_str:<30}")

        # Reference Data
        print("\nREFERENCE DATA (read-only):")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-' * 30} {'-' * 40}")

        ref_props = [
            ("status", self.status.name if self.status else None),
            ("currency", self.currency.name if self.currency else None),
            (
                "unit_of_measure",
                self.unit_of_measure.name if self.unit_of_measure else None,
            ),
            (
                "reinst_prem_base",
                self.reinst_prem_base.name if self.reinst_prem_base else None,
            ),
        ]

        for prop_name, value in ref_props:
            value_str = str(value)[:38] if value is not None else "None"
            print(f"  {prop_name:<30} {value_str:<40}")

        # Parent Program
        if self._program:
            print("\nPARENT PROGRAM:")
            print(
                f"  program                        {self._program.name} (ID: {self._program.id})"
            )

        # Limits & Attachments
        if self._limits_attachments:
            print("\nLIMITS & ATTACHMENTS:")
            print(f"  {'Attribute':<35} {'Value':<35}")
            print(f"  {'-' * 35} {'-' * 35}")
            for key, value in sorted(self._limits_attachments.items()):
                value_str = str(value)[:33] if value is not None else "None"
                print(f"  {key:<35} {value_str:<35}")

        # Premiums
        if self._premiums:
            print("\nPREMIUMS:")
            print(f"  {'Attribute':<35} {'Value':<35}")
            print(f"  {'-' * 35} {'-' * 35}")
            for key, value in sorted(self._premiums.items()):
                value_str = str(value)[:33] if value is not None else "None"
                print(f"  {key:<35} {value_str:<35}")

        # Custom Fields
        if self._custom_field_values:
            print("\nCUSTOM FIELDS:")
            print(f"  {'Field Name':<35} {'Value':<35}")
            print(f"  {'-' * 35} {'-' * 35}")
            for key, value in sorted(self._custom_field_values.items()):
                value_str = str(value)[:33] if value is not None else "None"
                print(f"  {key:<35} {value_str:<35}")
            print(
                f"\n  Available Custom Field Names: {', '.join(self.custom_fields_names) or '(none)'}"
            )
        else:
            print("\nCUSTOM FIELDS: (none)")

        # Custom Tables
        if self._custom_tables:
            print("\nCUSTOM TABLES:")
            print(f"  {'Table Name':<35} {'Rows':<10}")
            print(f"  {'-' * 35} {'-' * 10}")
            for snake_name in sorted(self._custom_tables.keys()):
                table = self._custom_tables[snake_name]
                row_count = table.row_count
                display_name = table.name
                print(f"  {display_name:<35} {row_count:<10}")
            print(
                f"\n  Available Custom Table Names: {', '.join(self.custom_tables_names) or '(none)'}"
            )
        else:
            print("\nCUSTOM TABLES: (none)")

        # Collections
        print("\nCOLLECTIONS:")
        participants_count = len(self.participants)
        reinstatements_count = len(self.reinstatements)
        print(f"  participants                   {participants_count} item(s)")
        print(f"  reinstatements                 {reinstatements_count} item(s)")

        # Methods
        print("\nAVAILABLE METHODS:")
        methods = [
            "get_limit(limit_type)",
            "get_attachment(attachment_type)",
            "get_premium(premium_type)",
            "get_custom_field(field_name)",
            "set_custom_field(field_name, value)",
            "get_custom_table(table_name)",
            "set_custom_table(table_name, rows)",
            "get_json()",
            "describe()",
        ]
        for method in methods:
            print(f"  ΓÇó {method}")

        print("\n" + "=" * 80)
        print(
            "TIP: Use dot notation to access any attribute (e.g., layer.gross_premium)"
        )
        print("=" * 80 + "\n")

    def __dir__(self) -> List[str]:
        """
        Return list of available attributes for autocomplete.

        Returns:
            List of attribute names including dynamic attributes
        """
        # Get base attributes
        attrs = set(super().__dir__())

        # Add standard properties
        attrs.update(
            [
                "id",
                "name",
                "number",
                "incept_date",
                "expiry_date",
                "note",
                "limit_percent",
                "structure_only_flag",
                "unlimited_reinst_flag",
                "status",
                "currency",
                "unit_of_measure",
                "reinst_prem_base",
                "participants",
                "reinstatements",
                "program",
                "custom_fields_info",
                "custom_fields_names",
                "custom_tables_info",
                "custom_tables_names",
            ]
        )

        # Add methods
        attrs.update(
            [
                "get_limit",
                "get_attachment",
                "get_premium",
                "get_custom_field",
                "set_custom_field",
                "get_custom_table",
                "set_custom_table",
                "get_json",
                "describe",
            ]
        )

        # Add dynamic attributes
        attrs.update(self._limits_attachments.keys())
        attrs.update(self._premiums.keys())
        attrs.update(self._custom_field_values.keys())
        attrs.update(self._custom_tables.keys())

        return sorted(list(attrs))
