"""Program model"""

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
from .collections import NamedCollection
from .custom_table import CustomTable
from .layer import Layer
from .program_broker import ProgramBroker
from .program_categories import ProgramCategories
from .program_external_refs import ProgramExternalRefs
from .run_configurations import RunConfigurations

if TYPE_CHECKING:
    import pandas as pd

    from .client import Client


class Program:
    """Represents a program with enriched data."""

    def __init__(
        self,
        program_data: Dict[str, Any],
        ref_cache: ReferenceDataCache,
        analyses_data: Optional[List[Dict[str, Any]]] = None,
        foundation_client: Optional[Any] = None,
    ):
        self._original_data = copy.deepcopy(program_data)
        self._data = program_data
        self._ref_cache = ref_cache
        self._modifications = {}
        self._foundation_client = foundation_client

        # Store run configurations if provided
        self._run_configurations = RunConfigurations(analyses_data or [])

        # Build collision-aware mapping for custom fields
        program_custom_field_names = []
        for cf in program_data.get("programCustomFields", []):
            field_def = ref_cache.get_custom_field(cf.get("customFieldId"))
            if field_def:
                program_custom_field_names.append(field_def["name"])

        self._custom_field_name_mapping = resolve_snake_case_collisions(
            program_custom_field_names
        )

        # Process custom fields with collision-aware names
        self._custom_field_values = {}
        for cf in program_data.get("programCustomFields", []):
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
        for ct in program_data.get("customTables", []):
            table_id = ct.get("customTableDefinitionId")
            table_def = ref_cache.get_custom_table(table_id)
            if table_def:
                table_name = camel_to_snake(table_def["name"])
                row_data = ref_cache.build_custom_table_row(ct, table_id)
                raw_table_rows.setdefault(table_name, []).append(row_data)
                self._custom_table_original_names[table_name] = table_def["name"]
                # Store original row with all metadata (id, sortOrder, etc.)
                self._custom_table_original_rows.setdefault(table_name, []).append(ct)
        self._custom_tables: Dict[str, CustomTable] = {}
        self._custom_table_dirty: Set[str] = set()

        # Process layers
        self._layers = [
            Layer(layer, ref_cache, program=self)
            for layer in program_data.get("layers", [])
        ]

        # Process brokers
        self._brokers = [
            ProgramBroker(broker, ref_cache)
            for broker in program_data.get("programBrokers", [])
        ]

        # Process categories
        self._categories = ProgramCategories(
            program_data.get("programCategories", []), ref_cache
        )

        # Process external references
        self._external_refs = ProgramExternalRefs(
            program_data.get("programExternalRefs", []), ref_cache
        )

        # Build custom fields info (all available program-level custom fields)
        # First collect all program-level field names to detect collisions
        all_program_field_names = []
        for field_id, field_def in ref_cache._custom_fields.items():
            if field_def.get("level") == 1:  # Program level
                all_program_field_names.append(field_def["name"])

        # Create collision-aware mapping for all program fields
        all_program_field_mapping = resolve_snake_case_collisions(
            all_program_field_names
        )

        self._custom_fields_info = []
        for field_id, field_def in ref_cache._custom_fields.items():
            if field_def.get("level") == 1:  # Program level
                original_name = field_def["name"]
                self._custom_fields_info.append(
                    {
                        "id": field_id,
                        "name": original_name,
                        "snake_case_name": all_program_field_mapping.get(
                            original_name, camel_to_snake(original_name)
                        ),
                        "value_type": field_def.get("valueType"),
                    }
                )
        self._custom_fields_info.sort(key=lambda x: x["name"])

        # Build custom tables info (all available program-level custom tables)
        self._custom_tables_info = []
        for table_id, table_def in ref_cache._custom_tables.items():
            # Program level can be dataLevelId 2 or 4
            if table_def.get("dataLevelId") in [2, 4]:
                columns = ref_cache.get_custom_table_columns(table_id)
                column_names = [col["name"] for col in columns]
                snake_name = camel_to_snake(table_def["name"])
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

    # Standard program properties with full type hints
    @property
    def id(self) -> int:
        """Program ID (read-only)."""
        return self._data["id"]

    @property
    def name(self) -> str:
        """Program name."""
        if "name" in self._modifications:
            return self._modifications["name"]
        return self._data.get("name", "")

    @name.setter
    def name(self, value: str) -> None:
        """Set program name."""
        self._modifications["name"] = value

    @property
    def description(self) -> Optional[str]:
        """Program description."""
        if "description" in self._modifications:
            return self._modifications["description"]
        return self._data.get("description")

    @description.setter
    def description(self, value: Optional[str]) -> None:
        """Set program description."""
        self._modifications["description"] = value

    @property
    def uw_year(self) -> Optional[int]:
        """Underwriting year."""
        if "uw_year" in self._modifications:
            return self._modifications["uw_year"]
        return self._data.get("uwYear")

    @uw_year.setter
    def uw_year(self, value: Optional[int]) -> None:
        """Set underwriting year."""
        self._modifications["uw_year"] = value

    @property
    def ceded_flag(self) -> bool:
        """Whether the program is ceded."""
        if "ceded_flag" in self._modifications:
            return self._modifications["ceded_flag"]
        return self._data.get("cededFlag", False)

    @ceded_flag.setter
    def ceded_flag(self, value: bool) -> None:
        """Set ceded flag."""
        self._modifications["ceded_flag"] = value

    @property
    def renewal_flag(self) -> bool:
        """Whether the program is a renewal."""
        if "renewal_flag" in self._modifications:
            return self._modifications["renewal_flag"]
        return self._data.get("renewalFlag", False)

    @renewal_flag.setter
    def renewal_flag(self, value: bool) -> None:
        """Set renewal flag."""
        self._modifications["renewal_flag"] = value

    @property
    def note(self) -> Optional[str]:
        """Program note."""
        if "note" in self._modifications:
            return self._modifications["note"]
        return self._data.get("note")

    @note.setter
    def note(self, value: Optional[str]) -> None:
        """Set program note."""
        self._modifications["note"] = value

    @property
    def years_on_account(self) -> Optional[int]:
        """Years on account."""
        if "years_on_account" in self._modifications:
            return self._modifications["years_on_account"]
        return self._data.get("yearsOnAccount")

    @years_on_account.setter
    def years_on_account(self, value: Optional[int]) -> None:
        """Set years on account."""
        self._modifications["years_on_account"] = value

    # Reference data properties (read-only, enriched)
    @property
    def client(self) -> Optional["Client"]:
        """Client information (read-only, enriched from reference data)."""
        client_id = self._data.get("clientId")
        if client_id:
            return self._ref_cache.get_client(client_id)
        return None

    @property
    def brokers(self) -> List[ProgramBroker]:
        """List of program brokers (read-only)."""
        return self._brokers

    @property
    def layers(self) -> "NamedCollection[Layer]":
        """List of program layers (read-only). Supports indexing by number or name."""
        collection: NamedCollection[Layer] = NamedCollection(self._layers)
        return collection

    @property
    def categories(self) -> ProgramCategories:
        """
        Program categories (read-only).

        Provides dynamic attribute access to categories:

        Example:
            >>> program.categories.modelling_status
            'Valuation'
        """
        return self._categories

    @property
    def custom_fields_info(self) -> List[Dict[str, Any]]:
        """
        List of all available custom fields for programs (read-only).

        Returns metadata about all program-level custom fields including:
        - id: Custom field ID
        - name: Display name (as in Foundation)
        - snake_case_name: Python-friendly attribute name
        - value_type: Data type of the field

        Example:
            >>> for field in program.custom_fields_info:
            ...     print(f"{field['name']} -> {field['snake_case_name']}")
        """
        return self._custom_fields_info

    @property
    def custom_fields_names(self) -> List[str]:
        """
        List of all available custom field names for programs (read-only).

        Returns a list of strings, where each string is the snake_case_name of a custom field.
        Names are sorted alphabetically.

        Example:
            >>> program.custom_fields_names
            ['portfolio_tag', 'underwriter_notes']
        """
        return sorted([field["snake_case_name"] for field in self._custom_fields_info])

    @property
    def custom_tables_info(self) -> List[Dict[str, Any]]:
        """
        List of all available custom tables for programs (read-only).

        Returns metadata about all program-level custom tables including:
        - id: Custom table ID
        - name: Display name (as in Foundation)
        - snake_case_name: Python-friendly attribute name
        - column_count: Number of columns
        - columns: List of column names

        Example:
            >>> for table in program.custom_tables_info:
            ...     print(f"{table['name']}: {table['columns']}")
        """
        return self._custom_tables_info

    @property
    def custom_tables_names(self) -> List[str]:
        """
        List of all available custom table names for programs (read-only).

        Returns a list of strings, where each string is the snake_case_name of a custom table.
        Names are sorted alphabetically.

        Example:
            >>> program.custom_tables_names
            ['buffer_table_1', 'claims_history']
        """
        return sorted([table["snake_case_name"] for table in self._custom_tables_info])

    @property
    def run_configurations(self) -> RunConfigurations:
        """
        Stored analyses (run configurations) for this program (read-only).

        Provides access to pre-configured analyses that have been saved
        in the Foundation Platform. These can be used to run loss analysis
        without manually specifying all parameters.

        Example:
            >>> # List all configurations
            >>> for config in program.run_configurations:
            ...     print(f"{config.label}: {config.sim_years} years")

            >>> # Get specific configuration by label
            >>> config = program.run_configurations.get_by_label("Default")
            >>> if config:
            ...     run_config = config.to_run_configuration()
            ...     losses = layer.layered_losses.as_polars(run_config)
        """
        return self._run_configurations

    @property
    def external_refs(self) -> ProgramExternalRefs:
        """
        Program external system references (read-only).

        Provides dynamic attribute access to external system identifiers:

        Example:
            >>> # Dot-style access (snake_case)
            >>> crm_id = program.external_refs.crm_pro
            >>> dynamics_id = program.external_refs.microsoft_dynamics_ax

            >>> # Iterate over all external refs
            >>> for ref in program.external_refs:
            ...     print(f"{ref.type_name}: {ref.value}")

            >>> # Get specific external ref by type name (alternative)
            >>> crm_id = program.external_refs.get_by_type("CRM Pro")
        """
        return self._external_refs

    # Custom field methods
    def get_custom_field(self, field_name: str) -> Any:
        """
        Get a custom field value by name (original name as defined in Foundation).

        Args:
            field_name: Custom field name as it appears in Foundation (e.g., 'Portfolio Tag')

        Returns:
            Custom field value, or None if not found

        Example:
            >>> program.get_custom_field('Portfolio Tag')
            'NewValue'
        """
        # Use the collision-aware mapping
        snake_name = self._custom_field_name_mapping.get(field_name)
        if snake_name:
            return self._custom_field_values.get(snake_name)

        # Not found
        return None

    def set_custom_field(self, field_name: str, value: Any) -> None:
        """
        Set a custom field value by name (snake_case).

        Args:
            field_name: Custom field name in snake_case (e.g., 'portfolio_tag')
            value: Value to set

        Example:
            >>> program.set_custom_field('portfolio_tag', 'NewValue')
        """
        parsed_value = parse_datetime_string(value)
        self._modifications[field_name] = parsed_value

    def get_custom_table(self, table_name: str) -> CustomTable:
        """
        Get a custom table wrapper by name (original name as defined in Foundation).

        Args:
            table_name: Custom table name as it appears in Foundation (e.g., 'Claims History')

        Returns:
            CustomTable instance for the requested table.

        Example:
            >>> claims_history = program.get_custom_table('Claims History')
            >>> df = claims_history.as_pandas()
            >>> df.head()
        """
        snake_name = camel_to_snake(table_name)
        table = self._custom_tables.get(snake_name)
        if table:
            return table

        # Try to locate by original name in metadata.
        for info in self._custom_tables_info:
            if info["name"] == table_name or info["snake_case_name"] == snake_name:
                return self._custom_tables[info["snake_case_name"]]

        # Fallback: create an empty table definition if it exists in reference data.
        target_id = None
        column_names: List[str] = []
        resolved_name = table_name
        for table_id, table_def in self._ref_cache._custom_tables.items():
            if table_def.get("dataLevelId") in [2, 4]:
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
        Set a custom table by name (accepts original name or snake_case).

        Args:
            table_name: Custom table name (e.g., 'Claims History' or 'claims_history')
            rows: DataFrame or list of table rows (dicts with column names as keys)

        Example:
            >>> program.set_custom_table('Claims History', [
            ...     {'Valuation Date': '2024-01-15', 'Breakout': 'Property'}
            ... ])
        """
        snake_name = camel_to_snake(table_name)
        if snake_name in self._modifications:
            del self._modifications[snake_name]
        table = self.get_custom_table(table_name)
        table.set_data(rows)

    def get_json(self) -> Dict[str, Any]:
        """
        Get the program data as JSON, applying any modifications.

        Returns:
            Dictionary representation of the program with modifications applied
        """
        result = copy.deepcopy(self._original_data)

        # Apply modifications to program level
        for key, value in list(self._modifications.items()):
            # Check if this is a custom field
            if key in self._custom_field_values or self._is_custom_field_name(key):
                # Update the custom field in programCustomFields array
                self._update_custom_field_in_json(result, key, value)
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

        tables_to_update = (
            list(self._custom_table_dirty)
            if self._custom_table_dirty
            else list(self._custom_tables.keys())
        )
        for table_name in tables_to_update:
            table = self._custom_tables.get(table_name)
            if table is None:
                continue
            rows = table.to_records()
            self._update_custom_table_in_json(result, table_name, rows)
        self._custom_table_dirty.clear()

        # Always apply layer JSON to capture any modifications
        # This ensures that any changes to layers (including limits, premiums, custom fields, etc.)
        # are reflected in the program JSON
        for i, layer in enumerate(self._layers):
            result["layers"][i] = layer.get_json()

        return result

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
        """Update a custom field value in the programCustomFields array."""
        # Find the custom field ID using the collision-aware mapping
        field_id = None
        for original_name, snake_name in self._custom_field_name_mapping.items():
            if snake_name == field_name:
                # Found the original name, now get the field ID
                for fid, field_def in self._ref_cache._custom_fields.items():
                    if (
                        field_def.get("level") == 1
                        and field_def["name"] == original_name
                    ):
                        field_id = fid
                        break
                break

        if field_id is None:
            return

        # Update or add the custom field in the array
        custom_fields = result.get("programCustomFields", [])
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
                    "programId": result.get("id"),
                    "program": None,
                    "customFieldId": field_id,
                    "customField": None,
                    "value": value,
                }
            )
            result["programCustomFields"] = custom_fields

    def _update_custom_table_in_json(
        self, result: Dict[str, Any], table_name: str, value: List[Dict[str, Any]]
    ) -> None:
        """
        Update a custom table in the customTables array.

        Converts user-friendly column names back to internal field names
        (numValue1, textValue1, dateValue1, etc.) and maintains row order.
        """
        # Find the custom table definition
        table_id = None
        for tid, table_def in self._ref_cache._custom_tables.items():
            # Program level can be dataLevelId 2 or 4
            if table_def.get("dataLevelId") in [2, 4]:
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
        lookup_columns: Dict[str, int] = {}  # col_name -> lookupObjectId
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

        # Remove existing rows for this table
        custom_tables = result.get("customTables", [])
        custom_tables = [
            ct
            for ct in custom_tables
            if ct.get("customTableDefinitionId") != table_id
            or ct.get("programId") != result.get("id")
        ]

        # Get original rows for this table to preserve metadata
        original_rows = self._custom_table_original_rows.get(table_name, [])

        # Add new rows
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
                    "programId": result.get("id"),
                    "layerId": None,
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

            custom_tables.append(raw_row)

        result["customTables"] = custom_tables

    def __getattr__(self, name: str) -> Any:
        """
        Dynamic attribute access for custom fields and custom tables.

        This enables dot notation access like:
        - program.portfolio_tag (instead of program.get_custom_field('portfolio_tag'))
        - program.buffer_table_1 (instead of program.get_custom_table('buffer_table_1'))

        Args:
            name: Attribute name in snake_case

        Returns:
            Attribute value

        Raises:
            AttributeError: If attribute not found
        """
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
        Dynamic attribute setting for custom fields.

        This enables dot notation assignment like:
        - program.portfolio_tag = "NewValue"

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

        if self._is_custom_table_name(name):
            self.set_custom_table(name, value)
            return

        # Otherwise, treat it as a custom field or modification
        parsed_value = parse_datetime_string(value)
        self._modifications[name] = parsed_value

    def describe(self) -> None:
        """
        Display a comprehensive overview of the program object's available features.

        Shows standard properties, custom fields, custom tables, categories,
        external references, and available methods for easy exploration and discovery.

        Example:
            >>> program.describe()
        """
        print("=" * 80)
        print(f"PROGRAM: {self.name or 'Untitled'} (ID: {self.id})")
        print("=" * 80)

        # Standard Properties
        print("\nSTANDARD PROPERTIES:")
        print(f"  {'Property':<30} {'Access':<10} {'Current Value':<30}")
        print(f"  {'-' * 30} {'-' * 10} {'-' * 30}")

        standard_props = [
            ("id", "read-only", self.id),
            ("name", "read-write", self.name),
            ("description", "read-write", self.description),
            ("uw_year", "read-write", self.uw_year),
            ("ceded_flag", "read-write", self.ceded_flag),
            ("renewal_flag", "read-write", self.renewal_flag),
            ("note", "read-write", self.note),
            ("years_on_account", "read-write", self.years_on_account),
        ]

        for prop_name, access, value in standard_props:
            value_str = str(value)[:28] if value is not None else "None"
            print(f"  {prop_name:<30} {access:<10} {value_str:<30}")

        # Reference Data
        print("\nREFERENCE DATA (read-only):")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-' * 30} {'-' * 40}")

        client_name = self.client.name if self.client else None
        print(
            f"  {'client':<30} {str(client_name)[:38] if client_name else 'None':<40}"
        )

        # Categories
        print("\nCATEGORIES:")
        if self.categories:
            print(f"  {'Category':<35} {'Value':<35}")
            print(f"  {'-' * 35} {'-' * 35}")
            category_attrs = [
                attr for attr in dir(self.categories) if not attr.startswith("_")
            ]
            for cat_name in sorted(category_attrs):
                try:
                    cat_value = getattr(self.categories, cat_name)
                    if not callable(cat_value):
                        value_str = (
                            str(cat_value)[:33] if cat_value is not None else "None"
                        )
                        print(f"  {cat_name:<35} {value_str:<35}")
                except AttributeError:
                    pass
        else:
            print("  (none)")

        # External References
        print("\nEXTERNAL REFERENCES:")
        if self.external_refs:
            print(f"  {'System':<35} {'Value':<35}")
            print(f"  {'-' * 35} {'-' * 35}")
            ext_ref_attrs = [
                attr for attr in dir(self.external_refs) if not attr.startswith("_")
            ]
            for ref_name in sorted(ext_ref_attrs):
                try:
                    ref_value = getattr(self.external_refs, ref_name)
                    if not callable(ref_value):
                        value_str = (
                            str(ref_value)[:33] if ref_value is not None else "None"
                        )
                        print(f"  {ref_name:<35} {value_str:<35}")
                except AttributeError:
                    pass
        else:
            print("  (none)")

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
        brokers_count = len(self.brokers)
        layers_count = len(self.layers)
        print(f"  brokers                        {brokers_count} item(s)")
        print(f"  layers                         {layers_count} item(s)")

        # Methods
        print("\nAVAILABLE METHODS:")
        methods = [
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
            "TIP: Use dot notation to access any attribute (e.g., program.portfolio_tag)"
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
                "description",
                "uw_year",
                "ceded_flag",
                "renewal_flag",
                "note",
                "years_on_account",
                "client",
                "brokers",
                "layers",
                "categories",
                "external_refs",
                "custom_fields_info",
                "custom_fields_names",
                "custom_tables_info",
                "custom_tables_names",
            ]
        )

        # Add methods
        attrs.update(
            [
                "get_custom_field",
                "set_custom_field",
                "get_custom_table",
                "set_custom_table",
                "get_json",
                "describe",
            ]
        )

        # Add dynamic attributes
        attrs.update(self._custom_field_values.keys())
        attrs.update(self._custom_tables.keys())

        return sorted(list(attrs))
