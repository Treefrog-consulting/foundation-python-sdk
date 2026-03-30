"""Reference Data Cache for Foundation Platform API"""

from typing import Dict, List, Any, Optional, Union
from .models.broker import Broker
from .models.broker_office import BrokerOffice
from .models.currency import Currency
from .models.unit_of_measure import UnitOfMeasure
from .models.layer_status import LayerStatus
from .models.layer_limit_attach_type import LayerLimitAttachType
from .models.layer_premium_type import LayerPremiumType
from .models.participant import Participant
from .models.participant_status import ParticipantStatus
from .models.client import Client
from .models.reinst_prem_base import ReinstPremBase
from .models.peril import Peril
from .models.region import Region
from .utils import parse_datetime_string


class ReferenceDataCache:
    """Cache for reference data from the Foundation Platform API."""

    def __init__(
        self,
        deal_ref_data: Dict[str, Any],
        clients_data: List[Dict[str, Any]],
        common_ref_data: Dict[str, Any],
        accessible_objects_data: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the reference data cache.

        Args:
            deal_ref_data: Reference data from Deal Module /api/ref endpoint
            clients_data: Client data from /api/clients endpoint
            common_ref_data: Reference data from Common Module /api/ref endpoint
            accessible_objects_data: Accessible object definitions and values for lookup field resolution
        """
        self._deal_ref = deal_ref_data
        self._common_ref = common_ref_data
        self._clients = {client["id"]: client for client in clients_data}

        # Index Deal Module reference data by ID for a quick lookup
        self._brokers = self._index_values(
            deal_ref_data.get("broker", {}).get("values", [])
        )
        self._broker_offices = self._index_values(
            deal_ref_data.get("brokerOffice", {}).get("values", [])
        )
        self._currencies = self._index_values(
            deal_ref_data.get("currency", {}).get("values", [])
        )
        self._units_of_measure = self._index_values(
            deal_ref_data.get("unitOfMeasure", {}).get("values", [])
        )
        self._layer_statuses = self._index_values(
            deal_ref_data.get("layerStatus", {}).get("values", [])
        )
        self._layer_limit_attach_types = self._index_values(
            deal_ref_data.get("layerLimitAttachType", {}).get("values", [])
        )
        self._layer_premium_types = self._index_values(
            deal_ref_data.get("layerPremiumType", {}).get("values", [])
        )
        self._custom_fields = self._index_values(
            deal_ref_data.get("customField", {}).get("values", [])
        )
        self._custom_tables = self._index_values(
            deal_ref_data.get("customTableDefinitions", {}).get("values", [])
        )
        self._custom_table_columns = deal_ref_data.get(
            "customTableColumnDefinitions", {}
        ).get("values", [])
        self._participants = self._index_values(
            deal_ref_data.get("participant", {}).get("values", [])
        )
        self._participant_statuses = self._index_values(
            deal_ref_data.get("participantStatus", {}).get("values", [])
        )
        self._reinst_prem_bases = self._index_values(
            deal_ref_data.get("reinstPremBase", {}).get("values", [])
        )
        self._layer_order_term_types = self._index_values(
            deal_ref_data.get("layerOrderTermType", {}).get("values", [])
        )
        self._categories = self._index_values(
            deal_ref_data.get("category", {}).get("values", [])
        )
        self._category_details = self._index_values(
            deal_ref_data.get("categoryDetail", {}).get("values", [])
        )
        self._external_ref_types = self._index_values(
            deal_ref_data.get("externalRefType", {}).get("values", [])
        )

        # Index Common Module reference data
        self._perils = self._index_values(
            common_ref_data.get("peril", {}).get("values", [])
        )
        self._perils_by_code = self._index_by_field(
            common_ref_data.get("peril", {}).get("values", []), "code"
        )
        self._regions = self._index_values(
            common_ref_data.get("region", {}).get("values", [])
        )
        self._regions_by_code = self._index_by_field(
            common_ref_data.get("region", {}).get("values", []), "code"
        )

        # Index model view details for model ID lookup by model view ID
        self._model_view_details = common_ref_data.get("modelViewDetail", {}).get(
            "values", []
        )
        self._model_ids_by_view_id = self._build_model_ids_by_view_id(
            self._model_view_details
        )

        # Index participants by name for enhanced lookup
        self._participants_by_name = self._index_by_field(
            deal_ref_data.get("participant", {}).get("values", []), "name"
        )

        # Build lookup maps for accessible objects (lookup field resolution)
        self._lookup_maps: Dict[int, Dict[Any, str]] = {}
        self._reverse_lookup_maps: Dict[int, Dict[str, Any]] = {}
        self._accessible_objects: Dict[int, Dict[str, Any]] = {}
        self._build_lookup_maps(accessible_objects_data or {})

    @staticmethod
    def _build_model_ids_by_view_id(
        model_view_details: List[Dict[str, Any]],
    ) -> Dict[int, List[int]]:
        """Build a mapping from model view ID to list of model IDs."""
        result: Dict[int, List[int]] = {}
        for detail in model_view_details:
            view_id = detail.get("modelViewId")
            model_id = detail.get("modelId")
            if view_id is not None and model_id is not None:
                if view_id not in result:
                    result[view_id] = []
                result[view_id].append(model_id)
        return result

    def _build_lookup_maps(self, accessible_objects_data: Dict[str, Any]) -> None:
        """Build forward and reverse lookup maps from accessible objects data.

        Forward maps resolve raw lookup IDs to display labels (for reading).
        Reverse maps resolve display labels back to raw IDs (for saving).

        Args:
            accessible_objects_data: Dict with 'definitions' and 'values' keys
                from _load_accessible_objects().
        """
        definitions = accessible_objects_data.get("definitions", [])
        values_list = accessible_objects_data.get("values", [])

        # Index definitions by ID
        for ao in definitions:
            ao_id = ao.get("id")
            if ao_id is not None:
                self._accessible_objects[ao_id] = ao

        # Build lookup maps from BulkSelection values
        for value_result in values_list:
            ao_id = value_result.get("accessibleObjectId")
            if ao_id is None:
                continue

            ao_def = self._accessible_objects.get(ao_id)
            if ao_def is None:
                continue

            pk_field = ao_def.get("internalFieldPKName")
            display_field = ao_def.get("internalFieldDisplayName")
            if not pk_field or not display_field:
                continue

            # The API lowercases the first letter of field names in the response
            pk_key = self._lowercase_first_letter(pk_field)
            display_key = self._lowercase_first_letter(display_field)

            forward: Dict[Any, str] = {}
            reverse: Dict[str, Any] = {}

            for item in value_result.get("accessibleObjectValues", []):
                pk_value = item.get(pk_key)
                display_value = item.get(display_key)
                if pk_value is not None and display_value is not None:
                    forward[pk_value] = display_value
                    reverse[display_value] = pk_value

            self._lookup_maps[ao_id] = forward
            self._reverse_lookup_maps[ao_id] = reverse

    @staticmethod
    def _lowercase_first_letter(s: str) -> str:
        """Lowercase the first character of a string.

        Mirrors the .NET LowercaseFirstLetter utility used by
        AccessibleObjectsController to serialize field names.
        """
        if not s:
            return s
        return s[0].lower() + s[1:]

    def resolve_lookup_value(self, accessible_object_id: int, raw_value: Any) -> Any:
        """Resolve a raw lookup ID to its display label.

        The custom table stores lookup values as strings (LookupValue1-10 are
        string columns in the DB), but accessible object PK values may be
        integers. This method tries the raw value first, then attempts numeric
        conversion to handle the type mismatch.

        Args:
            accessible_object_id: The accessible object ID for the lookup column.
            raw_value: The raw lookup value (string from the custom table row).

        Returns:
            The display label if found, otherwise the original raw_value.
        """
        forward_map = self._lookup_maps.get(accessible_object_id)
        if forward_map is None:
            return raw_value
        # Try direct lookup first
        result = forward_map.get(raw_value)
        if result is not None:
            return result
        # LookupValue columns are strings in the DB, but PK keys in the
        # forward map may be integers. Try numeric conversion.
        if isinstance(raw_value, str):
            try:
                numeric = int(raw_value)
            except (ValueError, TypeError):
                try:
                    numeric = float(raw_value)
                except (ValueError, TypeError):
                    return raw_value
            result = forward_map.get(numeric)
            if result is not None:
                return result
        return raw_value

    def reverse_lookup_value(
        self, accessible_object_id: int, display_value: Any
    ) -> Any:
        """Resolve a display label back to its raw lookup ID.

        Returns the PK value as a string since LookupValue columns in the
        custom table DB schema are string-typed.

        Args:
            accessible_object_id: The accessible object ID for the lookup column.
            display_value: The display label to reverse-resolve.

        Returns:
            The raw lookup ID (as string) if found, otherwise the original
            display_value (which allows passing raw IDs directly).
        """
        reverse_map = self._reverse_lookup_maps.get(accessible_object_id)
        if reverse_map is None:
            return display_value
        result = reverse_map.get(display_value)
        if result is not None:
            # LookupValue columns are strings in the DB, so convert back
            return str(result)
        return display_value

    def get_model_ids_by_view_id(self, model_view_id: int) -> List[int]:
        """Get list of model IDs for a given model view ID.

        Args:
            model_view_id: The model view ID to look up.

        Returns:
            List of model IDs associated with the model view, or empty list if not found.
        """
        return self._model_ids_by_view_id.get(model_view_id, [])

    @staticmethod
    def _index_values(values: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        """Index a list of values by their ID (returns raw dicts for internal use)."""
        return {item["id"]: item for item in values}

    @staticmethod
    def _index_by_field(
        values: List[Dict[str, Any]], field: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Index a list of values by a specific field (case-insensitive for strings).

        Args:
            values: List of dictionaries to index
            field: Field name to use as the key

        Returns:
            Dictionary indexed by the specified field (lowercase for strings)
        """
        result = {}
        for item in values:
            key = item.get(field)
            if key is not None:
                # Normalize string keys to lowercase for case-insensitive lookup
                if isinstance(key, str):
                    key = key.lower()
                result[key] = item
        return result

    def get_client(self, client_id: int) -> Optional[Client]:
        """Get a client by ID."""
        data = self._clients.get(client_id)
        return Client(data) if data else None

    def get_broker(self, broker_id: int) -> Optional[Broker]:
        """Get broker by ID."""
        data = self._brokers.get(broker_id)
        return Broker(data) if data else None

    def get_broker_office(self, office_id: int) -> Optional[BrokerOffice]:
        """Get broker office by ID."""
        data = self._broker_offices.get(office_id)
        return BrokerOffice(data) if data else None

    def get_currency(self, currency_id: int) -> Optional[Currency]:
        """Get currency by ID."""
        data = self._currencies.get(currency_id)
        return Currency(data) if data else None

    def get_unit_of_measure(self, unit_id: int) -> Optional[UnitOfMeasure]:
        """Get a unit of measure by ID."""
        data = self._units_of_measure.get(unit_id)
        return UnitOfMeasure(data) if data else None

    def get_layer_status(self, status_id: int) -> Optional[LayerStatus]:
        """Get layer status by ID."""
        data = self._layer_statuses.get(status_id)
        return LayerStatus(data) if data else None

    def get_layer_limit_attach_type(
        self, type_id: int
    ) -> Optional[LayerLimitAttachType]:
        """Get layer limit/attach type by ID."""
        data = self._layer_limit_attach_types.get(type_id)
        return LayerLimitAttachType(data) if data else None

    def get_layer_premium_type(self, type_id: int) -> Optional[LayerPremiumType]:
        """Get layer premium type by ID."""
        data = self._layer_premium_types.get(type_id)
        return LayerPremiumType(data) if data else None

    def get_custom_field(self, field_id: int) -> Optional[Dict[str, Any]]:
        """Get custom field definition by ID (returns dict for dynamic processing)."""
        return self._custom_fields.get(field_id)

    def get_custom_table(self, table_id: int) -> Optional[Dict[str, Any]]:
        """Get custom table definition by ID (returns dict for dynamic processing)."""
        return self._custom_tables.get(table_id)

    def get_participant(self, identifier: Union[int, str]) -> Optional[Participant]:
        """
        Get participant by ID or name.

        Args:
            identifier: Either an integer ID or a string name

        Returns:
            Participant instance or None if not found

        Example:
            >>> participant = cache.get_participant(1)  # By ID
            >>> participant = cache.get_participant("Company X")  # By name
        """
        if isinstance(identifier, int):
            data = self._participants.get(identifier)
        elif isinstance(identifier, str):
            data = self._participants_by_name.get(identifier.lower())
        else:
            return None
        return Participant(data) if data else None

    def get_all_participants(self) -> List[Participant]:
        """
        Get all participants.

        Returns:
            List of all Participant instances
        """
        return [Participant(data) for data in self._participants.values()]

    def get_participant_status(self, status_id: int) -> Optional[ParticipantStatus]:
        """Get participant status by ID."""
        data = self._participant_statuses.get(status_id)
        return ParticipantStatus(data) if data else None

    def get_reinst_prem_base(self, base_id: int) -> Optional[ReinstPremBase]:
        """Get reinstatement premium base by ID."""
        data = self._reinst_prem_bases.get(base_id)
        return ReinstPremBase(data) if data else None

    def get_layer_order_term_type(self, type_id: int) -> Optional[Dict[str, Any]]:
        """Get layer order term type by ID."""
        return self._layer_order_term_types.get(type_id)

    def get_category(self, category_id: int) -> Optional[Dict[str, Any]]:
        """Get category by ID."""
        return self._categories.get(category_id)

    def get_category_detail(self, detail_id: int) -> Optional[Dict[str, Any]]:
        """Get category detail by ID."""
        return self._category_details.get(detail_id)

    def get_external_ref_type(self, type_id: int) -> Optional[Dict[str, Any]]:
        """Get external ref type by ID."""
        return self._external_ref_types.get(type_id)

    def get_custom_table_columns(self, table_id: int) -> List[Dict[str, Any]]:
        """Get column definitions for a custom table."""
        return [
            col
            for col in self._custom_table_columns
            if col.get("customTableDefinitionId") == table_id
        ]

    def get_peril(self, identifier: Union[int, str]) -> Optional[Peril]:
        """
        Get peril by ID or code.

        Args:
            identifier: Either an integer ID or a string code

        Returns:
            Peril instance or None if not found

        Example:
            >>> peril = cache.get_peril(1)  # By ID
            >>> peril = cache.get_peril("EQ")  # By code
        """
        if isinstance(identifier, int):
            data = self._perils.get(identifier)
        elif isinstance(identifier, str):
            data = self._perils_by_code.get(identifier.lower())
        else:
            return None
        return Peril(data) if data else None

    def get_all_perils(self) -> List[Peril]:
        """
        Get all perils.

        Returns:
            List of all Peril instances
        """
        return [Peril(data) for data in self._perils.values()]

    def get_region(self, identifier: Union[int, str]) -> Optional[Region]:
        """
        Get region by ID or code.

        Args:
            identifier: Either an integer ID or a string code

        Returns:
            Region instance or None if not found

        Example:
            >>> region = cache.get_region(1)  # By ID
            >>> region = cache.get_region("NA")  # By code
        """
        if isinstance(identifier, int):
            data = self._regions.get(identifier)
        elif isinstance(identifier, str):
            data = self._regions_by_code.get(identifier.lower())
        else:
            return None
        return Region(data, self) if data else None

    def get_all_regions(self) -> List[Region]:
        """
        Get all regions.

        Returns:
            List of all Region instances
        """
        return [Region(data, self) for data in self._regions.values()]

    def build_custom_table_row(
        self, raw_row: Dict[str, Any], table_id: int
    ) -> Dict[str, Any]:
        """
        Build a custom table row with proper column names.

        Automatically converts datetime strings to date or datetime objects,
        same as custom fields. Lookup columns (valueType=5) are automatically
        resolved to display labels using accessible object definitions.

        Args:
            raw_row: Raw row data with numValue1, textValue1, etc.
            table_id: Custom table definition ID

        Returns:
            Dictionary with actual column names as keys
        """
        columns = self.get_custom_table_columns(table_id)
        result = {}

        for col in columns:
            col_name = col["name"]
            value_type = col["valueType"]
            field_num = col["valueFieldNum"]

            # Map valueType to field name
            field_map = {
                1: f"numValue{field_num}",  # Number
                2: f"dateValue{field_num}",  # Date
                3: f"bitValue{field_num}",  # Boolean
                4: f"textValue{field_num}",  # Text
                5: f"lookupValue{field_num}",  # Lookup
            }

            field_name = field_map.get(value_type)
            if field_name:
                value = raw_row.get(field_name)

                # Resolve lookup fields to display labels
                if value_type == 5 and value is not None:
                    lookup_object_id = col.get("lookupObjectId")
                    if lookup_object_id is not None:
                        value = self.resolve_lookup_value(lookup_object_id, value)

                # Parse datetime strings automatically (same as custom fields)
                result[col_name] = parse_datetime_string(value)

        return result
