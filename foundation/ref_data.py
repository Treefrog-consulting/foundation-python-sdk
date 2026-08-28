"""Reference Data Cache for Foundation Platform API"""

from collections.abc import Callable
from typing import Any

from .models._reference_tables import (
    CLIENTS,
    COMMON,
    DEAL,
    IDENTITY,
    LOSS,
    REFERENCE_TABLES,
)
from .models.base import BaseModel
from .models.broker import Broker
from .models.broker_office import BrokerOffice
from .models.category import Category
from .models.category_detail import CategoryDetail
from .models.client import Client
from .models.currency import Currency
from .models.custom_field import CustomField
from .models.custom_table_definition import CustomTableDefinition
from .models.event_set import EventSet
from .models.external_ref_type import ExternalRefType
from .models.layer_limit_attach_type import LayerLimitAttachType
from .models.layer_order_term_type import LayerOrderTermType
from .models.layer_premium_type import LayerPremiumType
from .models.layer_status import LayerStatus
from .models.participant import Participant
from .models.participant_status import ParticipantStatus
from .models.peril import Peril
from .models.perspective import Perspective
from .models.reference_table import ReferenceTable
from .models.region import Region
from .models.reinst_prem_base import ReinstPremBase
from .models.role import Role
from .models.unit_of_measure import UnitOfMeasure
from .models.user import User
from .models.variant import Variant
from .models.vendor import Vendor
from .utils import (
    camel_to_snake,
    claim_unique_name,
    parse_datetime_string,
    ref_values,
)

#: Which definition levels a Program (1) or a Layer (2) carries, mirroring the
#: server's CustomFieldLevel enum: ProgramAndLayer (3) belongs to both, and
#: LayerAndEcon (4) to the layer. Ylt (5) reaches neither.
_DEFINITION_LEVELS = {
    1: frozenset({1, 3}),
    2: frozenset({2, 3, 4}),
}

#: The levels this release knows how to place. A definition carrying anything
#: else -- a level the platform adds later, or none at all -- is offered to
#: both objects rather than dropped: over-reporting a field is recoverable,
#: silently losing the value a deal stores for it is not.
_KNOWN_DEFINITION_LEVELS = frozenset({1, 2, 3, 4, 5})


class ReferenceDataCache:
    """Cache for reference data from the Foundation Platform API."""

    def __init__(
        self,
        deal_ref_data: dict[str, Any],
        clients_data: list[dict[str, Any]],
        common_ref_data: dict[str, Any],
        accessible_objects_data: dict[str, Any] | None = None,
        loss_ref_data: dict[str, Any] | None = None,
        roles_data: list[dict[str, Any]] | None = None,
        users_data: list[dict[str, Any]] | None = None,
    ):
        """
        Initialize the reference data cache.

        Args:
            deal_ref_data: Reference data from Deal Module /api/ref endpoint
            clients_data: Client data from /api/clients endpoint
            common_ref_data: Reference data from Common Module /api/ref endpoint
            accessible_objects_data: Accessible object definitions and values for lookup field resolution
            loss_ref_data: Reference data from Loss Module /api/ref endpoint
            roles_data: Roles from the Identity Module /api/Roles endpoint
            users_data: Users from the Identity Module /api/Users endpoint
        """
        self._deal_ref = deal_ref_data or {}
        self._common_ref = common_ref_data or {}
        self._clients = self._index_values(clients_data or [])

        # code/name lookup indexes, built per table on first lookup and keyed
        # by the identity of that table's by-ID dict
        self._string_indexes: dict[int, dict[str, dict[str, Any]]] = {}

        deal_ref = self._deal_ref
        common_ref = self._common_ref

        # Index Deal Module reference data by ID for a quick lookup
        self._brokers = self._index_values(ref_values(deal_ref, "broker"))
        self._broker_offices = self._index_values(ref_values(deal_ref, "brokerOffice"))
        self._currencies = self._index_values(ref_values(deal_ref, "currency"))
        self._units_of_measure = self._index_values(
            ref_values(deal_ref, "unitOfMeasure")
        )
        self._layer_statuses = self._index_values(ref_values(deal_ref, "layerStatus"))
        self._layer_limit_attach_types = self._index_values(
            ref_values(deal_ref, "layerLimitAttachType")
        )
        self._layer_premium_types = self._index_values(
            ref_values(deal_ref, "layerPremiumType")
        )
        self._custom_fields = self._index_values(ref_values(deal_ref, "customField"))
        self._custom_tables = self._index_values(
            ref_values(deal_ref, "customTableDefinitions")
        )
        self._custom_table_columns = ref_values(
            deal_ref, "customTableColumnDefinitions"
        )
        self._participants = self._index_values(ref_values(deal_ref, "participant"))
        self._participant_statuses = self._index_values(
            ref_values(deal_ref, "participantStatus")
        )
        self._reinst_prem_bases = self._index_values(
            ref_values(deal_ref, "reinstPremBase")
        )
        self._layer_order_term_types = self._index_values(
            ref_values(deal_ref, "layerOrderTermType")
        )
        self._categories = self._index_values(ref_values(deal_ref, "category"))
        self._category_details = self._index_values(
            ref_values(deal_ref, "categoryDetail")
        )
        self._external_ref_types = self._index_values(
            ref_values(deal_ref, "externalRefType")
        )

        # Index Common Module reference data
        self._perils = self._index_values(ref_values(common_ref, "peril"))
        self._regions = self._index_values(ref_values(common_ref, "region"))

        # Index model view details for model ID lookup by model view ID
        self._model_view_details = ref_values(common_ref, "modelViewDetail")
        self._model_ids_by_view_id = self._build_model_ids_by_view_id(
            self._model_view_details
        )

        # Build lookup maps for accessible objects (lookup field resolution)
        self._lookup_maps: dict[int, dict[Any, str]] = {}
        self._reverse_lookup_maps: dict[int, dict[str, Any]] = {}
        self._accessible_objects: dict[int, dict[str, Any]] = {}
        self._build_lookup_maps(accessible_objects_data or {})

        # Loss-module reference data (used by loss-set upload for
        # string-name -> ID resolution). Index by id + name for O(1) lookup,
        # matching the Peril / Region / Participant pattern.
        loss_ref = self._loss_ref = loss_ref_data or {}
        # Clients arrive from their own endpoint; wrap them in the same envelope
        # shape so every reference table reads through one path.
        self._payloads = {
            DEAL: self._deal_ref,
            COMMON: self._common_ref,
            LOSS: loss_ref,
            CLIENTS: {"clients": {"values": clients_data or []}},
            IDENTITY: {
                "roles": {"values": roles_data or []},
                "users": {"values": users_data or []},
            },
        }
        self._vendors = self._index_values(ref_values(loss_ref, "vendor"))
        self._variants = self._index_values(ref_values(loss_ref, "variant"))
        self._perspectives = self._index_values(ref_values(loss_ref, "perspective"))
        self._event_sets = self._index_values(ref_values(loss_ref, "eventSet"))

        # Identity roles and users. Their ids are GUID strings rather than ints;
        # _index_values folds string keys and _lookup_ref tries the id index
        # before the name one, so a GUID resolves like any other identifier.
        self._roles = self._index_values(roles_data or [])
        self._users = self._index_values(users_data or [])

        # Reference tables are built on first access and kept for the session.
        self._ref_tables: dict[str, ReferenceTable] = {}
        self._category_snake_names: dict[Any, str] | None = None
        self._category_snake_claimed: set[str] = set()
        self._custom_field_snake_maps: dict[int, dict[Any, str]] = {}
        self._custom_field_definitions: dict[int, dict[Any, dict[str, Any]]] = {}
        self._custom_table_columns_by_table: dict[Any, list[dict[str, Any]]] | None = (
            None
        )

    def reference_table(self, name: str) -> ReferenceTable:
        """
        Get a reference table by its SDK property name.

        Args:
            name: Property name, e.g. "brokers" (see _reference_tables.py)

        Returns:
            ReferenceTable holding the active rows for that table

        Raises:
            KeyError: If the name is not a known reference table
        """
        cached = self._ref_tables.get(name)
        if cached is not None:
            return cached

        entry = REFERENCE_TABLES.get(name)
        if entry is None:
            raise KeyError(f"Unknown reference table '{name}'")
        source, payload_key, model_cls = entry
        rows = ref_values(self._payloads[source], payload_key)

        if model_cls is Region:
            # Region resolves its parent through the cache.
            def factory(row: dict[str, Any]) -> Any:
                return Region(row, self)
        else:
            factory = model_cls or BaseModel

        table = ReferenceTable(rows, factory, name)
        self._ref_tables[name] = table
        return table

    @staticmethod
    def _build_model_ids_by_view_id(
        model_view_details: list[dict[str, Any]],
    ) -> dict[int, list[int]]:
        """Build a mapping from model view ID to list of model IDs."""
        result: dict[int, list[int]] = {}
        for detail in model_view_details:
            view_id = detail.get("modelViewId")
            model_id = detail.get("modelId")
            if view_id is not None and model_id is not None:
                if view_id not in result:
                    result[view_id] = []
                result[view_id].append(model_id)
        return result

    def _build_lookup_maps(self, accessible_objects_data: dict[str, Any]) -> None:
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

            forward: dict[Any, str] = {}
            reverse: dict[str, Any] = {}

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

    def get_model_ids_by_view_id(self, model_view_id: int) -> list[int]:
        """Get list of model IDs for a given model view ID.

        Args:
            model_view_id: The model view ID to look up.

        Returns:
            List of model IDs associated with the model view, or empty list if not found.
        """
        return self._model_ids_by_view_id.get(model_view_id, [])

    def _lookup(
        self,
        identifier: int | str | None,
        by_id: dict[Any, dict[str, Any]],
        model_cls: Callable[[dict[str, Any]], Any],
    ) -> Any | None:
        """Resolve a reference row by ID, code or name and wrap it in a model."""
        return self._lookup_ref(
            identifier, by_id, self._string_index(by_id, model_cls), model_cls
        )

    def _string_index(
        self,
        by_id: dict[Any, dict[str, Any]],
        model_cls: Callable[[dict[str, Any]], Any],
    ) -> dict[str, dict[str, Any]]:
        """Return a case-insensitive name+code+email index for a reference table.

        Built on first lookup and kept. A row is reachable by its name, its code
        or its email. Code wins a collision, which preserves the code-keyed
        lookups that shipped before names were accepted (get_peril("EQ"),
        get_region("NA")).

        The name comes from the model rather than the payload: an Identity user
        record carries no ``name`` key and assembles one from its first and last,
        so indexing the raw field would leave every user unreachable by name.

        Cached against the identity of the by-ID dict rather than a hand-written
        key, so an index can never be served for the wrong table. Those dicts are
        instance attributes assigned once in __init__ and held for the lifetime
        of this cache, so their ids are stable.
        """
        cached = self._string_indexes.get(id(by_id))
        if cached is None:
            rows = list(by_id.values())
            by_name = self._index_by_field(rows, "name")
            # Only a row carrying no "name" needs its model built: an Identity
            # user record has none and assembles one from its first and last.
            # Every other table answers from the raw key, as it always did.
            for row in rows:
                if row.get("name") is None:
                    name = getattr(model_cls(row), "name", None)
                    if isinstance(name, str) and name:
                        by_name[name.lower()] = row
            cached = {
                **by_name,
                **self._index_by_field(rows, "email"),
                **self._index_by_field(rows, "code"),
            }
            self._string_indexes[id(by_id)] = cached
        return cached

    @classmethod
    def _index_values(cls, values: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
        """Index a list of values by their ID (returns raw dicts for internal use).

        Rows without an ``id`` are skipped rather than raising.
        """
        return cls._index_by_field(values, "id")

    @staticmethod
    def _index_by_field(
        values: list[dict[str, Any]], field: str
    ) -> dict[str, dict[str, Any]]:
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

    def get_client(self, identifier: int | str) -> Client | None:
        """Get a client by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._clients, Client)

    def get_broker(self, identifier: int | str) -> Broker | None:
        """Get broker by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._brokers, Broker)

    def get_broker_office(self, identifier: int | str) -> BrokerOffice | None:
        """Get broker office by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._broker_offices, BrokerOffice)

    def get_currency(self, identifier: int | str) -> Currency | None:
        """Get currency by ID, code or name (case-insensitive)."""
        return self._lookup(identifier, self._currencies, Currency)

    def get_unit_of_measure(self, identifier: int | str) -> UnitOfMeasure | None:
        """Get a unit of measure by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._units_of_measure, UnitOfMeasure)

    def get_layer_status(self, identifier: int | str) -> LayerStatus | None:
        """Get layer status by ID, code or name (case-insensitive)."""
        return self._lookup(identifier, self._layer_statuses, LayerStatus)

    def get_layer_limit_attach_type(
        self, identifier: int | str
    ) -> LayerLimitAttachType | None:
        """Get layer limit/attach type by ID or name (case-insensitive)."""
        return self._lookup(
            identifier, self._layer_limit_attach_types, LayerLimitAttachType
        )

    def get_layer_premium_type(self, identifier: int | str) -> LayerPremiumType | None:
        """Get layer premium type by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._layer_premium_types, LayerPremiumType)

    def get_custom_field(self, identifier: int | str) -> CustomField | None:
        """Get custom field definition by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._custom_fields, CustomField)

    def get_custom_table(self, identifier: int | str) -> CustomTableDefinition | None:
        """Get custom table definition by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._custom_tables, CustomTableDefinition)

    def get_participant(self, identifier: int | str) -> Participant | None:
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
        return self._lookup(identifier, self._participants, Participant)

    def get_participant_status(self, identifier: int | str) -> ParticipantStatus | None:
        """Get participant status by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._participant_statuses, ParticipantStatus)

    def get_reinst_prem_base(self, identifier: int | str) -> ReinstPremBase | None:
        """Get reinstatement premium base by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._reinst_prem_bases, ReinstPremBase)

    def get_layer_order_term_type(
        self, identifier: int | str
    ) -> LayerOrderTermType | None:
        """Get layer order term type by ID or name (case-insensitive)."""
        return self._lookup(
            identifier, self._layer_order_term_types, LayerOrderTermType
        )

    def get_category(self, identifier: int | str) -> Category | None:
        """Get category by ID, code or name (case-insensitive)."""
        return self._lookup(identifier, self._categories, Category)

    def get_category_detail(self, identifier: int | str) -> CategoryDetail | None:
        """Get category detail by ID, code or name (case-insensitive)."""
        return self._lookup(identifier, self._category_details, CategoryDetail)

    def active_categories(self) -> list[Category]:
        """Every category a deal can carry, in the platform's display order.

        A deal's Categories panel lists these whether or not the deal sets them,
        so ProgramCategories seeds itself from this rather than from the deal's
        own rows.
        """
        return self._active_in_display_order("categories")

    def category_snake_name(self, category: Category) -> str:
        """The attribute name a category is reachable under.

        Keyed by category ID, not by display name: two categories can carry the
        same name, and two different names can snake_case alike ("Loss Trigger"
        and "loss-trigger"). Either way the second gets a numeric suffix, the way
        custom fields resolve theirs -- keying by name would instead merge two
        categories' values into one attribute.

        Every active category is registered on first use and held for the
        session; a category a deal assigns after the platform retired it is
        registered on demand, and keeps the name it was given.
        """
        if self._category_snake_names is None:
            self._category_snake_names = {}
            self._category_snake_claimed = set()
            for active in self.active_categories():
                self._register_category_name(active)
        known = self._category_snake_names.get(category.id)
        return known if known is not None else self._register_category_name(category)

    def custom_field_definitions(self, level: int) -> dict[Any, dict[str, Any]]:
        """The custom-field definitions a Program (1) or Layer (2) carries.

        A definition's level says which object holds it, and two of them belong
        to both: the server puts a ProgramAndLayer field in a deal's array and
        in every layer's, and a LayerAndEcon field in the layer's. Matching the
        level exactly would drop those, so the rule is written once here rather
        than at each of the places that filters definitions.

        Held for the session, like the map built from it: the definitions do
        not change under the cache, and a deal's every layer would otherwise
        rebuild the same dict.
        """
        known = self._custom_field_definitions.get(level)
        if known is None:
            wanted = _DEFINITION_LEVELS.get(level, frozenset({level}))
            known = {
                field_id: field_def
                for field_id, field_def in self._custom_fields.items()
                if (field_def.get("level") in wanted)
                or (field_def.get("level") not in _KNOWN_DEFINITION_LEVELS)
            }
            self._custom_field_definitions[level] = known
        return known

    def custom_field_snake_map(self, level: int) -> dict[Any, str]:
        """Custom-field id -> snake_case name, for one level.

        Keyed by id, not by name: merging the levels above lets two definitions
        share a name, which the platform allows because its uniqueness index is
        on the name and level together. Keying by name would hand both fields
        one attribute and lose a value, the way categories would without
        :meth:`category_snake_name`.

        Resolved over every definition the level carries rather than the subset
        a deal happens to hold, so a collision lands on the same name for every
        deal and the name a value is stored under is the one
        ``custom_fields_info`` advertises. Held for the session: the
        definitions do not change under it, and a deal's every layer would
        otherwise rebuild the same map.
        """
        known = self._custom_field_snake_maps.get(level)
        if known is None:
            claimed: set[str] = set()
            # Sorted by id so the suffix a collision gets does not depend on
            # the order the server happened to return the definitions in.
            known = {
                field_id: claim_unique_name(
                    camel_to_snake(field_def.get("name") or ""), claimed
                )
                for field_id, field_def in sorted(
                    self.custom_field_definitions(level).items()
                )
            }
            self._custom_field_snake_maps[level] = known
        return known

    def _register_category_name(self, category: Category) -> str:
        """Claim a unique attribute name for a category and remember it."""
        resolved = claim_unique_name(
            camel_to_snake(category.name or ""), self._category_snake_claimed
        )
        self._category_snake_names[category.id] = resolved
        return resolved

    def _active_in_display_order(
        self, table: str, predicate: Callable[[Any], Any] | None = None
    ) -> list[Any]:
        """Active rows of a reference table, sorted by the platform's sort order.

        Reads the session-cached ReferenceTable, which already drops inactive
        rows and wraps each in its model, rather than rebuilding both per deal.
        Returns a new list every call, so a caller cannot disturb that cache.
        """
        return sorted(
            (
                row
                for row in self.reference_table(table)
                if predicate is None or predicate(row)
            ),
            key=lambda row: row.sort_order or 0,
        )

    def get_external_ref_type(self, identifier: int | str) -> ExternalRefType | None:
        """Get external ref type by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._external_ref_types, ExternalRefType)

    def get_custom_table_columns(self, table_id: int) -> list[dict[str, Any]]:
        """Get column definitions for a custom table.

        Grouped on first use: every Program and Layer asks for each of its
        tables, so scanning the whole column list per table meant re-reading
        every definition in the tenant once per table per object.
        """
        if self._custom_table_columns_by_table is None:
            grouped: dict[Any, list[dict[str, Any]]] = {}
            for col in self._custom_table_columns:
                grouped.setdefault(col.get("customTableDefinitionId"), []).append(col)
            self._custom_table_columns_by_table = grouped
        # A copy: the grouped lists are the cache's own, and a caller that
        # sorted or appended to one would reorder every later read.
        return list(self._custom_table_columns_by_table.get(table_id, ()))

    def get_peril(self, identifier: int | str) -> Peril | None:
        """
        Get peril by ID, code or name.

        Args:
            identifier: Either an integer ID or a string code/name

        Returns:
            Peril instance or None if not found

        Example:
            >>> peril = cache.get_peril(1)             # By ID
            >>> peril = cache.get_peril("EQ")          # By code
            >>> peril = cache.get_peril("Earthquake")  # By name
        """
        return self._lookup(identifier, self._perils, Peril)

    def get_region(self, identifier: int | str) -> Region | None:
        """
        Get region by ID, code or name.

        Args:
            identifier: Either an integer ID or a string code/name

        Returns:
            Region instance or None if not found

        Example:
            >>> region = cache.get_region(1)      # By ID
            >>> region = cache.get_region("NA")   # By code
        """
        # Region is the only reference model that also takes the cache.
        return self._lookup(identifier, self._regions, lambda row: Region(row, self))

    def get_all_regions(self) -> list[Region]:
        """
        Get all regions.

        Kept for Region.children / Region.all_descendants, which walk the full
        set to find their descendants. The public collection is client.regions.

        Returns:
            List of all Region instances
        """
        return [Region(data, self) for data in self._regions.values()]

    def get_vendor(self, identifier: int | str) -> Vendor | None:
        """Get vendor by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._vendors, Vendor)

    def get_variant(self, identifier: int | str) -> Variant | None:
        """Get variant by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._variants, Variant)

    def get_perspective(self, identifier: int | str) -> Perspective | None:
        """Get perspective by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._perspectives, Perspective)

    def get_event_set(self, identifier: int | str) -> EventSet | None:
        """Get event set by ID or name (case-insensitive)."""
        return self._lookup(identifier, self._event_sets, EventSet)

    def get_role(self, identifier: str | None) -> Role | None:
        """Get an Identity role by its GUID or name (case-insensitive)."""
        return self._lookup(identifier, self._roles, Role)

    def get_user(self, identifier: str | None) -> User | None:
        """Get an Identity user by GUID, email, or "First Last" name.

        The lookup is case-insensitive. Returns None when the identifier is
        None, so a caller can pass an unassigned ``userId`` straight through.
        """
        return self._lookup(identifier, self._users, User)

    def active_program_roles(self) -> list[Role]:
        """Every role a deal can assign, in the platform's display order.

        A deal's Roles panel lists these whether or not anyone holds them, so
        Program.roles walks this list rather than the deal's own rows.
        """
        return self._active_in_display_order(
            "roles", lambda role: role.program_role_flag
        )

    @staticmethod
    def _lookup_ref(
        identifier: int | str | None,
        by_id: dict[Any, dict[str, Any]],
        by_name: dict[str, dict[str, Any]],
        model_cls: Callable[[dict[str, Any]], Any],
    ) -> Any | None:
        # bool is a subclass of int, so True would otherwise resolve row 1.
        if isinstance(identifier, bool):
            return None
        if isinstance(identifier, int):
            data = by_id.get(identifier)
        elif isinstance(identifier, str):
            # A string can be an ID as well as a name: Identity keys roles and
            # users by GUID. _index_values folds string keys, so the ID index is
            # tried first, and a GUID cannot collide with a name.
            folded = identifier.lower()
            data = by_id.get(folded) or by_name.get(folded)
        else:
            return None
        return model_cls(data) if data else None

    def build_custom_table_row(
        self, raw_row: dict[str, Any], table_id: int
    ) -> dict[str, Any]:
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
        from .models._custom_table_fields import (
            LOOKUP_VALUE_TYPE,
            field_name_for_column,
        )

        columns = self.get_custom_table_columns(table_id)
        result = {}

        for col in columns:
            field_name = field_name_for_column(col)
            if field_name is None:
                continue

            value = raw_row.get(field_name)

            # Resolve lookup fields to display labels
            if col["valueType"] == LOOKUP_VALUE_TYPE and value is not None:
                lookup_object_id = col.get("lookupObjectId")
                if lookup_object_id is not None:
                    value = self.resolve_lookup_value(lookup_object_id, value)

            # Parse datetime strings automatically (same as custom fields)
            result[col["name"]] = parse_datetime_string(value)

        return result
