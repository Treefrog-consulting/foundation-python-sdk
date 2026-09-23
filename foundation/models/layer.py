"""Layer model"""

import copy
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from ..exceptions import ReadOnlyVersionError
from ..ref_data import ReferenceDataCache
from ..utils import camel_to_snake, parse_datetime_string
from ._init_helpers import CustomDataMixin, _DocField, raise_if_private
from .collections import NamedCollection
from .layer_participant import LayerParticipant
from .layered_losses import LayeredLosses
from .metric_results import MetricResults
from .reinstatement import Reinstatement
from .ylt import Ylt

if TYPE_CHECKING:
    from .currency import Currency
    from .layer_status import LayerStatus
    from .program import Program
    from .program_versions import ProgramVersions
    from .reinst_prem_base import ReinstPremBase
    from .unit_of_measure import UnitOfMeasure


#: Attribute suffix -> the layerLimitAttach key it reads and writes. Longest
#: suffix first, so the lookup cannot stop at a shorter one it also ends with.
_LIMIT_ATTACH_FIELDS = {
    "_attachment_franchise": "attachmentFranchiseFlag",
    "_limit_unlimited": "limitUnlimitedFlag",
    "_limit_binary": "limitBinaryFlag",
    "_attachment": "attachment",
    "_limit": "limit",
}

#: A layerLimitAttach row the payload did not carry, before the edited field
#: and the type are filled in. "id" is assigned by the API.
_BLANK_LIMIT_ATTACH: dict[str, Any] = {
    "id": None,
    "layerId": None,
    "layer": None,
    "layerLimitAttachTypeId": None,
    "layerLimitAttachType": None,
    "limit": None,
    "attachment": None,
    "limitUnlimitedFlag": False,
    "limitBinaryFlag": False,
    "attachmentFranchiseFlag": False,
}


class Layer(CustomDataMixin):
    """Represents a program layer with enriched data."""

    _custom_field_level = 2
    _custom_table_data_level_ids = (3,)
    _custom_fields_key = "layerCustomFields"
    _owner_label = "layer"
    _materialize_empty_tables = False
    _parse_custom_field_datetimes = True

    def __init__(
        self,
        layer_data: dict[str, Any],
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
        self._metric_results = MetricResults(self)

        # Custom field values and the raw custom-table rows
        raw_table_rows = self._init_custom_data(layer_data, ref_cache)

        # Process limits and attachments
        self._limits_attachments = {}
        for la in layer_data.get("layerLimitAttach", []):
            la_type = ref_cache.get_layer_limit_attach_type(
                la.get("layerLimitAttachTypeId")
            )
            if la_type:
                type_name = camel_to_snake(la_type.name)
                for suffix, document_key in _LIMIT_ATTACH_FIELDS.items():
                    self._limits_attachments[f"{type_name}{suffix}"] = la.get(
                        document_key
                    )

        # Process premiums
        self._premiums = {}
        for prem in layer_data.get("layerPremiums", []):
            prem_type = ref_cache.get_layer_premium_type(prem.get("premiumTypeId"))
            if prem_type:
                type_name = camel_to_snake(prem_type.name)
                self._premiums[f"{type_name}_premium"] = prem.get("amount")

        # Build the info lists for every layer-level custom field and table
        self._init_custom_metadata(ref_cache, raw_table_rows)

    def _read_custom_table_entry(
        self,
        entry: dict[str, Any],
        ref_cache: ReferenceDataCache,
        raw_table_rows: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Bucket one layer ``customTables`` entry under its table name.

        A layer entry may carry a ``rows`` list rather than standing for a
        single row, and its definition id arrives under either key.
        """
        table_id = entry.get("customTableDefinitionId") or entry.get("customTableId")
        table_def = ref_cache.get_custom_table(table_id)
        if not table_def:
            return
        table_name = camel_to_snake(table_def.name or "")
        rows_payload = entry.get("rows")
        if isinstance(rows_payload, list):
            normalized_rows = [
                self._normalize_table_row(table_id, row) for row in rows_payload
            ]
            # Store original rows to preserve metadata
            self._custom_table_original_rows.setdefault(table_name, []).extend(
                rows_payload
            )
        else:
            normalized_rows = [ref_cache.build_custom_table_row(entry, table_id)]
            # Store original row to preserve metadata
            self._custom_table_original_rows.setdefault(table_name, []).append(entry)
        raw_table_rows.setdefault(table_name, []).extend(normalized_rows)
        self._custom_table_original_names[table_name] = table_def.name

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

    @property
    def metric_results(self) -> MetricResults:
        """Access metric results for this layer.

        Returns:
            MetricResults accessor that can export data as Polars DataFrame or
            return a single metric by name, id or source name.

        Example:
            >>> config = program.run_configurations.get_by_label("Default")
            >>> df = layer.metric_results.as_polars(config)
            >>> layer.metric_results.get_by_name(config, "Combined Ratio")
        """
        return self._metric_results

    def _require_writable(self, action: str) -> None:
        """Reject a modification when this layer belongs to a past version."""
        version_id = self.version_id
        if version_id is not None:
            raise ReadOnlyVersionError(
                f"Cannot {action}: this layer was read at version "
                f"{version_id} and is read-only. Retrieve the current layer "
                "with client.get_layer() to make changes."
            )

    @property
    def version_id(self) -> int | None:
        """Version this layer was read at, or None for the current layer."""
        return self._program.version_id if self._program is not None else None

    @property
    def is_versioned(self) -> bool:
        """Whether this layer is a read-only snapshot of a past version."""
        return self.version_id is not None

    @property
    def versions(self) -> "ProgramVersions":
        """
        Saved versions of this layer's deal, newest first (read-only).

        Layers are versioned with the deal they belong to; there is no
        separate layer version.

        Example:
            >>> layer.versions.describe()
        """
        if self._program is None:
            raise RuntimeError(
                "Layer must be associated with a program to list versions. "
                "Retrieve the layer via client.get_layer() or program.layers."
            )
        return self._program.versions

    def get_version(self, version_id: int) -> "Layer":
        """
        Retrieve this layer as it stood at a saved version of its deal.

        The returned layer is read-only. Its terms, participants, limits and
        custom data are those recorded at that version, and losses exported
        from it are computed against that structure. Reference data is not
        versioned by the platform, so labels are always the current ones.

        Args:
            version_id: Saved version to read. See the versions property.

        Returns:
            Read-only Layer at the requested version.

        Raises:
            ValueError: If the version does not belong to this layer's deal,
                or if the layer did not exist yet at that version.

        Example:
            >>> previous = layer.get_version(23011)
            >>> print(previous.get_document_field("collateral_req_share"))
            >>> df = previous.layered_losses.as_polars(config)
        """
        if self._program is None:
            raise RuntimeError(
                "Layer must be associated with a program to read a version. "
                "Retrieve the layer via client.get_layer() or program.layers."
            )

        versioned_program = self._program.get_version(version_id)
        for layer in versioned_program.layers:
            if layer.id == self.id:
                return layer

        raise ValueError(
            f"Layer {self.id} was not part of deal {self._program.id} "
            f"at version {version_id}."
        )

    # Standard layer properties with full type hints
    @property
    def id(self) -> int | None:
        """Layer ID (read-only). None if the payload carried none."""
        return self._data.get("id")

    name = _DocField[str | None](
        "name",
        "name",
        'Layer name. None when unset -- "" used to stand in for absent.',
    )
    number = _DocField[int | None]("number", "number", "Layer number.")

    # incept_date and expiry_date parse the stored timestamp, so they keep
    # their hand-written form.
    @property
    def incept_date(self) -> datetime | None:
        """Inception date."""
        if "incept_date" in self._modifications:
            return self._modifications["incept_date"]
        value = self._data.get("inceptDate")
        return parse_datetime_string(value)

    @incept_date.setter
    def incept_date(self, value: datetime | None) -> None:
        """Set inception date."""
        parsed_value = parse_datetime_string(value)
        self._modifications["incept_date"] = parsed_value

    @property
    def expiry_date(self) -> datetime | None:
        """Expiry date."""
        if "expiry_date" in self._modifications:
            return self._modifications["expiry_date"]
        value = self._data.get("expiryDate")
        return parse_datetime_string(value)

    @expiry_date.setter
    def expiry_date(self, value: datetime | None) -> None:
        """Set expiry date."""
        parsed_value = parse_datetime_string(value)
        self._modifications["expiry_date"] = parsed_value

    note = _DocField[str | None]("note", "note", "Layer note.")
    limit_percent = _DocField[float | None](
        "limit_percent", "limitPercent", "Limit percentage."
    )
    structure_only_flag = _DocField[bool | None](
        "structure_only_flag",
        "structureOnlyFlag",
        "Whether this is a structure-only layer.",
    )
    unlimited_reinst_flag = _DocField[bool | None](
        "unlimited_reinst_flag",
        "unlimitedReinstFlag",
        "Whether reinstatements are unlimited.",
    )
    rol_type = _DocField[int | None](
        "rol_type",
        "rolType",
        "Basis the rate on line is quoted on: 1 occurrence, 2 aggregate.",
    )
    allocate_contrib_events_flag = _DocField[bool | None](
        "allocate_contrib_events_flag",
        "allocateContribEventsFlag",
        """Whether loss is allocated across every contributing event.

    When set, each event above the occurrence retention takes a
    proportional share regardless of aggregate exhaustion -- so two layers
    with the same limits and attachments can produce different layered
    losses, and this is the flag that explains it.
    """,
    )

    # The reference keys the layer row carries. Each has an enriched read-only
    # counterpart below; these are the settable form, and what a scenario edits.
    currency_id = _DocField[int | None](
        "currency_id",
        "currencyId",
        "ID of the layer's currency. See the currency property for the record.",
    )
    unit_of_measure_id = _DocField[int | None](
        "unit_of_measure_id", "unitOfMeasureId", "ID of the layer's unit of measure."
    )
    layer_status_id = _DocField[int | None](
        "layer_status_id", "layerStatusId", "ID of the layer's status."
    )
    reinst_prem_base_id = _DocField[int | None](
        "reinst_prem_base_id",
        "reinstPremBaseId",
        "ID of the layer's reinstatement premium base.",
    )

    # Reference data properties (read-only, enriched). Each reads its key
    # through the scalar property above, so an edited id resolves to the row
    # the export will actually use rather than the one the deal was read with.
    @property
    def status(self) -> Optional["LayerStatus"]:
        """Layer status (read-only, enriched from reference data)."""
        status_id = self.layer_status_id
        if status_id:
            return self._ref_cache.get_layer_status(status_id)
        return None

    @property
    def currency(self) -> Optional["Currency"]:
        """Currency (read-only, enriched from reference data)."""
        currency_id = self.currency_id
        if currency_id:
            return self._ref_cache.get_currency(currency_id)
        return None

    @property
    def unit_of_measure(self) -> Optional["UnitOfMeasure"]:
        """Unit of measure (read-only, enriched from reference data)."""
        unit_id = self.unit_of_measure_id
        if unit_id:
            return self._ref_cache.get_unit_of_measure(unit_id)
        return None

    @property
    def reinst_prem_base(self) -> Optional["ReinstPremBase"]:
        """Reinstatement premium base (read-only, enriched from reference data)."""
        reinst_prem_base_id = self.reinst_prem_base_id
        if reinst_prem_base_id:
            return self._ref_cache.get_reinst_prem_base(reinst_prem_base_id)
        return None

    @property
    def participants(self) -> "NamedCollection[LayerParticipant]":
        """List of layer participants (read-only).

        Reads through set_document_field("layer_participants") when one has been
        made, so a scenario reports the participants its export will use rather
        than the ones the layer was read with.
        """
        return NamedCollection(
            LayerParticipant(p, self._ref_cache)
            for p in self._document_rows("layer_participants", "layerParticipants")
        )

    @property
    def reinstatements(self) -> "NamedCollection[Reinstatement]":
        """List of layer reinstatements (read-only).

        Reads through set_document_field("layer_reinst") when one has been made.
        """
        return NamedCollection(
            Reinstatement(r) for r in self._document_rows("layer_reinst", "layerReinst")
        )

    @property
    def program(self) -> Optional["Program"]:
        """Parent program (read-only)."""
        return self._program

    # Limit and attachment methods
    def get_limit(self, limit_type: str) -> float | None:
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

    def get_attachment(self, attachment_type: str) -> float | None:
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
    def get_premium(self, premium_type: str) -> float | None:
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

    def get_json(self) -> dict[str, Any]:
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
                self._apply_scalar_modification(result, key, value)

        self._apply_dirty_tables(result)

        return result

    def _normalize_table_row(
        self, table_id: int | None, row: dict[str, Any]
    ) -> dict[str, Any]:
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

        from ._custom_table_fields import FIELD_PREFIXES_TUPLE

        if any(key.startswith(FIELD_PREFIXES_TUPLE) for key in row.keys()):
            return self._ref_cache.build_custom_table_row(row, table_id)
        return copy.deepcopy(row)

    def _update_limit_attachment_in_json(
        self, result: dict[str, Any], field_name: str, value: Any
    ) -> None:
        """Update a limit or attachment value in the layerLimitAttach array."""
        # Format: {type_name}_limit, {type_name}_attachment, etc.
        document_key = None
        for suffix, key in _LIMIT_ATTACH_FIELDS.items():
            if field_name.endswith(suffix):
                type_name = field_name[: -len(suffix)]
                document_key = key
                break
        if document_key is None:
            return

        # Find the layer limit attach type ID
        type_id = None
        for tid in self._ref_cache._layer_limit_attach_types:
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
                la[document_key] = value
                found = True
                break

        if not found:
            # Add new limit/attachment entry
            new_entry = dict(_BLANK_LIMIT_ATTACH)
            new_entry["layerId"] = result.get("id")
            new_entry["layerLimitAttachTypeId"] = type_id
            new_entry[document_key] = value

            limit_attach_list.append(new_entry)
            result["layerLimitAttach"] = limit_attach_list

    def _update_premium_in_json(
        self, result: dict[str, Any], field_name: str, value: Any
    ) -> None:
        """Update a premium value in the layerPremiums array."""
        # Parse the field name to determine type
        # Format: {type_name}_premium
        if not field_name.endswith("_premium"):
            return

        type_name = field_name[: -len("_premium")]

        # Find the layer premium type ID
        type_id = None
        for tid in self._ref_cache._layer_premium_types:
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

    def __getattr__(self, name: str) -> Any:
        """
        Dynamic attribute access for limits, attachments, and premiums.

        Custom fields and custom tables are answered by CustomDataMixin; the
        layer's own dictionaries are consulted first.

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
        raise_if_private(self, name)

        # Check limits and attachments
        if name in self._limits_attachments:
            return self._limits_attachments[name]

        # Check premiums
        if name in self._premiums:
            return self._premiums[name]

        return super().__getattr__(name)

    def _set_extra_attribute(self, name: str, value: Any) -> bool:
        """Route a write to the layer's limits, attachments or premiums.

        Called by CustomDataMixin.__setattr__ after the property and
        custom-table checks and before the custom-field one.
        """
        # Check if it's a limit/attachment (only if initialization is complete)
        if name in self._limits_attachments:
            self._limits_attachments[name] = value
            self._modifications[name] = value
            return True

        # Check if it's a premium (only if initialization is complete)
        if name in self._premiums:
            self._premiums[name] = value
            self._modifications[name] = value
            return True

        return False

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
            ("rol_type", "read-write", self.rol_type),
            (
                "allocate_contrib_events_flag",
                "read-write",
                self.allocate_contrib_events_flag,
            ),
            ("currency_id", "read-write", self.currency_id),
            ("unit_of_measure_id", "read-write", self.unit_of_measure_id),
            ("layer_status_id", "read-write", self.layer_status_id),
            ("reinst_prem_base_id", "read-write", self.reinst_prem_base_id),
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

        self._describe_custom_fields()
        self._describe_custom_tables()

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
            "get_document_field(name)",
            "set_document_field(name, value)",
            "get_json()",
            "describe()",
        ]
        for method in methods:
            print(f"  • {method}")

        print("\n" + "=" * 80)
        print(
            "TIP: Use dot notation to access any attribute (e.g., layer.gross_premium)"
        )
        print("=" * 80 + "\n")

    def __dir__(self) -> list[str]:
        """Autocomplete over the real members plus the dynamic ones.

        super() already lists every property and method; only the custom
        fields and tables, which exist per deal, have to be added.
        """
        return sorted(set(super().__dir__()) | self._custom_data_names())
