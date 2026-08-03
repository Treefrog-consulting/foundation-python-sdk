"""Program model"""

import copy
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Optional

from ..exceptions import ReadOnlyVersionError
from ..ref_data import ReferenceDataCache
from ..utils import snake_to_camel, to_wire_datetime
from ._init_helpers import CustomDataMixin
from .collections import NamedCollection
from .layer import Layer
from .program_broker import ProgramBroker
from .program_categories import ProgramCategories
from .program_external_refs import ProgramExternalRefs
from .program_versions import ProgramVersions
from .run_configurations import RunConfigurations

if TYPE_CHECKING:
    from .client import Client
    from .loss_group import LossGroup


class Program(CustomDataMixin):
    """Represents a program with enriched data."""

    _custom_field_level = 1
    _custom_table_data_level_ids = (2, 4)
    _custom_fields_key = "programCustomFields"
    _owner_label = "program"
    _materialize_empty_tables = True
    _parse_custom_field_datetimes = False

    def __init__(
        self,
        program_data: dict[str, Any],
        ref_cache: ReferenceDataCache,
        analyses_data: list[dict[str, Any]] | None = None,
        foundation_client: Any | None = None,
        version_id: int | None = None,
    ):
        self._version_id = version_id
        self._versions: ProgramVersions | None = None
        self._original_data = copy.deepcopy(program_data)
        self._data = program_data
        self._ref_cache = ref_cache
        self._modifications = {}
        self._foundation_client = foundation_client

        # One DuckDB export per (deal document, run configuration) holds the losses,
        # the YLT stream and the metric results of every layer, so the accessors share
        # these two dictionaries rather than each fetching the file for itself.
        self._export_cache: dict[str, Any] = {}
        self._export_paths: dict[int, Any] = {}

        # Store run configurations if provided
        self._run_configurations = RunConfigurations(analyses_data or [])

        # Custom field values and the raw custom-table rows
        raw_table_rows = self._init_custom_data(program_data, ref_cache)

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

        # Build the info lists for every program-level custom field and table
        self._init_custom_metadata(ref_cache, raw_table_rows)

    def _require_writable(self, action: str) -> None:
        """Reject a modification when this program is a past version.

        A snapshot cannot be saved back, and an edited one would change the
        numbers returned by layered_losses and ylt, which are computed from
        the program document the SDK uploads.
        """
        if self._version_id is not None:
            raise ReadOnlyVersionError(
                f"Cannot {action}: this deal was read at version "
                f"{self._version_id} and is read-only. Retrieve the current "
                "deal with client.get_program() to make changes."
            )

    @property
    def version_id(self) -> int | None:
        """Version this deal was read at, or None for the current deal."""
        return self._version_id

    @property
    def is_versioned(self) -> bool:
        """Whether this deal is a read-only snapshot of a past version."""
        return self._version_id is not None

    @property
    def versions(self) -> ProgramVersions:
        """
        Saved versions of this deal, newest first (read-only).

        Fetched on first access and kept for the lifetime of this object.

        Example:
            >>> program.versions.describe()
            >>> previous = program.get_version(program.versions[1].id)
        """
        if self._versions is None:
            if self._foundation_client is None:
                raise RuntimeError(
                    "Program must be associated with a client to list versions. "
                    "Use client.get_program() to retrieve the program."
                )
            self._versions = self._foundation_client.get_program_versions(self.id)
        return self._versions

    def get_version(self, version_id: int) -> "Program":
        """
        Retrieve this deal as it stood at a saved version.

        The returned deal is read-only and carries the layers, terms and
        custom data recorded at that version. Reference data (currencies,
        perils, statuses, participants) is not versioned by the platform, so
        those labels are always the current ones.

        Args:
            version_id: Saved version to read. See the versions property.

        Returns:
            Read-only Program at the requested version.

        Raises:
            ValueError: If the version does not belong to this deal.

        Example:
            >>> previous = program.get_version(23011)
            >>> print(previous.layers[0].collateral_req_share)
        """
        if self._foundation_client is None:
            raise RuntimeError(
                "Program must be associated with a client to read a version. "
                "Use client.get_program() to retrieve the program."
            )
        return self._foundation_client.get_program(self.id, version_id=version_id)

    # Standard program properties with full type hints
    @property
    def id(self) -> int | None:
        """Program ID (read-only). None if the payload carried none."""
        return self._data.get("id")

    @property
    def name(self) -> str | None:
        """Program name. None when unset -- "" used to stand in for absent."""
        if "name" in self._modifications:
            return self._modifications["name"]
        return self._data.get("name")

    @name.setter
    def name(self, value: str) -> None:
        """Set program name."""
        self._modifications["name"] = value

    @property
    def description(self) -> str | None:
        """Program description."""
        if "description" in self._modifications:
            return self._modifications["description"]
        return self._data.get("description")

    @description.setter
    def description(self, value: str | None) -> None:
        """Set program description."""
        self._modifications["description"] = value

    @property
    def uw_year(self) -> int | None:
        """Underwriting year."""
        if "uw_year" in self._modifications:
            return self._modifications["uw_year"]
        return self._data.get("uwYear")

    @uw_year.setter
    def uw_year(self, value: int | None) -> None:
        """Set underwriting year."""
        self._modifications["uw_year"] = value

    @property
    def ceded_flag(self) -> bool | None:
        """Whether the program is ceded."""
        if "ceded_flag" in self._modifications:
            return self._modifications["ceded_flag"]
        return self._data.get("cededFlag")

    @ceded_flag.setter
    def ceded_flag(self, value: bool | None) -> None:
        """Set ceded flag."""
        self._modifications["ceded_flag"] = value

    @property
    def renewal_flag(self) -> bool | None:
        """Whether the program is a renewal."""
        if "renewal_flag" in self._modifications:
            return self._modifications["renewal_flag"]
        return self._data.get("renewalFlag")

    @renewal_flag.setter
    def renewal_flag(self, value: bool | None) -> None:
        """Set renewal flag."""
        self._modifications["renewal_flag"] = value

    @property
    def note(self) -> str | None:
        """Program note."""
        if "note" in self._modifications:
            return self._modifications["note"]
        return self._data.get("note")

    @note.setter
    def note(self, value: str | None) -> None:
        """Set program note."""
        self._modifications["note"] = value

    @property
    def years_on_account(self) -> int | None:
        """Years on account."""
        if "years_on_account" in self._modifications:
            return self._modifications["years_on_account"]
        return self._data.get("yearsOnAccount")

    @years_on_account.setter
    def years_on_account(self, value: int | None) -> None:
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
    def brokers(self) -> "NamedCollection[ProgramBroker]":
        """List of program brokers (read-only)."""
        return NamedCollection(self._brokers)

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

    def create_loss_group(self, name: str) -> "LossGroup":
        """Create a new LossGroup builder attached to this program.

        Add loss sets via add_yelt / add_elt, then call upload().

        Args:
            name: Display name for the loss group.

        Returns:
            LossGroup builder instance.
        """
        # A loss group would attach to the live deal — the program ID is the
        # same in every version — so uploading from a snapshot would change
        # the current deal while appearing to act on the past one.
        self._require_writable("create a loss group")

        from .loss_group import LossGroup

        return LossGroup(client=self._foundation_client, program_id=self.id, name=name)

    def get_json(self) -> dict[str, Any]:
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
                    result[camel_key] = to_wire_datetime(value)
                elif isinstance(value, date):
                    result[camel_key] = f"{value}T00:00:00"
                else:
                    result[camel_key] = value

        # The edited tables are re-applied on every call rather than consumed by the
        # first one: a single export calls get_json() twice -- once to hash the deal
        # document, once to build the request body -- and consuming them made the two
        # calls disagree, which both sent the un-edited table and cost a second export.
        # Reading a table through as_pandas() marks it edited, so an in-place edit to
        # the frame it returns is covered without re-applying every table. Layer.get_json
        # resolves this the same way.
        for table_name in self._custom_table_dirty:
            table = self._custom_tables.get(table_name)
            if table is None:
                continue
            rows = table.to_records()
            self._update_custom_table_in_json(result, table_name, rows)

        # Always apply layer JSON to capture any modifications
        # This ensures that any changes to layers (including limits, premiums, custom fields, etc.)
        # are reflected in the program JSON
        for i, layer in enumerate(self._layers):
            result["layers"][i] = layer.get_json()

        return result

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

        self._describe_standard_properties()
        self._describe_reference_data()
        self._describe_attr_section("CATEGORIES", "Category", self.categories)
        self._describe_attr_section("EXTERNAL REFERENCES", "System", self.external_refs)
        self._describe_custom_fields()
        self._describe_custom_tables()

        print("\nCOLLECTIONS:")
        print(f"  brokers                        {len(self.brokers)} item(s)")
        print(f"  layers                         {len(self.layers)} item(s)")

        print("\nAVAILABLE METHODS:")
        for method in (
            "get_custom_field(field_name)",
            "set_custom_field(field_name, value)",
            "get_custom_table(table_name)",
            "set_custom_table(table_name, rows)",
            "get_json()",
            "describe()",
        ):
            print(f"  • {method}")

        print("\n" + "=" * 80)
        print(
            "TIP: Use dot notation to access any attribute (e.g., program.portfolio_tag)"
        )
        print("=" * 80 + "\n")

    def _describe_standard_properties(self) -> None:
        print("\nSTANDARD PROPERTIES:")
        print(f"  {'Property':<30} {'Access':<10} {'Current Value':<30}")
        print(f"  {'-' * 30} {'-' * 10} {'-' * 30}")
        for prop_name, access, value in (
            ("id", "read-only", self.id),
            ("name", "read-write", self.name),
            ("description", "read-write", self.description),
            ("uw_year", "read-write", self.uw_year),
            ("ceded_flag", "read-write", self.ceded_flag),
            ("renewal_flag", "read-write", self.renewal_flag),
            ("note", "read-write", self.note),
            ("years_on_account", "read-write", self.years_on_account),
        ):
            value_str = str(value)[:28] if value is not None else "None"
            print(f"  {prop_name:<30} {access:<10} {value_str:<30}")

    def _describe_reference_data(self) -> None:
        print("\nREFERENCE DATA (read-only):")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-' * 30} {'-' * 40}")
        client_name = self.client.name if self.client else None
        value = str(client_name)[:38] if client_name else "None"
        print(f"  {'client':<30} {value:<40}")

    def _describe_attr_section(self, title: str, label: str, obj: Any) -> None:
        """Print a section that lists every public attribute of ``obj`` as key/value."""
        print(f"\n{title}:")
        if not obj:
            print("  (none)")
            return
        print(f"  {label:<35} {'Value':<35}")
        print(f"  {'-' * 35} {'-' * 35}")
        for attr in sorted(a for a in dir(obj) if not a.startswith("_")):
            try:
                value = getattr(obj, attr)
            except AttributeError:
                continue
            if callable(value):
                continue
            value_str = str(value)[:33] if value is not None else "None"
            print(f"  {attr:<35} {value_str:<35}")

    def __dir__(self) -> list[str]:
        """Autocomplete over the real members plus the dynamic ones.

        super() already lists every property and method; only the custom
        fields and tables, which exist per deal, have to be added.
        """
        return sorted(set(super().__dir__()) | self._custom_data_names())
