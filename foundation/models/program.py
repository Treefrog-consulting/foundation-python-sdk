"""Program model"""

import copy
from typing import TYPE_CHECKING, Any, Optional

from ..exceptions import ReadOnlyVersionError
from ..ref_data import ReferenceDataCache
from ._init_helpers import CustomDataMixin, _DocField
from .collections import NamedCollection
from .layer import Layer
from .program_broker import ProgramBroker
from .program_categories import ProgramCategories
from .program_external_refs import ProgramExternalRefs
from .program_role import ProgramRole
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

        # Process roles. Every active program role gets a row whether or not
        # this deal fills it, matching the UI's Roles table.
        self._roles = self._build_roles(program_data.get("programRoles") or [])

        # Process external references
        self._external_refs = ProgramExternalRefs(
            program_data.get("programExternalRefs", []), ref_cache
        )

        # Build the info lists for every program-level custom field and table
        self._init_custom_metadata(ref_cache, raw_table_rows)

    def _build_roles(self, raw_roles: list[dict[str, Any]]) -> list[ProgramRole]:
        """Pair every active program role with the deal's row for it, if any.

        The platform stores at most one row per (deal, role) and leaves userId
        null on an unfilled one, so the first row for a role fills its slot. No
        row is dropped, though: a second row for the same role, or one naming a
        role the reference data does not know, follows the slots rather than
        disappearing -- losing either would hide an assignment entirely, and
        nothing in the schema enforces the one-row rule.
        """
        rows_by_role_id: dict[str, list[dict[str, Any]]] = {}
        extras: list[dict[str, Any]] = []
        for row in raw_roles:
            role_id = row.get("roleId")
            if role_id is None:
                # No role to slot it against, but it is still an assignment.
                extras.append(row)
            else:
                rows_by_role_id.setdefault(str(role_id).lower(), []).append(row)

        roles = []
        for role in self._ref_cache.active_program_roles():
            rows = rows_by_role_id.pop((role.id or "").lower(), [])
            roles.append(ProgramRole(rows[0] if rows else {}, self._ref_cache, role))
            extras.extend(rows[1:])

        for rows in rows_by_role_id.values():
            extras.extend(rows)
        roles.extend(ProgramRole(row, self._ref_cache) for row in extras)
        return roles

    def _edited(self, name: str) -> bool:
        """Whether set_document_field has replaced this collection's rows."""
        return name in self._modifications

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
            >>> print(previous.layers[0].get_document_field("collateral_req_share"))
        """
        if self._foundation_client is None:
            raise RuntimeError(
                "Program must be associated with a client to read a version. "
                "Use client.get_program() to retrieve the program."
            )
        return self._foundation_client.get_program(self.id, version_id=version_id)

    def get_previous(self) -> Optional["Program"]:
        """
        The deal this one was renewed from, or None if it is not a renewal.

        The deal document carries only the previous deal's ID, so this issues a
        fresh read for it -- and therefore an access check, which can raise
        PermissionDeniedError or NotFoundError for a deal this user cannot see.

        Returns:
            The previous Program, or None when renewed_from_id is unset.

        Example:
            >>> previous = program.get_previous()
            >>> if previous:
            ...     print(previous.name, previous.uw_year)
        """
        previous_id = self.renewed_from_id
        if previous_id is None:
            return None
        if self._foundation_client is None:
            raise RuntimeError(
                "Program must be associated with a client to read the deal it "
                "was renewed from. Use client.get_program() to retrieve the "
                "program."
            )
        return self._foundation_client.get_program(previous_id)

    # Standard program properties with full type hints
    @property
    def id(self) -> int | None:
        """Program ID (read-only). None if the payload carried none."""
        return self._data.get("id")

    name = _DocField[str | None](
        "name",
        "name",
        'Program name. None when unset -- "" used to stand in for absent.',
    )
    description = _DocField[str | None](
        "description", "description", "Program description."
    )
    uw_year = _DocField[int | None]("uw_year", "uwYear", "Underwriting year.")
    ceded_flag = _DocField[bool | None](
        "ceded_flag", "cededFlag", "Whether the program is ceded."
    )
    renewal_flag = _DocField[bool | None](
        "renewal_flag", "renewalFlag", "Whether the program is a renewal."
    )
    note = _DocField[str | None]("note", "note", "Program note.")
    years_on_account = _DocField[int | None](
        "years_on_account", "yearsOnAccount", "Years on account."
    )
    renewed_from_id = _DocField[int | None](
        "renewed_from_id",
        "renewedFromId",
        """ID of the deal this one was renewed from ("Renewed From" in the UI).

    None when the deal is not a renewal of another. Use get_previous() to
    read that deal.
    """,
    )
    client_id = _DocField[int | None](
        "client_id",
        "clientId",
        "ID of the deal's client. See the client property for the full record.",
    )
    program_group_id = _DocField[int | None](
        "program_group_id",
        "programGroupId",
        """ID of the deal group this deal belongs to ("Program Group" in the UI).

    See client.program_groups for the group names.
    """,
    )
    pricing_portfolio_id = _DocField[int | None](
        "pricing_portfolio_id",
        "pricingPortfolioId",
        "ID of the reference portfolio this deal is priced against.",
    )
    pricing_portfolio_src = _DocField[int | None](
        "pricing_portfolio_src",
        "pricingPortfolioSrc",
        "How the reference portfolio is linked: 1 static, 2 sourced.",
    )

    # Reference data properties (read-only, enriched)
    @property
    def client(self) -> Optional["Client"]:
        """Client information (read-only, enriched from reference data)."""
        client_id = self.client_id
        if client_id:
            return self._ref_cache.get_client(client_id)
        return None

    @property
    def brokers(self) -> "NamedCollection[ProgramBroker]":
        """List of program brokers (read-only)."""
        if self._edited("program_brokers"):
            return NamedCollection(
                ProgramBroker(broker, self._ref_cache)
                for broker in self._document_rows("program_brokers", "programBrokers")
            )
        return NamedCollection(self._brokers)

    @property
    def layers(self) -> "NamedCollection[Layer]":
        """List of program layers (read-only). Supports indexing by number or name."""
        collection: NamedCollection[Layer] = NamedCollection(self._layers)
        return collection

    @property
    def roles(self) -> "NamedCollection[ProgramRole]":
        """
        Who holds each role on this deal (read-only).

        Carries a row for every active program role, so a role nobody holds is
        present with user None -- the same list the UI's Roles table shows.

        Example:
            >>> for role in program.roles:
            ...     print(role)
            Underwriter: Ada Lovelace
            Actuary: (unassigned)

            >>> program.roles["Underwriter"].user.email
            'ada@example.com'

            >>> program.roles.as_pandas()
        """
        if self._edited("program_roles"):
            return NamedCollection(
                self._build_roles(self._document_rows("program_roles", "programRoles"))
            )
        return NamedCollection(self._roles)

    @property
    def categories(self) -> ProgramCategories:
        """
        Program categories (read-only).

        Provides dynamic attribute access to categories:

        Example:
            >>> program.categories.modelling_status
            'Valuation'
        """
        if self._edited("program_categories"):
            return ProgramCategories(
                self._document_rows("program_categories", "programCategories"),
                self._ref_cache,
            )
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
        if self._edited("program_external_refs"):
            return ProgramExternalRefs(
                self._document_rows("program_external_refs", "programExternalRefs"),
                self._ref_cache,
            )
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
                self._apply_scalar_modification(result, key, value)

        self._apply_dirty_tables(result)

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
        print(f"  roles                          {len(self.roles)} item(s)")

        print("\nAVAILABLE METHODS:")
        for method in (
            "get_custom_field(field_name)",
            "set_custom_field(field_name, value)",
            "get_custom_table(table_name)",
            "set_custom_table(table_name, rows)",
            "get_document_field(name)",
            "set_document_field(name, value)",
            "get_previous()",
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
            ("renewed_from_id", "read-write", self.renewed_from_id),
            ("client_id", "read-write", self.client_id),
            ("program_group_id", "read-write", self.program_group_id),
            ("pricing_portfolio_id", "read-write", self.pricing_portfolio_id),
            ("pricing_portfolio_src", "read-write", self.pricing_portfolio_src),
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
