"""Foundation Platform API Client"""

import copy
import warnings
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Optional
from urllib.parse import quote

import requests

from ..exceptions import raise_for_status
from ..ref_data import ReferenceDataCache
from ..sys_config import SysConfigCache
from ..utils import camel_to_snake, ref_values
from ._init_helpers import raise_if_private, raise_read_only
from ._reference_tables import REFERENCE_TABLES
from .module_custom_table import ModuleCustomTable
from .portfolio import Portfolio
from .portfolio_reference import PortfolioReference
from .program import Program
from .program_versions import ProgramVersions
from .reference_runs import ReferenceRuns
from .rollup_run import RollupRun
from .rows import Rows
from .scenario import Scenario
from .template import Template
from .templates import Templates

if TYPE_CHECKING:
    from .broker import Broker
    from .broker_office import BrokerOffice
    from .category import Category
    from .category_detail import CategoryDetail
    from .client import Client
    from .currency import Currency
    from .custom_field import CustomField
    from .event_set import EventSet
    from .external_ref_type import ExternalRefType
    from .layer import Layer
    from .layer_limit_attach_type import LayerLimitAttachType
    from .layer_order_term_type import LayerOrderTermType
    from .layer_premium_type import LayerPremiumType
    from .layer_status import LayerStatus
    from .participant import Participant
    from .participant_status import ParticipantStatus
    from .peril import Peril
    from .perspective import Perspective
    from .reference_table import ReferenceTable
    from .region import Region
    from .reinst_prem_base import ReinstPremBase
    from .unit_of_measure import UnitOfMeasure
    from .variant import Variant
    from .vendor import Vendor


class FoundationClient:
    """Client for interacting with the Foundation Platform API."""

    PORTS: ClassVar[dict[str, int]] = {
        "identity": 5000,
        "deal": 5001,
        "loss": 5002,
        "common": 5003,
        "metrics": 5005,
        "portfolio": 5006,
    }

    #: Seconds a socket may stall before a request is abandoned. requests
    #: applies this to the connection and to each wait for more bytes, so it
    #: does not cap a transfer's total duration -- a large upload that keeps
    #: sending is never cut off, while a server that stops responding is.
    DEFAULT_TIMEOUT = 30.0

    def __init__(self, base_url: str, timeout: float | None = DEFAULT_TIMEOUT):
        """
        Initialize the Foundation Platform client.

        Args:
            base_url: Base URL for the API (without port), e.g., "https://your-instance.foundationplatform.com"
            timeout: Per-socket timeout in seconds for every request this client
                makes. None waits forever, which is what every version before
                0.12 did.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._token: str | None = None
        self._ref_cache: ReferenceDataCache | None = None
        self._layer_headers: dict[int, int] = {}  # layer_id -> program_id mapping
        self._session = requests.Session()
        self._module_custom_tables: dict[
            str, ModuleCustomTable
        ] = {}  # snake_name -> table
        self._reference_runs: ReferenceRuns | None = None
        self._sys_config: SysConfigCache | None = None
        # Keyed by (program_id, version_id): stored analyses are recorded per
        # deal version, so a versioned read must not serve the live entry.
        self._analyses_cache: dict[tuple[int, int | None], list[dict[str, Any]]] = {}
        # (program_id, version_id) -> save date string exactly as the API
        # reported it. Populated whenever a deal's version list is fetched
        # (get_program_versions), since a version's save date never changes —
        # so a loop over a deal's versions costs a single list fetch.
        self._version_save_dates: dict[tuple[int, int], str] = {}
        # (program_id, version_id) -> deal document. A past version cannot
        # change, and each miss costs the server a full history rebuild, so a
        # loop over versions must not re-request one it has already read.
        self._versioned_programs: dict[tuple[int, int], dict[str, Any]] = {}
        self._portfolio_ref: PortfolioReference | None = None

    #: The only attributes a caller may assign. Everything else public on this
    #: class is a reference table, a module custom table or a method, and a
    #: write to one used to shadow it silently for the rest of the session.
    _SETTABLE: ClassVar[frozenset[str]] = frozenset({"base_url", "timeout"})

    def __setattr__(self, name: str, value: Any) -> None:
        """Take an internal or configurable write; refuse the rest."""
        if name in self._SETTABLE:
            object.__setattr__(self, name, value)
            return
        raise_read_only(self, name, value, "reference data and module tables")

    def _api_url(self, module: str, path: str) -> str:
        """Build an absolute URL for the given Foundation module and path."""
        return f"{self.base_url}:{self.PORTS[module]}/{path.lstrip('/')}"

    def _fetch(
        self, module: str, path: str, *, method: str = "GET", **kwargs: Any
    ) -> Any:
        """Issue an authenticated JSON request and return the parsed response.

        Args:
            module: Module key in PORTS (e.g. 'deal', 'common', 'loss').
            path: API path relative to the module root (e.g. '/api/ref').
            method: HTTP method.
            **kwargs: Forwarded to requests.Session.request (e.g. json=payload).
                Pass timeout= to override this client's default for one call.

        Raises:
            FoundationHTTPError: on non-2xx responses. The subclass identifies
                the status (AuthenticationError, NotFoundError, ...), and the
                message carries the server's response body.
        """
        return self.request(module, path, method=method, **kwargs).json()

    def request(
        self, module: str, path: str, *, method: str = "GET", **kwargs: Any
    ) -> requests.Response:
        """Issue an authenticated request and return the raw response.

        The timeout default and the error mapping belong to the transport, not
        to each caller: every site that reached past this method for the raw
        Response had to remember both, and none of them did until 0.12.

        Args:
            module: Module key in PORTS (e.g. 'deal', 'common', 'loss').
            path: API path relative to the module root (e.g. '/api/ref').
            method: HTTP method.
            **kwargs: Forwarded to requests.Session.request, including stream=.
                Pass timeout= to override this client's default for one call.

        Raises:
            FoundationHTTPError: on non-2xx responses. The subclass identifies
                the status (AuthenticationError, NotFoundError, ...), and the
                message carries the server's response body.
        """
        kwargs.setdefault("timeout", self.timeout)
        response = self._session.request(method, self._api_url(module, path), **kwargs)
        raise_for_status(response)
        return response

    def authenticate(
        self, email: str, password: str, api_client_id: str, api_client_secret: str
    ) -> None:
        """
        Authenticate with the Foundation Platform API.

        Args:
            email: User email
            password: User password
            api_client_id: API client ID
            api_client_secret: API client secret

        Raises:
            AuthenticationError: If the credentials are rejected.
        """
        data = self._fetch(
            "identity",
            "/api/account/login",
            method="POST",
            json={
                "email": email,
                "password": password,
                "apiClientId": api_client_id,
                "apiClientSecret": api_client_secret,
            },
        )
        self._token = data["tokenResponse"]["accessToken"]
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})

        # Load reference data after authentication
        self._load_reference_data()

    def _load_reference_data(self) -> None:
        """Load and cache reference data from the API."""
        # All nine calls are mutually independent — fetch in parallel to save
        # ~8x RTT on authenticate(). Accessible-objects is a follow-on of
        # deal_ref (needs its lookupObjectIds) and stays sequential below.
        endpoints = {
            "deal_ref": ("deal", "/api/ref"),
            "clients": ("deal", "/api/clients"),
            "common_ref": ("common", "/api/ref"),
            "layer_headers": ("deal", "/api/layers/headers"),
            "reference_runs": ("deal", "/api/referenceRuns"),
            "loss_ref": ("loss", "/api/ref"),
            "sys_configs": ("loss", "/api/SysConfigs"),
            "roles": ("identity", "/api/Roles"),
            "users": ("identity", "/api/Users"),
        }

        with ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
            futures = {
                key: executor.submit(self._fetch, module, path)
                for key, (module, path) in endpoints.items()
            }

        def result_or_warn(key: str, warn_msg: str, default: Any) -> Any:
            try:
                return futures[key].result()
            except Exception as exc:  # noqa: BLE001 - this fetch is optional
                warnings.warn(f"{warn_msg}: {exc}", stacklevel=3)
                return default

        deal_ref_data = futures["deal_ref"].result()
        clients_data = futures["clients"].result()
        common_ref_data = futures["common_ref"].result()
        layer_headers_data = futures["layer_headers"].result()
        reference_runs_data = futures["reference_runs"].result()
        loss_ref_data = result_or_warn(
            "loss_ref",
            "Failed to load Loss reference data from /api/ref. "
            "Loss-set upload string-based lookups will not work",
            default=None,
        )
        sys_configs_data = result_or_warn(
            "sys_configs",
            "Failed to load Loss SysConfig. "
            "Loss-set upload will not function until SysConfig is reachable",
            default=[],
        )
        # A deal's roles still resolve without these -- ProgramRole falls back
        # to the raw GUIDs -- so a failure here degrades rather than aborts.
        roles_data = result_or_warn(
            "roles",
            "Failed to load Identity roles from /api/Roles. program.roles will "
            "carry only the rows each deal stores, with no role names, so "
            "lookup by name will not find them",
            default=[],
        )
        users_data = result_or_warn(
            "users",
            "Failed to load Identity users from /api/Users. "
            "program.roles will report user IDs but not names",
            default=[],
        )

        # Accessible objects depend on deal_ref's column definitions.
        accessible_objects_data = self._load_accessible_objects(deal_ref_data)

        self._ref_cache = ReferenceDataCache(
            deal_ref_data,
            clients_data,
            common_ref_data,
            accessible_objects_data,
            loss_ref_data=loss_ref_data,
            roles_data=roles_data,
            users_data=users_data,
        )

        self._layer_headers = {
            item["id"]: item["programId"] for item in layer_headers_data
        }

        # Initialize module-level custom tables (dataLevelId=1)
        self._module_custom_tables = {}
        for table_id, table_def in self._ref_cache._custom_tables.items():
            if table_def.get("dataLevelId") == 1:  # Module level
                original_name = table_def.get("name")
                if not original_name:
                    warnings.warn(
                        f"Skipping module custom table {table_id}: it has no name.",
                        stacklevel=2,
                    )
                    continue

                columns = self._ref_cache.get_custom_table_columns(table_id)
                column_names = [col["name"] for col in columns]
                snake_name = camel_to_snake(original_name)

                table = ModuleCustomTable(
                    client=self,
                    ref_cache=self._ref_cache,
                    table_id=table_id,
                    snake_name=snake_name,
                    original_name=original_name,
                    column_names=column_names,
                )
                self._module_custom_tables[snake_name] = table

                # Any real attribute - a reference-table property, a method, or
                # an instance attribute such as base_url - wins normal lookup,
                # so __getattr__ never sees the name and the custom table is
                # unreachable by dot access. Check __dict__ rather than
                # hasattr(self, ...), which would route back into __getattr__.
                if snake_name in vars(self) or hasattr(type(self), snake_name):
                    warnings.warn(
                        f"Custom table '{original_name}' shares a name with "
                        f"'client.{snake_name}'. Use "
                        f"client.get_custom_table('{original_name}') to reach it.",
                        stacklevel=2,
                    )

        self._reference_runs = ReferenceRuns(reference_runs_data, self._ref_cache)
        self._sys_config = SysConfigCache(sys_configs_data)
        # Built from the reference data just replaced.
        self._portfolio_ref = None

    def _load_accessible_objects(self, deal_ref_data: dict[str, Any]) -> dict[str, Any]:
        """Load accessible objects and their values for lookup field resolution.

        Collects all unique lookupObjectId values from custom table column
        definitions and fetches the corresponding accessible object definitions
        and their values via the Common Module BulkSelection endpoint.

        This mirrors how the Deal UI resolves lookup fields in custom tables.

        Args:
            deal_ref_data: Deal reference data containing customTableColumnDefinitions.

        Returns:
            Dictionary with 'definitions' (list of accessible object definitions)
            and 'values' (list of AccessibleObjectResult from BulkSelection).
        """
        result: dict[str, Any] = {"definitions": [], "values": []}

        column_defs = ref_values(deal_ref_data, "customTableColumnDefinitions")
        lookup_ids = {
            col["lookupObjectId"]
            for col in column_defs
            if col.get("lookupObjectId") is not None
        }
        if not lookup_ids:
            return result

        # Fetch accessible object definitions from Common Module
        try:
            all_accessible_objects = self._fetch("common", "/api/accessibleObjects")
            result["definitions"] = [
                ao for ao in all_accessible_objects if ao.get("id") in lookup_ids
            ]
        except Exception as exc:  # noqa: BLE001 - reference data is best-effort
            warnings.warn(
                f"Failed to load accessible object definitions: {exc}",
                stacklevel=2,
            )
            return result

        # Fetch accessible object values via BulkSelection
        try:
            result["values"] = self._fetch(
                "common",
                "/api/accessibleObjects/BulkSelection",
                method="POST",
                json={"accessibleObjectIds": sorted(lookup_ids)},
            )
        except Exception as exc:  # noqa: BLE001 - reference data is best-effort
            warnings.warn(
                f"Failed to load accessible object values: {exc}",
                stacklevel=2,
            )

        return result

    @property
    def reference_runs(self) -> ReferenceRuns:
        """Get cached reference runs from the Deal Module.

        Reference runs are pre-stored run configurations that can be converted
        to RunConfiguration objects for loss analysis. Only active reference
        runs (activeFlag=True) are included.

        Returns:
            ReferenceRuns collection.

        Example:
            >>> # List all reference runs
            >>> for ref_run in client.reference_runs:
            ...     print(f"{ref_run.name}: {ref_run.sim_years} years")

            >>> # Get by name and convert to RunConfiguration
            >>> ref_run = client.reference_runs.get_by_name("Standard Analysis")
            >>> if ref_run:
            ...     config = ref_run.to_run_configuration(currency_code="USD")
            ...     losses = layer.layered_losses.as_polars(config)
        """
        if self._reference_runs is None:
            return ReferenceRuns([])
        return self._reference_runs

    @property
    def sys_config(self) -> SysConfigCache:
        """Get the Loss module SysConfig alias cache.

        Returns:
            SysConfigCache with the twelve YELTColAlias* / ELTColAliasList*
            entries that define canonical loss-set upload column names.

        Raises:
            RuntimeError: If client is not authenticated.
        """
        if self._sys_config is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        return self._sys_config

    def _cache(self) -> ReferenceDataCache:
        """Return the reference data cache, or refuse if not authenticated.

        Raises RuntimeError rather than AttributeError: an AttributeError
        escaping a property body is swallowed by __getattr__ and re-reported
        as "object has no attribute", hiding the real cause.
        """
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        return self._ref_cache

    # Reference Tables (v0.9.0)
    #
    # Every server reference table is exposed as a property returning a
    # ReferenceTable: a list of models with .by_id / .by_name / .by_code
    # lookups and .as_polars() / .as_pandas() output. Rows are filtered to
    # active ones. All payloads are already fetched by authenticate(), so
    # reading a property costs no HTTP request.

    def _reference(self, name: str) -> "ReferenceTable":
        """Return a reference table by name."""
        return self._cache().reference_table(name)

    # --- Deal module ---

    @property
    def brokers(self) -> "ReferenceTable":
        """Brokers.

        Example:
            >>> client.brokers.by_name["Aon"]
            >>> client.brokers.as_polars()
        """
        return self._reference("brokers")

    @property
    def broker_offices(self) -> "ReferenceTable":
        """Broker offices."""
        return self._reference("broker_offices")

    @property
    def categories(self) -> "ReferenceTable":
        """Deal categories."""
        return self._reference("categories")

    @property
    def category_details(self) -> "ReferenceTable":
        """Deal category details."""
        return self._reference("category_details")

    @property
    def category_detail_maps(self) -> "ReferenceTable":
        """Mappings between categories and category details."""
        return self._reference("category_detail_maps")

    @property
    def currencies(self) -> "ReferenceTable":
        """Currencies. Look up by ISO code with ``by_code["USD"]``."""
        return self._reference("currencies")

    @property
    def custom_fields(self) -> "ReferenceTable":
        """Custom field definitions."""
        return self._reference("custom_fields")

    @property
    def external_ref_types(self) -> "ReferenceTable":
        """External reference types."""
        return self._reference("external_ref_types")

    @property
    def layer_limit_attach_types(self) -> "ReferenceTable":
        """Layer limit/attachment types."""
        return self._reference("layer_limit_attach_types")

    @property
    def loss_group_types(self) -> "ReferenceTable":
        """Loss group types."""
        return self._reference("loss_group_types")

    @property
    def layer_order_term_types(self) -> "ReferenceTable":
        """Layer order term types."""
        return self._reference("layer_order_term_types")

    @property
    def layer_premium_types(self) -> "ReferenceTable":
        """Layer premium types."""
        return self._reference("layer_premium_types")

    @property
    def metrics(self) -> "ReferenceTable":
        """Metric definitions."""
        return self._reference("metrics")

    @property
    def participants(self) -> "ReferenceTable":
        """Participants (markets)."""
        return self._reference("participants")

    @property
    def program_groups(self) -> "ReferenceTable":
        """Deal groups."""
        return self._reference("program_groups")

    @property
    def participant_statuses(self) -> "ReferenceTable":
        """Participant statuses."""
        return self._reference("participant_statuses")

    @property
    def roles(self) -> "ReferenceTable":
        """Identity roles. Deals assign the ones flagged program_role_flag."""
        return self._reference("roles")

    @property
    def users(self) -> "ReferenceTable":
        """Identity users."""
        return self._reference("users")

    @property
    def tokens(self) -> "ReferenceTable":
        """UI tokens."""
        return self._reference("tokens")

    @property
    def token_module_maps(self) -> "ReferenceTable":
        """Token-to-module mappings."""
        return self._reference("token_module_maps")

    @property
    def token_types(self) -> "ReferenceTable":
        """Token types."""
        return self._reference("token_types")

    @property
    def units_of_measure(self) -> "ReferenceTable":
        """Units of measure."""
        return self._reference("units_of_measure")

    @property
    def custom_field_lookups(self) -> "ReferenceTable":
        """Custom field lookup values."""
        return self._reference("custom_field_lookups")

    @property
    def reinst_prem_bases(self) -> "ReferenceTable":
        """Reinstatement premium bases."""
        return self._reference("reinst_prem_bases")

    @property
    def custom_table_definitions(self) -> "ReferenceTable":
        """Custom table definitions.

        For the data in a module-level custom table use
        ``client.get_custom_table(name)`` instead.
        """
        return self._reference("custom_table_definitions")

    @property
    def custom_table_column_definitions(self) -> "ReferenceTable":
        """Custom table column definitions."""
        return self._reference("custom_table_column_definitions")

    @property
    def custom_table_definitions_data_sort(self) -> "ReferenceTable":
        """Custom table sort definitions."""
        return self._reference("custom_table_definitions_data_sort")

    @property
    def pages(self) -> "ReferenceTable":
        """UI pages."""
        return self._reference("pages")

    @property
    def components(self) -> "ReferenceTable":
        """UI components."""
        return self._reference("components")

    @property
    def ui_locations(self) -> "ReferenceTable":
        """UI locations."""
        return self._reference("ui_locations")

    @property
    def ui_location_options(self) -> "ReferenceTable":
        """UI location options."""
        return self._reference("ui_location_options")

    @property
    def layer_statuses(self) -> "ReferenceTable":
        """Layer statuses."""
        return self._reference("layer_statuses")

    @property
    def ref_exceptions(self) -> "ReferenceTable":
        """Reference exceptions."""
        return self._reference("ref_exceptions")

    @property
    def reports(self) -> "ReferenceTable":
        """Report definitions."""
        return self._reference("reports")

    @property
    def reference_run(self) -> "ReferenceTable":
        """Reference run reference table.

        Singular: ``client.reference_runs`` is a different thing - the active
        reference runs from /api/referenceRuns, which convert to run
        configurations.
        """
        return self._reference("reference_run")

    @property
    def functions(self) -> "ReferenceTable":
        """Function definitions."""
        return self._reference("functions")

    @property
    def layout_options(self) -> "ReferenceTable":
        """UI layout options."""
        return self._reference("layout_options")

    @property
    def sys_configs(self) -> "ReferenceTable":
        """Deal module system configuration entries.

        Distinct from ``client.sys_config``, the Loss module column-alias cache.
        """
        return self._reference("sys_configs")

    @property
    def clients(self) -> "ReferenceTable":
        """Clients (cedants)."""
        return self._reference("clients")

    # --- Common module ---

    @property
    def perils(self) -> "ReferenceTable":
        """Perils. Look up by code with ``by_code["EQ"]``."""
        return self._reference("perils")

    @property
    def regions(self) -> "ReferenceTable":
        """Regions. Look up by code with ``by_code["NA"]``."""
        return self._reference("regions")

    @property
    def lobs(self) -> "ReferenceTable":
        """Lines of business. Resolves the ``lob`` column of layered losses."""
        return self._reference("lobs")

    @property
    def models(self) -> "ReferenceTable":
        """Catastrophe models (not the foundation.models package)."""
        return self._reference("models")

    @property
    def model_views(self) -> "ReferenceTable":
        """Model views."""
        return self._reference("model_views")

    @property
    def model_view_details(self) -> "ReferenceTable":
        """Model view details."""
        return self._reference("model_view_details")

    @property
    def accessible_objects(self) -> "ReferenceTable":
        """Accessible object definitions used by lookup columns."""
        return self._reference("accessible_objects")

    @property
    def ref_grid_displays(self) -> "ReferenceTable":
        """Reference grid display definitions."""
        return self._reference("ref_grid_displays")

    @property
    def ref_grid_display_columns(self) -> "ReferenceTable":
        """Reference grid display column definitions."""
        return self._reference("ref_grid_display_columns")

    # --- Loss module ---

    @property
    def event_sets(self) -> "ReferenceTable":
        """Event sets."""
        return self._reference("event_sets")

    @property
    def loss_types(self) -> "ReferenceTable":
        """Loss types."""
        return self._reference("loss_types")

    @property
    def perspectives(self) -> "ReferenceTable":
        """Loss perspectives."""
        return self._reference("perspectives")

    @property
    def variants(self) -> "ReferenceTable":
        """Loss variants."""
        return self._reference("variants")

    @property
    def vendors(self) -> "ReferenceTable":
        """Model vendors (RMS, AIR, ...)."""
        return self._reference("vendors")

    @property
    def upload_statuses(self) -> "ReferenceTable":
        """Loss set upload statuses."""
        return self._reference("upload_statuses")

    @property
    def event_split_views(self) -> "ReferenceTable":
        """Event split views."""
        return self._reference("event_split_views")

    def get_model_ids_by_view_id(self, model_view_id: int) -> list[int]:
        """Get list of model IDs for a given model view ID.

        Model views are collections of models defined in the Common Module.
        This method resolves a model view ID to its constituent model IDs.

        Args:
            model_view_id: The model view ID to look up.

        Returns:
            List of model IDs associated with the model view.
            Returns empty list if not found or not authenticated.

        Example:
            >>> model_ids = client.get_model_ids_by_view_id(1)
            >>> print(f"Models in view: {model_ids}")
        """
        if self._ref_cache is None:
            return []
        return self._ref_cache.get_model_ids_by_view_id(model_view_id)

    def get_program_versions(self, program_id: int) -> ProgramVersions:
        """
        List the saved versions of a deal, newest first.

        Args:
            program_id: The program ID whose versions to list

        Returns:
            ProgramVersions collection

        Raises:
            FoundationHTTPError: If the request fails

        Example:
            >>> versions = client.get_program_versions(2455)
            >>> versions.describe()
        """
        # ByProgramId, not the generic ProgramVersions/{id} route: only this
        # one tags SaveDate as UTC, and an untagged timestamp would be read as
        # local time and select the wrong instant.
        versions_data = self._fetch(
            "deal", f"/api/ProgramVersions/ByProgramId/{program_id}"
        )
        versions = ProgramVersions(versions_data)
        # A version's save date never changes, so cache each one: a later
        # versioned read of this deal — including via program.versions then
        # get_version — resolves without re-fetching the list. The list itself
        # is not cached; it grows as the deal is saved.
        for version in versions:
            if version.raw_save_date:
                self._version_save_dates[(program_id, version.id)] = (
                    version.raw_save_date
                )
        return versions

    def _resolve_version_save_date(self, program_id: int, version_id: int) -> str:
        """Return the save date identifying a version of a deal.

        Looking the version up within its deal's own version list also rules
        out a version ID belonging to a different deal, which would otherwise
        silently return that deal's state at an unrelated moment.
        """
        cached = self._version_save_dates.get((program_id, version_id))
        if cached is not None:
            return cached

        # Fetching the list caches every version's save date (see
        # get_program_versions), so this is the only fetch a loop over a deal's
        # versions costs.
        version = self.get_program_versions(program_id).get_by_id(version_id)
        if version is None:
            raise ValueError(
                f"Version {version_id} does not belong to deal {program_id}. "
                "Use program.versions to list the versions of a deal."
            )
        if not version.raw_save_date:
            raise ValueError(
                f"Version {version_id} of deal {program_id} has no save date, "
                "so the deal cannot be read at that version."
            )
        return version.raw_save_date

    def get_program(self, program_id: int, version_id: int | None = None) -> Program:
        """
        Retrieve a program by ID, optionally as it stood at a past version.

        Args:
            program_id: The program ID to retrieve
            version_id: Saved version to read; omit for the current deal

        Returns:
            Program instance with enriched data. A versioned program is
            read-only.

        Raises:
            FoundationHTTPError: If the request fails
            ValueError: If the version does not belong to this deal

        Example:
            >>> program = client.get_program(2455)
            >>> print(program.name)
            >>> print(program.uw_year)

            >>> previous = client.get_program(2455, version_id=23011)
            >>> print(previous.layers[0].get_document_field("collateral_req_share"))
        """
        if version_id is None:
            program_data = self._fetch("deal", f"/api/programs/{program_id}")
            analyses_path = f"/api/analysesByProgram/{program_id}"
        else:
            cached_program = self._versioned_programs.get((program_id, version_id))
            if cached_program is None:
                save_date = self._resolve_version_save_date(program_id, version_id)
                # The /Restore route, not the plain AsOfDate one: the latter
                # returns the live deal whenever the requested instant is at or
                # after the newest save, which is exactly the case when reading
                # the newest version. That would report edits made since the
                # save as though they were part of it. /Restore always
                # reconstructs from history and performs no writes.
                cached_program = self._fetch(
                    "deal",
                    f"/api/Programs/{program_id}"
                    f"/AsOfDate/{quote(save_date, safe='')}/Restore",
                )
                self._versioned_programs[(program_id, version_id)] = cached_program
            # Each Program owns its document, so hand out a copy rather than
            # letting two snapshots of one version share mutable state.
            program_data = copy.deepcopy(cached_program)
            # Stored analyses live in the current tables tagged with the
            # version that produced them, so they are selected by version ID
            # rather than reconstructed from history.
            analyses_path = (
                f"/api/AnalysesByProgram/{program_id}/AsOfVersion/{version_id}"
            )

        # Fetch stored analyses from Metrics API (if available). Cache per
        # (program_id, version_id) so repeated get_program() calls don't
        # re-hit the endpoint.
        cache_key = (program_id, version_id)
        if cache_key not in self._analyses_cache:
            try:
                self._analyses_cache[cache_key] = self._fetch("deal", analyses_path)
            except Exception:  # noqa: BLE001 - stored analyses are optional
                self._analyses_cache[cache_key] = []

        return Program(
            program_data,
            self._ref_cache,
            self._analyses_cache[cache_key],
            self,
            version_id=version_id,
        )

    def get_layer(self, layer_id: int, version_id: int | None = None) -> "Layer":
        """
        Retrieve a layer by ID, optionally as it stood at a past version.

        The layer must exist in the deal as it stands now: layers are located
        through a map of the current deals. A layer that has since been
        deleted is still reachable through its deal, via
        ``client.get_program(deal_id, version_id=...).layers``.

        Args:
            layer_id: The layer ID to retrieve
            version_id: Saved version to read; omit for the current layer

        Returns:
            Layer instance with enriched data. A versioned layer is read-only.

        Raises:
            FoundationHTTPError: If the request fails
            ValueError: If the layer ID is not found, or did not exist yet in
                the requested version

        Example:
            >>> layer = client.get_layer(10581)
            >>> print(layer.name)
            >>> print(layer.incept_date)

            >>> previous = client.get_layer(10581, version_id=23011)
            >>> print(previous.get_document_field("collateral_req_share"))
        """

        # Find the program ID for this layer
        program_id = self._layer_headers.get(layer_id)
        if program_id is None:
            raise ValueError(f"Layer {layer_id} not found")

        # Get the full program
        program = self.get_program(program_id, version_id=version_id)

        # Find and return the specific layer
        for layer in program.layers:
            if layer.id == layer_id:
                return layer

        if version_id is not None:
            raise ValueError(
                f"Layer {layer_id} was not part of deal {program_id} "
                f"at version {version_id}."
            )

        # Layer not found in program (should not happen)
        raise ValueError(f"Layer {layer_id} not found in program {program_id}")

    def get_portfolio(self, portfolio_id: int) -> Portfolio:
        """
        Retrieve a portfolio by ID.

        Args:
            portfolio_id: The portfolio ID to retrieve

        Returns:
            Portfolio instance, whose metric results and loss files load on
            first use

        Raises:
            NotFoundError: If there is no such portfolio

        Example:
            >>> p = client.get_portfolio(9181)
            >>> p.metric_results.as_polars()
        """
        return Portfolio(
            self._fetch("portfolio", f"/api/Portfolios/{portfolio_id}"), self
        )

    def get_templates(self) -> Templates:
        """
        List every portfolio template, active or not.

        Each template's groups, models, dimensions and scenarios load when
        first read.

        Example:
            >>> client.get_templates().as_polars()
            >>> tpl = client.get_templates()["Q3 Property"]
        """
        return Templates(
            Template(self, header=row)
            for row in self._fetch("portfolio", "/api/Templates")
        )

    def get_template(self, template: int | str) -> Template:
        """
        Retrieve a portfolio template by ID or by name.

        Args:
            template: The template ID, or its exact name in any case

        Raises:
            NotFoundError: If there is no template with that ID
            ValueError: If no template, or more than one, has that name

        Example:
            >>> tpl = client.get_template(253)
            >>> tpl = client.get_template("Q3 Property")
        """
        if isinstance(template, str):
            folded = template.casefold()
            matches = [
                row
                for row in self._fetch("portfolio", "/api/Templates")
                if (row.get("name") or "").casefold() == folded
            ]
            if not matches:
                raise ValueError(f"No template is named {template!r}.")
            if len(matches) > 1:
                found = ", ".join(
                    f"{row['id']} {'active' if row.get('activeFlag') else 'inactive'}"
                    for row in matches
                )
                raise ValueError(
                    f"More than one template is named {template!r} (IDs {found}). "
                    "Pass the ID instead."
                )
            template = matches[0]["id"]
        data = self._fetch("portfolio", f"/api/Templates/{template}")
        return Template(self, data["template"], data.get("schedule"))

    def new_template(
        self,
        name: str,
        *,
        groups: Mapping[str, str | Sequence[str]],
        vendor: int | str,
        variant: int | str,
        perspective: int | str,
        event_set: int | str,
        currency: str,
        sim_years: int,
        models: Sequence[int | str] | None = None,
        model_view: int | str | None = None,
        dimensions: Sequence[int | str] = (),
        participant: int | str | None = None,
        event_split_view: int | str | None = None,
        as_of_date: datetime | date | None = None,
        scenarios: Sequence[Scenario] = (),
        generate_scenarios_only: bool = False,
        apply_loss_splits: bool = True,
        description: str | None = None,
        pricing: bool = False,
    ) -> Template:
        """
        Build a new, unsaved portfolio template.

        Run it as it is, as often as needed, or save() it to make it a regular
        template. Reference data is given by ID or by name.

        Args:
            name: Template name, at most 100 characters
            groups: Deal selection rules, group name -> criterion or list of
                criteria, in the Portfolio UI formula builder's format.
                Criteria in a group are ANDed and groups are ORed.
            vendor, variant, perspective, event_set: The losses to roll up
            currency: Currency code the results are reported in
            sim_years: Simulation years the losses must have
            models: Model IDs or names; pass this or model_view
            model_view: An active model view's ID or name; its models are used
            dimensions: Dimensions to slice the results by, IDs or names
            participant: The share to roll layers up at; None for 100%
            event_split_view: An event split view to apply to the losses
            as_of_date: Read deals as they stood then; None for their current
                state
            scenarios: Scenarios, each creating one extra portfolio per run
            generate_scenarios_only: Create only scenario portfolios
            apply_loss_splits: Apply loss set event splits
            description: Optional description
            pricing: Make the template's portfolios available for pricing deals

        Raises:
            ValueError: If both or neither of models and model_view are given,
                or a name is unknown

        Example:
            >>> tpl = client.new_template(
            ...     "Europe wind", groups={"Europe": 'Geography == "Europe"'},
            ...     vendor="Vendor A", variant="Standard", perspective="Gross",
            ...     event_set="Stochastic", model_view="All Europe",
            ...     currency="USD", sim_years=10000)
        """
        if (models is None) == (model_view is None):
            raise ValueError("Pass exactly one of models and model_view.")
        template = Template.blank(self, name)
        template.description = description
        template.vendor_id = vendor
        template.variant_id = variant
        template.perspective_id = perspective
        template.event_set_id = event_set
        template.currency_code = currency
        template.sim_years = sim_years
        template.participant_id = participant
        template.event_split_view_id = event_split_view
        template.as_of_date = as_of_date
        template.generate_scenarios_only = generate_scenarios_only
        template.apply_loss_splits = apply_loss_splits
        template.pricing_flag = pricing
        template.groups = groups
        if models is not None:
            template.model_ids = models
        else:
            template.model_view = model_view
        template.dimensions = dimensions
        template.scenarios = scenarios
        return template

    def get_rollup_run(self, request_id: int) -> RollupRun:
        """
        Follow a template run by its request ID, e.g. after logging in again.

        Args:
            request_id: The run's request_id

        Raises:
            NotFoundError: If there is no such run

        Example:
            >>> run = client.get_rollup_run(42).wait()
        """
        return RollupRun(self, request_id).refresh()

    @property
    def criteria_fields(self) -> Rows:
        """The Portfolio formula builder's fields: the names a selection
        formula can use, their token names, and the lookup a quoted label is
        resolved in.

        Columns: name, token_name, lookup.
        """
        return self._portfolio_reference().formulas.criteria_fields

    def _portfolio_reference(self) -> PortfolioReference:
        if self._portfolio_ref is None:
            self._portfolio_ref = PortfolioReference(self)
        return self._portfolio_ref

    @property
    def is_authenticated(self) -> bool:
        """Check if the client is authenticated."""
        return self._token is not None

    # Reference Data Access Methods (v0.3.0)

    def get_peril(self, identifier: int | str) -> Optional["Peril"]:
        """
        Get peril by ID or code.

        Args:
            identifier: Either an integer ID or a string code

        Returns:
            Peril instance or None if not found

        Example:
            >>> peril = client.get_peril(1)  # By ID
            >>> peril = client.get_peril("EQ")  # By code
            >>> print(peril.name)
        """
        return self._cache().get_peril(identifier)

    def get_region(self, identifier: int | str) -> Optional["Region"]:
        """
        Get region by ID or code.

        Args:
            identifier: Either an integer ID or a string code

        Returns:
            Region instance or None if not found

        Example:
            >>> region = client.get_region(1)  # By ID
            >>> region = client.get_region("NA")  # By code
            >>> print(region.name)
            >>> if region.parent:
            ...     print(f"Parent: {region.parent.name}")
        """
        return self._cache().get_region(identifier)

    def get_participant(self, identifier: int | str) -> Optional["Participant"]:
        """
        Get participant by ID or name.

        Args:
            identifier: Either an integer ID or a string name

        Returns:
            Participant instance or None if not found

        Example:
            >>> participant = client.get_participant(1)  # By ID
            >>> participant = client.get_participant("Company X")  # By name
            >>> print(participant.name)
        """
        return self._cache().get_participant(identifier)

    def get_vendor(self, identifier: int | str) -> Optional["Vendor"]:
        """
        Get vendor by ID or name.

        Args:
            identifier: Either an integer ID or a string name

        Returns:
            Vendor instance or None if not found

        Example:
            >>> vendor = client.get_vendor("RMS")
        """
        return self._cache().get_vendor(identifier)

    def get_variant(self, identifier: int | str) -> Optional["Variant"]:
        """
        Get variant by ID or name.

        Args:
            identifier: Either an integer ID or a string name

        Returns:
            Variant instance or None if not found

        Example:
            >>> variant = client.get_variant("EP")
        """
        return self._cache().get_variant(identifier)

    def get_perspective(self, identifier: int | str) -> Optional["Perspective"]:
        """
        Get perspective by ID or name.

        Args:
            identifier: Either an integer ID or a string name

        Returns:
            Perspective instance or None if not found

        Example:
            >>> perspective = client.get_perspective("Gross")
        """
        return self._cache().get_perspective(identifier)

    def get_event_set(self, identifier: int | str) -> Optional["EventSet"]:
        """
        Get event set by ID or name.

        Args:
            identifier: Either an integer ID or a string name

        Returns:
            EventSet instance or None if not found

        Example:
            >>> event_set = client.get_event_set("Standard")
        """
        return self._cache().get_event_set(identifier)

    def get_client(self, identifier: int | str) -> Optional["Client"]:
        """Get a client by ID or name (case-insensitive)."""
        return self._cache().get_client(identifier)

    def get_broker(self, identifier: int | str) -> Optional["Broker"]:
        """Get a broker by ID or name (case-insensitive)."""
        return self._cache().get_broker(identifier)

    def get_broker_office(self, identifier: int | str) -> Optional["BrokerOffice"]:
        """Get a broker office by ID or name (case-insensitive)."""
        return self._cache().get_broker_office(identifier)

    def get_currency(self, identifier: int | str) -> Optional["Currency"]:
        """Get a currency by ID, code or name (case-insensitive)."""
        return self._cache().get_currency(identifier)

    def get_unit_of_measure(self, identifier: int | str) -> Optional["UnitOfMeasure"]:
        """Get a unit of measure by ID or name (case-insensitive)."""
        return self._cache().get_unit_of_measure(identifier)

    def get_layer_status(self, identifier: int | str) -> Optional["LayerStatus"]:
        """Get a layer status by ID, code or name (case-insensitive)."""
        return self._cache().get_layer_status(identifier)

    def get_layer_limit_attach_type(
        self, identifier: int | str
    ) -> Optional["LayerLimitAttachType"]:
        """Get a layer limit/attach type by ID or name (case-insensitive)."""
        return self._cache().get_layer_limit_attach_type(identifier)

    def get_layer_premium_type(
        self, identifier: int | str
    ) -> Optional["LayerPremiumType"]:
        """Get a layer premium type by ID or name (case-insensitive)."""
        return self._cache().get_layer_premium_type(identifier)

    def get_layer_order_term_type(
        self, identifier: int | str
    ) -> Optional["LayerOrderTermType"]:
        """Get a layer order term type by ID or name (case-insensitive)."""
        return self._cache().get_layer_order_term_type(identifier)

    def get_participant_status(
        self, identifier: int | str
    ) -> Optional["ParticipantStatus"]:
        """Get a participant status by ID or name (case-insensitive)."""
        return self._cache().get_participant_status(identifier)

    def get_reinst_prem_base(self, identifier: int | str) -> Optional["ReinstPremBase"]:
        """Get a reinstatement premium base by ID or name (case-insensitive)."""
        return self._cache().get_reinst_prem_base(identifier)

    def get_external_ref_type(
        self, identifier: int | str
    ) -> Optional["ExternalRefType"]:
        """Get an external reference type by ID or name (case-insensitive)."""
        return self._cache().get_external_ref_type(identifier)

    def get_custom_field(self, identifier: int | str) -> Optional["CustomField"]:
        """Get a custom field definition by ID or name (case-insensitive)."""
        return self._cache().get_custom_field(identifier)

    def get_category(self, identifier: int | str) -> Optional["Category"]:
        """Get a category by ID, code or name (case-insensitive)."""
        return self._cache().get_category(identifier)

    def get_category_detail(self, identifier: int | str) -> Optional["CategoryDetail"]:
        """Get a category detail by ID, code or name (case-insensitive)."""
        return self._cache().get_category_detail(identifier)

    # Module-Level Custom Tables Access Methods (v0.5.0)

    def get_custom_table(self, table_name: str) -> ModuleCustomTable:
        """
        Get a module-level custom table by name (original name as defined in Foundation).

        Module-level custom tables (dataLevelId=1) apply to all programs and are not
        returned as part of program data. They are loaded on-demand when first accessed.

        Args:
            table_name: Original table name (e.g., "Historical Events") or snake_case name

        Returns:
            ModuleCustomTable instance

        Raises:
            ValueError: If table not found
            RuntimeError: If client not authenticated

        Example:
            >>> table = client.get_custom_table("Historical Events")
            >>> df = table.as_pandas()
            >>> print(df.head())

        Note:
            Module-level custom tables are mutable on the client side but changes
            won't be saved on the backend and won't affect the analysis.
        """
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")

        # Try to find by snake_case name first
        snake_name = camel_to_snake(table_name)
        if snake_name in self._module_custom_tables:
            return self._module_custom_tables[snake_name]

        # Try to find by original name
        for table in self._module_custom_tables.values():
            if table.name == table_name:
                return table

        raise ValueError(f"Module-level custom table '{table_name}' not found")

    @property
    def custom_tables_names(self) -> list[str]:
        """
        List all available module-level custom table names (snake_case).

        Returns:
            List of snake_case table names

        Example:
            >>> tables = client.custom_tables_names
            >>> for table_name in tables:
            ...     print(table_name)
        """
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")

        return list(self._module_custom_tables.keys())

    def describe(self) -> None:
        """Display a summary of the Foundation client and available resources.

        Example:
            >>> client.describe()
        """
        auth_status = "authenticated" if self._token else "not authenticated"
        print("=" * 60)
        print("FOUNDATION CLIENT")
        print("=" * 60)
        print(f"  Base URL:    {self.base_url}")
        print(f"  Status:      {auth_status}")

        if self._reference_runs is not None:
            print(f"  Ref. Runs:   {len(self._reference_runs)} active")
        else:
            print("  Ref. Runs:   (not loaded)")

        table_names = list(self._module_custom_tables)
        if table_names:
            print(f"  Custom Tables: {len(table_names)} module-level")
            for t in table_names:
                print(f"    - {t}")
        print(
            f"  Ref. Tables: {len(REFERENCE_TABLES)} (e.g. brokers, clients, participants, vendors)"
        )
        print()
        print("PROPERTIES:")
        print("  reference_runs       -> ReferenceRuns")
        print("  custom_tables_names  -> list[str]")
        print("  <reference table>    -> ReferenceTable")
        print()
        print("METHODS:")
        print("  get_program(id)      -> Program")
        print("  get_portfolio(id)    -> Portfolio")
        print("  get_template(id|nm)  -> Template")
        print("  get_templates()      -> Templates")
        print("  new_template(nm,...) -> Template")
        print("  get_rollup_run(id)   -> RollupRun")
        print("  get_custom_table(nm) -> ModuleCustomTable")
        print("  describe()           -> None")
        print("=" * 60)
        print()

    def __dir__(self):
        """Return list of available attributes for autocomplete.

        Includes standard properties, methods, and dynamically-available
        module custom table names.
        """
        # Every property, method and instance attribute is a real attribute,
        # so super().__dir__() already covers them; only the custom tables
        # reached through __getattr__ have to be added.
        return sorted({*super().__dir__(), *self._module_custom_tables})

    def __getattr__(self, name: str) -> Any:
        """
        Dynamic attribute access for module-level custom tables.

        This enables dot notation access like:
        - client.historical_events (instead of client.get_custom_table('Historical Events'))

        Args:
            name: Attribute name in snake_case

        Returns:
            ModuleCustomTable instance

        Raises:
            AttributeError: If attribute not found
        """
        raise_if_private(self, name)

        # Check module-level custom tables
        if name in self._module_custom_tables:
            return self._module_custom_tables[name]

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )
