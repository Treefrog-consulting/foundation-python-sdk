"""Foundation Platform API Client"""

import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import requests

from ..utils import camel_to_snake

if TYPE_CHECKING:
    from ..ref_data import ReferenceDataCache
    from .module_custom_table import ModuleCustomTable
    from .participant import Participant
    from .peril import Peril
    from .program import Program
    from .reference_runs import ReferenceRuns
    from .region import Region


class FoundationClient:
    """Client for interacting with the Foundation Platform API."""

    PORTS = {
        "identity": 5000,
        "deal": 5001,
        "loss": 5002,
        "common": 5003,
        "metrics": 5005,
        "portfolio": 5006,
        "admin": 5010,
    }

    def __init__(self, base_url: str):
        """
        Initialize the Foundation Platform client.

        Args:
            base_url: Base URL for the API (without port), e.g., "https://your-instance.foundationplatform.com"
        """
        self.base_url = base_url.rstrip("/")
        self._token: Optional[str] = None
        self._ref_cache: Optional["ReferenceDataCache"] = None
        self._layer_headers: Dict[int, int] = {}  # layer_id -> program_id mapping
        self._session = requests.Session()
        self._module_custom_tables: Dict[
            str, "ModuleCustomTable"
        ] = {}  # snake_name -> table
        self._reference_runs: Optional["ReferenceRuns"] = None

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
            requests.HTTPError: If authentication fails
        """
        from ..ref_data import ReferenceDataCache

        url = f"{self.base_url}:{self.PORTS['identity']}/api/account/login"
        payload = {
            "email": email,
            "password": password,
            "apiClientId": api_client_id,
            "apiClientSecret": api_client_secret,
        }

        response = self._session.post(url, json=payload)
        response.raise_for_status()

        data = response.json()
        self._token = data["tokenResponse"]["accessToken"]
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})

        # Load reference data after authentication
        self._load_reference_data()

    def _load_reference_data(self) -> None:
        """Load and cache reference data from the API."""
        from ..ref_data import ReferenceDataCache
        from .module_custom_table import ModuleCustomTable

        # Load Deal reference data
        deal_ref_url = f"{self.base_url}:{self.PORTS['deal']}/api/ref"
        deal_ref_response = self._session.get(deal_ref_url)
        deal_ref_response.raise_for_status()
        deal_ref_data = deal_ref_response.json()

        # Load clients data
        clients_url = f"{self.base_url}:{self.PORTS['deal']}/api/clients"
        clients_response = self._session.get(clients_url)
        clients_response.raise_for_status()
        clients_data = clients_response.json()

        # Load Common reference data
        common_ref_url = f"{self.base_url}:{self.PORTS['common']}/api/ref"
        common_ref_response = self._session.get(common_ref_url)
        common_ref_response.raise_for_status()
        common_ref_data = common_ref_response.json()

        # Load accessible objects for lookup field resolution in custom tables
        accessible_objects_data = self._load_accessible_objects(deal_ref_data)

        # Create reference data cache with all data
        self._ref_cache = ReferenceDataCache(
            deal_ref_data, clients_data, common_ref_data, accessible_objects_data
        )

        # Load layer headers for layer-to-program mapping
        layer_headers_url = f"{self.base_url}:{self.PORTS['deal']}/api/layers/headers"
        layer_headers_response = self._session.get(layer_headers_url)
        layer_headers_response.raise_for_status()
        layer_headers_data = layer_headers_response.json()

        # Build layer_id -> program_id mapping
        self._layer_headers = {
            item["id"]: item["programId"] for item in layer_headers_data
        }

        # Initialize module-level custom tables (dataLevelId=1)
        self._module_custom_tables = {}
        for table_id, table_def in self._ref_cache._custom_tables.items():
            if table_def.get("dataLevelId") == 1:  # Module level
                columns = self._ref_cache.get_custom_table_columns(table_id)
                column_names = [col["name"] for col in columns]
                snake_name = camel_to_snake(table_def["name"])
                original_name = table_def["name"]

                table = ModuleCustomTable(
                    client=self,
                    ref_cache=self._ref_cache,
                    table_id=table_id,
                    snake_name=snake_name,
                    original_name=original_name,
                    column_names=column_names,
                )
                self._module_custom_tables[snake_name] = table

        # Load reference runs from Deal Module
        self._load_reference_runs()

    def _load_accessible_objects(self, deal_ref_data: Dict[str, Any]) -> Dict[str, Any]:
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
        result: Dict[str, Any] = {"definitions": [], "values": []}

        # Collect unique lookupObjectId values from custom table column definitions
        column_defs = deal_ref_data.get("customTableColumnDefinitions", {}).get(
            "values", []
        )
        lookup_ids = set()
        for col in column_defs:
            lookup_id = col.get("lookupObjectId")
            if lookup_id is not None:
                lookup_ids.add(lookup_id)

        if not lookup_ids:
            return result

        # Fetch accessible object definitions from Common Module
        try:
            ao_url = f"{self.base_url}:{self.PORTS['common']}/api/accessibleObjects"
            ao_response = self._session.get(ao_url)
            ao_response.raise_for_status()
            all_accessible_objects = ao_response.json()

            # Filter to only the ones we need
            result["definitions"] = [
                ao for ao in all_accessible_objects if ao.get("id") in lookup_ids
            ]
        except Exception as exc:
            warnings.warn(
                f"Failed to load accessible object definitions: {exc}",
                stacklevel=2,
            )
            return result

        # Fetch accessible object values via BulkSelection
        try:
            bulk_url = f"{self.base_url}:{self.PORTS['common']}/api/accessibleObjects/BulkSelection"
            bulk_response = self._session.post(
                bulk_url, json={"accessibleObjectIds": sorted(lookup_ids)}
            )
            bulk_response.raise_for_status()
            result["values"] = bulk_response.json()
        except Exception as exc:
            warnings.warn(
                f"Failed to load accessible object values: {exc}",
                stacklevel=2,
            )

        return result

    def _load_reference_runs(self) -> None:
        """Load and cache reference runs from the Deal Module.

        Reference runs are loaded once and cached for the session.
        Only active reference runs (activeFlag=True) are exposed.
        """
        from .reference_runs import ReferenceRuns

        reference_runs_url = f"{self.base_url}:{self.PORTS['deal']}/api/referenceRuns"
        reference_runs_response = self._session.get(reference_runs_url)
        reference_runs_response.raise_for_status()
        reference_runs_data = reference_runs_response.json()

        self._reference_runs = ReferenceRuns(reference_runs_data, self._ref_cache)

    @property
    def reference_runs(self) -> "ReferenceRuns":
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
        from .reference_runs import ReferenceRuns

        if self._reference_runs is None:
            return ReferenceRuns([])
        return self._reference_runs

    def get_model_ids_by_view_id(self, model_view_id: int) -> List[int]:
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

    def get_program(self, program_id: int) -> "Program":
        """
        Retrieve a program by ID.

        Args:
            program_id: The program ID to retrieve

        Returns:
            Program instance with enriched data

        Raises:
            requests.HTTPError: If the request fails

        Example:
            >>> program = client.get_program(2455)
            >>> print(program.name)
            >>> print(program.uw_year)
        """
        from .program import Program

        url = f"{self.base_url}:{self.PORTS['deal']}/api/programs/{program_id}"
        response = self._session.get(url)
        response.raise_for_status()

        program_data = response.json()

        # Fetch stored analyses from Metrics API (if available)
        analyses_data = []
        try:
            analyses_url = f"{self.base_url}:{self.PORTS['deal']}/api/analysesByProgram/{program_id}"
            analyses_response = self._session.get(analyses_url)
            analyses_response.raise_for_status()
            analyses_data = analyses_response.json()
        except Exception:
            # Analyses not available (older Foundation version or other error)
            # Continue without analyses
            pass

        return Program(program_data, self._ref_cache, analyses_data, self)

    def get_layer(self, layer_id: int) -> "Layer":
        """
        Retrieve a layer by ID.

        Args:
            layer_id: The layer ID to retrieve

        Returns:
            Layer instance with enriched data

        Raises:
            requests.HTTPError: If the request fails
            ValueError: If the layer ID is not found

        Example:
            >>> layer = client.get_layer(10581)
            >>> print(layer.name)
            >>> print(layer.incept_date)
        """
        from .layer import Layer

        # Find the program ID for this layer
        program_id = self._layer_headers.get(layer_id)
        if program_id is None:
            raise ValueError(f"Layer {layer_id} not found")

        # Get the full program
        program = self.get_program(program_id)

        # Find and return the specific layer
        for layer in program.layers:
            if layer.id == layer_id:
                return layer

        # Layer not found in program (should not happen)
        raise ValueError(f"Layer {layer_id} not found in program {program_id}")

    @property
    def is_authenticated(self) -> bool:
        """Check if the client is authenticated."""
        return self._token is not None

    # Reference Data Access Methods (v0.3.0)

    def get_peril(self, identifier: Union[int, str]) -> Optional["Peril"]:
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
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        return self._ref_cache.get_peril(identifier)

    def get_all_perils(self) -> List["Peril"]:
        """
        Get all perils.

        Returns:
            List of all Peril instances

        Example:
            >>> perils = client.get_all_perils()
            >>> for peril in perils:
            ...     print(f"{peril.code}: {peril.name}")
        """
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        return self._ref_cache.get_all_perils()

    def get_region(self, identifier: Union[int, str]) -> Optional["Region"]:
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
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        return self._ref_cache.get_region(identifier)

    def get_all_regions(self) -> List["Region"]:
        """
        Get all regions.

        Returns:
            List of all Region instances

        Example:
            >>> regions = client.get_all_regions()
            >>> for region in regions:
            ...     print(f"{region.code}: {region.name}")
        """
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        return self._ref_cache.get_all_regions()

    def get_participant(self, identifier: Union[int, str]) -> Optional["Participant"]:
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
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        return self._ref_cache.get_participant(identifier)

    def get_all_participants(self) -> List["Participant"]:
        """
        Get all participants.

        Returns:
            List of all Participant instances

        Example:
            >>> participants = client.get_all_participants()
            >>> for participant in participants:
            ...     print(participant.name)
        """
        if self._ref_cache is None:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        return self._ref_cache.get_all_participants()

    # Module-Level Custom Tables Access Methods (v0.5.0)

    def get_custom_table(self, table_name: str) -> "ModuleCustomTable":
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
    def custom_tables_names(self) -> List[str]:
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

        table_names = (
            list(self._module_custom_tables.keys())
            if hasattr(self, "_module_custom_tables")
            else []
        )
        if table_names:
            print(f"  Custom Tables: {len(table_names)} module-level")
            for t in table_names:
                print(f"    - {t}")
        print()
        print("PROPERTIES:")
        print("  reference_runs       -> ReferenceRuns")
        print("  custom_tables_names  -> list[str]")
        print()
        print("METHODS:")
        print("  get_program(id)      -> Program")
        print("  save_program(prog)   -> Program")
        print("  describe()           -> None")
        print("=" * 60)
        print()

    def __dir__(self):
        """Return list of available attributes for autocomplete.

        Includes standard properties, methods, and dynamically-available
        module custom table names.
        """
        attrs = set(super().__dir__())
        attrs.update(
            [
                "base_url",
                "reference_runs",
                "authenticate",
                "get_program",
                "save_program",
                "describe",
            ]
        )
        # Add module-level custom table names
        if hasattr(self, "_module_custom_tables"):
            attrs.update(self._module_custom_tables.keys())
        return sorted(attrs)

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
        # Check module-level custom tables
        if name in self._module_custom_tables:
            return self._module_custom_tables[name]

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )
