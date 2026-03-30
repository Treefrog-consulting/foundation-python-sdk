"""YLT (Year Loss Table) accessor for layers."""

import hashlib
import json
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

    from .layer import Layer
    from .run_configuration import RunConfiguration


class Ylt:
    """Accessor for YLT (Year Loss Table) data.

    This class provides access to YLT custom field data for a specific layer.
    It handles caching of the DuckDB file and lazy loading of data.

    Calling the accessor directly returns a Polars DataFrame by default:

        >>> config = RunConfiguration(sim_years=10000, vendor_id=1, currency_code="USD")
        >>> df = layer.ylt(config)  # returns Polars DataFrame
        >>> print(df.head())

    You can also explicitly request a specific format:

        >>> df_polars = layer.ylt.as_polars(config)
        >>> df_pandas = layer.ylt.as_pandas(config)
    """

    def __init__(self, layer: "Layer"):
        """Initialize YLT accessor.

        Args:
            layer: The layer this accessor is associated with.
        """
        self._layer = layer
        self._cache: dict = {}  # Cache key -> (duckdb_path, program_json_hash)

    def __call__(self, run_config: "RunConfiguration") -> "pl.DataFrame":
        """Get YLT data as a Polars DataFrame (default format).

        Calling the accessor directly is the recommended way to retrieve data:

            >>> df = layer.ylt(config)

        This is equivalent to calling ``as_polars(run_config)``.

        Args:
            run_config: Run configuration specifying analysis parameters.

        Returns:
            Polars DataFrame with YLT data for this layer.
        """
        return self.as_polars(run_config)

    def as_polars(self, run_config: "RunConfiguration") -> "pl.DataFrame":
        """Get YLT data as a Polars DataFrame.

        This method calls the Metrics Module API if data is not cached,
        saves the DuckDB file to a temporary location, and reads the
        YLT custom fields for this specific layer.

        Args:
            run_config: Run configuration specifying analysis parameters.

        Returns:
            Polars DataFrame with YLT data for this layer.
            Columns: year, loss, rip_amt, rip_pct, rip_brok_amt, rip_brok_pct,
                    [custom_field_1], [custom_field_2], ...
            First columns are AEP aggregated losses, followed by custom field
            columns that are dynamically determined based on the program's
            YLT custom field configuration.

        Raises:
            ImportError: If polars is not installed.
            RuntimeError: If the API call fails, data cannot be loaded,
                         or YLT table doesn't exist.
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "Polars is required to use as_polars(). "
                "Install it with: pip install polars"
            )

        if self._layer._program is None:
            raise RuntimeError(
                "Layer must be associated with a program to export YLT data. "
                "Retrieve the layer via program.layers or client.get_program()."
            )

        # Get or create cached DuckDB file
        duckdb_path = self._get_or_create_duckdb(run_config)

        # Read YLT data for this layer from DuckDB
        import duckdb

        con = duckdb.connect(str(duckdb_path), read_only=True)
        try:
            # Check if YLT table exists
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ylt_losses'"
            ).fetchall()

            if not tables:
                raise RuntimeError(
                    "No YLT data available for this program. "
                    "The DuckDB file does not contain a ylt_losses table."
                )

            # Get all columns for YLT table
            columns = con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'ylt_losses' ORDER BY ordinal_position"
            ).fetchall()

            # Build SELECT clause excluding layer_id
            column_names = [col[0] for col in columns if col[0] != "layer_id"]
            select_clause = ", ".join(column_names)

            # Query YLT data for this layer
            df = con.execute(
                f"SELECT {select_clause} FROM ylt_losses WHERE layer_id = ?",
                [self._layer.id],
            ).pl()
            return df
        finally:
            con.close()

    def as_pandas(self, run_config: "RunConfiguration") -> "pd.DataFrame":
        """Get YLT data as a pandas DataFrame.

        This method calls the Metrics Module API if data is not cached,
        saves the DuckDB file to a temporary location, and reads the
        YLT custom fields for this specific layer.

        Args:
            run_config: Run configuration specifying analysis parameters.

        Returns:
            pandas DataFrame with YLT data for this layer.
            Columns: year, loss, rip_amt, rip_pct, rip_brok_amt, rip_brok_pct,
                    [custom_field_1], [custom_field_2], ...

        Raises:
            ImportError: If pandas is not installed.
            RuntimeError: If the API call fails, data cannot be loaded,
                         or YLT table doesn't exist.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required to use as_pandas(). "
                "Install it with: pip install pandas"
            )

        if self._layer._program is None:
            raise RuntimeError(
                "Layer must be associated with a program to export YLT data. "
                "Retrieve the layer via program.layers or client.get_program()."
            )

        # Get or create cached DuckDB file
        duckdb_path = self._get_or_create_duckdb(run_config)

        # Read YLT data for this layer from DuckDB
        import duckdb

        con = duckdb.connect(str(duckdb_path), read_only=True)
        try:
            # Check if YLT table exists
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ylt_losses'"
            ).fetchall()

            if not tables:
                raise RuntimeError(
                    "No YLT data available for this program. "
                    "The DuckDB file does not contain a ylt_losses table."
                )

            # Get all columns for YLT table
            columns = con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'ylt_losses' ORDER BY ordinal_position"
            ).fetchall()

            # Build SELECT clause excluding layer_id
            column_names = [col[0] for col in columns if col[0] != "layer_id"]
            select_clause = ", ".join(column_names)

            # Query YLT data for this layer
            df = con.execute(
                f"SELECT {select_clause} FROM ylt_losses WHERE layer_id = ?",
                [self._layer.id],
            ).df()
            return df
        finally:
            con.close()

    def __dir__(self):
        """Return list of available attributes for autocomplete."""
        attrs = set(super().__dir__())
        attrs.update(["as_polars", "as_pandas", "describe", "__call__"])
        return sorted(attrs)

    def describe(self) -> None:
        """Display a summary of this YLT accessor.

        Example:
            >>> layer.ylt.describe()
        """
        print("=" * 60)
        print("YLT (Year Loss Table)")
        print("=" * 60)
        print(f"  Layer: {self._layer.name} (id={self._layer.id})")
        print(f"  Cached configs: {len(self._cache)}")
        print()
        print("USAGE:")
        print("  layer.ylt(run_config)  -> polars.DataFrame (default)")
        print()
        print("METHODS:")
        print("  as_polars(run_config) -> polars.DataFrame")
        print("  as_pandas(run_config) -> pandas.DataFrame")
        print("=" * 60)
        print()

    def _get_or_create_duckdb(self, run_config: "RunConfiguration") -> Path:
        """Get cached DuckDB file or create it by calling the API.

        Args:
            run_config: Run configuration.

        Returns:
            Path to the DuckDB file.

        Raises:
            RuntimeError: If API call fails.
        """
        # Create cache key from program and run configuration
        cache_key = self._create_cache_key(run_config)

        # Check if we have a cached file
        if cache_key in self._cache:
            cached_path, program_hash = self._cache[cache_key]
            current_program_hash = self._hash_program_json()

            # Verify program hasn't changed
            if cached_path.exists() and program_hash == current_program_hash:
                return cached_path

        # Call API to get DuckDB file
        duckdb_path = self._call_loss_export_api(run_config)

        # Cache the path and program hash
        self._cache[cache_key] = (duckdb_path, self._hash_program_json())

        return duckdb_path

    def _create_cache_key(self, run_config: "RunConfiguration") -> str:
        """Create a cache key from run configuration and program state.

        Args:
            run_config: Run configuration.

        Returns:
            Cache key string.
        """
        # Include both run config and program hash in cache key
        # This ensures cache is invalidated when program is modified
        program_hash = self._hash_program_json()
        return f"{hash(run_config)}_{program_hash}"

    def _hash_program_json(self) -> str:
        """Generate hash of program JSON for cache validation.

        Returns:
            SHA256 hash of program JSON.
        """
        program_dict = self._layer._program.get_json()

        program_json = json.dumps(program_dict, sort_keys=True)
        return hashlib.sha256(program_json.encode()).hexdigest()

    def _call_loss_export_api(self, run_config: "RunConfiguration") -> Path:
        """Call the Metrics Module API to export losses to DuckDB.

        Args:
            run_config: Run configuration.

        Returns:
            Path to the downloaded DuckDB file.

        Raises:
            RuntimeError: If API call fails.
        """
        import requests

        program = self._layer._program
        client = program._foundation_client

        if client is None:
            raise RuntimeError(
                "Program must be associated with a client to export YLT data. "
                "Use client.get_program() to retrieve the program."
            )

        # Prepare API request
        payload = {
            "program": program.get_json(),
            "runConfiguration": run_config.to_dict(),
        }

        # Call the loss export API
        url = f"{client.base_url}:{client.PORTS['metrics']}/fis/SdkExport"
        headers = {
            "Authorization": f"Bearer {client._token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(url, json=payload, headers=headers, stream=True)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Try to get detailed error message from response body
            error_detail = ""
            try:
                error_detail = f" Response: {e.response.text}"
            except Exception:
                pass
            raise RuntimeError(f"Failed to export YLT data from API: {e}{error_detail}")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to export YLT data from API: {e}")

        # Save DuckDB file to temporary location
        temp_dir = Path(tempfile.gettempdir()) / "foundation_sdk_losses"
        temp_dir.mkdir(exist_ok=True)

        # Create filename based on program ID and run config hash
        config_hash = hashlib.sha256(
            json.dumps(run_config.to_dict(), sort_keys=True).encode()
        ).hexdigest()[:16]
        filename = f"losses_program_{program.id}_{config_hash}.duckdb"
        duckdb_path = temp_dir / filename

        # Write response to file
        with open(duckdb_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return duckdb_path
