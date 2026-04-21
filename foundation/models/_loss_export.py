"""Shared base for DuckDB-backed loss accessors (Ylt, LayeredLosses).

Both accessors talk to the same Metrics ``/fis/SdkExport`` endpoint to get a
DuckDB file, cache it on disk keyed by program-hash + run-config, and then
issue a per-layer SELECT. Everything except the SQL query and error-label
noun is shared, so it lives here.
"""

import hashlib
import json
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .layer import Layer
    from .run_configuration import RunConfiguration


@contextmanager
def _duckdb_read(path: Path):
    """Yield a read-only DuckDB connection, always closed on exit."""
    import duckdb

    con = duckdb.connect(str(path), read_only=True)
    try:
        yield con
    finally:
        con.close()


def _require(module_name: str, pip_name: str, purpose: str):
    """Import a soft dependency or raise ImportError with a helpful hint."""
    try:
        return __import__(module_name)
    except ImportError:
        raise ImportError(
            f"{pip_name} is required to use {purpose}(). "
            f"Install it with: pip install {pip_name}"
        )


class _LossExportAccessor:
    """Base for layer-scoped accessors backed by a cached DuckDB export.

    Subclasses set:
        _noun — label used in error messages (e.g. ``"YLT data"`` / ``"losses"``)

    Subclasses implement:
        _read_frame(con, format_method) — run the layer-specific SELECT and
        return the frame. ``format_method`` is ``"pl"`` or ``"df"``.
    """

    _noun: str = "loss data"

    def __init__(self, layer: "Layer"):
        self._layer = layer
        # cache_key -> (duckdb_path, program_json_hash)
        self._cache: dict = {}

    def __call__(self, run_config: "RunConfiguration") -> Any:
        return self.as_polars(run_config)

    def as_polars(self, run_config: "RunConfiguration") -> Any:
        _require("polars", "polars", "as_polars")
        return self._execute(run_config, "pl")

    def as_pandas(self, run_config: "RunConfiguration") -> Any:
        _require("pandas", "pandas", "as_pandas")
        return self._execute(run_config, "df")

    def _execute(self, run_config: "RunConfiguration", format_method: str) -> Any:
        if self._layer._program is None:
            raise RuntimeError(
                f"Layer must be associated with a program to export {self._noun}. "
                "Retrieve the layer via program.layers or client.get_program()."
            )

        duckdb_path = self._get_or_create_duckdb(run_config)
        with _duckdb_read(duckdb_path) as con:
            return self._read_frame(con, format_method)

    def _read_frame(self, con: Any, format_method: str) -> Any:
        raise NotImplementedError

    def _get_or_create_duckdb(self, run_config: "RunConfiguration") -> Path:
        """Return a cached DuckDB path, invalidating it if the program changed."""
        cache_key = self._create_cache_key(run_config)
        if cache_key in self._cache:
            cached_path, program_hash = self._cache[cache_key]
            if cached_path.exists() and program_hash == self._hash_program_json():
                return cached_path

        duckdb_path = self._call_loss_export_api(run_config)
        self._cache[cache_key] = (duckdb_path, self._hash_program_json())
        return duckdb_path

    def _create_cache_key(self, run_config: "RunConfiguration") -> str:
        return f"{hash(run_config)}_{self._hash_program_json()}"

    def _hash_program_json(self) -> str:
        program_dict = self._layer._program.get_json()
        program_json = json.dumps(program_dict, sort_keys=True)
        return hashlib.sha256(program_json.encode()).hexdigest()

    def _call_loss_export_api(self, run_config: "RunConfiguration") -> Path:
        """POST to /fis/SdkExport and stream the DuckDB file to a temp path."""
        import requests

        program = self._layer._program
        client = program._foundation_client
        if client is None:
            raise RuntimeError(
                f"Program must be associated with a client to export {self._noun}. "
                "Use client.get_program() to retrieve the program."
            )

        url = f"{client.base_url}:{client.PORTS['metrics']}/fis/SdkExport"
        headers = {
            "Authorization": f"Bearer {client._token}",
            "Content-Type": "application/json",
        }
        payload = {
            "program": program.get_json(),
            "runConfiguration": run_config.to_dict(),
        }

        try:
            response = requests.post(url, json=payload, headers=headers, stream=True)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            detail = ""
            try:
                detail = f" Response: {e.response.text}"
            except Exception:
                pass
            raise RuntimeError(f"Failed to export {self._noun} from API: {e}{detail}")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to export {self._noun} from API: {e}")

        temp_dir = Path(tempfile.gettempdir()) / "foundation_sdk_losses"
        temp_dir.mkdir(exist_ok=True)
        config_hash = hashlib.sha256(
            json.dumps(run_config.to_dict(), sort_keys=True).encode()
        ).hexdigest()[:16]
        duckdb_path = temp_dir / f"losses_program_{program.id}_{config_hash}.duckdb"

        with open(duckdb_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return duckdb_path
