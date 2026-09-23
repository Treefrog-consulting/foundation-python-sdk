"""Shared base for DuckDB-backed export accessors (Ylt, LayeredLosses, MetricResults).

The accessors talk to the same Metrics ``/fis/SdkExport`` endpoint to get a
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

from ..exceptions import ExportError, FoundationHTTPError
from ..utils import require

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

    from .layer import Layer
    from .program import Program
    from .run_configuration import RunConfiguration


@contextmanager
def _duckdb_read(path: Path):
    """Yield a read-only DuckDB connection, always closed on exit."""
    duckdb = require("duckdb", "the loss export")

    con = duckdb.connect(str(path), read_only=True)
    try:
        yield con
    finally:
        con.close()


class _LossExportAccessor:
    """Base for layer-scoped accessors backed by a cached DuckDB export.

    Subclasses set:
        _noun — label used in error messages (e.g. ``"YLT data"`` / ``"losses"``)

    Subclasses implement:
        _read_frame(con, format_method) — run the layer-specific SELECT and
        return the frame. ``format_method`` is ``"pl"`` or ``"df"``.
    """

    #: The export is computed server-side before a byte is sent, so the
    #: client's 30 s default would abandon a large deal mid-calculation.
    EXPORT_TIMEOUT: float = 900.0

    _noun: str = "loss data"

    def __init__(self, layer: "Layer"):
        self._layer = layer

    def _require_program(self) -> "Program":
        """The program the export belongs to, or a message explaining how to get one."""
        program = self._layer._program
        if program is None:
            raise RuntimeError(
                f"Layer must be associated with a program to export {self._noun}. "
                "Retrieve the layer via program.layers or client.get_program()."
            )
        return program

    @property
    def _cache(self) -> dict:
        """cache_key -> (duckdb_path, program_json_hash).

        Held on the program, not the accessor: one export file carries the losses,
        the YLT stream and the metric results of every layer, so a file fetched for
        one accessor or one layer serves all the others for the same run config.
        """
        return self._require_program()._export_cache

    def _cached_export_count(self) -> int:
        """How many exports this deal has cached, for describe() to report.

        Returns 0 rather than raising for a layer with no program: describe() is a
        diagnostic and has to work on whatever the caller is holding.
        """
        program = self._layer._program
        return 0 if program is None else len(program._export_cache)

    @property
    def _export_paths(self) -> dict[int, Path]:
        """run-config identity -> the export file currently on disk for it.

        Each run config keeps one file: re-exporting it (after the deal is edited)
        deletes the stale one, but a different run config's file is left alone, so
        alternating configs do not thrash. Shared per program for the same reason
        as :attr:`_cache`.
        """
        return self._require_program()._export_paths

    def __call__(self, run_config: "RunConfiguration") -> Any:
        return self.as_polars(run_config)

    def as_polars(self, run_config: "RunConfiguration") -> "pl.DataFrame":
        """Get this accessor's data as a Polars DataFrame.

        See the class docstring for the columns.

        Raises:
            ImportError: If polars is not installed.
            RuntimeError: If the API call fails or data cannot be loaded.
        """
        require("polars", "as_polars")
        require("pyarrow", "as_polars")
        return self._execute(run_config, "pl")

    def as_pandas(self, run_config: "RunConfiguration") -> "pd.DataFrame":
        """Get this accessor's data as a pandas DataFrame.

        See the class docstring for the columns.

        Raises:
            ImportError: If pandas is not installed.
            RuntimeError: If the API call fails or data cannot be loaded.
        """
        require("pandas", "as_pandas")
        return self._execute(run_config, "df")

    @contextmanager
    def _open(self, run_config: "RunConfiguration"):
        """Yield a read-only connection to this run config's export, fetching it first."""
        self._require_program()
        with _duckdb_read(self._get_or_create_duckdb(run_config)) as con:
            yield con

    def _execute(self, run_config: "RunConfiguration", format_method: str) -> Any:
        with self._open(run_config) as con:
            return self._read_frame(con, format_method)

    def _require_table(self, con: Any, table_name: str) -> None:
        """Fail with a readable message when the export predates this table."""
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchall()
        if not tables:
            raise RuntimeError(
                f"No {self._noun} available for this program. The DuckDB file does "
                f"not contain a {table_name} table."
            )

    def _read_frame(self, con: Any, format_method: str) -> Any:
        raise NotImplementedError

    def _get_or_create_duckdb(self, run_config: "RunConfiguration") -> Path:
        """Return a cached DuckDB path, invalidating it if the program changed."""
        # Hashing the deal document walks every layer and custom table, so it
        # is done once here and reused for the cache key, the entry and the
        # file name.
        program_hash = self._hash_program_json()
        cache_key = self._create_cache_key(run_config, program_hash)
        if cache_key in self._cache:
            cached_path, cached_hash = self._cache[cache_key]
            if cached_path.exists() and cached_hash == program_hash:
                return cached_path

        duckdb_path = self._call_loss_export_api(run_config, program_hash)
        self._cache[cache_key] = (duckdb_path, program_hash)
        self._discard_superseded_export(run_config, duckdb_path)
        return duckdb_path

    def _discard_superseded_export(
        self, run_config: "RunConfiguration", current: Path
    ) -> None:
        """Delete the file this run config exported before ``current``.

        A full export runs to hundreds of megabytes, and every edit to the
        deal produces a new one, so keeping them would fill the temp
        directory. Pruning is per run config, so exporting a second config
        does not delete the first's file. A cache entry pointing at the
        removed file simply misses and re-exports.
        """
        key = hash(run_config)
        previous = self._export_paths.get(key)
        if previous is not None and previous != current:
            try:
                previous.unlink(missing_ok=True)
            except OSError:
                # A file still open elsewhere is not worth failing an export
                # over; it will be replaced on the next run.
                pass
        self._export_paths[key] = current

    def _create_cache_key(
        self, run_config: "RunConfiguration", program_hash: str
    ) -> str:
        return f"{hash(run_config)}_{program_hash}"

    def _hash_program_json(self) -> str:
        program_dict = self._layer._program.get_json()
        program_json = json.dumps(program_dict, sort_keys=True)
        return hashlib.sha256(program_json.encode()).hexdigest()

    def _call_loss_export_api(
        self, run_config: "RunConfiguration", program_hash: str
    ) -> Path:
        """POST to /fis/SdkExport and stream the DuckDB file to a temp path."""
        import requests

        program = self._layer._program
        client = program._foundation_client
        if client is None:
            raise RuntimeError(
                f"Program must be associated with a client to export {self._noun}. "
                "Use client.get_program() to retrieve the program."
            )

        payload = {
            "program": program.get_json(),
            "runConfiguration": run_config.to_dict(),
        }

        try:
            # Through the client so this shares the session and the typed error
            # mapping. The export is computed server-side before a single byte
            # is sent, so it gets its own read timeout rather than the client
            # default meant for ordinary calls.
            response = client.request(
                "metrics",
                "/fis/SdkExport",
                method="POST",
                json=payload,
                stream=True,
                timeout=self.EXPORT_TIMEOUT,
            )
        except FoundationHTTPError:
            # Already typed and already carrying the server's body. Re-raised
            # ahead of the clause below, which would otherwise swallow it:
            # FoundationHTTPError is a requests.HTTPError is a RequestException.
            raise
        except requests.exceptions.RequestException as e:
            raise ExportError(f"Failed to export {self._noun} from API: {e}") from e

        temp_dir = Path(tempfile.gettempdir()) / "foundation_sdk_losses"
        temp_dir.mkdir(exist_ok=True)
        config_hash = hashlib.sha256(
            json.dumps(run_config.to_dict(), sort_keys=True).encode()
        ).hexdigest()[:16]
        # The program hash belongs in the filename, not just the in-memory
        # cache key: a deal keeps its ID across versions and across edits, so
        # without it two exports of the same deal overwrite each other's file
        # while both accessors still consider their own entry valid.
        duckdb_path = (
            temp_dir
            / f"losses_program_{program.id}_{program_hash[:16]}_{config_hash}.duckdb"
        )

        with open(duckdb_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return duckdb_path
