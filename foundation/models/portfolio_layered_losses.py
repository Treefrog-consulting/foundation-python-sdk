"""Accessor for a portfolio's AEP, OEP and event loss files."""

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests

from ..enums import LossFileType
from ..exceptions import ExportError
from ..utils import require

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

    from .portfolio import Portfolio

#: The whole portfolio's slice key, and its name as the Portfolio UI labels it.
ROOT_SLICE_KEY = "dimension-0"
ROOT_SLICE = "Full Portfolio"


class PortfolioLayeredLosses:
    """A portfolio's loss files, one per kind and slice.

    ``kind`` is a :class:`~foundation.LossFileType` or its string ("aep",
    "oep", "event"). ``slice`` is a slice from ``portfolio.slices``: its name
    ("Europe / Windstorm"), its name after its dimension's ("Region - Peril /
    Europe / Windstorm") when two dimensions share the name, or its key; None
    is the whole portfolio. Each call downloads
    the file the platform stored and reads it; nothing is cached, and no
    compressed file is left behind.

    Calling the accessor returns a Polars DataFrame:

        >>> p.layered_losses(LossFileType.AEP)
        >>> p.layered_losses.as_pandas("event", slice="Europe / Windstorm")
        >>> p.layered_losses.to_csv(LossFileType.OEP, "oep.csv")

    All three need duckdb, which the pandas and polars extras include.
    """

    #: The file is streamed from S3 in chunks; this bounds each wait for more bytes.
    DOWNLOAD_TIMEOUT: float = 300.0

    def __init__(self, portfolio: "Portfolio"):
        self._portfolio = portfolio

    def __call__(
        self, kind: LossFileType | str, slice: str | None = None
    ) -> "pl.DataFrame":
        return self.as_polars(kind, slice)

    def as_polars(
        self, kind: LossFileType | str, slice: str | None = None
    ) -> "pl.DataFrame":
        """Read one loss file as a Polars DataFrame.

        Args:
            kind: The file: AEP, OEP or EVENT.
            slice: A slice name or key from ``portfolio.slices``; None for the
                whole portfolio.

        Raises:
            ValueError: If ``kind`` or ``slice`` is not one of the portfolio's.
            NotFoundError: If the portfolio or the file does not exist.
            PermissionDeniedError: If the user cannot access every deal in the
                portfolio.
            ExportError: If the portfolio's results were deleted, or the
                download failed.
            ImportError: If duckdb, polars or pyarrow is not installed.
        """
        require("polars", "as_polars")
        require("pyarrow", "as_polars")
        return self._read(kind, slice, "pl")

    def as_pandas(
        self, kind: LossFileType | str, slice: str | None = None
    ) -> "pd.DataFrame":
        """Read one loss file as a pandas DataFrame.

        Same arguments and errors as :meth:`as_polars`; needs duckdb and pandas.
        """
        require("pandas", "as_pandas")
        return self._read(kind, slice, "df")

    def to_csv(
        self,
        kind: LossFileType | str,
        path: str | os.PathLike[str],
        slice: str | None = None,
    ) -> Path:
        """Write one loss file to ``path`` as plain, uncompressed CSV.

        Cell text is copied as stored. Empty and missing values both come out
        empty, and quoting can differ from the stored file.

        Same arguments and errors as :meth:`as_polars`; needs duckdb.

        Returns:
            The path written.
        """
        duckdb = require("duckdb", "to_csv")
        destination = Path(path)
        with self._downloaded(kind, slice) as source, duckdb.connect() as con:
            con.read_csv(str(source), header=True, all_varchar=True).write_csv(
                str(destination), header=True, compression="none"
            )
        return destination

    def _read(
        self, kind: LossFileType | str, slice: str | None, format_method: str
    ) -> Any:
        duckdb = require("duckdb", "layered_losses")
        with self._downloaded(kind, slice) as source, duckdb.connect() as con:
            relation = con.read_csv(str(source), header=True, sample_size=-1)
            return getattr(relation, format_method)()

    @contextmanager
    def _downloaded(
        self, kind: LossFileType | str, slice: str | None
    ) -> Iterator[Path]:
        """Download one file to a temporary path, deleted on exit."""
        kind = LossFileType(kind)
        portfolio = self._portfolio
        if portfolio.auto_deleted_on is not None:
            raise ExportError(
                f"Portfolio {portfolio.id}'s results were deleted on "
                f"{portfolio.auto_deleted_on:%Y-%m-%d}; its loss files are gone."
            )
        link = portfolio._client._fetch(
            "portfolio",
            f"/api/Export/Url/{portfolio.id}/{kind}",
            params={"s3Key": self._slice_key(slice)},
        )
        # duckdb picks the decompression from the extension.
        suffix = ".csv.zst" if link["compression"] == "zstd" else ".csv"
        handle, name = tempfile.mkstemp(
            prefix=f"portfolio_{portfolio.id}_{kind}_", suffix=suffix
        )
        path = Path(name)
        try:
            self._download(
                link["url"], handle, f"the {kind} file of portfolio {portfolio.id}"
            )
            yield path
        finally:
            path.unlink(missing_ok=True)

    def _download(self, url: str, handle: int, what: str) -> None:
        """Stream a presigned URL into an open file.

        Errors name the file, never the URL: a presigned URL is a working
        download link until it expires.
        """
        try:
            # A plain request, not the client's session: S3 rejects a presigned
            # URL that also carries the Foundation Authorization header.
            with (
                os.fdopen(handle, "wb") as file,
                requests.get(
                    url, stream=True, timeout=self.DOWNLOAD_TIMEOUT
                ) as response,
            ):
                if not response.ok:
                    raise ExportError(
                        f"Downloading {what} failed: {response.status_code} {response.reason}."
                    )
                for chunk in response.iter_content(chunk_size=1 << 20):
                    file.write(chunk)
        except requests.RequestException as exc:
            raise ExportError(
                f"Downloading {what} failed: {type(exc).__name__}."
            ) from None

    def _slice_key(self, slice: str | None) -> str:
        """Resolve a slice name or key to the key the platform takes."""
        if slice is None or slice.casefold() in (ROOT_SLICE.casefold(), ROOT_SLICE_KEY):
            return ROOT_SLICE_KEY
        if slice.startswith("dimension-"):
            # A key goes as given: the platform checks it.
            return slice
        matches = self._slices_by_name.get(slice.casefold(), {})
        if len(matches) == 1:
            return next(iter(matches))
        if not matches:
            raise ValueError(
                f"Portfolio {self._portfolio.id} has no slice {slice!r} with loss "
                "files. portfolio.slices lists the ones it has."
            )
        found = ", ".join(f"'{dimension} / {slice}'" for dimension in matches.values())
        raise ValueError(
            f"More than one dimension of portfolio {self._portfolio.id} has a "
            f"slice named {slice!r}; pass one of {found}."
        )

    @cached_property
    def _slices_by_name(self) -> dict[str, dict[str, str | None]]:
        """Each case-folded slice name, alone and after its dimension's, to
        {key: dimension}."""
        index: dict[str, dict[str, str | None]] = {}
        for row in self._portfolio.slices:
            key, dimension = row["slice_key"], row["dimension"]
            for name in (row["slice"], f"{dimension} / {row['slice']}"):
                index.setdefault(name.casefold(), {})[key] = dimension
        return index

    def describe(self) -> None:
        """Display a summary of this accessor."""
        print("=" * 60)
        print("PORTFOLIO LAYERED LOSSES")
        print("=" * 60)
        print(f"  Portfolio: {self._portfolio.name} (id={self._portfolio.id})")
        print(f"  Kinds:     {', '.join(LossFileType)}")
        print()
        print("METHODS:")
        print("  (kind, slice=None)            -> polars.DataFrame")
        print("  as_polars(kind, slice=None)   -> polars.DataFrame")
        print("  as_pandas(kind, slice=None)   -> pandas.DataFrame")
        print("  to_csv(kind, path, slice=None) -> pathlib.Path")
        print("=" * 60)
        print()
