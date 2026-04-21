"""Build UTF-8 CSV bytes from a pandas or polars DataFrame after validating
and reordering against a canonical column list."""

from __future__ import annotations

import io
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Union

import pandas as pd
import polars as pl

from .exceptions import LossSetValidationError


ColumnSpec = Union[str, Sequence[str]]


def _normalize_spec(spec: ColumnSpec) -> List[str]:
    """Accept either a single canonical name or a list of aliases for one slot."""
    if isinstance(spec, str):
        return [spec]
    aliases = [a for a in spec if a]
    if not aliases:
        raise LossSetValidationError("canonical column spec must not be empty")
    return list(aliases)


def build_csv(
    data: Any,
    canonical_columns: Iterable[ColumnSpec],
    column_map: Optional[Mapping[str, str]] = None,
) -> bytes:
    """Validate required columns, reorder / rename, and emit CSV bytes.

    - Accepts pandas or polars DataFrame.
    - Each entry in ``canonical_columns`` is either a canonical name (str) or a
      list of acceptable aliases for one slot. The first alias is used as the
      output header.
    - Column match is case-insensitive. Emitted header uses the canonical casing.
    - Duplicate columns under case-fold raise — there's no unambiguous source.
    - Optional column_map renames source columns before matching.
    """
    slots: List[List[str]] = [_normalize_spec(spec) for spec in canonical_columns]
    if isinstance(data, pd.DataFrame):
        frame = data
    elif isinstance(data, pl.DataFrame):
        frame = data.to_pandas()
    else:
        raise LossSetValidationError(
            f"data must be a pandas or polars DataFrame, got {type(data).__name__}"
        )

    if column_map:
        unknown = [src for src in column_map if src not in frame.columns]
        if unknown:
            raise LossSetValidationError(
                f"column_map references columns not in the DataFrame: {unknown}"
            )
        frame = frame.rename(columns=dict(column_map))

    source_by_canonical: dict[str, str] = {}
    duplicates: list[str] = []
    missing: list[list[str]] = []
    for aliases in slots:
        canonical = aliases[0]
        alias_fold = {a.casefold() for a in aliases}
        matches = [c for c in frame.columns if c.casefold() in alias_fold]
        if len(matches) == 0:
            missing.append(aliases)
        elif len(matches) > 1:
            duplicates.append(f"{canonical} (matched: {matches})")
        else:
            source_by_canonical[canonical] = matches[0]

    if missing:
        formatted = [m[0] if len(m) == 1 else f"{m[0]} (aliases: {m})" for m in missing]
        raise LossSetValidationError(f"missing required columns: {formatted}")
    if duplicates:
        raise LossSetValidationError(
            f"duplicate columns (ambiguous case-insensitive match): {duplicates}"
        )

    canonical_names = [aliases[0] for aliases in slots]
    subset = frame[[source_by_canonical[c] for c in canonical_names]].copy()
    subset.columns = canonical_names

    buf = io.StringIO()
    subset.to_csv(buf, index=False, lineterminator="\n")
    return buf.getvalue().encode("utf-8")
