"""Cache of Loss module SysConfig alias values, used to validate DataFrame
columns and drive CSV column order for loss-set uploads.

Each SysConfig alias entry (e.g. ``YELTColAliasYear``) stores a comma-separated
list of acceptable column names, matching the server's behavior in
``Loss/Foundation.Loss/Helpers.cs``. The first alias in each list is treated as
the canonical output-header name.
"""

from __future__ import annotations

import warnings
from typing import Iterable, List, Literal


LossType = Literal["yelt", "elt"]

_YELT_KEYS_IN_ORDER: list[str] = [
    "YELTColAliasYear",
    "YELTColAliasEvent",
    "YELTColAliasRegion",
    "YELTColAliasSequence",
    "YELTColAliasLoss",
    "YELTColAliasPeril",
]

_ELT_KEYS_IN_ORDER: list[str] = [
    "ELTColAliasListLoss",
    "ELTColAliasListEvent",
    "ELTColAliasListStdDevI",
    "ELTColAliasListStdDevc",
    "ELTColAliasListExposure",
    "ELTColAliasListRate",
]


def _parse_aliases(raw: str) -> list[str]:
    return [p.strip() for p in raw.split(",") if p.strip()]


class SysConfigCache:
    def __init__(self, raw_entries: Iterable[dict]):
        by_key = {
            str(e["name"]): str(e.get("value") or "")
            for e in raw_entries
            if e.get("name")
        }
        self._yelt_aliases: list[list[str]] = [_parse_aliases(by_key.get(k, "")) for k in _YELT_KEYS_IN_ORDER]
        self._elt_aliases: list[list[str]] = [_parse_aliases(by_key.get(k, "")) for k in _ELT_KEYS_IN_ORDER]

        missing_yelt = [k for k, v in zip(_YELT_KEYS_IN_ORDER, self._yelt_aliases) if not v]
        missing_elt = [k for k, v in zip(_ELT_KEYS_IN_ORDER, self._elt_aliases) if not v]
        if missing_yelt:
            warnings.warn(
                f"SysConfig missing YELT alias keys: {missing_yelt}; "
                "add_yelt() will raise until these are configured.",
                stacklevel=2,
            )
        if missing_elt:
            warnings.warn(
                f"SysConfig missing ELT alias keys: {missing_elt}; "
                "add_elt() will raise until these are configured.",
                stacklevel=2,
            )

    def is_loaded_for(self, loss_type: LossType) -> bool:
        return all(bool(aliases) for aliases in self._aliases_or_empty(loss_type))

    def yelt_columns(self) -> list[str]:
        return [aliases[0] if aliases else "" for aliases in self._yelt_aliases]

    def elt_columns(self) -> list[str]:
        return [aliases[0] if aliases else "" for aliases in self._elt_aliases]

    def columns_for(self, loss_type: LossType) -> list[list[str]]:
        """Return aliases per canonical slot; the first alias is the output header."""
        return [list(aliases) for aliases in self._aliases_or_empty(loss_type)]

    def missing_keys(self, loss_type: LossType) -> list[str]:
        keys = _YELT_KEYS_IN_ORDER if loss_type == "yelt" else _ELT_KEYS_IN_ORDER
        values = self._aliases_or_empty(loss_type)
        return [k for k, v in zip(keys, values) if not v]

    def missing_columns(self, loss_type: LossType, user_columns: Iterable[str]) -> list[str]:
        expected = self._aliases_or_empty(loss_type)
        user_fold = {c.casefold() for c in user_columns}
        return [
            aliases[0]
            for aliases in expected
            if aliases and not any(a.casefold() in user_fold for a in aliases)
        ]

    def duplicate_columns(self, loss_type: LossType, user_columns: Iterable[str]) -> list[str]:
        """Return canonical-column names for which the user's DataFrame has two
        columns matching (case-insensitive) the same alias slot."""
        expected = self._aliases_or_empty(loss_type)
        out: list[str] = []
        for aliases in expected:
            if not aliases:
                continue
            matches = [
                c
                for c in user_columns
                if any(c.casefold() == a.casefold() for a in aliases)
            ]
            if len(matches) > 1:
                out.append(f"{aliases[0]} (matched: {matches})")
        return out

    def _aliases_or_empty(self, loss_type: LossType) -> list[list[str]]:
        if loss_type == "yelt":
            return self._yelt_aliases
        if loss_type == "elt":
            return self._elt_aliases
        raise ValueError(f"unknown loss type: {loss_type}")
