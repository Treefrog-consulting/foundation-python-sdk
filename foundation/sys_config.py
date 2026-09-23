"""Cache of Loss module SysConfig alias values, used to validate DataFrame
columns and drive CSV column order for loss-set uploads.

Each SysConfig alias entry (e.g. ``YELTColAliasYear``) stores a comma-separated
list of acceptable column names, matching the server's behavior in
``Loss/Foundation.Loss/Helpers.cs``. The first alias in each list is treated as
the canonical output-header name.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable

from .enums import LossType

_KEYS_IN_ORDER: dict[LossType, list[str]] = {
    LossType.YELT: [
        "YELTColAliasYear",
        "YELTColAliasEvent",
        "YELTColAliasRegion",
        "YELTColAliasSequence",
        "YELTColAliasLoss",
        "YELTColAliasPeril",
    ],
    LossType.ELT: [
        "ELTColAliasListLoss",
        "ELTColAliasListEvent",
        "ELTColAliasListStdDevI",
        "ELTColAliasListStdDevc",
        "ELTColAliasListExposure",
        "ELTColAliasListRate",
    ],
}


def _parse_aliases(raw: str) -> list[str]:
    return [p.strip() for p in raw.split(",") if p.strip()]


class SysConfigCache:
    def __init__(self, raw_entries: Iterable[dict]):
        by_key = {
            str(e["name"]): str(e.get("value") or "")
            for e in raw_entries
            if e.get("name")
        }
        self._aliases: dict[LossType, list[list[str]]] = {
            loss_type: [_parse_aliases(by_key.get(k, "")) for k in keys]
            for loss_type, keys in _KEYS_IN_ORDER.items()
        }

        for loss_type in _KEYS_IN_ORDER:
            if missing := self.missing_keys(loss_type):
                warnings.warn(
                    f"SysConfig missing {loss_type.upper()} alias keys: {missing}; "
                    f"add_{loss_type}() will raise until these are configured.",
                    stacklevel=2,
                )

    def is_loaded_for(self, loss_type: LossType | str) -> bool:
        return all(bool(aliases) for aliases in self._aliases_or_empty(loss_type))

    def yelt_columns(self) -> list[str]:
        return [
            aliases[0] if aliases else "" for aliases in self._aliases[LossType.YELT]
        ]

    def elt_columns(self) -> list[str]:
        return [
            aliases[0] if aliases else "" for aliases in self._aliases[LossType.ELT]
        ]

    def columns_for(self, loss_type: LossType | str) -> list[list[str]]:
        """Return aliases per canonical slot; the first alias is the output header."""
        return [list(aliases) for aliases in self._aliases_or_empty(loss_type)]

    def missing_keys(self, loss_type: LossType | str) -> list[str]:
        keys = _KEYS_IN_ORDER[LossType(loss_type)]
        values = self._aliases_or_empty(loss_type)
        return [k for k, v in zip(keys, values, strict=True) if not v]

    def missing_columns(
        self, loss_type: LossType | str, user_columns: Iterable[str]
    ) -> list[str]:
        expected = self._aliases_or_empty(loss_type)
        user_fold = {c.casefold() for c in user_columns}
        return [
            aliases[0]
            for aliases in expected
            if aliases and not any(a.casefold() in user_fold for a in aliases)
        ]

    def duplicate_columns(
        self, loss_type: LossType | str, user_columns: Iterable[str]
    ) -> list[str]:
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

    def _aliases_or_empty(self, loss_type: LossType | str) -> list[list[str]]:
        return self._aliases[LossType(loss_type)]
