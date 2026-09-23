"""Sends formulas written in the Portfolio UI formula builder's format to the
platform, which converts them to the token form DVE evaluates."""

from collections.abc import Iterable
from functools import cached_property
from typing import TYPE_CHECKING, Any

from .rows import Rows

if TYPE_CHECKING:
    from .foundation_client import FoundationClient

# DVE finds programVersionId in a formula itself; it has no source field.
_DETECTED_BY_DVE = frozenset({"programVersionId"})


class FormulaConverter:
    """Converts the Portfolio UI's formulas for one client's reference data.

    A formula names fields as the builder lists them ("UW Year", "Layer
    Status") and compares lookup fields with quoted labels (``Client == "Acme
    Re"``). The platform converts them: ``POST api/Templates/ConvertFormulas``
    runs the formula builder's own pipeline, so the SDK and the UI store the
    same thing. This class supplies the names and labels it converts against,
    and keeps what came back.
    """

    def __init__(self, client: "FoundationClient"):
        self._client = client
        self._cache = client._cache()
        # formula -> its stored form, or a one-tuple holding why it has none.
        self._converted: dict[str, str | tuple[str]] = {}

    @cached_property
    def _tokens_by_id(self) -> dict[Any, dict[str, Any]]:
        # Every token, active or not, as DVE reads them.
        return self._cache._by_id("tokens")

    @cached_property
    def fields(self) -> list[dict[str, Any]]:
        """The builder's formula fields, longest name first."""
        rows = self._client._fetch("portfolio", "/api/CriteriaFields")
        fields = [
            row
            for row in rows
            if row.get("activeFlag")
            and row.get("usedInFormulaFlag")
            and row.get("dealTokenId") in self._tokens_by_id
        ]
        return sorted(fields, key=lambda row: len(row["name"]), reverse=True)

    def token_name(self, field: dict[str, Any]) -> str:
        return self._tokens_by_id[field["dealTokenId"]]["name"]

    @cached_property
    def _token_names(self) -> frozenset[str]:
        return frozenset(token["name"] for token in self._tokens_by_id.values())

    @cached_property
    def _usable_token_names(self) -> frozenset[str]:
        tokens = self._tokens_by_id
        return (
            frozenset(
                token["name"]
                for token in tokens.values()
                if "customTable" not in token["name"]
                and (token.get("parentId") is None or token["parentId"] in tokens)
                and token.get("sourceFieldName")
            )
            | _DETECTED_BY_DVE
        )

    @cached_property
    def criteria_fields(self) -> Rows:
        """Columns: name, token_name, lookup (the list a quoted label is looked
        up in, or None for a field that takes plain values)."""
        return Rows(
            {
                "name": field["name"],
                "token_name": self.token_name(field),
                "lookup": self._lookup_name(field),
            }
            for field in sorted(self.fields, key=lambda row: row["name"].casefold())
        )

    def field(self, name: str, where: str = "") -> dict[str, Any] | None:
        """A formula field by its builder name or, when no builder name
        matches, its token name.

        Raises:
            ValueError: If the token name stands for fields with different
                lookups.
        """
        by_name = next((f for f in self.fields if f["name"] == name), None)
        if by_name is not None:
            return by_name
        on_token = [f for f in self.fields if self.token_name(f) == name]
        self._single_lookup(on_token, name, where)
        return on_token[0] if on_token else None

    def _single_lookup(
        self, fields: list[dict[str, Any]], token: str, where: str
    ) -> None:
        """Refuse a token name whose builder fields look labels up differently."""
        lookups = {
            (f.get("lookupAccessibleObjectId"), self._category_id(f)) for f in fields
        }
        if len(lookups) > 1:
            names = " or ".join(
                repr(name) for name in sorted(f["name"] for f in fields)
            )
            raise ValueError(
                f"{where}: {token} is {names}; write the field by that name."
            )

    def field_name(self, token_id: int | None) -> str:
        """The builder name of a token's field, the shortest when several
        fields share the token; the token name when none does."""
        names = [f["name"] for f in self.fields if f["dealTokenId"] == token_id]
        if names:
            return min(names, key=len)
        token = self._tokens_by_id.get(token_id)
        return token["name"] if token else str(token_id)

    def prime(self, formulas: "Iterable[str]") -> None:
        """Convert every formula not converted yet, in one request."""
        wanted = sorted({f for f in formulas if f and f not in self._converted})
        if wanted:
            self._convert(wanted, {})

    def convert(self, formula: str, where: str) -> str:
        """The stored form of one formula, converted by the platform.

        The Portfolio module runs the formula builder's own conversion, so a
        formula written here is converted by the same code as one written in
        the UI: field names become token names, quoted labels become their IDs.

        Args:
            formula: The formula as the builder shows it.
            where: Names the formula in error messages, e.g. "group 'Europe'".

        Raises:
            ValueError: If the platform cannot convert the formula.
        """
        if formula not in self._converted:
            self._convert([formula], {formula: where})
        converted = self._converted[formula]
        if isinstance(converted, str):
            return converted
        raise ValueError(f"{where}: {converted[0]}")

    def _convert(self, formulas: list[str], wheres: dict[str, str]) -> None:
        """Convert formulas and keep each result, the text or its error."""
        response = self._client._fetch(
            "portfolio",
            "/api/Templates/ConvertFormulas",
            method="POST",
            json={
                "formulas": formulas,
                "identifiers": self._identifiers,
                "lookups": self._lookups,
            },
        )
        for result in response.get("results") or []:
            formula = result.get("formula")
            converted = result.get("converted")
            if converted is None:
                self._converted[formula] = (
                    result.get("error") or "the platform could not convert it",
                )
                continue
            self._converted[formula] = (
                self._unusable(result.get("names") or []) or converted
            )
        missing = [f for f in formulas if f not in self._converted]
        if missing:
            where = wheres.get(missing[0], "")
            raise ValueError(
                f"{where}: the platform returned no conversion for {missing[0]!r}."
            )

    def _unusable(self, names: list[str]) -> tuple[str] | None:
        """Why the platform could not evaluate these names, if it could not.

        The names come from the platform, which parsed the formula; what they
        may be is this client's reference data, which the SDK holds.
        """
        used = set(names)
        unknown = sorted(used - self._token_names - _DETECTED_BY_DVE)
        if unknown:
            return (
                f"unknown name(s): {', '.join(unknown)}. "
                "client.criteria_fields lists the names a formula can use.",
            )
        unusable = sorted((used & self._token_names) - self._usable_token_names)
        if unusable:
            return (
                f"the platform cannot evaluate: {', '.join(unusable)}. "
                "client.criteria_fields lists the names a formula can use.",
            )
        return None

    @cached_property
    def _identifiers(self) -> list[dict[str, str]]:
        """Every builder name and the token name it normalizes to."""
        return [
            {"name": field["name"], "tokenName": self.token_name(field)}
            for field in self.fields
        ]

    @cached_property
    def _lookups(self) -> list[dict[str, Any]]:
        """Every label a quoted value can resolve to, as the UI resolves them:
        by name across every formula field's lookup, first definition winning."""
        seen: dict[str, Any] = {}
        for field in self.fields:
            for label, ids in (self._labels(field) or {}).items():
                seen.setdefault(label, ids[0])
        return [{"name": label, "id": value} for label, value in seen.items()]

    def _labels(self, field: dict[str, Any]) -> dict[str, list[Any]] | None:
        """label -> IDs for a lookup field; None when the field takes plain
        values. A label can name several rows."""
        object_id = field.get("lookupAccessibleObjectId")
        if object_id is not None:
            if self._cache.lookup_values(object_id) is None:
                self._load_lookups()
            pairs = (self._cache.lookup_values(object_id) or {}).items()
        else:
            category_id = self._category_id(field)
            if category_id is None:
                return None
            pairs = (
                (detail["id"], detail["name"])
                for detail in self._cache._by_id("category_details").values()
                if detail.get("categoryId") == category_id and detail.get("name")
            )
        labels: dict[str, list[Any]] = {}
        for value, label in pairs:
            labels.setdefault(label, []).append(value)
        return labels

    def _category_id(self, field: dict[str, Any]) -> Any:
        category = self._category(field)
        return None if category is None else category["id"]

    def _category(self, field: dict[str, Any]) -> dict[str, Any] | None:
        """The category behind a category-backed field: the one whose token is
        the field token's parent."""
        parent_id = self._tokens_by_id[field["dealTokenId"]].get("parentId")
        if parent_id is None:
            return None
        return next(
            (
                category
                for category in self._cache._by_id("categories").values()
                if category.get("tokenId") == parent_id
            ),
            None,
        )

    @property
    def _accessible_objects(self) -> dict[Any, dict[str, Any]]:
        # Every definition, as the Common reference data carries them.
        return self._cache._by_id("accessible_objects")

    def _load_lookups(self) -> None:
        """Load every formula field's lookup labels not loaded yet, in one request."""
        wanted = sorted(
            {
                field["lookupAccessibleObjectId"]
                for field in self.fields
                if field.get("lookupAccessibleObjectId") is not None
                and self._cache.lookup_values(field["lookupAccessibleObjectId"]) is None
            }
        )
        values = self._client._fetch(
            "common",
            "/api/accessibleObjects/BulkSelection",
            method="POST",
            json={"accessibleObjectIds": wanted},
        )
        definitions = [
            self._accessible_objects[oid]
            for oid in wanted
            if oid in self._accessible_objects
        ]
        self._cache.add_lookup_objects({"definitions": definitions, "values": values})

    def _lookup_name(self, field: dict[str, Any]) -> str | None:
        if field.get("lookupAccessibleObjectId") is not None:
            definition = self._accessible_objects.get(field["lookupAccessibleObjectId"])
            return (definition or {}).get(
                "name"
            ) or f"accessible object {field['lookupAccessibleObjectId']}"
        category = self._category(field)
        return None if category is None else f"category {category['name']}"
