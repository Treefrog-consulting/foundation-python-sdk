"""Custom-field and custom-table machinery shared by Program and Layer.

Program and Layer expose the same custom-data surface: the ``custom_*_info``
and ``custom_*_names`` properties, the get/set accessors, dot access and dot
assignment, and the round-trip that folds edits back into the deal document.
``CustomDataMixin`` owns all of it, so the two call sites cannot drift apart.

Everything the two classes do differently is a class attribute declared at the
top of the mixin plus two hooks (``_read_custom_table_entry`` for the shape of
a ``customTables`` entry, ``_set_extra_attribute`` for a subclass's own dynamic
names); nothing branches on type.

``raise_if_private`` stays a free function: FoundationClient, ProgramCategories
and ProgramExternalRefs need it without the rest of the mixin.
"""

import copy
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Union

from ..ref_data import ReferenceDataCache
from ..utils import (
    camel_to_snake,
    parse_datetime_string,
    snake_to_camel,
    to_wire_datetime,
)
from .custom_table import CustomTable

if TYPE_CHECKING:
    import pandas as pd


def raise_if_private(owner: Any, name: str) -> None:
    """Refuse a ``_``-prefixed lookup from a dynamic ``__getattr__``.

    copy, deepcopy and pickle probe for hooks such as ``__deepcopy__`` and
    ``__getstate__`` before ``__init__`` has run. A ``__getattr__`` that answers
    those from its own instance state re-enters itself for the very attribute it
    is reading, and recurses until RecursionError.
    """
    if name.startswith("_"):
        raise AttributeError(
            f"'{type(owner).__name__}' object has no attribute '{name}'"
        )


def raise_read_only(owner: Any, name: str, value: Any, what: str) -> None:
    """Take a ``_``-prefixed write; refuse every public one.

    The write-side companion to :func:`raise_if_private`, for the same dynamic
    accessors. Assignment on one of those used to create a plain instance
    attribute that shadowed ``__getattr__``, so the value read back while the
    deal document -- and every metric computed from it -- kept the original.
    """
    if name.startswith("_"):
        object.__setattr__(owner, name, value)
        return
    raise AttributeError(
        f"'{type(owner).__name__}' is read-only: {what} cannot be set through the SDK."
    )


class _DocField[T](property):
    """A deal-document field whose accessors carry no logic of their own.

    Reads ``_data[camel]`` unless an edit has already been recorded under
    ``snake``, and a write records one. Program and Layer each declare a dozen
    of these; a field that parses, coerces or validates keeps its hand-written
    property instead.
    """

    def __init__(self, snake: str, camel: str, doc: str) -> None:
        def read(owner: Any) -> Any:
            if snake in owner._modifications:
                return owner._modifications[snake]
            return owner._data.get(camel)

        def write(owner: Any, value: Any) -> None:
            owner._modifications[snake] = value

        super().__init__(read, write, None, doc)

    def __get__(self, obj: Any, objtype: type | None = None) -> T:
        # Plain delegation to property. Declared only so a type checker reports
        # each field's own type; a bare property subclass would give them Any.
        return super().__get__(obj, objtype)


class CustomDataMixin:
    """Custom fields and custom tables for one level of the deal document.

    Program and Layer hold the same machinery over different levels of the
    same document. It lives here so the two cannot drift apart. What remains
    per-class is the part that genuinely differs: their own document fields,
    get_json, and the collections each exposes.

    A subclass sets the six class attributes below and provides ``_data``,
    ``_modifications``, ``_ref_cache`` and ``_require_writable``. Its
    ``__init__`` calls :meth:`_init_custom_data`, builds whatever state of its
    own it needs, then calls :meth:`_init_custom_metadata` with the row buckets
    the first call returned.
    """

    #: Reference-cache ``level`` of the custom fields this class owns.
    _custom_field_level: ClassVar[int]
    #: ``dataLevelId`` values of the custom tables this class owns. Table
    #: definitions carry ``dataLevelId``, not the ``level`` key used by field
    #: definitions; matching on ``level`` finds nothing at all.
    _custom_table_data_level_ids: ClassVar[tuple[int, ...]]
    #: Document key holding this class's custom-field array.
    _custom_fields_key: ClassVar[str]
    #: Owner word: the label in the unknown-name error, and the stem of the
    #: ``<label>Id`` / ``<label>`` keys written into document rows.
    _owner_label: ClassVar[str]
    #: Whether to materialize a CustomTable for a declared table the payload
    #: carried no rows for. Program does; Layer builds only the ones with rows.
    #: A legacy divergence preserved deliberately, not a choice a new owner
    #: should have to make -- the two classes simply grew apart.
    _materialize_empty_tables: ClassVar[bool]
    #: Document keys :meth:`get_json` rebuilds from this object's own state, so
    #: a raw write to one is either overwritten or, for ``layers``, left
    #: inconsistent with the models get_json walks -- ``program.layers`` and
    #: ``get_json()["layers"]`` would disagree on length and raise IndexError.
    _MANAGED_DOCUMENT_KEYS: ClassVar[frozenset[str]] = frozenset(
        {"layers", "customTables", "programCustomFields", "layerCustomFields"}
    )
    #: Whether :meth:`get_custom_field` parses timestamp strings into
    #: datetimes. Layer does; Program returns the stored value unchanged.
    #: Also a legacy divergence: Program was never given the parse. Changing
    #: it would alter what get_custom_field returns, so it waits for a major.
    _parse_custom_field_datetimes: ClassVar[bool]

    def _custom_data_names(self) -> set[str]:
        """The dot-accessible names that exist only for this deal."""
        return set(self._custom_field_values) | set(self._custom_tables)

    # --- describe -----------------------------------------------------

    def _describe_custom_fields(self) -> None:
        if not self._custom_field_values:
            print("\nCUSTOM FIELDS: (none)")
            return
        print("\nCUSTOM FIELDS:")
        print(f"  {'Field Name':<35} {'Value':<35}")
        print(f"  {'-' * 35} {'-' * 35}")
        for key, value in sorted(self._custom_field_values.items()):
            value_str = str(value)[:33] if value is not None else "None"
            print(f"  {key:<35} {value_str:<35}")
        print(
            f"\n  Available Custom Field Names: "
            f"{', '.join(self.custom_fields_names) or '(none)'}"
        )

    def _describe_custom_tables(self) -> None:
        if not self._custom_tables:
            print("\nCUSTOM TABLES: (none)")
            return
        print("\nCUSTOM TABLES:")
        print(f"  {'Table Name':<35} {'Rows':<10}")
        print(f"  {'-' * 35} {'-' * 10}")
        for snake_name in sorted(self._custom_tables.keys()):
            table = self._custom_tables[snake_name]
            print(f"  {table.name:<35} {table.row_count:<10}")
        print(
            f"\n  Available Custom Table Names: "
            f"{', '.join(self.custom_tables_names) or '(none)'}"
        )

    # --- construction -----------------------------------------------------

    def _init_custom_data(
        self, data: dict[str, Any], ref_cache: ReferenceDataCache
    ) -> dict[str, list[dict[str, Any]]]:
        """Read the custom-field values and bucket the raw custom-table rows.

        Returns the snake_case table name -> rows mapping that
        :meth:`_init_custom_metadata` needs. The two halves are separate calls
        because each class builds state of its own in between.
        """
        self._custom_field_values = self._resolve_custom_field_values(data, ref_cache)

        raw_table_rows: dict[str, list[dict[str, Any]]] = {}
        self._custom_table_original_names: dict[str, str] = {}
        self._custom_table_original_rows: dict[str, list[dict[str, Any]]] = {}
        for entry in data.get("customTables", []):
            self._read_custom_table_entry(entry, ref_cache, raw_table_rows)
        self._custom_tables: dict[str, CustomTable] = {}
        self._custom_table_dirty: set[str] = set()
        return raw_table_rows

    def _resolve_custom_field_values(
        self, data: dict[str, Any], ref_cache: ReferenceDataCache
    ) -> dict[str, Any]:
        """Resolve this level's custom-field array into snake_case name -> value."""
        snake_by_id = ref_cache.custom_field_snake_map(self._custom_field_level)

        values: dict[str, Any] = {}
        for cf in data.get(self._custom_fields_key, []):
            # A row naming a definition this level does not carry belongs to
            # another level and has no attribute here.
            field_name = snake_by_id.get(cf.get("customFieldId"))
            if field_name is not None:
                values[field_name] = cf.get("value")

        return values

    def _read_custom_table_entry(
        self,
        entry: dict[str, Any],
        ref_cache: ReferenceDataCache,
        raw_table_rows: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Bucket one ``customTables`` entry under its snake_case table name.

        One entry is one row. Layer overrides this because a layer entry may
        instead carry a ``rows`` list.
        """
        table_id = entry.get("customTableDefinitionId")
        table_def = ref_cache.get_custom_table(table_id)
        if not table_def:
            return
        table_name = camel_to_snake(table_def.name or "")
        raw_table_rows.setdefault(table_name, []).append(
            ref_cache.build_custom_table_row(entry, table_id)
        )
        self._custom_table_original_names[table_name] = table_def.name
        # Keep the original row: it carries id, sortOrder and the rest of the
        # metadata a later save has to hand back.
        self._custom_table_original_rows.setdefault(table_name, []).append(entry)

    def _init_custom_metadata(
        self,
        ref_cache: ReferenceDataCache,
        raw_table_rows: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Build the info lists for every custom field and table at this level."""
        level = self._custom_field_level
        # The same map the values were keyed by, so a collision resolves one
        # way whether or not this object carries a value for the field.
        snake_by_id = ref_cache.custom_field_snake_map(level)

        self._custom_fields_info: list[dict[str, Any]] = [
            {
                "id": field_id,
                "name": field_def.get("name") or "",
                "snake_case_name": snake_by_id[field_id],
                "value_type": field_def.get("valueType"),
            }
            for field_id, field_def in ref_cache.custom_field_definitions(level).items()
        ]
        self._custom_fields_info.sort(key=lambda x: x["name"])
        # __setattr__ tests every public write against this, so keep it a set
        # rather than rescanning a list that grows with tenant configuration.
        self._custom_field_snake_names = frozenset(
            info["snake_case_name"] for info in self._custom_fields_info
        )

        self._custom_tables_info: list[dict[str, Any]] = []
        for table_id, table_def in ref_cache._custom_tables.items():
            if table_def.get("dataLevelId") not in self._custom_table_data_level_ids:
                continue
            columns = ref_cache.get_custom_table_columns(table_id)
            column_names = [col["name"] for col in columns]
            snake_name = camel_to_snake(table_def["name"])

            if self._materialize_empty_tables or snake_name in raw_table_rows:
                self._custom_tables[snake_name] = CustomTable(
                    owner=self,
                    ref_cache=ref_cache,
                    table_id=table_id,
                    snake_name=snake_name,
                    original_name=table_def["name"],
                    column_names=column_names,
                    initial_rows=raw_table_rows.get(snake_name, []),
                )
                self._custom_table_original_names.setdefault(
                    snake_name, table_def["name"]
                )

            # The info list carries every declared table, rows or not.
            self._custom_tables_info.append(
                {
                    "id": table_id,
                    "name": table_def["name"],
                    "snake_case_name": snake_name,
                    "column_count": len(column_names),
                    "columns": column_names,
                }
            )
        self._custom_tables_info.sort(key=lambda x: x["name"])

    # --- discovery --------------------------------------------------------

    @property
    def custom_fields_info(self) -> list[dict[str, Any]]:
        """
        List of all available custom fields at this level (read-only).

        Returns metadata about every custom field defined for this object:
        - id: Custom field ID
        - name: Display name (as in Foundation)
        - snake_case_name: Python-friendly attribute name
        - value_type: Data type of the field

        Example:
            >>> for field in owner.custom_fields_info:   # a Program or a Layer
            ...     print(f"{field['name']} -> {field['snake_case_name']}")
        """
        return self._custom_fields_info

    @property
    def custom_fields_names(self) -> list[str]:
        """
        List of all available custom field names at this level (read-only).

        Returns a list of strings, where each string is the snake_case_name of a custom field.
        Names are sorted alphabetically.

        Example:
            >>> owner.custom_fields_names                # a Program or a Layer
            ['portfolio_tag', 'underwriter_notes']
        """
        return sorted([field["snake_case_name"] for field in self._custom_fields_info])

    @property
    def custom_tables_info(self) -> list[dict[str, Any]]:
        """
        List of all available custom tables at this level (read-only).

        Returns metadata about every custom table defined for this object:
        - id: Custom table ID
        - name: Display name (as in Foundation)
        - snake_case_name: Python-friendly attribute name
        - column_count: Number of columns
        - columns: List of column names

        Example:
            >>> for table in program.custom_tables_info:
            ...     print(f"{table['name']}: {table['columns']}")
        """
        return self._custom_tables_info

    @property
    def custom_tables_names(self) -> list[str]:
        """
        List of all available custom table names at this level (read-only).

        Returns a list of strings, where each string is the snake_case_name of a custom table.
        Names are sorted alphabetically.

        Example:
            >>> owner.custom_tables_names                # a Program or a Layer
            ['buffer_table_1', 'claims_history']
        """
        return sorted([table["snake_case_name"] for table in self._custom_tables_info])

    # --- accessors --------------------------------------------------------

    def get_document_field(self, name: str) -> Any:
        """
        Read a raw deal-document field the SDK exposes no property for.

        The escape hatch for the parts of the document the SDK has not modelled
        -- ``layer_participants``, ``layer_reinst``, ``program_loss_groups`` and
        the rest. Values come back in the server's own shape (lists of dicts,
        camelCase keys, timestamps as strings), which is not a stable contract:
        prefer a property where one exists.

        Args:
            name: Field name in snake_case, e.g. "layer_participants"

        Returns:
            The value, with any edit made through set_document_field applied.

        Raises:
            AttributeError: If the document carries no such field

        Example:
            >>> layer.get_document_field("layer_participants")
            [{'id': 88, 'participantId': 10, 'share1': 0.1, ...}]
        """
        # The document check comes first so this method and set_document_field
        # accept the same names: a custom field carries a modification too, and
        # answering it here would serve a name the setter rejects.
        key = self._document_key(name)
        if key is None:
            raise AttributeError(
                f"'{type(self).__name__}' has no document field '{name}'."
            )
        if name in self._modifications:
            return self._modifications[name]
        return self._data[key]

    def set_document_field(self, name: str, value: Any) -> None:
        """
        Edit a raw deal-document field the SDK exposes no property for.

        This is how a what-if scenario changes something the SDK has not
        modelled -- a layer's participants or reinstatements, a deal's loss
        groups. Until 0.13 the same edit was made by plain assignment, which
        also silently accepted names that were not document fields at all.

        The edit is local. The SDK has no endpoint that saves a deal, so it
        changes only the document posted for layered_losses, ylt and
        metric_results -- never the stored deal.

        Args:
            name: Field name in snake_case, e.g. "layer_participants"
            value: Replacement value, in the server's own shape

        Raises:
            AttributeError: If the document carries no such field
            ReadOnlyVersionError: If this object is a past version

        Example:
            >>> layer.set_document_field(
            ...     "layer_participants", [{"participantId": 10, "share1": 0.25}]
            ... )
            >>> layer.layered_losses.as_pandas(run_config)   # reflects the edit
        """
        self._require_writable(f"set document field '{name}'")
        if snake_to_camel(name) in self._MANAGED_DOCUMENT_KEYS:
            raise AttributeError(
                f"'{name}' is rebuilt by get_json() from this object's own state "
                f"and cannot be set as a raw document field. Edit it through "
                f"{self._owner_label}.layers, set_custom_field() or "
                f"set_custom_table() instead."
            )
        if self._document_key(name) is None:
            raise AttributeError(
                f"'{type(self).__name__}' has no document field '{name}'. "
                f"It holds: {', '.join(self._settable_document_names())}"
            )
        self._modifications[name] = value

    def _document_key(self, name: str) -> str | None:
        """The document key a snake_case name refers to, or None.

        The camelCase fold is tried first, then the document's own keys are
        folded back: snake_to_camel lowercases a trailing acronym run, so
        ``dcf_id`` becomes ``dcfId`` and would never find a ``dcfID`` key that
        camel_to_snake nonetheless names ``dcf_id``.
        """
        camel_name = snake_to_camel(name)
        if camel_name in self._MANAGED_DOCUMENT_KEYS:
            return None
        if camel_name in self._data:
            return camel_name
        for key in self._data:
            if key not in self._MANAGED_DOCUMENT_KEYS and camel_to_snake(key) == name:
                return key
        return None

    def _settable_document_names(self) -> list[str]:
        """Every name set_document_field accepts, sorted."""
        return sorted(
            camel_to_snake(key)
            for key in self._data
            if key not in self._MANAGED_DOCUMENT_KEYS
        )

    def _document_rows(self, name: str, camel_name: str) -> list[dict[str, Any]]:
        """The document's rows for a collection, with any edit applied."""
        if name in self._modifications:
            return self._modifications[name] or []
        return self._data.get(camel_name) or []

    def get_custom_field(self, field_name: str) -> Any:
        """
        Get a custom field value by name (original name as defined in Foundation).

        Args:
            field_name: Custom field name as it appears in Foundation

        Returns:
            Custom field value, or None if not found

        Example:
            >>> owner.get_custom_field('Portfolio Tag')  # a Program or a Layer
            'NewValue'
        """
        # _custom_fields_info carries this level's definitions, so it answers
        # both directions the collision-aware name mapping used to.
        snake_name = next(
            (
                info["snake_case_name"]
                for info in self._custom_fields_info
                if info["name"] == field_name
            ),
            None,
        )
        if snake_name:
            value = self._custom_field_values.get(snake_name)
            if self._parse_custom_field_datetimes:
                return parse_datetime_string(value)
            return value

        # Not found
        return None

    def set_custom_field(self, field_name: str, value: Any) -> None:
        """
        Set a custom field value by name (snake_case).

        Args:
            field_name: Custom field name in snake_case (e.g., 'portfolio_tag')
            value: Value to set

        Example:
            >>> owner.set_custom_field('portfolio_tag', 'NewValue')
        """
        self._require_writable(f"set custom field '{field_name}'")
        parsed_value = parse_datetime_string(value)
        self._modifications[field_name] = parsed_value

    def get_custom_table(self, table_name: str) -> CustomTable:
        """
        Get a custom table wrapper by name (original name or snake_case).

        The lookup chain, in order: an already-materialized table, then the info
        list (which handles an on-the-fly rename), then the reference-cache
        definition at this object's data level. Falls through to an empty
        placeholder so a caller can always dot-access a declared table.

        Args:
            table_name: Custom table name as it appears in Foundation
                (e.g., 'Claims History') or its snake_case form

        Returns:
            CustomTable instance for the requested table.

        Example:
            >>> claims_history = program.get_custom_table('Claims History')
            >>> df = claims_history.as_pandas()
            >>> df.head()
        """
        snake_name = camel_to_snake(table_name)
        # `is not None`, not truthiness: CustomTable.__bool__ is row_count > 0, so a
        # materialized table that happens to be empty is falsy and must not be
        # mistaken for absent — rebuilding it would discard the live instance (and
        # any edits already made through it).
        table = self._custom_tables.get(snake_name)
        if table is not None:
            return table

        for info in self._custom_tables_info:
            if info["name"] == table_name or info["snake_case_name"] == snake_name:
                existing = self._custom_tables.get(info["snake_case_name"])
                if existing is not None:
                    return existing
                # Layers list every definition here but only materialize the ones
                # the payload carried rows for, so a known-but-empty table lands
                # in this branch and has to be built below under the names the
                # info entry records.
                table_name = info["name"]
                snake_name = info["snake_case_name"]
                break

        target_id = None
        column_names: list[str] = []
        resolved_name = table_name
        for table_id, table_def in self._ref_cache._custom_tables.items():
            if table_def.get("dataLevelId") not in self._custom_table_data_level_ids:
                continue
            current_snake = camel_to_snake(table_def["name"])
            if table_def["name"] == table_name or current_snake == snake_name:
                target_id = table_id
                resolved_name = table_def["name"]
                snake_name = current_snake
                columns = self._ref_cache.get_custom_table_columns(table_id)
                column_names = [col["name"] for col in columns]
                break

        new_table = CustomTable(
            owner=self,
            ref_cache=self._ref_cache,
            table_id=target_id,
            snake_name=snake_name,
            original_name=resolved_name,
            column_names=column_names,
            initial_rows=[],
        )
        self._custom_tables[snake_name] = new_table
        self._custom_table_original_names.setdefault(snake_name, resolved_name)

        if not any(
            info["snake_case_name"] == snake_name for info in self._custom_tables_info
        ):
            self._custom_tables_info.append(
                {
                    "id": target_id,
                    "name": resolved_name,
                    "snake_case_name": snake_name,
                    "column_count": len(column_names),
                    "columns": column_names,
                }
            )
            self._custom_tables_info.sort(key=lambda x: x["name"])

        return new_table

    def _mark_custom_table_modified(self, table_name: str) -> None:
        """Mark a custom table as modified to include it in JSON output."""
        self._custom_table_dirty.add(table_name)

    def set_custom_table(
        self, table_name: str, rows: Union[list[dict[str, Any]], "pd.DataFrame"]
    ) -> None:
        """
        Set a custom table by name (accepts original name or snake_case).

        Args:
            table_name: Custom table name (e.g., 'Claims History' or 'claims_history')
            rows: DataFrame or list of table rows (dicts with column names as keys)

        Example:
            >>> owner.set_custom_table('Claims History', [
            ...     {'Valuation Date': '2024-01-15', 'Breakout': 'Property'}
            ... ])
        """
        self._require_writable(f"set custom table '{table_name}'")
        snake_name = camel_to_snake(table_name)
        if snake_name in self._modifications:
            del self._modifications[snake_name]
        table = self.get_custom_table(table_name)
        table.set_data(rows)

    # --- name tests -------------------------------------------------------

    def _is_custom_field_name(self, name: str) -> bool:
        """Check if a name matches any custom field defined at this level.

        _custom_field_values holds only the fields this object carries a value
        for, so it cannot be the whole test: a field that exists but has never
        been set must still be writable, and __setattr__ rejects names that
        match nothing. _custom_field_snake_names is every declared name.
        """
        return (
            name in self._custom_field_values or name in self._custom_field_snake_names
        )

    def _is_custom_table_name(self, name: str) -> bool:
        """Check if a name matches any custom table."""
        if name in self._custom_tables:
            return True
        return any(info["snake_case_name"] == name for info in self._custom_tables_info)

    # --- document round-trip ----------------------------------------------

    def _apply_scalar_modification(
        self, result: dict[str, Any], key: str, value: Any
    ) -> None:
        """Write one edited plain field into ``result``, serializing dates.

        _document_key, not a bare fold: a key whose name ends in an acronym
        (dcfID) folds to something else (dcfId), and writing that would add a
        second key the server ignores.
        """
        camel_key = self._document_key(key) or snake_to_camel(key)
        # Convert date/datetime objects to ISO strings for JSON serialization
        if isinstance(value, datetime):
            result[camel_key] = to_wire_datetime(value)
        elif isinstance(value, date):
            result[camel_key] = f"{value}T00:00:00"
        else:
            result[camel_key] = value

    def _apply_dirty_tables(self, result: dict[str, Any]) -> None:
        """Fold every edited custom table into ``result``.

        The edited tables are re-applied on every call rather than consumed by
        the first one: a single export calls get_json() twice -- once to hash
        the deal document, once to build the request body -- and consuming them
        made the two calls disagree, which both sent the un-edited table and
        cost a second export. Reading a table through as_pandas() marks it
        edited, so an in-place edit to the frame it returns is covered without
        re-applying every table.
        """
        for table_name in self._custom_table_dirty:
            table = self._custom_tables.get(table_name)
            if table is None:
                continue
            rows = table.to_records()
            self._update_custom_table_in_json(result, table_name, rows)

    def _update_custom_field_in_json(
        self, result: dict[str, Any], field_name: str, value: Any
    ) -> None:
        """Update a custom field value in this level's custom-field array."""
        field_id = next(
            (
                info["id"]
                for info in self._custom_fields_info
                if info["snake_case_name"] == field_name
            ),
            None,
        )
        if field_id is None:
            return

        # Update or add the custom field in the array
        custom_fields = result.get(self._custom_fields_key, [])
        found = False
        for cf in custom_fields:
            if cf.get("customFieldId") == field_id:
                cf["value"] = value
                found = True
                break

        if not found:
            # Add new custom field entry
            custom_fields.append(
                {
                    "id": None,  # Will be assigned by API
                    f"{self._owner_label}Id": result.get("id"),
                    self._owner_label: None,
                    "customFieldId": field_id,
                    "customField": None,
                    "value": value,
                }
            )
            result[self._custom_fields_key] = custom_fields

    def _find_custom_table_id(self, table_name: str) -> int | None:
        """Resolve a snake_case table_name to its table id at this data level."""
        for tid, table_def in self._ref_cache._custom_tables.items():
            if table_def.get("dataLevelId") in self._custom_table_data_level_ids:
                if camel_to_snake(table_def["name"]) == table_name:
                    return tid
        return None

    def _build_column_mapping(
        self, table_id: int
    ) -> "tuple[dict[str, str], dict[str, int]]":
        """Return (column_name -> raw-field-name, lookup-column-name -> accessible-object-id)."""
        from ._custom_table_fields import LOOKUP_VALUE_TYPE, field_name_for_column

        column_to_field: dict[str, str] = {}
        lookup_columns: dict[str, int] = {}
        for col in self._ref_cache.get_custom_table_columns(table_id):
            field_name = field_name_for_column(col)
            if field_name is None:
                continue
            col_name = col["name"]
            column_to_field[col_name] = field_name
            if col["valueType"] == LOOKUP_VALUE_TYPE:
                lookup_object_id = col.get("lookupObjectId")
                if lookup_object_id is not None:
                    lookup_columns[col_name] = lookup_object_id
        return column_to_field, lookup_columns

    def _build_raw_table_row(
        self,
        table_id: int,
        owner_id: Any,
        sort_order: int,
        original_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Return a raw row dict, preserving id/metadata from the original if present."""
        from ._custom_table_fields import initialize_blank_fields

        if sort_order < len(original_rows):
            raw_row = copy.deepcopy(original_rows[sort_order])
            raw_row["sortOrder"] = sort_order
            return raw_row

        raw_row = {
            "id": None,  # Will be assigned by API
            "customTableDefinitionId": table_id,
            "customTableDefinition": None,
            "programId": None,
            "layerId": None,
            "layer": None,
            "sortOrder": sort_order,
        }
        raw_row[f"{self._owner_label}Id"] = owner_id
        initialize_blank_fields(raw_row)
        return raw_row

    def _apply_row_values(
        self,
        raw_row: dict[str, Any],
        row_data: dict[str, Any],
        column_to_field: dict[str, str],
        lookup_columns: dict[str, int],
    ) -> None:
        """Project user values into the raw typed slots on ``raw_row``."""
        for col_name, user_value in row_data.items():
            field_name = column_to_field.get(col_name)
            if not field_name:
                continue
            if col_name in lookup_columns and user_value is not None:
                user_value = self._ref_cache.reverse_lookup_value(
                    lookup_columns[col_name], user_value
                )
            if isinstance(user_value, (date, datetime)):
                user_value = (
                    to_wire_datetime(user_value)
                    if isinstance(user_value, datetime)
                    else f"{user_value}T00:00:00"
                )
            raw_row[field_name] = user_value

    def _update_custom_table_in_json(
        self, result: dict[str, Any], table_name: str, value: list[dict[str, Any]]
    ) -> None:
        """Update a custom table in the customTables array.

        Converts user-friendly column names back to internal field names
        (numValue1, textValue1, dateValue1, etc.) and maintains row order.
        """
        table_id = self._find_custom_table_id(table_name)
        if table_id is None:
            return

        column_to_field, lookup_columns = self._build_column_mapping(table_id)
        owner_id = result.get("id")
        owner_id_key = f"{self._owner_label}Id"
        original_rows = self._custom_table_original_rows.get(table_name, [])

        # Drop existing rows for this owner + table, then rebuild in order.
        custom_tables = [
            ct
            for ct in result.get("customTables", [])
            if ct.get("customTableDefinitionId") != table_id
            or ct.get(owner_id_key) != owner_id
        ]
        for sort_order, row_data in enumerate(value):
            raw_row = self._build_raw_table_row(
                table_id, owner_id, sort_order, original_rows
            )
            self._apply_row_values(raw_row, row_data, column_to_field, lookup_columns)
            custom_tables.append(raw_row)

        result["customTables"] = custom_tables

    # --- dynamic attributes -----------------------------------------------

    def __getattr__(self, name: str) -> Any:
        """
        Dynamic attribute access for custom fields and custom tables.

        This enables dot notation access like:
        - program.portfolio_tag (instead of program.get_custom_field('portfolio_tag'))
        - program.buffer_table_1 (instead of program.get_custom_table('buffer_table_1'))

        Args:
            name: Attribute name in snake_case

        Returns:
            Attribute value

        Raises:
            AttributeError: If attribute not found
        """
        raise_if_private(self, name)

        # Check custom fields (check modifications first)
        if name in self._modifications:
            return self._modifications[name]

        if name in self._custom_field_values:
            return self._custom_field_values[name]

        # Check custom tables
        if name in self._custom_tables:
            return self._custom_tables[name]

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def _set_extra_attribute(self, name: str, value: Any) -> bool:
        """Hook for a subclass's own dynamic names; True if it took the write.

        Runs after the property and custom-table checks and before the
        custom-field one. Layer uses it for limits, attachments and premiums.
        """
        return False

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Dynamic attribute setting for custom fields and custom tables.

        This enables dot notation assignment like:
        - program.portfolio_tag = "NewValue"

        Args:
            name: Attribute name in snake_case
            value: Value to set
        """
        # Internal attributes (prefixed with _) are set directly
        if name.startswith("_"):
            super().__setattr__(name, value)
            return

        self._require_writable(f"set '{name}'")

        # Check if it's a defined property with a setter
        prop = getattr(type(self), name, None)
        if isinstance(prop, property):
            if prop.fset is None:
                raise AttributeError(
                    f"'{name}' is read-only on {self.__class__.__name__}."
                )
            prop.fset(self, value)
            # Property setter already handles modifications
            return

        if self._is_custom_table_name(name):
            self.set_custom_table(name, value)
            return

        if self._set_extra_attribute(name, value):
            return

        # Only a property, a custom field or a custom table is writable. Until
        # 0.13 any name whose camelCase form was a key in the deal document was
        # accepted too, which made the write half of the surface wider than the
        # read half: `program.renewed_from_id = 1460` was taken and the matching
        # read raised AttributeError. A name genuinely worth setting now has a
        # property; the rest of the document is reachable through
        # get_document_field / set_document_field, which read and write the same
        # names as each other.
        if not self._is_custom_field_name(name):
            hint = (
                f" Use {self._owner_label}.set_document_field('{name}', ...) to "
                "edit the raw document field of that name."
                if snake_to_camel(name) in self._data
                else ""
            )
            raise AttributeError(
                f"'{type(self).__name__}' has no settable attribute or custom "
                f"field '{name}'. Use {self._owner_label}.custom_fields_info to "
                f"list the custom-field names it accepts.{hint}"
            )

        parsed_value = parse_datetime_string(value)
        self._modifications[name] = parsed_value
