"""Template model: a portfolio template, readable, editable and savable."""

import copy
import itertools
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from ..exceptions import FoundationError, NotFoundError
from ..utils import camel_to_snake, parse_utc_timestamp, to_wire_datetime
from .collections import NamedCollection
from .field_adjustment import FieldAdjustment
from .portfolio import Portfolio
from .rollup_run import RollupRun
from .rows import Rows
from .scenario import Scenario
from .template_rows import DETACHED, checked, detached, read_criterion, sorted_by_order
from .template_scenarios import (
    adjustment_row,
    changed_adjustment_row,
    read_scenarios,
    scenario_row,
)

if TYPE_CHECKING:
    from .foundation_client import FoundationClient

#: The template's child collections. Everything else in the graph is a plain field.
_SECTIONS = (
    "templateGroups",
    "templateModels",
    "templateDimensions",
    "scenarios",
    "templateDirectories",
    "templateViews",
    "portfolios",
)

#: The live structures a template keeps beside its graph, each compared with
#: the saved graph to tell whether its section changed.
_STRUCTURES = ("groups", "pricing_groups", "model_ids", "dimensions", "scenarios")

#: Seconds to wait for a preview's response; previews can take minutes.
PREVIEW_TIMEOUT = 300.0

# Column limits of TemplateGroup and TemplateGroupCriteria.
_GROUP_NAME_LENGTH = 100
_CRITERIA_NAME_LENGTH = 200


def _field(key: str, doc: str) -> property:
    """A plain template field, read and written in place."""

    def get(self: "Template") -> Any:
        return self._fields.get(key)

    def set_(self: "Template", value: Any) -> None:
        self._graph[key] = value

    return property(get, set_, doc=doc)


def _reference(
    key: str,
    noun: str,
    find: Callable[["FoundationClient", int | str], Any],
    doc: str,
    *,
    required: bool = False,
) -> property:
    """A reference-data ID: assign an ID or a name; reads back the ID."""

    def get(self: "Template") -> int | None:
        return self._fields.get(key)

    def set_(self: "Template", value: int | str | None) -> None:
        if value is None and not required:
            self._graph[key] = None
            return
        found = None if value is None else find(self._client, value)
        if found is None:
            raise ValueError(f"No {noun} is {value!r}.")
        self._graph[key] = found.id

    return property(get, set_, doc=doc)


class Template:
    """A portfolio template.

    Every template is editable in place, saved or not: assign its fields (an
    ID or a name for reference data), and change ``groups``,
    ``pricing_groups``, ``model_ids`` and ``dimensions`` like any dict or
    list. ``save()`` creates an unsaved template or updates a saved one;
    ``layers()`` previews the template as it is in memory.

    Selection formulas are written as in the Portfolio UI formula builder,
    with its field names and quoted labels, e.g. ``Client == "Acme Re" && UW
    Year >= 2024``. Criteria in a group are ANDed and groups are ORed.

    Example:
        >>> tpl = client.get_template("Q3 Property")
        >>> tpl.groups["UW 2025"] = ["UW Year == 2025"]
        >>> tpl.layers().as_polars()
        >>> tpl.save()
    """

    def __init__(
        self,
        client: "FoundationClient",
        graph: dict[str, Any] | None = None,
        schedule: dict[str, Any] | None = None,
        *,
        header: dict[str, Any] | None = None,
    ):
        """Wrap a template graph, or a header row whose graph loads on first use."""
        self._client = client
        self._header = header
        self._loaded_graph: dict[str, Any] | None = None
        if graph is not None:
            self._load(graph, schedule)

    @classmethod
    def blank(cls, client: "FoundationClient", name: str) -> "Template":
        """An unsaved template with nothing selected, to fill in."""
        return cls(
            client,
            {
                "id": 0,
                "name": name,
                "description": None,
                "vendorId": 0,
                "eventSetId": None,
                "variantId": None,
                "perspectiveId": None,
                "simYears": 0,
                "eventSplitViewId": None,
                "currencyCode": "",
                "participantId": None,
                "scheduleId": None,
                "activeFlag": True,
                "pricingFlag": False,
                "asOfDate": None,
                "exportToClickHouseFlag": False,
                "exportToDatabricksFlag": False,
                "createdBy": "00000000-0000-0000-0000-000000000000",
                "applyLossSplitsFlag": True,
                "generateScenariosOnlyFlag": False,
                **{section: [] for section in _SECTIONS},
            },
        )

    # ---- state ----

    def _load(self, graph: dict[str, Any], schedule: dict[str, Any] | None) -> None:
        """Take a graph as the server returned it: the saved state, and the
        live structures parsed from it."""
        self._saved = copy.deepcopy(graph)
        self._schedule = schedule
        self._loaded_graph = graph
        self._groups = self._saved_structure("groups")
        self._pricing_groups = self._saved_structure("pricing_groups")
        self._model_ids = self._saved_structure("model_ids")
        # Named on first read: naming them costs a request.
        self._dimensions: list[str] | None = None
        self._scenarios: list[Scenario] | None = None
        self._saved_scenarios: list[Scenario] | None = None

    @property
    def _graph(self) -> dict[str, Any]:
        if self._loaded_graph is None:
            data = self._client._fetch(
                "portfolio", f"/api/Templates/{self._header['id']}"
            )
            self._load(data["template"], data.get("schedule"))
        return self._loaded_graph

    @property
    def _fields(self) -> dict[str, Any]:
        """The template's plain fields: a header row carries them all, so
        reading one does not load the graph."""
        return self._loaded_graph if self._loaded_graph is not None else self._header

    @property
    def _reference_data(self):
        return self._client._portfolio_reference()

    def _dimension_names(self) -> list[str]:
        self._ensure_loaded()
        if self._dimensions is None:
            self._dimensions = self._saved_structure("dimensions")
        return self._dimensions

    def _saved_structure(self, key: str) -> Any:
        """A live structure as the saved graph holds it."""
        saved = self._saved
        if key in ("groups", "pricing_groups"):
            named = _named_groups(saved.get("templateGroups") or [], key == "groups")
            return {name: _read_group(row) for name, row in named.items()}
        if key == "model_ids":
            return [row["modelId"] for row in saved.get("templateModels") or []]
        if key == "scenarios":
            if self._saved_scenarios is None:
                self._saved_scenarios = read_scenarios(
                    saved.get("scenarios") or [], self._client
                )
            return self._saved_scenarios
        reference = self._reference_data
        return [
            reference.dimension_name(row["dimensionId"])
            for row in saved.get("templateDimensions") or []
        ]

    def _changed(self, key: str) -> bool:
        """Whether a live structure differs from the saved graph's."""
        live = {
            "groups": lambda: _normalized(self._groups),
            "pricing_groups": lambda: _normalized(self._pricing_groups),
            "model_ids": lambda: self._model_ids,
            "dimensions": lambda: self._dimensions,
            "scenarios": lambda: self._scenarios,
        }[key]()
        # Dimensions or scenarios never read cannot have changed.
        return live is not None and live != self._saved_structure(key)

    def _ensure_loaded(self) -> None:
        self._graph  # noqa: B018 - loads a header-only template's graph

    # ---- fields ----

    @property
    def id(self) -> int | None:
        """Template ID; None until the template is saved."""
        return self._fields.get("id") or None

    name = _field("name", "Template name, at most 100 characters.")
    description = _field("description", "Optional description, at most 500 characters.")
    active_flag = _field("activeFlag", "Whether the template can be run.")
    pricing_flag = _field(
        "pricingFlag",
        "Whether the template's portfolios are available for pricing deals.",
    )
    export_to_clickhouse = _field(
        "exportToClickHouseFlag", "Whether results are exported to ClickHouse."
    )
    export_to_databricks = _field(
        "exportToDatabricksFlag", "Whether results are exported to Databricks."
    )
    sim_years = _field("simYears", "Simulation years the losses must have.")
    apply_loss_splits = _field(
        "applyLossSplitsFlag", "Whether loss set event splits are applied."
    )
    generate_scenarios_only = _field(
        "generateScenariosOnlyFlag",
        "Whether only scenario portfolios are created, no base portfolio.",
    )
    vendor_id = _reference(
        "vendorId",
        "vendor",
        lambda c, v: c.get_vendor(v),
        "The losses' model vendor.",
        required=True,
    )
    variant_id = _reference(
        "variantId", "variant", lambda c, v: c.get_variant(v), "The losses' variant."
    )
    perspective_id = _reference(
        "perspectiveId",
        "perspective",
        lambda c, v: c.get_perspective(v),
        "The losses' perspective.",
    )
    event_set_id = _reference(
        "eventSetId",
        "event set",
        lambda c, v: c.get_event_set(v),
        "The losses' event set.",
    )
    participant_id = _reference(
        "participantId",
        "participant",
        lambda c, v: c.get_participant(v),
        "The share layers are rolled up at; None for 100%.",
    )
    event_split_view_id = _reference(
        "eventSplitViewId",
        "event split view",
        lambda c, v: c._portfolio_reference().event_split_view(v),
        "The event split view applied to the layered losses.",
    )

    @property
    def currency_code(self) -> str:
        """Currency the results are reported in, e.g. "USD"."""
        return self._fields["currencyCode"]

    @currency_code.setter
    def currency_code(self, value: str) -> None:
        currency = self._client.get_currency(value)
        if currency is None or not currency.code:
            raise ValueError(f"No currency is {value!r}.")
        self._graph["currencyCode"] = currency.code

    @property
    def as_of_date(self) -> datetime | None:
        """The point in time deals are read at; None for their current state."""
        return parse_utc_timestamp(self._fields.get("asOfDate"))

    @as_of_date.setter
    def as_of_date(self, value: datetime | date | None) -> None:
        if value is not None and not isinstance(value, datetime):
            value = datetime(value.year, value.month, value.day, tzinfo=UTC)
        self._graph["asOfDate"] = None if value is None else to_wire_datetime(value)

    # ---- live structures ----

    @property
    def groups(self) -> dict[str, list[str]]:
        """Deal selection rules: group name -> criteria, as a live dict.

        A criterion stored negated reads as ``!(formula)``, and a negated
        group as one criterion ``!((c1) && (c2))``.
        """
        self._ensure_loaded()
        return self._groups

    @groups.setter
    def groups(self, value: Mapping[str, str | Sequence[str]]) -> None:
        self._ensure_loaded()
        self._groups = _normalized(value)

    @property
    def pricing_groups(self) -> dict[str, list[str]]:
        """Pricing portfolio selection rules, in the same shape as ``groups``."""
        self._ensure_loaded()
        return self._pricing_groups

    @pricing_groups.setter
    def pricing_groups(self, value: Mapping[str, str | Sequence[str]]) -> None:
        self._ensure_loaded()
        self._pricing_groups = _normalized(value)

    @property
    def model_ids(self) -> list[int]:
        """The models whose losses are rolled up, as a live list. Assign IDs or
        names from ``client.models``, plus "Unknown" for model 0."""
        self._ensure_loaded()
        return self._model_ids

    @model_ids.setter
    def model_ids(self, value: Sequence[int | str]) -> None:
        self._ensure_loaded()
        self._model_ids = [self._reference_data.model_id(model) for model in value]

    @property
    def model_view(self) -> str | None:
        """The active model view whose models are exactly ``model_ids``, as the
        Portfolio UI shows it; None if no view matches. Assign a view's ID or
        name to replace ``model_ids`` with its models."""
        wanted = set(self.model_ids)
        reference = self._reference_data
        for view in self._client.model_views:
            if set(reference.model_view_models(view)) == wanted:
                return view.name
        return None

    @model_view.setter
    def model_view(self, value: int | str) -> None:
        self._ensure_loaded()
        reference = self._reference_data
        models = reference.model_view_models(reference.model_view(value))
        if not models:
            raise ValueError(f"Model view {value!r} has no models.")
        self._model_ids = models

    @property
    def dimensions(self) -> list[str]:
        """The dimensions results are sliced by, as a live list of names.
        Assign names or IDs from the Portfolio module's dimensions."""
        return self._dimension_names()

    @dimensions.setter
    def dimensions(self, value: Sequence[int | str]) -> None:
        self._dimension_names()
        self._dimensions = [self._reference_data.dimension(d).name for d in value]

    @property
    def scenarios(self) -> list[Scenario]:
        """The template's scenarios, the UI's Proforma Adjustments, as a live
        list: change the Scenario objects and their adjustments in place, or
        assign a new list. Each run creates one extra portfolio per scenario."""
        self._ensure_loaded()
        if self._scenarios is None:
            # A copy: the saved read-back is what edits are compared against.
            self._scenarios = copy.deepcopy(self._saved_structure("scenarios"))
        return self._scenarios

    @scenarios.setter
    def scenarios(self, value: Sequence[Scenario]) -> None:
        self._ensure_loaded()
        # Copies: a Scenario shared with another template would change both.
        self._scenarios = copy.deepcopy(list(value))

    @property
    def portfolios(self) -> NamedCollection[Portfolio]:
        """The portfolios this template created, newest first; empty for an
        unsaved template. Read afresh on every access."""
        if self.id is None:
            return NamedCollection([])
        rows = self._client._fetch(
            "portfolio", f"/api/Portfolios/PortfolioByTemplateId/{self.id}"
        )
        rows.sort(key=lambda row: row.get("dateCreated") or "", reverse=True)
        return NamedCollection(Portfolio(row, self._client) for row in rows)

    @property
    def has_unsaved_changes(self) -> bool:
        """True for an unsaved template, or a saved one changed since it was read."""
        if self.id is None:
            return True
        if self._loaded_graph is None:
            return False
        fields = {k: v for k, v in self._graph.items() if k not in _SECTIONS}
        saved = {k: v for k, v in self._saved.items() if k not in _SECTIONS}
        return fields != saved or any(self._changed(key) for key in _STRUCTURES)

    # ---- actions ----

    def layers(self, *, timeout: float | None = PREVIEW_TIMEOUT) -> Rows:
        """Preview the layers a run of the template, as it is in memory, would
        roll up now.

        Every layer that passes the selection rules is listed, including those
        a run keeps although another version of their deal group was already
        chosen (``program_group_duplicate``). Columns: program_id,
        program_name, uw_year, program_group, layer_id, layer_name,
        program_version_id, program_group_duplicate, plus the columns the
        platform's selection preview grid is configured with.

        Args:
            timeout: Seconds to wait for the response; None waits forever.

        Raises:
            ValueError: If a formula cannot be converted.
            ValidationError: If DVE rejects a formula.
            ServerError: 504 when the preview outlives the load balancer's
                timeout, which an as-of date makes likelier.
        """
        self._ensure_loaded()
        self._reference_data.formulas.prime(self._formulas())
        response = self._client._fetch(
            "portfolio",
            "/api/Portfolios/GetSelectedAndExcludedDeals",
            method="POST",
            json={
                "selectionRules": self._wire_groups(itertools.count(-1, -1)),
                "asOfDate": self._graph.get("asOfDate"),
            },
            timeout=timeout,
        )
        return Rows(
            {
                **{
                    camel_to_snake(k): v
                    for k, v in record.items()
                    if k != "exclusionReason"
                },
                "program_group_duplicate": record.get("exclusionReason") == 1,
            }
            for record in [*response.get("included", []), *response.get("excluded", [])]
        )

    def run(self) -> RollupRun:
        """Run the template: a saved template as saved, an unsaved one as it
        is in memory, without saving it.

        A run of a saved template first deactivates the template's current
        portfolios, before the platform validates the run. A run of an
        unsaved template deactivates nothing, and its portfolios carry no
        template ID.

        Returns:
            The run, as just queued and not read yet: wait() follows it to
            the end, and refresh() reads it once.

        Raises:
            ValueError: If a saved template has unsaved changes or is
                inactive.
            ValidationError: If the platform refuses the run, such as a
                scenario-only template without scenarios.
            FoundationError: If the platform is older than v49.0 and cannot
                report the run.
        """
        if self.id is not None:
            if self.has_unsaved_changes:
                raise ValueError(
                    f"Template {self.id} has unsaved changes and run() runs the saved "
                    "template: save() first, or copy() to run the changed version ad hoc."
                )
            if not self.active_flag:
                raise ValueError(
                    f"Template {self.id} is inactive; the platform runs only active templates."
                )
            response = self._client.request("portfolio", f"/app/Template/{self.id}")
        else:
            try:
                response = self._client.request(
                    "portfolio",
                    "/app/Template/Run",
                    method="POST",
                    json=self._wire_graph(),
                )
            except NotFoundError:
                raise FoundationError(
                    "Running an unsaved template needs Foundation v49.0 or later."
                ) from None
        found = re.search(r"/Run/(\d+)$", response.headers.get("Location") or "")
        if found is None:
            raise FoundationError(
                "The platform queued the run but did not say which one it is; "
                "following a run needs Foundation v49.0 or later."
            )
        return RollupRun(self._client, int(found.group(1)))

    def copy(self, name: str | None = None) -> "Template":
        """An unsaved copy of this template, as it is in memory, to change and
        run or save.

        The copy is active, has no schedule, and is neither a pricing template
        nor exported to ClickHouse or Databricks.
        """
        twin = Template(
            self._client,
            {
                **copy.deepcopy(self._graph),
                "id": 0,
                "name": name or self.name,
                "scheduleId": None,
                "activeFlag": True,
                "pricingFlag": False,
                "exportToClickHouseFlag": False,
                "exportToDatabricksFlag": False,
            },
        )
        # The graph's sections are as saved; the copy takes this template's
        # structures as they are in memory.
        twin._groups = copy.deepcopy(self._groups)
        twin._pricing_groups = copy.deepcopy(self._pricing_groups)
        twin._model_ids = list(self._model_ids)
        twin._dimensions = None if self._dimensions is None else list(self._dimensions)
        twin._scenarios = copy.deepcopy(self._scenarios)
        return twin

    def _check_scenario_only(self) -> None:
        """A scenario-only template needs a scenario to create any portfolio."""
        if self._graph.get("generateScenariosOnlyFlag") and not self._scenario_names():
            raise ValueError(
                "generate_scenarios_only is set but the template has no scenarios."
            )

    def _scenario_names(self) -> list[str]:
        """The scenarios' names, without reading the scenarios back if they
        were never touched."""
        if self._scenarios is not None:
            return [scenario.name for scenario in self._scenarios]
        return [row["name"] for row in self._saved.get("scenarios") or []]

    def save(self, folder: int | str | None = None) -> "Template":
        """Create this template, or update it if it is saved, then read it back.

        Args:
            folder: For a new template, the Available Templates folder to put it
                in: a folder ID, a folder name, or None for the top level.
                Templates are moved between folders in the UI.

        Returns:
            This template, now saved.

        Raises:
            ValueError: If ``folder`` is given for a saved template or names no
                single folder, or a formula cannot be converted.
        """
        self._check_scenario_only()
        if self.id is not None:
            if folder is not None:
                raise ValueError(
                    "folder applies only to a new template; move a saved "
                    "template between folders in the UI."
                )
            template_id = self.id
            self._client._fetch(
                "portfolio",
                f"/api/Templates/{template_id}",
                method="PUT",
                json={"template": self._wire_graph(), "schedule": self._schedule},
            )
        else:
            graph = self._wire_graph()
            # The template's leaf in the Available Templates tree, created with
            # it: a leaf has no name, as the UI adds it.
            graph["templateDirectories"] = [
                {
                    "id": -1,
                    "parentId": self._reference_data.folder_id(folder),
                    "templateId": None,
                    "name": None,
                    "description": None,
                }
            ]
            created = self._client._fetch(
                "portfolio",
                "/api/Templates",
                method="POST",
                json={"template": graph, "schedule": None},
            )
            template_id = created["template"]["id"]
        data = self._client._fetch("portfolio", f"/api/Templates/{template_id}")
        self._load(data["template"], data.get("schedule"))
        return self

    # ---- the wire graph ----

    def _wire_graph(self) -> dict[str, Any]:
        """The graph to send: fields as edited, unchanged rows as read, changed
        rows rebuilt with negative IDs."""
        self._reference_data.formulas.prime(self._formulas())
        ids = itertools.count(-1, -1)
        graph = {
            k: copy.deepcopy(v) for k, v in self._graph.items() if k not in _SECTIONS
        }
        reference = self._reference_data
        graph.update(
            templateGroups=self._wire_groups(ids),
            templateModels=self._wire_section(
                "templateModels",
                "model_ids",
                ids,
                lambda: [
                    {"modelId": model_id}
                    for model_id in dict.fromkeys(
                        reference.model_id(m) for m in self._model_ids
                    )
                ],
            ),
            templateDimensions=self._wire_section(
                "templateDimensions",
                "dimensions",
                ids,
                lambda: [
                    {"dimensionId": dimension_id}
                    for dimension_id in dict.fromkeys(
                        reference.dimension(name).id for name in self._dimension_names()
                    )
                ],
            ),
            scenarios=self._wire_scenarios(ids),
            templateDirectories=[],
            templateViews=[],
            portfolios=[],
        )
        return graph

    def _formulas(self) -> Iterator[str]:
        """Every formula the template carries, for one conversion request."""
        for groups in (self._groups, self._pricing_groups):
            for criteria in groups.values():
                yield from criteria
        for scenario in self._scenarios or []:
            for adjustment in scenario.adjustments:
                if isinstance(adjustment, FieldAdjustment):
                    yield adjustment.formula
                    where = adjustment.where
                    yield from [where] if isinstance(where, str) else where

    def _wire_groups(self, ids: Iterator[int]) -> list[dict[str, Any]]:
        """Both sections of groups, compared one group at a time: a group
        whose criteria read as before goes back as read."""
        saved = self._saved.get("templateGroups") or []
        rows = []
        for deal_selection, live in (
            (True, self._groups),
            (False, self._pricing_groups),
        ):
            saved_groups = _named_groups(saved, deal_selection)
            for name, formulas in _normalized(live).items():
                row = saved_groups.get(name)
                if row is not None and _read_group(row) == formulas:
                    rows += self._kept("templateGroups", [row], ids)
                else:
                    rows.append(
                        self._group_row(name, formulas, deal_selection, row, ids)
                    )
        return rows

    def _group_row(
        self,
        name: str,
        formulas: list[str],
        deal_selection: bool,
        saved: dict[str, Any] | None,
        ids: Iterator[int],
    ) -> dict[str, Any]:
        """A changed or new group. A changed group keeps its row, name and
        description, and each criterion still written the same way keeps its
        row; the rest are converted afresh."""
        kind = "group" if deal_selection else "pricing group"
        if len(name) > _GROUP_NAME_LENGTH:
            raise ValueError(
                f"{kind} name {name!r} is longer than {_GROUP_NAME_LENGTH} characters."
            )
        if not formulas:
            raise ValueError(
                f"{kind} {name!r} has no criteria; it would select every layer."
            )
        # A negated group reads as one criterion, so its rows cannot be kept.
        reusable = saved is not None and not saved.get("notFlag")
        if reusable and self.id is not None:
            group = {
                k: copy.deepcopy(v)
                for k, v in saved.items()
                if k != "templateGroupCriterias"
            }
        else:
            description = saved.get("description") if saved is not None else None
            group = self._new_row("templateGroups", {"description": description}, ids)
        kept_criteria = {}
        if reusable:
            for criterion in sorted_by_order(saved.get("templateGroupCriterias") or []):
                kept_criteria.setdefault(read_criterion(criterion), criterion)
        converter = self._reference_data.formulas
        criteria = []
        for order, formula in enumerate(formulas, 1):
            where = f"{kind} {name!r}, criterion {order}"
            kept = kept_criteria.pop(formula, None)
            if kept is not None:
                criterion = (
                    copy.deepcopy(kept)
                    if group["id"] > 0
                    else detached([kept], "templateGroupCriterias", ids)[0]
                )
            else:
                if not formula.strip():
                    raise ValueError(f"{where}: the criterion is empty.")
                checked(formula, where)
                json_formula = checked(converter.convert(formula, where), where)
                criterion = {
                    **DETACHED["templateGroupCriterias"],
                    "id": next(ids),
                    "name": formula[:_CRITERIA_NAME_LENGTH],
                    "criteriaFormula": formula,
                    "jsonFormula": json_formula,
                    "notFlag": False,
                }
            if group["id"] > 0:
                criterion["templateGroupId"] = group["id"]
            criteria.append({**criterion, "sortOrder": order})
        group.update(
            name=name,
            notFlag=False,
            dealSelectionFlag=deal_selection,
            templateGroupCriterias=criteria,
        )
        return group

    def _wire_scenarios(self, ids: Iterator[int]) -> list[dict[str, Any]]:
        """The scenarios, each matched to the stored row it was read from: an
        unchanged one goes back as read, and a changed one keeps its row and
        the rows of adjustments and criteria written as before."""
        saved = self._saved.get("scenarios") or []
        if self._scenarios is None:
            return self._kept("scenarios", saved, ids)
        stored = {
            row["id"]: (row, read)
            for row, read in zip(saved, self._saved_structure("scenarios"), strict=True)
        }
        rows = []
        for scenario in self._scenarios:
            row, read = stored.pop(scenario._row_id, (None, None))
            if row is None:
                rows.append(scenario_row(scenario, self._client, ids, self.id))
            elif read == scenario:
                rows += self._kept("scenarios", [row], ids)
            else:
                rows.append(self._changed_scenario_row(scenario, row, read, ids))
        return rows

    def _changed_scenario_row(
        self,
        scenario: Scenario,
        row: dict[str, Any],
        read: Scenario,
        ids: Iterator[int],
    ) -> dict[str, Any]:
        """A stored scenario that changed. A saved template keeps its rows;
        an unsaved one sends detached copies."""
        keep_ids = self.id is not None
        stored = {
            adjust["id"]: (adjust, old)
            for adjust, old in zip(
                sorted_by_order(row.get("scenarioAdjusts") or []),
                read.adjustments,
                strict=True,
            )
        }
        adjustments = []
        for order, adjustment in enumerate(scenario.adjustments, 1):
            saved, old = stored.pop(adjustment._row_id, (None, None))
            if saved is None:
                new = adjustment_row(
                    adjustment, order, scenario.name, self._client, ids
                )
                adjustments.append({**new, "scenarioId": row["id"] if keep_ids else 0})
            elif old == adjustment:
                kept = (
                    copy.deepcopy(saved)
                    if keep_ids
                    else detached([saved], "scenarioAdjusts", ids)[0]
                )
                adjustments.append({**kept, "sortOrder": order})
            else:
                adjustments.append(
                    changed_adjustment_row(
                        adjustment,
                        saved,
                        old,
                        order,
                        scenario.name,
                        self._client,
                        ids,
                        keep_ids,
                    )
                )
        if not keep_ids:
            return scenario_row(scenario, self._client, ids, None, adjustments)
        kept = {k: copy.deepcopy(v) for k, v in row.items() if k != "scenarioAdjusts"}
        return {**kept, "name": scenario.name, "scenarioAdjusts": adjustments}

    def _wire_section(
        self, section: str, key: str, ids: Iterator[int], build
    ) -> list[dict[str, Any]]:
        if not self._changed(key):
            return self._kept(section, self._saved.get(section) or [], ids)
        return [self._new_row(section, row, ids) for row in build()]

    def _kept(
        self, section: str, rows: list[dict[str, Any]], ids: Iterator[int]
    ) -> list[dict[str, Any]]:
        """Rows as read: exactly so for a saved template, detached for a new one."""
        if self.id is not None:
            return copy.deepcopy(rows)
        return detached(rows, section, ids)

    def _new_row(
        self, section: str, row: dict[str, Any], ids: Iterator[int]
    ) -> dict[str, Any]:
        """A row that is not in the database yet, under this template."""
        parent = {"templateId": self.id} if self.id is not None else {}
        return {**DETACHED[section], **parent, **row, "id": next(ids)}

    def describe(self) -> None:
        """Display a summary of this template.

        Example:
            >>> tpl.describe()
        """
        print("=" * 60)
        print(f"TEMPLATE: {self.name}")
        print("=" * 60)
        print(f"  ID:          {self.id or '(unsaved)'}")
        print(f"  Active:      {self.active_flag}")
        print(f"  Pricing:     {self.pricing_flag}")
        print(f"  Currency:    {self.currency_code}, {self.sim_years} years")
        print(f"  As of:       {self.as_of_date or '(current deals)'}")
        print(
            f"  Models:      {len(self.model_ids)} ({self.model_view or 'no matching view'})"
        )
        print(f"  Dimensions:  {', '.join(self.dimensions) or '(none)'}")
        print(f"  Scenarios:   {', '.join(self._scenario_names()) or '(none)'}")
        for label, groups in (
            ("Groups", self.groups),
            ("Pricing", self.pricing_groups),
        ):
            print(f"  {label + ':':<12} {len(groups)}")
            for name, formulas in groups.items():
                print(
                    f"    {name}: {' && '.join(formulas) if isinstance(formulas, list) else formulas}"
                )
        if self.id is not None and self.has_unsaved_changes:
            print("  (unsaved changes)")
        print()
        print("METHODS:")
        print("  layers()            -> Rows")
        print("  copy(name)          -> Template")
        print("  save(folder=None)   -> Template")
        print("  run()               -> RollupRun")
        print("=" * 60)
        print()

    def __repr__(self) -> str:
        """String representation of the template."""
        return f"Template(id={self.id}, name={self.name!r})"


def _normalized(groups: Mapping[str, str | Sequence[str]]) -> dict[str, list[str]]:
    """group name -> list of criteria; a single formula may be given as a string."""
    return {
        name: [formulas] if isinstance(formulas, str) else list(formulas)
        for name, formulas in groups.items()
    }


def _named_groups(
    groups: list[dict[str, Any]], deal_selection: bool
) -> dict[str, dict[str, Any]]:
    """One section's stored groups by name. Names are not unique in the
    database, so a repeated name gets " (2)", " (3)" and so on."""
    named: dict[str, dict[str, Any]] = {}
    for group in groups:
        if bool(group.get("dealSelectionFlag")) != deal_selection:
            continue
        unique = name = group["name"]
        for n in itertools.count(2):
            if unique not in named:
                break
            unique = f"{name} ({n})"
        named[unique] = group
    return named


def _read_group(group: dict[str, Any]) -> list[str]:
    """A stored group's criteria as the SDK shows them, negations written into
    the formulas: a negated group reads as one criterion ``!((c1) && (c2))``."""
    formulas = [
        read_criterion(c)
        for c in sorted_by_order(group.get("templateGroupCriterias") or [])
    ]
    if group.get("notFlag"):
        formulas = ["!(" + " && ".join(f"({f})" for f in formulas) + ")"]
    return formulas
