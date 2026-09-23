"""A template's scenarios, read from and written to the Portfolio module's rows."""

import copy
import json
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from .field_adjustment import FieldAdjustment
from .loss_adjustment import LossAdjustment
from .loss_scale import LossScale
from .scenario import Scenario
from .template_rows import DETACHED, checked, detached, read_criterion, sorted_by_order

if TYPE_CHECKING:
    from .foundation_client import FoundationClient

#: What the Portfolio UI stores as a loss adjustment's formula.
LOSS_ADJUSTMENT_FORMULA = "Loss Adjustment"

#: Column limit of Scenario.Name and ScenarioAdjustCriteria.Name.
_NAME_LENGTH = 100


def read_scenarios(
    rows: Sequence[dict[str, Any]], client: "FoundationClient"
) -> list[Scenario]:
    """Stored scenario rows as Scenario objects, adjustments in their order.

    A criterion stored negated reads as ``!(formula)``.
    """
    converter = client._portfolio_reference().formulas
    return [
        Scenario(
            row["name"],
            [
                _read_adjustment(a, converter, row["name"])
                for a in sorted_by_order(row.get("scenarioAdjusts") or [])
            ],
            _row_id=row.get("id"),
        )
        for row in rows
    ]


def _read_adjustment(
    row: dict[str, Any], converter: Any, scenario: str
) -> FieldAdjustment | LossAdjustment:
    if row.get("lossAdjustmentFlag"):
        try:
            data = json.loads(row.get("jsonFormula") or "")
        except json.JSONDecodeError:
            data = None
        if not isinstance(data, dict):
            raise ValueError(
                f"Scenario {scenario!r}: loss adjustment {row.get('id')} holds no valid filter; "
                "fix it in the Portfolio UI."
            )
        # A missing weight reads as the platform reads it: 0.
        return LossAdjustment(
            weight=data.get("weight", 0.0),
            scales=[
                LossScale(
                    scale.get("regionId"),
                    scale.get("perilId"),
                    scale.get("modelId"),
                    scale.get("lobId"),
                    scale.get("weight", 0.0),
                )
                for scale in data.get("programLossFilterScales") or []
            ],
            _row_id=row.get("id"),
        )
    return FieldAdjustment(
        converter.field_name(row.get("field")),
        row["formula"],
        [read_criterion(c) for c in row.get("scenarioAdjustCriterias") or []],
        _row_id=row.get("id"),
    )


def scenario_row(
    scenario: Scenario,
    client: "FoundationClient",
    ids: Iterator[int],
    template_id: int | None,
    adjustments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """A new scenario row, with new rows for its adjustments unless given."""
    if len(scenario.name) > _NAME_LENGTH:
        raise ValueError(
            f"Scenario name {scenario.name!r} is longer than {_NAME_LENGTH} characters."
        )
    scenario_id = next(ids)
    if adjustments is None:
        adjustments = [
            adjustment_row(adjustment, order, scenario.name, client, ids)
            for order, adjustment in enumerate(scenario.adjustments, 1)
        ]
    return {
        **DETACHED["scenarios"],
        "id": scenario_id,
        "name": scenario.name,
        "templateId": template_id,
        "scenarioAdjusts": adjustments,
    }


def adjustment_row(
    adjustment: FieldAdjustment | LossAdjustment,
    order: int,
    scenario: str,
    client: "FoundationClient",
    ids: Iterator[int],
) -> dict[str, Any]:
    """A new row for an adjustment, the ``order``-th of scenario ``scenario``."""
    where = f"scenario {scenario!r}, adjustment {order}"
    if isinstance(adjustment, LossAdjustment):
        row = _loss_adjustment_row(adjustment, client, where)
    elif isinstance(adjustment, FieldAdjustment):
        row = {**_field_head(adjustment, client, where), "scenarioAdjustCriterias": []}
        row["scenarioAdjustCriterias"] = _criteria_rows(
            adjustment, None, client, ids, where
        )
    else:
        raise TypeError(
            f"{where}: expected a FieldAdjustment or LossAdjustment, got {type(adjustment).__name__}."
        )
    return {**row, **DETACHED["scenarioAdjusts"], "id": next(ids), "sortOrder": order}


def changed_adjustment_row(
    adjustment: FieldAdjustment | LossAdjustment,
    saved: dict[str, Any],
    read: FieldAdjustment | LossAdjustment,
    order: int,
    scenario: str,
    client: "FoundationClient",
    ids: Iterator[int],
    keep_ids: bool,
) -> dict[str, Any]:
    """A stored adjustment that changed. With ``keep_ids`` it keeps its row;
    either way a formula written as before keeps its stored conversion, and a
    criterion written as before keeps its row."""
    where = f"scenario {scenario!r}, adjustment {order}"
    row = (
        copy.deepcopy(saved)
        if keep_ids
        else detached([saved], "scenarioAdjusts", ids)[0]
    )
    row["sortOrder"] = order
    if isinstance(adjustment, LossAdjustment) and isinstance(read, LossAdjustment):
        row.update(_loss_adjustment_row(adjustment, client, where))
    elif isinstance(adjustment, FieldAdjustment) and isinstance(read, FieldAdjustment):
        if (adjustment.field, adjustment.formula) != (read.field, read.formula):
            row.update(_field_head(adjustment, client, where))
        kept = row.get("scenarioAdjustCriterias") or []
        row["scenarioAdjustCriterias"] = _criteria_rows(
            adjustment,
            {read_criterion(c): c for c in kept},
            client,
            ids,
            where,
            row["id"] if keep_ids else 0,
        )
    else:
        new = adjustment_row(adjustment, order, scenario, client, ids)
        return {**new, "id": row["id"], "scenarioId": row["scenarioId"]}
    return row


def _field_head(
    adjustment: FieldAdjustment, client: "FoundationClient", where: str
) -> dict[str, Any]:
    converter = client._portfolio_reference().formulas
    field = converter.field(adjustment.field, where)
    if field is None:
        raise ValueError(
            f"{where}: no formula field is named {adjustment.field!r}; "
            "client.criteria_fields lists them."
        )
    return {
        "field": field["dealTokenId"],
        "formula": adjustment.formula,
        "jsonFormula": checked(converter.convert(adjustment.formula, where), where),
        "lossAdjustmentFlag": False,
    }


def _criteria_rows(
    adjustment: FieldAdjustment,
    kept: dict[str, dict[str, Any]] | None,
    client: "FoundationClient",
    ids: Iterator[int],
    where: str,
    adjust_id: int = 0,
) -> list[dict[str, Any]]:
    """The adjustment's criteria; one written as a stored one was keeps its row."""
    converter = client._portfolio_reference().formulas
    criteria = (
        [adjustment.where] if isinstance(adjustment.where, str) else adjustment.where
    )
    rows = []
    for number, criterion in enumerate(criteria, 1):
        stored = (kept or {}).pop(criterion, None)
        if stored is not None:
            rows.append(
                stored
                if adjust_id
                else {**stored, **DETACHED["scenarioAdjustCriterias"], "id": next(ids)}
            )
            continue
        rows.append(
            {
                **DETACHED["scenarioAdjustCriterias"],
                "id": next(ids),
                "scenarioAdjustId": adjust_id,
                "name": criterion[:_NAME_LENGTH],
                "criteriaFormula": checked(criterion, where),
                "jsonFormula": checked(
                    converter.convert(criterion, f"{where}, criterion {number}"), where
                ),
                "notFlag": False,
            }
        )
    return rows


def _loss_adjustment_row(
    adjustment: LossAdjustment, client: "FoundationClient", where: str
) -> dict[str, Any]:
    reference = client._portfolio_reference()
    scales = []
    for number, scale in enumerate(adjustment.scales, 1):
        at = f"{where}, scale {number}"
        if scale.model is not None and (
            scale.region is not None or scale.peril is not None
        ):
            raise ValueError(
                f"{at}: a model cannot be combined with a region or a peril."
            )
        if all(
            value is None
            for value in (scale.region, scale.peril, scale.model, scale.lob)
        ):
            raise ValueError(f"{at}: give a region, peril, model or line of business.")
        scales.append(
            {
                "id": 0,
                "programLossFilterId": 0,
                "regionId": _resolve(client.get_region, scale.region, "region", at),
                "perilId": _resolve(client.get_peril, scale.peril, "peril", at),
                "modelId": _model_id(reference, scale.model, at),
                "lobId": _resolve(reference.lob, scale.lob, "line of business", at),
                "weight": scale.weight,
            }
        )
    loss_filter = {
        # The shape the Portfolio UI's dialog writes; the platform reads the weights.
        "weight": adjustment.weight,
        "regionPerilIncludeFlag": True,
        "programLossFilterDetails": [],
        "programLossFilterScales": scales,
        "programLossFilterSets": [],
    }
    return {
        "field": None,
        "formula": LOSS_ADJUSTMENT_FORMULA,
        "jsonFormula": checked(json.dumps(loss_filter), where),
        "lossAdjustmentFlag": True,
        "scenarioAdjustCriterias": [],
    }


def _model_id(reference: Any, value: int | str | None, where: str) -> int | None:
    if value is None or isinstance(value, int):
        return value
    try:
        return reference.model_id(value)
    except ValueError as exc:
        raise ValueError(f"{where}: {exc}") from None


def _resolve(find: Any, value: int | str | None, noun: str, where: str) -> int | None:
    """An ID as given, or the ID a name resolves to."""
    if value is None or isinstance(value, int):
        return value
    found = find(value)
    if found is None:
        raise ValueError(f"{where}: no {noun} is {value!r}.")
    return found.id
