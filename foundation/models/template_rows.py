"""Row helpers shared by a template's groups and scenarios."""

import copy
from collections.abc import Iterator
from typing import Any

#: The parent references each child row carries, and what a row that is not
#: yet in the database sends for them. The server links a new row through the
#: collection holding it; a stale parent ID would point it at the old parent.
DETACHED = {
    "templateGroups": {"templateId": None, "template": None},
    "templateGroupCriterias": {"templateGroupId": None, "templateGroup": None},
    "templateModels": {"templateId": 0, "template": None},
    "templateDimensions": {"templateId": 0, "template": None, "dimension": None},
    "scenarios": {
        "templateId": None,
        "portfolioId": None,
        "template": None,
        "portfolio": None,
    },
    "scenarioAdjusts": {"scenarioId": 0, "scenario": None},
    "scenarioAdjustCriterias": {"scenarioAdjustId": 0, "scenarioAdjust": None},
}

#: Column limit of every formula and JSON formula column.
FORMULA_LENGTH = 8000


def sorted_by_order(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows in the order the UI shows them: SortOrder, then ID."""
    return sorted(rows, key=lambda row: (row.get("sortOrder") or 0, row.get("id") or 0))


def read_criterion(criterion: dict[str, Any]) -> str:
    """A stored criterion as the SDK shows it, a negation written as ``!(formula)``."""
    formula = criterion["criteriaFormula"]
    return f"!({formula})" if criterion.get("notFlag") else formula


def detached(
    rows: list[dict[str, Any]], section: str, ids: Iterator[int]
) -> list[dict[str, Any]]:
    """Copies of stored rows with new negative IDs and no parent references,
    their child collections likewise."""
    copies = []
    for row in rows:
        fresh = {**copy.deepcopy(row), **DETACHED[section], "id": next(ids)}
        for child in DETACHED:
            if isinstance(fresh.get(child), list):
                fresh[child] = detached(fresh[child], child, ids)
        copies.append(fresh)
    return copies


def checked(text: str, where: str) -> str:
    """Refuse a formula longer than its column holds."""
    if len(text) > FORMULA_LENGTH:
        raise ValueError(f"{where}: longer than {FORMULA_LENGTH} characters.")
    return text
