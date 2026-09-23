"""Role reference data model"""

from typing import Any

from .base import BaseModel


class Role(BaseModel):
    """An Identity role.

    Identity keys roles by GUID string, not by integer, so ``id`` is a ``str``
    here where every other reference model returns an ``int``.
    """

    @property
    def id(self) -> str | None:  # type: ignore[override]
        """Role ID (an Identity GUID)."""
        value: Any = self._data.get("id")
        return str(value) if value is not None else None

    @property
    def program_role_flag(self) -> bool | None:
        """Whether the role is assignable on a deal.

        Identity holds every role in the platform; only the ones flagged here
        appear in a deal's Roles panel.
        """
        return self._data.get("programRoleFlag")

    @property
    def sort_order(self) -> int | None:
        """Display order in the deal's Roles panel."""
        return self._data.get("sortOrder")
