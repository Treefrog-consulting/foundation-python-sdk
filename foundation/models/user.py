"""User reference data model"""

from typing import Any

from .base import BaseModel


class User(BaseModel):
    """An Identity user.

    Identity keys users by GUID string, not by integer, so ``id`` is a ``str``
    here where every other reference model returns an ``int``.
    """

    @property
    def id(self) -> str | None:  # type: ignore[override]
        """User ID (an Identity GUID)."""
        value: Any = self._data.get("id")
        return str(value) if value is not None else None

    @property
    def name(self) -> str | None:
        """Display name: "First Last", as the platform's UI shows it.

        The user record carries no single name field, so this is assembled the
        same way the Deal UI assembles it. None when neither part is set.
        """
        parts = [self.first_name, self.last_name]
        joined = " ".join(p for p in parts if p)
        return joined or None

    @property
    def first_name(self) -> str | None:
        """Given name."""
        return self._data.get("firstName")

    @property
    def middle_name(self) -> str | None:
        """Middle name."""
        return self._data.get("middleName")

    @property
    def last_name(self) -> str | None:
        """Family name."""
        return self._data.get("lastName")

    @property
    def email(self) -> str | None:
        """Email address."""
        return self._data.get("email")
