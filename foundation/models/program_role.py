"""Program role model"""

from typing import TYPE_CHECKING, Any, Optional

from .base import BaseModel

if TYPE_CHECKING:
    from ..ref_data import ReferenceDataCache
    from .role import Role
    from .user import User


class ProgramRole(BaseModel):
    """One row of a deal's Roles panel: a role, and the user holding it.

    A deal carries a slot for every active program role whether or not anyone
    is assigned, so ``user`` is None on an unfilled slot -- the row itself still
    exists. That mirrors the platform's UI, which lists every role and leaves
    the unassigned ones blank.

    Example:
        >>> for role in program.roles:
        ...     print(f"{role.name}: {role.user_name or '(unassigned)'}")
        Administrator: Ada Lovelace
        Actuary: (unassigned)
    """

    def __init__(
        self,
        role_data: dict[str, Any],
        ref_cache: "ReferenceDataCache",
        role: Optional["Role"] = None,
    ):
        """
        Initialize a program role.

        Args:
            role_data: One ``programRoles`` entry from the deal document. May be
                empty for a role the deal carries no row for.
            ref_cache: Reference data cache
            role: The already-resolved role definition. Passed in because the
                caller walks the role list to build these, so re-resolving here
                would repeat that lookup for every deal.
        """
        super().__init__(role_data)
        # _role has to exist before role_id is read: role_id falls back to it
        # for a slot the deal carries no row for.
        self._role = role
        if self._role is None:
            self._role = ref_cache.get_role(role_data.get("roleId"))
        self._user = ref_cache.get_user(self.user_id)

    @property
    def name(self) -> str | None:
        """Role name, e.g. "Underwriter" (read-only, from reference data)."""
        return self._role.name if self._role else None

    @property
    def role_id(self) -> str | None:
        """Identity role GUID.

        Falls back to the role when the deal carries no row, so an unfilled
        slot still reports the role it stands for.
        """
        value = self._data.get("roleId")
        if value is not None:
            return str(value)
        return self._role.id if self._role else None

    @property
    def role(self) -> Optional["Role"]:
        """Full role reference model, or None when it is not in reference data."""
        return self._role

    @property
    def user_id(self) -> str | None:
        """Identity user GUID, or None when nobody is assigned."""
        value = self._data.get("userId")
        return str(value) if value is not None else None

    @property
    def user(self) -> Optional["User"]:
        """The assigned user, or None when nobody is assigned.

        Also None when a user IS assigned but is absent from the reference data
        this session loaded; ``user_id`` still carries the GUID in that case.
        """
        return self._user

    @property
    def user_name(self) -> str | None:
        """Assigned user's display name, or None when nobody is assigned."""
        return self._user.name if self._user else None

    def __str__(self) -> str:
        """Return "Role: User", the shape the UI's Roles table shows."""
        return f"{self.name or 'Unknown role'}: {self.user_name or '(unassigned)'}"

    def __repr__(self) -> str:
        """String representation."""
        return f"<ProgramRole: {self.name}={self.user_name}>"

    def describe(self) -> None:
        """
        Display an overview of this role assignment.

        Example:
            >>> program.roles[0].describe()
        """
        print("=" * 80)
        print(f"PROGRAM ROLE: {self.name or 'Unknown'}")
        print("=" * 80)

        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-' * 30} {'-' * 40}")
        for prop_name, value in (
            ("name", self.name),
            ("user_name", self.user_name),
            ("role_id", self.role_id),
            ("user_id", self.user_id),
        ):
            print(
                f"  {prop_name:<30} {str(value) if value is not None else 'None':<40}"
            )

        if self._user:
            print("\nUSER DETAILS:")
            print(f"  {'Property':<30} {'Value':<40}")
            print(f"  {'-' * 30} {'-' * 40}")
            for prop_name, value in (
                ("user.first_name", self._user.first_name),
                ("user.last_name", self._user.last_name),
                ("user.email", self._user.email),
                ("user.active_flag", self._user.active_flag),
            ):
                print(
                    f"  {prop_name:<30} {str(value) if value is not None else 'None':<40}"
                )

        print("\n" + "=" * 80 + "\n")
