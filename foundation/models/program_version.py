"""ProgramVersion model"""

from datetime import datetime
from typing import Any

from ..utils import parse_utc_timestamp


class ProgramVersion:
    """A saved version of a deal.

    A version is a timestamped bookmark rather than a copy: the platform
    reconstructs the deal as it stood by replaying its audit history up to the
    version's save date. That date is what identifies the version to the
    server, so the original string is kept alongside the parsed value — the
    API reports finer resolution than a Python datetime can hold.

    Example:
        >>> for version in program.versions:
        ...     print(f"{version.id}: {version.name} ({version.save_date})")
    """

    def __init__(self, version_data: dict[str, Any]):
        """Initialize from an /api/ProgramVersions response row."""
        self._data = version_data or {}

    @classmethod
    def from_dict(cls, version_data: dict[str, Any]) -> "ProgramVersion":
        """Build a ProgramVersion from an API response row."""
        return cls(version_data)

    @property
    def id(self) -> int:
        """Version ID."""
        return self._data["id"]

    @property
    def program_id(self) -> int:
        """ID of the deal this version belongs to."""
        return self._data["programId"]

    @property
    def name(self) -> str | None:
        """Version name as entered by the user who saved it."""
        return self._data.get("name")

    @property
    def description(self) -> str | None:
        """Version description."""
        return self._data.get("description")

    @property
    def info_tag(self) -> str | None:
        """Free-form tag stored with the version."""
        return self._data.get("infoTag")

    @property
    def user_id(self) -> str | None:
        """ID of the user who saved the version."""
        return self._data.get("userId")

    @property
    def save_date(self) -> datetime | None:
        """When the version was saved, as an aware UTC datetime.

        Truncated to microseconds. Use raw_save_date when the exact instant
        matters.
        """
        return parse_utc_timestamp(self._data.get("saveDate"))

    @property
    def raw_save_date(self) -> str:
        """The save date exactly as the API returned it.

        This is the value sent back to the platform to select the version, so
        it must not be round-tripped through a datetime.
        """
        return self._data.get("saveDate") or ""

    def __repr__(self) -> str:
        """String representation of the version."""
        return (
            f"ProgramVersion(id={self.id}, program_id={self.program_id}, "
            f"name={self.name!r}, save_date={self.save_date})"
        )
