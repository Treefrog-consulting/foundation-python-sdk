"""Collection for the saved versions of a deal."""

from datetime import UTC, datetime
from typing import Any

from ._frames import RecordFrames
from .program_version import ProgramVersion

# Sorts before every real save date, so versions carrying no usable date rank
# last instead of displacing genuine ones.
_NO_SAVE_DATE = datetime.min.replace(tzinfo=UTC)


class ProgramVersions(RecordFrames):
    """Collection of a deal's saved versions, newest first.

    ``as_pandas()`` / ``as_polars()`` give the versions in columns: id,
    program_id, name, description, save_date, user_id, info_tag.

    Example:
        >>> versions = program.versions
        >>> versions.describe()
        >>> previous = program.get_version(versions[1].id)
        >>> df = versions.as_pandas()
        >>> df = versions.as_polars()
    """

    def __init__(self, versions_data: list[dict[str, Any]]):
        """Initialize collection from an /api/ProgramVersions response.

        Args:
            versions_data: List of version dictionaries from the API.
        """
        self._versions = [
            ProgramVersion.from_dict(data) for data in versions_data or []
        ]
        # Newest first by save date, with the ID only breaking ties. The
        # platform selects a deal's newest version by save date too, and rows
        # do exist whose ID order contradicts their dates — ordering by ID
        # would make this collection disagree with the server about which
        # version is current.
        self._versions.sort(
            key=lambda v: (v.save_date or _NO_SAVE_DATE, v.id), reverse=True
        )
        self._by_id = {v.id: v for v in self._versions}

    def get_by_id(self, id: int) -> ProgramVersion | None:
        """Get a version by its ID.

        Args:
            id: The version ID to search for.

        Returns:
            ProgramVersion instance if found, None otherwise.
        """
        return self._by_id.get(id)

    def get_by_name(self, name: str) -> ProgramVersion | None:
        """Get the newest version with the given name (case-insensitive).

        Version names are not unique, so the most recent match wins.

        Args:
            name: The version name to search for.

        Returns:
            ProgramVersion instance if found, None otherwise.
        """
        target = name.lower()
        for version in self._versions:
            if (version.name or "").lower() == target:
                return version
        return None

    @property
    def latest(self) -> ProgramVersion | None:
        """The most recently saved version, or None if the deal has none."""
        return self._versions[0] if self._versions else None

    def __iter__(self):
        """Iterate over all versions, newest first."""
        return iter(self._versions)

    def __len__(self) -> int:
        """Get the number of versions."""
        return len(self._versions)

    def __getitem__(self, index: int) -> ProgramVersion:
        """Get a version by index (0 is the newest)."""
        return self._versions[index]

    def _to_records(self) -> list[dict]:
        return [
            {
                "id": v.id,
                "program_id": v.program_id,
                "name": v.name,
                "description": v.description,
                "save_date": v.save_date,
                "user_id": v.user_id,
                "info_tag": v.info_tag,
            }
            for v in self._versions
        ]

    def describe(self) -> None:
        """Display a summary of the deal's saved versions."""
        print("=" * 80)
        print(f"DEAL VERSIONS ({len(self._versions)} saved)")
        print("=" * 80)
        print()
        print(f"  {'ID':<8} {'Saved (UTC)':<26} {'Name':<40}")
        print(f"  {'-' * 8} {'-' * 26} {'-' * 40}")
        for v in self._versions:
            saved = v.save_date.strftime("%Y-%m-%d %H:%M:%S") if v.save_date else ""
            print(f"  {v.id!s:<8} {saved:<26} {str(v.name or '')[:38]:<40}")
        print()
        print("METHODS:")
        print("  get_by_id(id)      -> ProgramVersion")
        print("  get_by_name(name)  -> ProgramVersion")
        print("  as_pandas()        -> pandas.DataFrame")
        print("  as_polars()        -> polars.DataFrame")
        print("=" * 80)
        print()

    def __repr__(self) -> str:
        """String representation of the collection."""
        return f"ProgramVersions({len(self._versions)} versions)"
