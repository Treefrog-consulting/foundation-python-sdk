"""Participant status reference data model"""

from .base import BaseModel


class ParticipantStatus(BaseModel):
    """Participant status reference data model."""

    @property
    def code(self) -> str | None:
        """Status code."""
        return self._data.get("code")

    @property
    def description(self) -> str | None:
        """Status description."""
        return self._data.get("description")

    @property
    def seniority(self) -> int | None:
        """Status seniority."""
        return self._data.get("seniority")

    @property
    def contributes_to_program_score_flag(self) -> bool | None:
        """Whether this status contributes to program score."""
        return self._data.get("contributesToProgramScoreFlag")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the status is active."""
        return self._data.get("activeFlag")
