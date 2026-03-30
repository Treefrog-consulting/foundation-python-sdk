"""Participant status reference data model"""

from typing import Optional
from .base import BaseModel


class ParticipantStatus(BaseModel):
    """Participant status reference data model."""

    @property
    def code(self) -> Optional[str]:
        """Status code."""
        return self._data.get("code")

    @property
    def description(self) -> Optional[str]:
        """Status description."""
        return self._data.get("description")

    @property
    def seniority(self) -> Optional[int]:
        """Status seniority."""
        return self._data.get("seniority")

    @property
    def contributes_to_program_score_flag(self) -> bool:
        """Whether this status contributes to program score."""
        return self._data.get("contributesToProgramScoreFlag", False)

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the status is active."""
        return self._data.get("activeFlag", False)
