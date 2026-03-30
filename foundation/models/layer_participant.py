"""Layer participant model"""

from typing import Dict, Any, Optional, TYPE_CHECKING
from ..ref_data import ReferenceDataCache

if TYPE_CHECKING:
    from .participant import Participant
    from .participant_status import ParticipantStatus


class LayerParticipant:
    """Represents a layer participant with enriched data."""

    def __init__(self, participant_data: Dict[str, Any], ref_cache: ReferenceDataCache):
        self._data = participant_data
        self._ref_cache = ref_cache

        # Enrich with participant data
        self._participant = ref_cache.get_participant(
            participant_data.get("participantId")
        )
        self._status = ref_cache.get_participant_status(
            participant_data.get("participantStatusId")
        )

    @property
    def name(self) -> Optional[str]:
        """Participant name."""
        return self._participant.name if self._participant else None

    @property
    def participant(self) -> Optional["Participant"]:
        """Full participant reference data model."""
        return self._participant

    @property
    def share1(self) -> Optional[float]:
        """Participant share 1."""
        return self._data.get("share1")

    @property
    def share2(self) -> Optional[float]:
        """Participant share 2."""
        return self._data.get("share2")

    @property
    def share3(self) -> Optional[float]:
        """Participant share 3."""
        return self._data.get("share3")

    @property
    def status(self) -> Optional["ParticipantStatus"]:
        """Participant status reference data model."""
        return self._status

    def __str__(self) -> str:
        """Return the participant name."""
        return str(self._participant) if self._participant else "Unknown Participant"

    def describe(self) -> None:
        """
        Display a comprehensive overview of the layer participant.
        
        Shows participant details including shares and status.
        
        Example:
            >>> layer.participants[0].describe()
        """
        print("=" * 80)
        print(f"LAYER PARTICIPANT: {self.name or 'Unknown'}")
        print("=" * 80)
        
        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-'*30} {'-'*40}")
        print(f"  {'name':<30} {str(self.name or 'None')[:38]:<40}")
        print(f"  {'share1':<30} {str(self.share1 or 'None'):<40}")
        print(f"  {'share2':<30} {str(self.share2 or 'None'):<40}")
        print(f"  {'share3':<30} {str(self.share3 or 'None'):<40}")
        
        if self._status:
            print(f"  {'status.name':<30} {str(self._status.name or 'None')[:38]:<40}")
        else:
            print(f"  {'status':<30} {'None':<40}")
        
        if self._participant:
            print(f"\nPARTICIPANT DETAILS:")
            print(f"  {'Property':<30} {'Value':<40}")
            print(f"  {'-'*30} {'-'*40}")
            print(f"  {'participant.id':<30} {str(self._participant.id):<40}")
            print(f"  {'participant.name':<30} {str(self._participant.name or 'None')[:38]:<40}")
            print(f"  {'participant.external_ref':<30} {str(self._participant.external_ref or 'None')[:38]:<40}")
            print(f"  {'participant.active_flag':<30} {str(self._participant.active_flag):<40}")
        
        print("\n" + "=" * 80 + "\n")
