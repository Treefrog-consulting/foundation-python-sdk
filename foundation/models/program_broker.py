"""Program broker model"""

from typing import Dict, Any, Optional, TYPE_CHECKING
from ..ref_data import ReferenceDataCache

if TYPE_CHECKING:
    from .broker import Broker
    from .broker_office import BrokerOffice


class ProgramBroker:
    """Represents a program broker with enriched data."""

    def __init__(self, broker_data: Dict[str, Any], ref_cache: ReferenceDataCache):
        self._data = broker_data
        self._ref_cache = ref_cache

        # Enrich with broker and office data
        self._broker = ref_cache.get_broker(broker_data.get("brokerId"))
        self._office = ref_cache.get_broker_office(broker_data.get("brokerOfficeId"))

    @property
    def name(self) -> Optional[str]:
        """Broker name."""
        return self._broker.name if self._broker else None

    @property
    def broker(self) -> Optional["Broker"]:
        """Full broker reference data model."""
        return self._broker

    @property
    def primary_flag(self) -> bool:
        """Whether this is the primary broker."""
        return self._data.get("primaryFlag", False)

    @property
    def office(self) -> Optional["BrokerOffice"]:
        """Broker office reference data model."""
        return self._office

    def __str__(self) -> str:
        """Return the broker name."""
        return str(self._broker) if self._broker else "Unknown Broker"

    def describe(self) -> None:
        """
        Display a comprehensive overview of the program broker.
        
        Shows broker details including name, office, and primary flag.
        
        Example:
            >>> program.brokers[0].describe()
        """
        print("=" * 80)
        print(f"PROGRAM BROKER: {self.name or 'Unknown'}")
        print("=" * 80)
        
        print("\nPROPERTIES:")
        print(f"  {'Property':<30} {'Value':<40}")
        print(f"  {'-'*30} {'-'*40}")
        print(f"  {'name':<30} {str(self.name or 'None')[:38]:<40}")
        print(f"  {'primary_flag':<30} {str(self.primary_flag):<40}")
        
        if self.office:
            print(f"  {'office.name':<30} {str(self.office.name or 'None')[:38]:<40}")
        else:
            print(f"  {'office':<30} {'None':<40}")
        
        if self._broker:
            print(f"\nBROKER DETAILS:")
            print(f"  {'Property':<30} {'Value':<40}")
            print(f"  {'-'*30} {'-'*40}")
            print(f"  {'broker.id':<30} {str(self._broker.id):<40}")
            print(f"  {'broker.name':<30} {str(self._broker.name or 'None')[:38]:<40}")
            print(f"  {'broker.external_ref':<30} {str(self._broker.external_ref or 'None')[:38]:<40}")
            print(f"  {'broker.active_flag':<30} {str(self._broker.active_flag):<40}")
        
        print("\n" + "=" * 80 + "\n")
