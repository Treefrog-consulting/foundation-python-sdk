"""Broker office reference data model"""

from typing import Optional
from .base import BaseModel


class BrokerOffice(BaseModel):
    """Broker office reference data model."""

    @property
    def broker_id(self) -> Optional[int]:
        """Broker ID."""
        return self._data.get("brokerId")

    @property
    def external_ref(self) -> Optional[str]:
        """External reference."""
        return self._data.get("externalRef")

    @property
    def sort_order(self) -> Optional[int]:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool:
        """Whether the office is active."""
        return self._data.get("activeFlag", False)
