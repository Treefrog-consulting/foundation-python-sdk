"""Broker office reference data model"""

from .base import BaseModel


class BrokerOffice(BaseModel):
    """Broker office reference data model."""

    @property
    def broker_id(self) -> int | None:
        """Broker ID."""
        return self._data.get("brokerId")

    @property
    def external_ref(self) -> str | None:
        """External reference."""
        return self._data.get("externalRef")

    @property
    def sort_order(self) -> int | None:
        """Sort order."""
        return self._data.get("sortOrder")

    @property
    def active_flag(self) -> bool | None:
        """Whether the office is active."""
        return self._data.get("activeFlag")
