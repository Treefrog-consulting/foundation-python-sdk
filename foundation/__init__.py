"""Foundation Platform Python SDK

A Python SDK for interacting with the Foundation Platform API.
"""

from .exceptions import LossGroupAttachError, LossSetValidationError
from .models import (
    FoundationClient,
    Layer,
    LossGroup,
    LossGroupUploadResult,
    LossSetResult,
    Program,
    ReferenceRun,
    ReferenceRuns,
    RunConfiguration,
)

__version__ = "0.8.0"
__all__ = [
    "FoundationClient",
    "Program",
    "Layer",
    "ReferenceRun",
    "ReferenceRuns",
    "RunConfiguration",
    "LossSetValidationError",
    "LossGroupAttachError",
    "LossGroup",
    "LossSetResult",
    "LossGroupUploadResult",
]


def get_client(
    email: str, password: str, api_client_id: str, api_client_secret: str, base_url: str
) -> FoundationClient:
    """
    Create and authenticate a Foundation Platform client.

    Args:
        email: User email for authentication
        password: User password
        api_client_id: API client ID
        api_client_secret: API client secret
        base_url: Base URL for the API (e.g., "https://your-instance.foundationplatform.com")

    Returns:
        Authenticated FoundationClient instance

    Example:
        >>> import foundation
        >>> client = foundation.get_client(
        ...     email="user@example.com",
        ...     password="password",
        ...     api_client_id="client_id",
        ...     api_client_secret="client_secret",
        ...     base_url="https://your-instance.foundationplatform.com"
        ... )
        >>> program = client.get_program(2455)
        >>> print(program.name)
    """
    client = FoundationClient(base_url=base_url)
    client.authenticate(email, password, api_client_id, api_client_secret)
    return client
