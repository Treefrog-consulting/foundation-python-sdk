"""Foundation Platform Python SDK

A Python SDK for interacting with the Foundation Platform API.
"""

from .enums import LossFileType, LossType
from .exceptions import (
    AuthenticationError,
    ConflictError,
    ExportError,
    FoundationError,
    FoundationHTTPError,
    LossGroupAttachError,
    LossSetValidationError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    ReadOnlyVersionError,
    RollupError,
    ServerError,
    ValidationError,
)
from .models import (
    FieldAdjustment,
    FoundationClient,
    Layer,
    LayeredLosses,
    LossAdjustment,
    LossGroup,
    LossGroupUploadResult,
    LossScale,
    LossSetResult,
    MetricResults,
    ModuleCustomTable,
    NamedCollection,
    Portfolio,
    PortfolioLayeredLosses,
    Program,
    ProgramVersion,
    ProgramVersions,
    ReferenceRun,
    ReferenceRuns,
    ReferenceTable,
    RollupRun,
    Rows,
    RunConfiguration,
    RunConfigurations,
    Scenario,
    Template,
    Templates,
    Ylt,
)

__version__ = "0.14.0"
__all__ = [
    "AuthenticationError",
    "ConflictError",
    "ExportError",
    "FieldAdjustment",
    "FoundationClient",
    "FoundationError",
    "FoundationHTTPError",
    "Layer",
    "LayeredLosses",
    "LossAdjustment",
    "LossFileType",
    "LossGroup",
    "LossGroupAttachError",
    "LossGroupUploadResult",
    "LossScale",
    "LossSetResult",
    "LossSetValidationError",
    "LossType",
    "MetricResults",
    "ModuleCustomTable",
    "NamedCollection",
    "NotFoundError",
    "PermissionDeniedError",
    "Portfolio",
    "PortfolioLayeredLosses",
    "Program",
    "ProgramVersion",
    "ProgramVersions",
    "RateLimitError",
    "ReadOnlyVersionError",
    "ReferenceRun",
    "ReferenceRuns",
    "ReferenceTable",
    "RollupError",
    "RollupRun",
    "Rows",
    "RunConfiguration",
    "RunConfigurations",
    "Scenario",
    "ServerError",
    "Template",
    "Templates",
    "ValidationError",
    "Ylt",
    "get_client",
]


def get_client(
    *,
    email: str,
    password: str,
    api_client_id: str,
    api_client_secret: str,
    base_url: str,
    timeout: float | None = FoundationClient.DEFAULT_TIMEOUT,
) -> FoundationClient:
    """
    Create and authenticate a Foundation Platform client.

    Args:
        email: User email for authentication
        password: User password
        api_client_id: API client ID
        api_client_secret: API client secret
        base_url: Base URL for the API (e.g., "https://your-instance.foundationplatform.com")
        timeout: Per-socket timeout in seconds for every request; None waits forever

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
    client = FoundationClient(base_url=base_url, timeout=timeout)
    client.authenticate(email, password, api_client_id, api_client_secret)
    return client
