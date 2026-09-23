"""Custom exception types raised by the Foundation SDK.

Everything the SDK raises derives from :class:`FoundationError`, so a caller can
catch the whole surface with one clause. Each type also keeps the base it had
before 0.12 -- ``requests.HTTPError``, ``ValueError`` or ``RuntimeError`` -- so
existing handlers keep working unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from .models.loss_set_result import LossGroupUploadResult
    from .models.rollup_run import RollupRun

#: Server bodies can be a whole HTML error page; keep enough to identify the
#: failure without turning a traceback into a document.
_MAX_DETAIL = 500


class FoundationError(Exception):
    """Base class for every error raised by the Foundation SDK."""


class FoundationHTTPError(FoundationError, requests.HTTPError):
    """An API call returned a non-2xx response.

    Subclasses identify the common cases. ``response`` and ``request`` are
    carried through from requests, so ``err.response.status_code`` and
    ``err.response.text`` work as they always have.
    """


class AuthenticationError(FoundationHTTPError):
    """401 -- credentials were rejected, or the access token has expired.

    The SDK authenticates once and never refreshes, so a long-running session
    reaches this eventually. Call ``client.authenticate(...)`` again.
    """


class PermissionDeniedError(FoundationHTTPError):
    """403 -- authenticated, but this user may not see or change the resource."""


class NotFoundError(FoundationHTTPError):
    """404 -- no such deal, layer, loss set or route."""


class ValidationError(FoundationHTTPError):
    """400 or 422 -- the server rejected the payload.

    Distinct from :class:`LossSetValidationError`, which the SDK raises locally
    before any request is sent.
    """


class ConflictError(FoundationHTTPError):
    """409 -- the resource changed underneath this request, or already exists."""


class RateLimitError(FoundationHTTPError):
    """429 -- too many requests."""


class ServerError(FoundationHTTPError):
    """5xx -- the platform failed to handle an otherwise valid request."""


_STATUS_MAP: dict[int, type[FoundationHTTPError]] = {
    400: ValidationError,
    401: AuthenticationError,
    403: PermissionDeniedError,
    404: NotFoundError,
    409: ConflictError,
    422: ValidationError,
    429: RateLimitError,
}


def raise_for_status(response: requests.Response) -> None:
    """Raise the mapped :class:`FoundationHTTPError` for a failed response.

    requests' own ``raise_for_status`` reports only the status line and URL, so
    the server's explanation -- which is where Foundation puts its error code
    and message -- never reached the caller.
    """
    if response.ok:
        return

    status = response.status_code
    error_type = _STATUS_MAP.get(
        status, ServerError if status >= 500 else FoundationHTTPError
    )

    detail = ""
    body = response.text.strip()
    if body:
        if len(body) > _MAX_DETAIL:
            body = body[:_MAX_DETAIL] + "..."
        detail = f": {body}"

    # Joined rather than interpolated: an absent reason would leave a double
    # space, and squeezing it afterwards would also re-indent the body this
    # function exists to surface.
    status_line = " ".join(filter(None, (str(status), response.reason)))
    raise error_type(
        f"{status_line} for {response.url}{detail}",
        response=response,
        request=response.request,
    )


class ExportError(FoundationError, RuntimeError):
    """A loss export could not be fetched.

    Keeps RuntimeError as a base: the accessors raised bare RuntimeError before
    0.12, and code written against it must keep working.
    """


class LossSetValidationError(FoundationError, ValueError):
    """Raised from LossGroup.add_yelt / add_elt when input fails validation
    (missing columns, unknown vendor/variant/etc., invalid sim_years, wrong
    DataFrame type). Raised synchronously — no network I/O has started yet."""


class ReadOnlyVersionError(FoundationError, RuntimeError):
    """Raised when code tries to modify a Program or Layer that was retrieved
    at a past version. A historical object exists only to be read: its edits
    could never be saved back, and a modified snapshot would silently change
    the numbers returned by layered_losses / ylt, which compute from the
    program document the SDK uploads."""


class LossGroupAttachError(FoundationError, RuntimeError):
    """Raised from LossGroup.upload() when all per-set uploads completed but
    the final POST /api/Programs/{id}/AttachLossGroup call failed. The
    partial_result attribute carries the populated LossGroupUploadResult so
    the caller can retry attach without re-uploading."""

    def __init__(self, message: str, partial_result: LossGroupUploadResult):
        super().__init__(message)
        self.partial_result = partial_result


class RollupError(FoundationError, RuntimeError):
    """A template run failed, was cancelled, or did not finish in time.

    The ``run`` attribute holds the RollupRun, whose ``jobs`` show each job's
    status and note; the run itself carries on after a timeout.
    """

    def __init__(self, message: str, run: RollupRun | None = None):
        super().__init__(message)
        self.run = run
