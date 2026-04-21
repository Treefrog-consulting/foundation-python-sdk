"""Custom exception types raised by the Foundation SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models.loss_set_result import LossGroupUploadResult


class LossSetValidationError(ValueError):
    """Raised from LossGroup.add_yelt / add_elt when input fails validation
    (missing columns, unknown vendor/variant/etc., invalid sim_years, wrong
    DataFrame type). Raised synchronously — no network I/O has started yet."""


class LossGroupAttachError(RuntimeError):
    """Raised from LossGroup.upload() when all per-set uploads completed but
    the final POST /api/Programs/{id}/AttachLossGroup call failed. The
    partial_result attribute carries the populated LossGroupUploadResult so
    the caller can retry attach without re-uploading."""

    def __init__(self, message: str, partial_result: "LossGroupUploadResult"):
        super().__init__(message)
        self.partial_result = partial_result
