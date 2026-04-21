"""Result value classes returned by LossGroup.upload()."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional


LossSetStatus = Literal["success", "warning", "error", "timeout"]


@dataclass
class LossSetResult:
    name: str
    status: LossSetStatus
    loss_set_id: Optional[int] = None
    file_guid: Optional[str] = None
    row_count: Optional[int] = None
    log_file_url: Optional[str] = None
    status_message: Optional[str] = None


@dataclass
class LossGroupUploadResult:
    loss_set_group_id: int
    loss_sets: list[LossSetResult] = field(default_factory=list)
    program_loss_group_id: Optional[int] = None  # None until attach succeeds
    program_version_id: Optional[int] = None

    @property
    def successes(self) -> list[LossSetResult]:
        return [r for r in self.loss_sets if r.status == "success"]

    @property
    def warnings(self) -> list[LossSetResult]:
        return [r for r in self.loss_sets if r.status == "warning"]

    @property
    def errors(self) -> list[LossSetResult]:
        return [r for r in self.loss_sets if r.status == "error"]

    @property
    def timeouts(self) -> list[LossSetResult]:
        return [r for r in self.loss_sets if r.status == "timeout"]
