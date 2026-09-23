"""Enumerations the SDK accepts as arguments. Each member equals its string
value, so ``LossType.YELT`` and ``"yelt"`` are interchangeable."""

from enum import StrEnum
from typing import NoReturn


class _StrEnum(StrEnum):
    @classmethod
    def _missing_(cls, value: object) -> NoReturn:
        raise ValueError(
            f"{value!r} is not a valid {cls.__name__}; expected one of {[m.value for m in cls]}"
        )


class LossType(_StrEnum):
    """The kind of loss file a loss set holds."""

    YELT = "yelt"
    ELT = "elt"


class LossFileType(_StrEnum):
    """A portfolio's loss files: annual and occurrence exceedance, and event losses."""

    AEP = "aep"
    OEP = "oep"
    EVENT = "event"
