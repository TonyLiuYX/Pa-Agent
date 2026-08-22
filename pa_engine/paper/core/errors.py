"""Typed errors for the small paper-account boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class PaperError(Exception):
    """Base error for invalid paper-account inputs."""

    code = "paper_error"

    def __init__(self, message: str, *, code: str | None = None, path: str = "$", details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        self.path = path
        self.details = details


class InvalidPaperInput(PaperError):
    code = "invalid_input"


class InvalidDecimal(PaperError):
    code = "invalid_decimal"


class InvalidLevelOrder(PaperError):
    code = "invalid_level_order"


class EngineFieldLeak(PaperError):
    code = "engine_field_leak"


@dataclass(frozen=True)
class PayloadIssue:
    code: str
    path: str
    message: str


class IdempotencyConflict(PaperError):
    code = "idempotency_conflict"


__all__ = [
    "EngineFieldLeak",
    "IdempotencyConflict",
    "InvalidDecimal",
    "InvalidLevelOrder",
    "InvalidPaperInput",
    "PaperError",
    "PayloadIssue",
]
