"""Stable, JSON-native errors and result values for G018.

The finalizer is an integration boundary.  Callers therefore receive typed
rejections rather than implementation exceptions or tracebacks.  Messages are
short and do not echo arbitrary model output.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class DecisionIssue:
    """One deterministic validation issue."""

    code: str
    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "path": self.path, "message": self.message}


@dataclass
class DecisionValidationResult:
    """Validation result used by parser and grounding helpers."""

    valid: bool
    code: str = "ok"
    message: str = "decision candidate is valid"
    candidate: dict[str, Any] | None = None
    issues: list[DecisionIssue] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.valid = bool(self.valid)
        self.candidate = copy.deepcopy(self.candidate)
        self.issues = [item if isinstance(item, DecisionIssue) else DecisionIssue(**dict(item)) for item in self.issues]
        self.details = copy.deepcopy(self.details)

    def __bool__(self) -> bool:
        return self.valid

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "code": self.code,
            "message": self.message,
            "candidate": copy.deepcopy(self.candidate),
            "issues": [issue.to_dict() for issue in self.issues],
            "details": copy.deepcopy(self.details),
        }


class DecisionValidationError(ValueError):
    """Raised by the strict parser when a candidate is not valid."""

    def __init__(self, code: str, message: str, *, path: str = "$", details: Mapping[str, Any] | None = None):
        self.code = str(code)
        self.message = str(message)
        self.path = str(path)
        self.details = copy.deepcopy(dict(details or {}))
        super().__init__(f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "path": self.path, "details": copy.deepcopy(self.details)}


@dataclass
class DecisionFinalization:
    """Result of the G018 parser/grounding/G014 mapping pipeline."""

    valid: bool
    code: str = "ok"
    message: str = "decision finalized"
    candidate: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    instruction: dict[str, Any] | None = None
    issues: list[DecisionIssue] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.valid = bool(self.valid)
        self.candidate = copy.deepcopy(self.candidate)
        self.result = copy.deepcopy(self.result)
        self.instruction = copy.deepcopy(self.instruction)
        self.issues = [item if isinstance(item, DecisionIssue) else DecisionIssue(**dict(item)) for item in self.issues]
        self.details = copy.deepcopy(self.details)
        # A rejection must never accidentally carry executable output.
        if not self.valid:
            self.result = None
            self.instruction = None

    @property
    def ok(self) -> bool:
        return self.valid

    def __bool__(self) -> bool:
        return self.valid

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "code": self.code,
            "message": self.message,
            "candidate": copy.deepcopy(self.candidate),
            "result": copy.deepcopy(self.result),
            "instruction": copy.deepcopy(self.instruction),
            "issues": [issue.to_dict() for issue in self.issues],
            "details": copy.deepcopy(self.details),
        }


def issue(code: str, path: str, message: str) -> DecisionIssue:
    """Create a short stable issue value."""

    return DecisionIssue(str(code), str(path or "$"), str(message))


__all__ = [
    "DecisionFinalization",
    "DecisionIssue",
    "DecisionValidationError",
    "DecisionValidationResult",
    "issue",
]
