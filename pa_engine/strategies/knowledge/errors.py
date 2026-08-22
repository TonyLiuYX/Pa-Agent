"""Typed errors and result values for strategy knowledge loading."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Mapping


class StrategyKnowledgeError(ValueError):
    """A deterministic, caller-visible strategy knowledge failure.

    ``code`` is intentionally stable and suitable for runtime policy routing;
    ``details`` contains only JSON-native diagnostic context.
    """

    def __init__(self, code: str, message: str, details: Mapping[str, Any] | None = None):
        self.code = str(code)
        self.message = str(message)
        self.details = copy.deepcopy(dict(details or {}))
        super().__init__(f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": copy.deepcopy(self.details)}


@dataclass
class PacketLoadOutcome:
    """Typed result returned by ``Registry.load_packet``.

    Payload dictionaries are copied at construction and by ``to_dict`` so a
    caller cannot mutate the registry cache through a returned outcome.
    """

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    executable: bool = False
    packet: dict[str, Any] | None = None
    profile_ref: dict[str, Any] | None = None
    evidence_package: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        self.details = copy.deepcopy(self.details)
        self.packet = copy.deepcopy(self.packet)
        self.profile_ref = copy.deepcopy(self.profile_ref)
        self.evidence_package = copy.deepcopy(self.evidence_package)
        self.executable = bool(self.executable and self.code == "ok")

    @property
    def ok(self) -> bool:
        return self.code == "ok" and self.executable

    def __bool__(self) -> bool:
        return self.ok

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": copy.deepcopy(self.details),
            "executable": self.executable,
            "packet": copy.deepcopy(self.packet),
            "profile_ref": copy.deepcopy(self.profile_ref),
            "evidence_package": copy.deepcopy(self.evidence_package),
        }


@dataclass
class ProfileBindingResult:
    """Typed result for profile/packet/artifact lineage validation."""

    valid: bool
    code: str = "ok"
    message: str = "profile lineage is valid"
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.valid = bool(self.valid)
        self.details = copy.deepcopy(self.details)

    def __bool__(self) -> bool:
        return self.valid

    def to_dict(self) -> dict[str, Any]:
        return {"valid": self.valid, "code": self.code, "message": self.message, "details": copy.deepcopy(self.details)}


__all__ = ["PacketLoadOutcome", "ProfileBindingResult", "StrategyKnowledgeError"]
