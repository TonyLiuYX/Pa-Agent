"""Typed callback boundaries shared with G024-G026.

The scheduler accepts these small result objects or equivalent booleans,
mappings and objects with the documented attributes.  The flexibility keeps
the boundary convenient for deterministic fakes while preserving typed
transition semantics in the scheduler.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol


@dataclass(frozen=True)
class OpenRequestResult:
    accepted: bool = False
    status: str = "rejected"
    position_id: str | None = None
    pending_id: str | None = None
    fresh_price_rejected: bool = False
    exhausted: bool = False
    reason: str | None = None

    def __post_init__(self) -> None:
        status = str(self.status).lower()
        if status in {"exhausted", "zero_balance", "no_capital"} and not self.exhausted:
            object.__setattr__(self, "exhausted", True)
        if status in {"accepted", "success", "filled", "open"} and not self.accepted and not self.exhausted:
            object.__setattr__(self, "accepted", True)

    @classmethod
    def accepted_result(cls, position_id: str | None = None) -> "OpenRequestResult":
        return cls(True, "accepted", position_id=position_id)

    @classmethod
    def rejected_result(cls, *, fresh_price: bool = False, reason: str | None = None) -> "OpenRequestResult":
        return cls(False, "rejected", fresh_price_rejected=fresh_price, reason=reason)

    @classmethod
    def exhausted_result(cls, reason: str | None = None) -> "OpenRequestResult":
        return cls(False, "exhausted", exhausted=True, reason=reason)


@dataclass(frozen=True)
class PendingFillResult:
    filled: bool = True
    position_id: str | None = None
    status: str = "filled"


@dataclass(frozen=True)
class PendingExpiryResult:
    expired: bool = True
    status: str = "expired"


@dataclass(frozen=True)
class PositionCloseResult:
    closed: bool = True
    exhausted: bool = False
    status: str = "closed"


class OpenRequester(Protocol):
    def __call__(self, strategy_id: str, instruction: Mapping[str, Any], *, generation: int, idempotency_key: str) -> Any:
        ...


class PendingCancellation(Protocol):
    def __call__(self, strategy_id: str, pending_id: str, *, generation: int) -> Any:
        ...


class PendingExpiryCallback(Protocol):
    def __call__(self, strategy_id: str, pending_id: str, *, generation: int) -> Any:
        ...


class PendingFillCallback(Protocol):
    def __call__(self, strategy_id: str, pending_id: str, *, generation: int, position_id: str | None = None) -> Any:
        ...


class PositionCloseCallback(Protocol):
    def __call__(self, strategy_id: str, position_id: str, *, generation: int, reason: str | None = None) -> Any:
        ...


def callback_value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def normalize_open_result(value: Any) -> OpenRequestResult:
    if isinstance(value, OpenRequestResult):
        return value
    if value is True:
        return OpenRequestResult.accepted_result()
    if value is False or value is None:
        return OpenRequestResult.rejected_result()
    status = str(callback_value(value, "status", "" if value is None else "rejected")).lower()
    accepted = bool(callback_value(value, "accepted", status in {"accepted", "success", "filled", "open"}))
    exhausted = bool(callback_value(value, "exhausted", status in {"exhausted", "zero_balance", "no_capital"}))
    fresh = bool(callback_value(value, "fresh_price_rejected", callback_value(value, "fresh_price", False)))
    return OpenRequestResult(
        accepted=accepted and not exhausted,
        status=status or ("accepted" if accepted else "rejected"),
        position_id=callback_value(value, "position_id"),
        pending_id=callback_value(value, "pending_id"),
        fresh_price_rejected=fresh,
        exhausted=exhausted,
        reason=callback_value(value, "reason"),
    )


PaperOpenResult = OpenRequestResult
OpenResult = OpenRequestResult
FillResult = PendingFillResult
ExpiryResult = PendingExpiryResult
CloseResult = PositionCloseResult


__all__ = [
    "OpenRequestResult",
    "OpenResult",
    "OpenRequester",
    "PendingCancellation",
    "PendingExpiryCallback",
    "PendingExpiryResult",
    "PendingFillCallback",
    "PendingFillResult",
    "PaperOpenResult",
    "PositionCloseCallback",
    "PositionCloseResult",
    "CloseResult",
    "ExpiryResult",
    "FillResult",
    "callback_value",
    "normalize_open_result",
]
