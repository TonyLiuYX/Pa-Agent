"""Immutable domain values returned by the paper-account core."""

from __future__ import annotations

from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping

from .errors import InvalidPaperInput
from .math import INITIAL_BALANCE, PAPER_MATH_VERSION, stable_hash


class _FrozenMap(ABCMapping):
    """Tiny recursively immutable mapping used at event/result boundaries."""

    __slots__ = ("_data",)

    def __init__(self, value: ABCMapping) -> None:
        object.__setattr__(self, "_data", MappingProxyType(dict(value)))

    def __getitem__(self, key: Any) -> Any:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise TypeError("immutable mapping")

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_FrozenMap":
        return self


def _freeze(value: Any) -> Any:
    if isinstance(value, ABCMapping):
        return _FrozenMap({key: _freeze(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(child) for child in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, ABCMapping):
        return {key: _thaw(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_thaw(child) for child in value]
    return value


@dataclass(frozen=True)
class PaperTrade:
    """One whole fractional-share position owned by one strategy."""

    strategy_id: str
    trade_id: str
    symbol: str
    direction: str
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    quantity: Decimal
    planned_r: Decimal
    allocation_fraction: Decimal = Decimal("1")
    paper_math_version: str = PAPER_MATH_VERSION
    immediate: bool = False
    requested_entry_price: Decimal | None = None
    opened: bool = True

    def to_dict(self) -> dict[str, Any]:
        value = {
            "strategy_id": self.strategy_id,
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "target_price": self.target_price,
            "quantity": self.quantity,
            "planned_r": self.planned_r,
            "allocation_fraction": self.allocation_fraction,
            "paper_math_version": self.paper_math_version,
            "immediate": self.immediate,
            "requested_entry_price": self.requested_entry_price,
            "opened": self.opened,
        }
        return value

    @property
    def content_hash(self) -> str:
        return stable_hash(self.to_dict())


@dataclass(frozen=True)
class PendingTrade:
    """Validated conditional entry awaiting G025's touch/expiry decision."""

    strategy_id: str
    pending_id: str
    symbol: str
    direction: str
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    planned_r: Decimal
    requested_entry_price: Decimal
    paper_math_version: str = PAPER_MATH_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "pending_id": self.pending_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "target_price": self.target_price,
            "planned_r": self.planned_r,
            "requested_entry_price": self.requested_entry_price,
            "paper_math_version": self.paper_math_version,
        }

@dataclass(frozen=True)
class AccountSnapshot:
    """Read-only account state projection for downstream state owners."""

    strategy_id: str
    balance: Decimal = INITIAL_BALANCE
    initial_balance: Decimal = INITIAL_BALANCE
    exhausted: bool = False
    open_trade: PaperTrade | None = None
    pending_trade: PendingTrade | None = None
    pending: bool = False
    paper_math_version: str = PAPER_MATH_VERSION

    @property
    def available_balance(self) -> Decimal:
        return self.balance

    @property
    def has_open_position(self) -> bool:
        return self.open_trade is not None

    @property
    def pending_id(self) -> str | None:
        return self.pending_trade.pending_id if self.pending_trade is not None else None

    @property
    def account_id(self) -> str:
        # Account identity is deliberately strategy-scoped and content-free;
        # G027 may persist the same value without introducing an event schema.
        return f"paper:{self.strategy_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "account_id": self.account_id,
            "balance": self.balance,
            "initial_balance": self.initial_balance,
            "available_balance": self.balance,
            "exhausted": self.exhausted,
            "open_trade": self.open_trade.to_dict() if self.open_trade is not None else None,
            "pending_trade": self.pending_trade.to_dict() if self.pending_trade is not None else None,
            "pending": self.pending,
            "paper_math_version": self.paper_math_version,
        }

    @property
    def content_hash(self) -> str:
        return stable_hash(self.to_dict())


@dataclass(frozen=True)
class PaperEvent:
    """A domain event projection, not G027's persisted runtime envelope."""

    event_type: str
    strategy_id: str
    content_hash: str
    event_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    paper_math_version: str = PAPER_MATH_VERSION

    def __post_init__(self) -> None:
        if type(self.event_type) is not str or not self.event_type.strip():
            raise InvalidPaperInput("event_type must be a non-empty string", code="invalid_event_type", path="$.event_type")
        if type(self.strategy_id) is not str or not self.strategy_id.strip():
            raise InvalidPaperInput("strategy_id must be a non-empty string", code="invalid_strategy_id", path="$.strategy_id")
        object.__setattr__(self, "payload", _freeze(self.payload))
        digest = stable_hash(
            {
                "paper_math_version": self.paper_math_version,
                "event_type": self.event_type,
                "strategy_id": self.strategy_id,
                "payload": self.payload,
            }
        )
        if type(self.content_hash) is not str or self.content_hash != digest:
            raise InvalidPaperInput("event content_hash does not match its payload", code="event_hash_mismatch", path="$.content_hash")
        if type(self.event_id) is not str or self.event_id != "pev_" + digest:
            raise InvalidPaperInput("event_id does not match its content_hash", code="event_id_mismatch", path="$.event_id")

    @classmethod
    def make(cls, event_type: str, strategy_id: str, payload: Mapping[str, Any]) -> "PaperEvent":
        projection = {
            "paper_math_version": PAPER_MATH_VERSION,
            "event_type": event_type,
            "strategy_id": strategy_id,
            "payload": dict(payload),
        }
        digest = stable_hash(projection)
        return cls(event_type, strategy_id, digest, "pev_" + digest, payload)

    def __deepcopy__(self, _memo: dict[int, Any]) -> "PaperEvent":
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "paper_math_version": self.paper_math_version,
            "event_type": self.event_type,
            "strategy_id": self.strategy_id,
            "content_hash": self.content_hash,
            "event_id": self.event_id,
            "payload": _thaw(self.payload),
        }

@dataclass(frozen=True)
class TransitionResult:
    """Deterministic typed result shared with G025/G026/G027."""

    status: str
    strategy_id: str
    operation: str
    changed: bool = False
    accepted: bool = False
    replayed: bool = False
    conflict: bool = False
    idempotency_key: str | None = None
    request_hash: str | None = None
    event: PaperEvent | None = None
    account: AccountSnapshot | None = None
    trade: PaperTrade | None = None
    pending_trade: PendingTrade | None = None
    pending_id: str | None = None
    closed_trade: PaperTrade | None = None
    calculation: Mapping[str, Any] | None = None
    settlement: Mapping[str, Any] | None = None
    reason: str | None = None
    code: str | None = None
    fresh_price_rejected: bool = False
    untradeable: bool = False
    exhausted: bool = False
    original_status: str | None = None

    def __post_init__(self) -> None:
        if self.calculation is not None:
            object.__setattr__(self, "calculation", _freeze(self.calculation))
        if self.settlement is not None:
            object.__setattr__(self, "settlement", _freeze(self.settlement))

    def __deepcopy__(self, _memo: dict[int, Any]) -> "TransitionResult":
        return self

    def __bool__(self) -> bool:
        return self.accepted

    @property
    def position(self) -> PaperTrade | None:
        return self.trade or self.closed_trade

    @property
    def position_id(self) -> str | None:
        position = self.position
        return position.trade_id if position is not None else None

    @property
    def trade_id(self) -> str | None:
        return self.position_id

    @property
    def pending(self) -> bool:
        return self.pending_trade is not None or self.status == "pending"

    @property
    def balance_before(self) -> Decimal | None:
        if self.settlement is not None:
            return self.settlement.get("balance_before")
        if self.calculation is not None:
            return self.calculation.get("balance_before")
        return None

    @property
    def balance_after(self) -> Decimal | None:
        if self.settlement is not None:
            return self.settlement.get("balance_after")
        return self.account.balance if self.account is not None else None

    @property
    def closing_balance(self) -> Decimal | None:
        return self.balance_after

    @property
    def quantity(self) -> Decimal | None:
        if self.trade is not None:
            return self.trade.quantity
        if self.closed_trade is not None:
            return self.closed_trade.quantity
        if self.calculation is not None:
            return self.calculation.get("quantity")
        return None

    @property
    def planned_r(self) -> Decimal | None:
        if self.trade is not None:
            return self.trade.planned_r
        if self.calculation is not None:
            return self.calculation.get("planned_r")
        return None

    @property
    def r(self) -> Decimal | None:
        return self.planned_r

    @property
    def gross_pnl(self) -> Decimal | None:
        return self.settlement.get("gross_pnl") if self.settlement is not None else None

    @property
    def applied_pnl(self) -> Decimal | None:
        return self.settlement.get("applied_pnl") if self.settlement is not None else None

    @property
    def net_pnl(self) -> Decimal | None:
        return self.settlement.get("net_pnl") if self.settlement is not None else None

    @property
    def event_hash(self) -> str | None:
        return self.event.content_hash if self.event is not None else None

    @property
    def content_hash(self) -> str | None:
        return self.event_hash or self.request_hash

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "strategy_id": self.strategy_id,
            "operation": self.operation,
            "changed": self.changed,
            "accepted": self.accepted,
            "replayed": self.replayed,
            "conflict": self.conflict,
            "idempotency_key": self.idempotency_key,
            "request_hash": self.request_hash,
            "event": self.event.to_dict() if self.event else None,
            "account": self.account.to_dict() if self.account else None,
            "trade": self.trade.to_dict() if self.trade else None,
            "pending_trade": self.pending_trade.to_dict() if self.pending_trade else None,
            "pending_id": self.pending_id,
            "pending": self.pending,
            "closed_trade": self.closed_trade.to_dict() if self.closed_trade else None,
            "calculation": _thaw(self.calculation) if self.calculation is not None else None,
            "settlement": _thaw(self.settlement) if self.settlement is not None else None,
            "balance_before": self.balance_before,
            "balance_after": self.balance_after,
            "closing_balance": self.closing_balance,
            "quantity": self.quantity,
            "planned_r": self.planned_r,
            "gross_pnl": self.gross_pnl,
            "applied_pnl": self.applied_pnl,
            "net_pnl": self.net_pnl,
            "reason": self.reason,
            "code": self.code,
            "fresh_price_rejected": self.fresh_price_rejected,
            "untradeable": self.untradeable,
            "exhausted": self.exhausted,
            "original_status": self.original_status,
        }

    # Lightweight Mapping-like ergonomics make transition results convenient
    # at callback boundaries without making them mutable dictionaries.
    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


@dataclass(frozen=True)
class PayloadValidationResult:
    valid: bool
    code: str = "ok"
    message: str = "payload is accepted"
    path: str = "$"
    issues: tuple[Any, ...] = ()

    def __bool__(self) -> bool:
        return self.valid

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "code": self.code,
            "message": self.message,
            "path": self.path,
            "issues": [issue.__dict__.copy() if hasattr(issue, "__dict__") else issue for issue in self.issues],
        }

__all__ = [
    "AccountSnapshot",
    "PaperEvent",
    "PendingTrade",
    "PaperTrade",
    "PayloadValidationResult",
    "TransitionResult",
]
