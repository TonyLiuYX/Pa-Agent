"""Immutable state and transition values for G026 position monitoring."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from collections.abc import Mapping
from typing import Any

from .boundary import canonical_timestamp, freeze, identity_hash, thaw, utc_instant


UTC = timezone.utc


def _stamp(value: datetime | str | None) -> str | None:
    return canonical_timestamp(value) if value is not None else None


def _decimal_text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


@dataclass(frozen=True, slots=True)
class ExitEvent:
    """Hash-bound internal event; it is not G027's persisted envelope."""

    event_type: str
    strategy_id: str
    trade_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    event_hash: str = field(init=False)
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        frozen = freeze(self.payload)
        object.__setattr__(self, "payload", frozen)
        projection = {
            "schema_version": "0.1.0",
            "event_type": self.event_type,
            "strategy_id": self.strategy_id,
            "trade_id": self.trade_id,
            "payload": thaw(frozen),
        }
        digest = identity_hash(projection)
        object.__setattr__(self, "event_hash", digest)
        object.__setattr__(self, "event_id", "pex_" + digest)

    @property
    def content_hash(self) -> str:
        return self.event_hash

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "0.1.0",
            "artifact_kind": "paper_exit_event",
            "event_type": self.event_type,
            "strategy_id": self.strategy_id,
            "trade_id": self.trade_id,
            "event_id": self.event_id,
            "event_hash": self.event_hash,
            "content_hash": self.event_hash,
            "payload": thaw(self.payload),
        }

    as_dict = to_dict

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


@dataclass(frozen=True, slots=True)
class SchedulerCloseNotification:
    """Typed, one-shot notification result returned by the G023 bridge."""

    attempted: bool = False
    accepted: bool = False
    status: str = "not_attempted"
    strategy_id: str | None = None
    trade_id: str | None = None
    generation: int | None = None
    error: str | None = None
    callback_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempted": self.attempted,
            "accepted": self.accepted,
            "status": self.status,
            "strategy_id": self.strategy_id,
            "trade_id": self.trade_id,
            "generation": self.generation,
            "error": self.error,
            "callback_count": self.callback_count,
        }

    as_dict = to_dict


@dataclass(frozen=True, slots=True)
class ExitLineage:
    """Convenient immutable grouping for the original trade/decision refs."""

    trade: Mapping[str, Any] = field(default_factory=dict)
    decision: Mapping[str, Any] = field(default_factory=dict)
    entry_event: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "trade", freeze(self.trade))
        object.__setattr__(self, "decision", freeze(self.decision))
        object.__setattr__(self, "entry_event", freeze(self.entry_event) if self.entry_event is not None else None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade": thaw(self.trade),
            "decision": thaw(self.decision),
            "entry_event": thaw(self.entry_event) if self.entry_event is not None else None,
        }

    as_dict = to_dict


@dataclass(frozen=True, slots=True)
class ExitState:
    """One immutable open/settled position-monitor snapshot.

    ``trade_lineage`` is a copied G024 trade projection and ``decision_lineage``
    is copied from the accepted entry instruction.  Neither is rewritten when
    a later market event settles the position.
    """

    strategy_id: str
    trade_id: str
    symbol: str
    direction: str
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    quantity: Decimal
    trade_lineage: Mapping[str, Any] = field(default_factory=dict)
    decision_lineage: Mapping[str, Any] = field(default_factory=dict)
    entry_event: Mapping[str, Any] | None = None
    entry_fill_interval: tuple[str, str] | None = None
    entry_filled_at: datetime | None = None
    generation: int = 0
    market_feed: Mapping[str, Any] = field(default_factory=dict)
    status: str = "open"
    outcome_status: str = "open"
    terminal_reason: str | None = None
    partial_data: bool = False
    gap_active: bool = False
    gap_intervals: tuple[tuple[str, str | None], ...] = ()
    processed_intervals: tuple[tuple[str, str], ...] = ()
    processed_event_ids: tuple[str, ...] = ()
    processed_watermark: str | None = None
    recovery_lineage: tuple[Mapping[str, Any], ...] = ()
    recovery_attempted: bool = False
    selected_exit: Decimal | None = None
    exit_reason: str | None = None
    ambiguity_reason: str | None = None
    uncertainty: Mapping[str, Any] | None = None
    settlement: Mapping[str, Any] | None = None
    scheduler_notification: Mapping[str, Any] | None = None
    closed_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("entry_price", "stop_price", "target_price"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a positive Decimal")
        if not isinstance(self.quantity, Decimal) or not self.quantity.is_finite() or self.quantity < 0:
            raise ValueError("quantity must be a non-negative Decimal")
        if self.direction == "long" and not (self.stop_price < self.entry_price < self.target_price):
            raise ValueError("long levels must satisfy stop < entry < target")
        if self.direction == "short" and not (self.target_price < self.entry_price < self.stop_price):
            raise ValueError("short levels must satisfy target < entry < stop")
        if self.direction not in {"long", "short"}:
            raise ValueError("direction must be long or short")
        if not isinstance(self.market_feed, Mapping):
            raise ValueError("market_feed must be a mapping")
        if self.entry_fill_interval is None:
            raise ValueError("entry_fill_interval is required for executable exit monitoring")
        if self.entry_fill_interval is not None:
            if not isinstance(self.entry_fill_interval, (tuple, list)) or len(self.entry_fill_interval) != 2:
                raise ValueError("entry_fill_interval must contain start and end")
            start, end = canonical_timestamp(self.entry_fill_interval[0]), canonical_timestamp(self.entry_fill_interval[1])
            if utc_instant(end) <= utc_instant(start):
                raise ValueError("entry_fill_interval must be increasing")
            object.__setattr__(self, "entry_fill_interval", (start, end))
        for name in ("entry_filled_at", "closed_at"):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, datetime) or value.tzinfo is None:
                    raise ValueError(f"{name} must be timezone-aware")
                object.__setattr__(self, name, value.astimezone(UTC))
        if self.processed_watermark is not None:
            object.__setattr__(self, "processed_watermark", canonical_timestamp(self.processed_watermark))
        object.__setattr__(self, "trade_lineage", freeze(self.trade_lineage))
        object.__setattr__(self, "decision_lineage", freeze(self.decision_lineage))
        object.__setattr__(self, "entry_event", freeze(self.entry_event) if self.entry_event is not None else None)
        object.__setattr__(self, "market_feed", freeze(self.market_feed))
        object.__setattr__(self, "gap_intervals", tuple((str(a), None if b is None else str(b)) for a, b in self.gap_intervals))
        object.__setattr__(self, "processed_intervals", tuple((str(a), str(b)) for a, b in self.processed_intervals))
        object.__setattr__(self, "processed_event_ids", tuple(str(value) for value in self.processed_event_ids))
        object.__setattr__(self, "recovery_lineage", tuple(freeze(value) for value in self.recovery_lineage))
        if self.uncertainty is not None:
            object.__setattr__(self, "uncertainty", freeze(self.uncertainty))
        if self.settlement is not None:
            object.__setattr__(self, "settlement", freeze(self.settlement))
        if self.scheduler_notification is not None:
            object.__setattr__(self, "scheduler_notification", freeze(self.scheduler_notification))

    @property
    def terminal(self) -> bool:
        return self.status == "settled"

    @property
    def gap_overlap(self) -> bool:
        return self.gap_active

    @property
    def open(self) -> bool:
        return not self.terminal

    @property
    def trade(self) -> Mapping[str, Any]:
        return self.trade_lineage

    @property
    def position_id(self) -> str:
        return self.trade_id

    @property
    def episode_id(self) -> str:
        return identity_hash(
            {
                "strategy_id": self.strategy_id,
                "trade_id": self.trade_id,
                "generation": self.generation,
                "trade_lineage": thaw(self.trade_lineage),
                "decision_lineage": thaw(self.decision_lineage),
                "entry_event": thaw(self.entry_event) if self.entry_event is not None else None,
                "entry_fill_interval": self.entry_fill_interval,
                "entry_filled_at": self.entry_filled_at,
            }
        )

    @property
    def state_hash(self) -> str:
        return identity_hash(self.to_dict())

    @property
    def content_hash(self) -> str:
        return self.state_hash

    @property
    def state_id(self) -> str:
        return "exs_" + self.state_hash

    @property
    def fill_interval(self) -> tuple[str, str] | None:
        return self.entry_fill_interval

    @property
    def last_processed_watermark(self) -> str | None:
        return self.processed_watermark

    @property
    def uncertainty_codes(self) -> tuple[str, ...]:
        if not isinstance(self.uncertainty, Mapping):
            return ()
        codes = self.uncertainty.get("codes", ())
        return tuple(str(code) for code in codes) if isinstance(codes, (list, tuple)) else ()

    @property
    def recovery_bar_id(self) -> str | None:
        if not self.recovery_lineage:
            return None
        value = self.recovery_lineage[-1]
        return value.get("recovery_bar_id") if isinstance(value, Mapping) else None

    @property
    def recovery_bar_hash(self) -> str | None:
        if not self.recovery_lineage:
            return None
        value = self.recovery_lineage[-1]
        if not isinstance(value, Mapping):
            return None
        return value.get("recovery_bar_hash") or value.get("content_hash")

    @property
    def path_precision(self) -> str | None:
        if not self.recovery_lineage:
            return None
        value = self.recovery_lineage[-1]
        return value.get("path_precision") if isinstance(value, Mapping) else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "0.1.0",
            "artifact_kind": "paper_exit_state",
            "strategy_id": self.strategy_id,
            "trade_id": self.trade_id,
            "position_id": self.trade_id,
            "episode_id": self.episode_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "entry_price": _decimal_text(self.entry_price),
            "stop_price": _decimal_text(self.stop_price),
            "target_price": _decimal_text(self.target_price),
            "quantity": _decimal_text(self.quantity),
            "trade_lineage": thaw(self.trade_lineage),
            "decision_lineage": thaw(self.decision_lineage),
            "entry_event": thaw(self.entry_event) if self.entry_event is not None else None,
            "entry_fill_interval": list(self.entry_fill_interval) if self.entry_fill_interval is not None else None,
            "entry_filled_at": _stamp(self.entry_filled_at),
            "generation": self.generation,
            "market_feed": thaw(self.market_feed),
            "status": self.status,
            "outcome_status": self.outcome_status,
            "terminal_reason": self.terminal_reason,
            "partial_data": self.partial_data,
            "gap_active": self.gap_active,
            "gap_intervals": [list(item) for item in self.gap_intervals],
            "processed_intervals": [list(item) for item in self.processed_intervals],
            "processed_event_ids": list(self.processed_event_ids),
            "processed_watermark": self.processed_watermark,
            "recovery_lineage": [thaw(item) for item in self.recovery_lineage],
            "recovery_attempted": self.recovery_attempted,
            "selected_exit": _decimal_text(self.selected_exit),
            "exit_reason": self.exit_reason,
            "ambiguity_reason": self.ambiguity_reason,
            "uncertainty": thaw(self.uncertainty) if self.uncertainty is not None else None,
            "settlement": thaw(self.settlement) if self.settlement is not None else None,
            "scheduler_notification": thaw(self.scheduler_notification) if self.scheduler_notification is not None else None,
            "closed_at": _stamp(self.closed_at),
        }

    as_dict = to_dict

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __deepcopy__(self, _memo: dict[int, Any]) -> "ExitState":
        return self


@dataclass(frozen=True, slots=True)
class ExitTransition:
    """Typed reducer result crossing the G026 boundary."""

    status: str
    state: ExitState
    changed: bool = False
    accepted: bool = False
    replayed: bool = False
    reason: str | None = None
    code: str | None = None
    event: ExitEvent | None = None
    lineage: Mapping[str, Any] | None = None
    settlement_result: Any = None
    scheduler_callback: SchedulerCloseNotification | None = None
    selected_exit: Decimal | None = None
    exit_reason: str | None = None
    ambiguity_reason: str | None = None
    outcome_status: str | None = None
    not_evaluated: bool = False
    partial_data: bool = False
    gap_overlap: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "lineage", freeze(self.lineage) if self.lineage is not None else None)
        if self.settlement_result is not None and isinstance(self.settlement_result, Mapping):
            object.__setattr__(self, "settlement_result", freeze(self.settlement_result))

    def __bool__(self) -> bool:
        return self.accepted

    @property
    def terminal(self) -> bool:
        return self.state.terminal

    @property
    def closed(self) -> bool:
        return self.state.terminal

    @property
    def settled(self) -> bool:
        return self.state.terminal

    @property
    def outcome(self) -> str:
        return "not_evaluated" if self.not_evaluated else (self.outcome_status or self.state.outcome_status)

    @property
    def evaluation_status(self) -> str:
        return self.outcome

    @property
    def exit_price(self) -> Decimal | None:
        return self.selected_exit

    @property
    def selected_level(self) -> Decimal | None:
        return self.selected_exit

    @property
    def uncertainty(self) -> Mapping[str, Any] | None:
        return self.state.uncertainty

    @property
    def strategy_id(self) -> str:
        return self.state.strategy_id

    @property
    def trade_id(self) -> str:
        return self.state.trade_id

    position_id = property(lambda self: self.trade_id)

    @property
    def settlement(self) -> Any:
        return self.settlement_result

    @property
    def close_result(self) -> Any:
        return self.settlement_result

    @property
    def settlement_data(self) -> Any:
        value = self.settlement_result
        if isinstance(value, Mapping):
            return value.get("settlement", value)
        return getattr(value, "settlement", None)

    @property
    def balance_before(self) -> Any:
        value = self.settlement_data
        return value.get("balance_before") if isinstance(value, Mapping) else None

    @property
    def balance_after(self) -> Any:
        value = self.settlement_data
        return value.get("balance_after", value.get("closing_balance")) if isinstance(value, Mapping) else None

    @property
    def gross_pnl(self) -> Any:
        value = self.settlement_data
        return value.get("gross_pnl") if isinstance(value, Mapping) else None

    @property
    def applied_pnl(self) -> Any:
        value = self.settlement_data
        return value.get("applied_pnl") if isinstance(value, Mapping) else None

    @property
    def net_pnl(self) -> Any:
        value = self.settlement_data
        return value.get("net_pnl") if isinstance(value, Mapping) else None

    @property
    def callback_result(self) -> SchedulerCloseNotification | None:
        return self.scheduler_callback

    @property
    def event_hash(self) -> str | None:
        return self.event.event_hash if self.event is not None else None

    @property
    def content_hash(self) -> str | None:
        return self.event_hash or self.state.state_hash

    @property
    def episode_id(self) -> str:
        return self.state.episode_id

    @property
    def recovery_bar_id(self) -> str | None:
        if isinstance(self.lineage, Mapping):
            return self.lineage.get("recovery_bar_id")
        return None

    @property
    def recovery_bar_hash(self) -> str | None:
        if isinstance(self.lineage, Mapping):
            return self.lineage.get("recovery_bar_hash") or self.lineage.get("content_hash")
        return None

    @property
    def path_precision(self) -> str | None:
        if isinstance(self.lineage, Mapping):
            return self.lineage.get("path_precision")
        return None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "status": self.status,
            "outcome_status": self.outcome_status or self.state.outcome_status,
            "evaluation_status": "not_evaluated" if self.not_evaluated else (self.outcome_status or self.state.outcome_status),
            "state": self.state.to_dict(),
            "changed": self.changed,
            "accepted": self.accepted,
            "replayed": self.replayed,
            "terminal": self.terminal,
            "strategy_id": self.strategy_id,
            "trade_id": self.trade_id,
            "position_id": self.trade_id,
            "episode_id": self.episode_id,
            "reason": self.reason,
            "code": self.code,
            "event": self.event.to_dict() if self.event else None,
            "lineage": thaw(self.lineage) if self.lineage is not None else None,
            "settlement_result": thaw(self.settlement_result) if isinstance(self.settlement_result, Mapping) else self.settlement_result,
            "scheduler_callback": self.scheduler_callback.to_dict() if self.scheduler_callback else None,
            "selected_exit": _decimal_text(self.selected_exit),
            "exit_reason": self.exit_reason,
            "ambiguity_reason": self.ambiguity_reason,
            "not_evaluated": self.not_evaluated,
            "partial_data": self.partial_data,
            "gap_overlap": self.gap_overlap,
        }
        return result

    as_dict = to_dict

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __deepcopy__(self, _memo: dict[int, Any]) -> "ExitTransition":
        return self


ExitResult = ExitTransition
ExitOutcome = ExitTransition
PositionExitOutcome = ExitTransition
PositionExitTransition = ExitTransition
PositionExitState = ExitState
LevelExitState = ExitState
OpenPositionState = ExitState
ExitMonitorState = ExitState


__all__ = [
    "ExitEvent",
    "ExitLineage",
    "ExitMonitorState",
    "ExitOutcome",
    "ExitResult",
    "ExitState",
    "ExitTransition",
    "LevelExitState",
    "OpenPositionState",
    "PositionExitOutcome",
    "PositionExitState",
    "PositionExitTransition",
    "SchedulerCloseNotification",
]
