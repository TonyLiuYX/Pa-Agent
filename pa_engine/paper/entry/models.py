"""Immutable values for the G025 pending-entry boundary.

The entry monitor is deliberately a small in-memory state machine.  These
values carry strategy/decision/market lineage, but no account balance,
quantity, R or P&L fields.  Mappings are recursively frozen before they cross
the public boundary so a caller cannot mutate a completed transition.
"""

from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from collections.abc import Mapping as ABCMapping
from typing import Any, Mapping

from pa_engine.contracts import canonical_json_bytes


class _FrozenMap(ABCMapping):
    __slots__ = ("_data",)

    def __init__(self, value: Mapping[str, Any]) -> None:
        object.__setattr__(self, "_data", dict(value))

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


def freeze(value: Any) -> Any:
    """Recursively freeze JSON-like values (Decimal is already immutable)."""

    if isinstance(value, Mapping):
        return _FrozenMap({str(key): freeze(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(child) for child in value)
    if isinstance(value, set):
        return tuple(sorted((freeze(child) for child in value), key=repr))
    return value


def thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: thaw(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [thaw(child) for child in value]
    return copy.deepcopy(value)


def identity_hash(value: Mapping[str, Any]) -> str:
    """Content identity for entry values; no Python hash/random telemetry."""

    return hashlib.sha256(canonical_json_bytes(_json_projection(value))).hexdigest()


def _json_projection(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        return {str(k): _json_projection(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_projection(v) for v in value]
    return value


def json_projection(value: Any) -> Any:
    """Public JSON-safe projection used by serialized boundary views."""
    if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
        try:
            return _json_projection(value.to_dict())
        except Exception:
            pass
    return _json_projection(value)


@dataclass(frozen=True, slots=True)
class EntryLineage:
    """Stable decision lineage copied from the accepted G018 instruction."""

    instruction_id: str | None = None
    instruction_hash: str | None = None
    result_id: str | None = None
    result_hash: str | None = None
    evidence_package_id: str | None = None
    evidence_package_hash: str | None = None
    profile_ref_id: str | None = None
    profile_ref_hash: str | None = None
    market_view_ref: Mapping[str, Any] | None = None
    instruction: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "market_view_ref", freeze(self.market_view_ref) if self.market_view_ref is not None else None)
        object.__setattr__(self, "instruction", freeze(self.instruction))

    def to_dict(self) -> dict[str, Any]:
        return _json_projection({
            "instruction_id": self.instruction_id,
            "instruction_hash": self.instruction_hash,
            "result_id": self.result_id,
            "result_hash": self.result_hash,
            "evidence_package_id": self.evidence_package_id,
            "evidence_package_hash": self.evidence_package_hash,
            "profile_ref_id": self.profile_ref_id,
            "profile_ref_hash": self.profile_ref_hash,
            "market_view_ref": thaw(self.market_view_ref) if self.market_view_ref is not None else None,
            "instruction": thaw(self.instruction),
        })

    as_dict = to_dict

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


@dataclass(frozen=True, slots=True)
class EntryEvent:
    """Hash-bound internal event emitted by the reducer."""

    event_type: str
    strategy_id: str
    pending_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    event_hash: str = field(init=False)
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        frozen_payload = freeze(self.payload)
        object.__setattr__(self, "payload", frozen_payload)
        projection = {
            "schema_version": "0.1.0",
            "event_type": self.event_type,
            "strategy_id": self.strategy_id,
            "pending_id": self.pending_id,
            "payload": thaw(frozen_payload),
        }
        digest = identity_hash(projection)
        object.__setattr__(self, "event_hash", digest)
        object.__setattr__(self, "event_id", "pee_" + digest)

    @property
    def content_hash(self) -> str:
        return self.event_hash

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "0.1.0",
            "event_type": self.event_type,
            "strategy_id": self.strategy_id,
            "pending_id": self.pending_id,
            "event_id": self.event_id,
            "event_hash": self.event_hash,
            "content_hash": self.event_hash,
            "payload": json_projection(thaw(self.payload)),
        }

    as_dict = to_dict

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


@dataclass(frozen=True, slots=True)
class PendingEntryState:
    """One immutable snapshot of a pending entry or its terminal outcome."""

    strategy_id: str
    pending_id: str
    symbol: str
    direction: str
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    decision_lineage: EntryLineage
    decision_accepted_at: datetime
    submitted_at: datetime
    expires_at: datetime
    resolution_at: datetime
    generation: int
    market_feed: Mapping[str, Any]
    status: str = "pending"
    terminal_reason: str | None = None
    gap_overlap: bool = False
    partial_data: bool = False
    gap_intervals: tuple[tuple[str, str | None], ...] = ()
    recovery_attempted: bool = False
    processed_intervals: tuple[tuple[str, str], ...] = ()
    processed_event_ids: tuple[str, ...] = ()
    recovery_lineage: tuple[Mapping[str, Any], ...] = ()
    filled_at: datetime | None = None
    filled_price: Decimal | None = None
    exit_eligible_after: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in ("entry_price", "stop_price", "target_price"):
            value = getattr(self, field_name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"{field_name} must be a positive finite Decimal")
        if self.direction == "long" and not (self.stop_price < self.entry_price < self.target_price):
            raise ValueError("long levels must satisfy stop < entry < target")
        if self.direction == "short" and not (self.target_price < self.entry_price < self.stop_price):
            raise ValueError("short levels must satisfy target < entry < stop")
        if self.direction not in {"long", "short"}:
            raise ValueError("direction must be long or short")
        if not isinstance(self.market_feed, Mapping):
            raise ValueError("market_feed must be a mapping")
        if any(not isinstance(getattr(self, field_name), datetime) for field_name in ("decision_accepted_at", "submitted_at", "expires_at", "resolution_at")):
            raise ValueError("entry timestamps must be datetime values")
        if self.decision_accepted_at.tzinfo is None or self.submitted_at.tzinfo is None or self.expires_at.tzinfo is None or self.resolution_at.tzinfo is None:
            raise ValueError("entry timestamps must be timezone-aware")
        for field_name in ("decision_accepted_at", "submitted_at", "expires_at", "resolution_at", "filled_at", "exit_eligible_after"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, value.astimezone(timezone.utc))
        object.__setattr__(self, "market_feed", freeze(self.market_feed))
        object.__setattr__(self, "processed_intervals", tuple(tuple(item) for item in self.processed_intervals))
        object.__setattr__(self, "processed_event_ids", tuple(str(item) for item in self.processed_event_ids))
        object.__setattr__(self, "gap_intervals", tuple((str(item[0]), item[1] if item[1] is None else str(item[1])) for item in self.gap_intervals))
        object.__setattr__(self, "recovery_lineage", tuple(freeze(item) for item in self.recovery_lineage))

    @property
    def terminal(self) -> bool:
        return self.status in {"filled", "expired", "cancelled"}

    @property
    def pending(self) -> bool:
        return self.status == "pending"

    @property
    def uncertainty(self) -> str:
        return "partial_data" if self.partial_data else "none"

    @property
    def feed_identity(self) -> Mapping[str, Any]:
        return self.market_feed

    @property
    def submitted_time(self) -> datetime:
        return self.submitted_at

    @property
    def accepted_at(self) -> datetime:
        return self.decision_accepted_at

    @property
    def wait_seconds(self) -> int:
        return int((self.expires_at - self.decision_accepted_at).total_seconds())

    @property
    def x_seconds(self) -> int:
        return self.wait_seconds

    @property
    def resolved_at(self) -> datetime | None:
        return self.filled_at if self.status == "filled" else (self.resolution_at if self.terminal else None)

    @property
    def same_second_exit_allowed(self) -> bool:
        return False

    @property
    def path_precision(self) -> str | None:
        if not self.recovery_lineage:
            return None
        value = self.recovery_lineage[-1]
        return value.get("path_precision") if isinstance(value, Mapping) else None

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
    def content_hash(self) -> str:
        return identity_hash(self.to_dict())

    @property
    def state_hash(self) -> str:
        return self.content_hash

    @property
    def state_id(self) -> str:
        return "pes_" + self.content_hash

    @property
    def episode_id(self) -> str:
        """Stable idempotency identity for this pending decision episode."""

        return identity_hash({
            "pending_id": self.pending_id,
            "generation": self.generation,
            "decision_accepted_at": self.decision_accepted_at,
            "expires_at": self.expires_at,
        })

    @property
    def entry(self) -> Decimal:
        return self.entry_price

    @property
    def requested_entry_price(self) -> Decimal:
        return self.entry_price

    @property
    def fill_price(self) -> Decimal | None:
        return self.filled_price

    @property
    def stop(self) -> Decimal:
        return self.stop_price

    @property
    def target(self) -> Decimal:
        return self.target_price

    @classmethod
    def from_instruction(cls, instruction: Mapping[str, Any], **kwargs: Any) -> "PendingEntryState":
        # Local import avoids a boundary/reducer import cycle at module load.
        from .boundary import make_pending_state

        return make_pending_state(instruction, **kwargs)

    create = from_instruction

    def to_dict(self) -> dict[str, Any]:
        def stamp(value: datetime | None) -> str | None:
            return value.isoformat().replace("+00:00", "Z") if value is not None else None

        return {
            "schema_version": "0.1.0",
            "artifact_kind": "pending_entry_state",
            "strategy_id": self.strategy_id,
            "pending_id": self.pending_id,
            "episode_id": self.episode_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "entry_price": format(self.entry_price, "f"),
            "stop_price": format(self.stop_price, "f"),
            "target_price": format(self.target_price, "f"),
            "decision_lineage": self.decision_lineage.to_dict(),
            "decision_accepted_at": stamp(self.decision_accepted_at),
            "submitted_at": stamp(self.submitted_at),
            "expires_at": stamp(self.expires_at),
            "resolution_at": stamp(self.resolution_at),
            "generation": self.generation,
            "market_feed": thaw(self.market_feed),
            "status": self.status,
            "terminal_reason": self.terminal_reason,
            "gap_overlap": self.gap_overlap,
            "partial_data": self.partial_data,
            "uncertainty": self.uncertainty,
            "gap_intervals": [list(item) for item in self.gap_intervals],
            "recovery_attempted": self.recovery_attempted,
            "processed_intervals": [list(item) for item in self.processed_intervals],
            "processed_event_ids": list(self.processed_event_ids),
            "recovery_lineage": [thaw(item) for item in self.recovery_lineage],
            "filled_at": stamp(self.filled_at),
            "filled_price": format(self.filled_price, "f") if self.filled_price is not None else None,
            "exit_eligible_after": stamp(self.exit_eligible_after),
            "same_second_exit_allowed": False,
        }

    as_dict = to_dict


@dataclass(frozen=True, slots=True)
class PendingEntryTransition:
    """Typed reducer result; Mapping-like helpers keep integrations ergonomic."""

    status: str
    state: PendingEntryState
    changed: bool = False
    accepted: bool = False
    replayed: bool = False
    terminal: bool = False
    strategy_id: str = ""
    pending_id: str | None = None
    generation: int | None = None
    reason: str | None = None
    code: str | None = None
    event: EntryEvent | None = None
    lineage: Mapping[str, Any] | None = None
    fill_result: Any = None
    expiry_result: Any = None
    filled_price: Decimal | None = None
    exit_eligible_after: datetime | None = None
    not_evaluated: bool = False
    partial_data: bool = False
    gap_overlap: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "lineage", freeze(self.lineage) if self.lineage is not None else None)
        object.__setattr__(self, "fill_result", freeze(self.fill_result))
        object.__setattr__(self, "expiry_result", freeze(self.expiry_result))

    def __bool__(self) -> bool:
        return bool(self.accepted)

    @property
    def outcome(self) -> str:
        return "not_evaluated" if self.not_evaluated else self.status

    @property
    def evaluation_status(self) -> str:
        return self.outcome

    @property
    def position_id(self) -> str | None:
        value = self.fill_result
        if isinstance(value, Mapping):
            return value.get("position_id")
        return getattr(value, "position_id", None)

    @property
    def event_hash(self) -> str | None:
        return self.event.event_hash if self.event is not None else None

    @property
    def content_hash(self) -> str | None:
        return self.event_hash or self.state.content_hash

    @property
    def decision_lineage(self) -> EntryLineage:
        return self.state.decision_lineage

    @property
    def episode_id(self) -> str:
        return self.state.episode_id

    @property
    def recovery_bar_id(self) -> str | None:
        return self.lineage.get("recovery_bar_id") if isinstance(self.lineage, Mapping) else None

    @property
    def recovery_bar_hash(self) -> str | None:
        if not isinstance(self.lineage, Mapping):
            return None
        return self.lineage.get("recovery_bar_hash") or self.lineage.get("content_hash")

    @property
    def path_precision(self) -> str | None:
        return self.lineage.get("path_precision") if isinstance(self.lineage, Mapping) else None

    def to_dict(self) -> dict[str, Any]:
        def stamp(value: datetime | None) -> str | None:
            return value.isoformat().replace("+00:00", "Z") if value is not None else None

        callback_projection = None
        if self.fill_result is not None:
            value = thaw(self.fill_result)
            if isinstance(value, Mapping):
                callback_projection = {
                    key: json_projection(value.get(key))
                    for key in ("status", "accepted", "changed", "replayed", "pending_id", "position_id", "trade_id", "code")
                    if key in value
                }
            else:
                callback_projection = {
                    key: json_projection(getattr(value, key))
                    for key in ("status", "accepted", "changed", "replayed", "pending_id", "position_id", "trade_id", "code")
                    if hasattr(value, key)
                }
        expiry_projection = None
        if self.expiry_result is not None:
            value = thaw(self.expiry_result)
            if isinstance(value, Mapping):
                expiry_projection = {key: json_projection(value.get(key)) for key in ("status", "accepted", "changed", "replayed", "pending_id", "code") if key in value}
            else:
                expiry_projection = {key: json_projection(getattr(value, key)) for key in ("status", "accepted", "changed", "replayed", "pending_id", "code") if hasattr(value, key)}

        return {
            "status": self.status,
            "outcome": self.outcome,
            "evaluation_status": self.evaluation_status,
            "state": self.state.to_dict(),
            "decision_lineage": self.state.decision_lineage.to_dict(),
            "changed": self.changed,
            "accepted": self.accepted,
            "replayed": self.replayed,
            "terminal": self.terminal,
            "strategy_id": self.strategy_id,
            "pending_id": self.pending_id,
            "episode_id": self.episode_id,
            "generation": self.generation,
            "reason": self.reason,
            "code": self.code,
            "event": self.event.to_dict() if self.event else None,
            "lineage": thaw(self.lineage) if self.lineage is not None else None,
            "fill_result": callback_projection,
            "expiry_result": expiry_projection,
            "filled_price": format(self.filled_price, "f") if self.filled_price is not None else None,
            "exit_eligible_after": stamp(self.exit_eligible_after),
            "not_evaluated": self.not_evaluated,
            "partial_data": self.partial_data,
            "gap_overlap": self.gap_overlap,
        }

    as_dict = to_dict

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


PendingEntryOutcome = PendingEntryTransition
EntryOutcome = PendingEntryTransition
PendingEntry = PendingEntryState
PendingEntryResult = PendingEntryTransition
TouchOutcome = PendingEntryTransition


__all__ = [
    "EntryEvent",
    "EntryLineage",
    "EntryOutcome",
    "PendingEntryOutcome",
    "PendingEntry",
    "PendingEntryResult",
    "TouchOutcome",
    "PendingEntryState",
    "PendingEntryTransition",
    "freeze",
    "identity_hash",
    "thaw",
]
