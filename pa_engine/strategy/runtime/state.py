"""Typed state and work records for the G023 scheduler.

These records intentionally contain strategy lineage and market identifiers,
but no account, balance, quantity, R or P&L values.  Account ownership stays at
the G024/G027 boundaries.
"""

from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Mapping

from pa_engine.contracts import canonical_json_bytes

from .clock import utc_instant


class StrategyState(str, Enum):
    ANALYSIS_DUE = "analysis_due"
    ANALYZING = "analyzing"
    SLEEPING = "sleeping"
    WAITING_ENTRY = "waiting_entry"
    OPEN_POSITION = "open_position"
    RETRY_WAIT = "retry_wait"
    MARKET_WAIT = "market_wait"
    BLOCKED = "blocked"
    DISABLED = "disabled"
    EXHAUSTED = "exhausted"


TERMINAL_STATES = frozenset({StrategyState.BLOCKED, StrategyState.DISABLED, StrategyState.EXHAUSTED})


@dataclass(frozen=True)
class MarketReadiness:
    """Explicit quality/session facts supplied by G021 or a deterministic fake."""

    regular_session: bool = True
    fresh: bool = True
    complete: bool = True
    non_gapped: bool = True
    sufficient: bool = True
    frozen_second: bool = True

    @property
    def ready(self) -> bool:
        return all((self.regular_session, self.fresh, self.complete, self.non_gapped, self.sufficient, self.frozen_second))


@dataclass(frozen=True)
class MarketView:
    """Minimal typed view/status boundary used by the scheduler."""

    view_id: str
    closed_snapshot_id: str
    readiness: MarketReadiness = field(default_factory=MarketReadiness)
    market_view_ref: Mapping[str, Any] | None = None

    @property
    def shared_market_view_id(self) -> str:
        return self.view_id

    @property
    def regular_session(self) -> bool:
        return bool(self.readiness.get("regular_session", True) if isinstance(self.readiness, Mapping) else self.readiness.regular_session)

    @property
    def fresh(self) -> bool:
        return bool(self.readiness.get("fresh", True) if isinstance(self.readiness, Mapping) else self.readiness.fresh)

    @property
    def complete(self) -> bool:
        return bool(self.readiness.get("complete", True) if isinstance(self.readiness, Mapping) else self.readiness.complete)

    @property
    def non_gapped(self) -> bool:
        return bool(self.readiness.get("non_gapped", True) if isinstance(self.readiness, Mapping) else self.readiness.non_gapped)

    @property
    def sufficient(self) -> bool:
        return bool(self.readiness.get("sufficient", True) if isinstance(self.readiness, Mapping) else self.readiness.sufficient)

    @property
    def frozen_second(self) -> bool:
        return bool(self.readiness.get("frozen_second", True) if isinstance(self.readiness, Mapping) else self.readiness.frozen_second)

    @property
    def ready(self) -> bool:
        return all((self.regular_session, self.fresh, self.complete, self.non_gapped, self.sufficient, self.frozen_second))


def stable_view_id(view: Mapping[str, Any] | Any) -> str:
    """Return the stable shared view ID used for dedupe/gating.

    G021 views normally carry ``shared_market_view_id`` (or a nested G018
    ``market_view_ref``).  Small test fakes can supply ``view_id``/``id``;
    otherwise a canonical digest gives deterministic identity without a
    volatile timestamp.
    """

    if isinstance(view, Mapping):
        for key in ("shared_market_view_id", "market_view_id", "view_id", "id"):
            value = view.get(key)
            if isinstance(value, str) and value:
                return value
        for key in ("market_view_ref", "shared_market_view_ref", "shared_view_ref"):
            nested = view.get(key)
            if isinstance(nested, Mapping):
                value = nested.get("shared_market_view_id")
                if isinstance(value, str) and value:
                    return value
        try:
            return "view_" + hashlib.sha256(canonical_json_bytes(dict(view))).hexdigest()
        except Exception:
            pass
    for key in ("shared_market_view_id", "market_view_id", "view_id", "id"):
        value = getattr(view, key, None)
        if isinstance(value, str) and value:
            return value
    return "view_" + hashlib.sha256(repr(view).encode("utf-8", "replace")).hexdigest()


def closed_snapshot_id(view: Mapping[str, Any] | Any) -> str | None:
    if isinstance(view, Mapping):
        for key in ("closed_snapshot_id", "selected_timeframe_closed_snapshot_id", "market_snapshot_id"):
            value = view.get(key)
            if isinstance(value, str) and value:
                return value
        for key in ("market_view_ref", "shared_market_view_ref", "shared_view_ref"):
            nested = view.get(key)
            if isinstance(nested, Mapping):
                value = nested.get("market_snapshot_id") or nested.get("closed_snapshot_id")
                if isinstance(value, str) and value:
                    return value
    for key in ("closed_snapshot_id", "selected_timeframe_closed_snapshot_id", "market_snapshot_id"):
        value = getattr(view, key, None)
        if isinstance(value, str) and value:
            return value
    return None


def view_ref(view: Mapping[str, Any] | Any) -> Mapping[str, Any] | None:
    if not isinstance(view, Mapping):
        nested = getattr(view, "market_view_ref", None) or getattr(view, "shared_market_view_ref", None) or getattr(view, "shared_view_ref", None)
        return copy.deepcopy(dict(nested)) if isinstance(nested, Mapping) else None
    for key in ("market_view_ref", "shared_market_view_ref", "shared_view_ref"):
        value = view.get(key)
        if isinstance(value, Mapping):
            return copy.deepcopy(dict(value))
    # An already compact G018 view reference is accepted directly.
    required = {"schema_version", "shared_market_view_id", "shared_market_view_hash", "market_snapshot_id", "market_snapshot_hash", "observation_frame_id", "observation_frame_hash"}
    if required.issubset(view):
        return copy.deepcopy({key: view[key] for key in required})
    return None


def market_ready(view: Mapping[str, Any] | Any) -> bool:
    """Evaluate the explicit regular-session/fresh/complete quality facts."""

    if view is None:
        return False
    # Typed G021 boundary values carry an explicit aggregate and all six
    # quality facts.  Keep that ergonomic API while treating untyped mappings
    # as fail-closed execution input below.
    if not isinstance(view, Mapping):
        if isinstance(view, MarketReadiness):
            return bool(view.ready)
        if isinstance(view, MarketView):
            return bool(view.ready)
        ready = getattr(view, "ready", None)
        # An explicit false aggregate always rejects.  A true aggregate on an
        # arbitrary object is not sufficient by itself: execution still needs
        # every quality fact to be observable.
        if ready is False:
            return False
        readiness = getattr(view, "readiness", None) or getattr(view, "status", None)
        if readiness is not None:
            if isinstance(readiness, Mapping):
                return market_ready(dict(readiness))
            names = {
                "regular_session": ("regular_session", "session_open", "is_regular_session"),
                "fresh": ("fresh", "is_fresh"),
                "complete": ("complete", "is_complete"),
                "non_gapped": ("non_gapped", "not_gapped", "gap_free"),
                "sufficient": ("sufficient", "sufficient_history", "enough_history"),
                "frozen_second": ("frozen_second", "has_frozen_second", "current_second_frozen"),
            }
            for aliases in names.values():
                values = [getattr(readiness, name) for name in aliases if hasattr(readiness, name)]
                if not values or any(value is not True for value in values):
                    return False
            return True
        return ready is True and all(
            getattr(view, name, None) is True
            for name in ("regular_session", "fresh", "complete", "non_gapped", "sufficient", "frozen_second")
        )
    explicit = view.get("ready")
    if explicit is False:
        return False
    # Mappings must carry every execution-quality fact.  An aggregate flag is
    # useful as an explicit rejection, but cannot substitute for the facts.
    status = view.get("status") or view.get("readiness") or view.get("market_status")
    if isinstance(status, Mapping):
        merged = dict(view)
        merged.update(status)
        view = merged
    elif status is not None:
        merged = dict(view)
        for key in ("regular_session", "fresh", "complete", "non_gapped", "sufficient", "frozen_second", "ready"):
            if hasattr(status, key):
                merged[key] = getattr(status, key)
        view = merged
    if view.get("ready") is False:
        return False
    fields = {
        "regular_session": ("regular_session", "session_open", "is_regular_session"),
        "fresh": ("fresh", "is_fresh"),
        "complete": ("complete", "is_complete"),
        "non_gapped": ("non_gapped", "not_gapped", "gap_free"),
        "sufficient": ("sufficient", "sufficient_history", "enough_history"),
        "frozen_second": ("frozen_second", "has_frozen_second", "current_second_frozen"),
    }
    for names in fields.values():
        values = [view[name] for name in names if name in view]
        # Unknown/missing quality is unsafe for execution.  Stable IDs and
        # snapshot lineage establish identity only; they never imply readiness.
        if not values or any(value is not True for value in values):
            return False
    return True


def view_to_mapping(view: Mapping[str, Any] | Any) -> dict[str, Any]:
    """Project a typed ``MarketView``/status object into JSON-like fields."""

    if isinstance(view, Mapping):
        return copy.deepcopy(dict(view))
    result: dict[str, Any] = {}
    for key in ("view_id", "shared_market_view_id", "closed_snapshot_id", "selected_timeframe_closed_snapshot_id", "market_snapshot_id"):
        value = getattr(view, key, None)
        if value is not None:
            result[key] = copy.deepcopy(value)
    readiness = getattr(view, "readiness", None)
    if readiness is not None:
        result["readiness"] = {
            key: bool(readiness[key] if isinstance(readiness, Mapping) and key in readiness else getattr(readiness, key))
            for key in ("regular_session", "fresh", "complete", "non_gapped", "sufficient", "frozen_second")
            if (key in readiness if isinstance(readiness, Mapping) else hasattr(readiness, key))
        }
    for key in ("regular_session", "fresh", "complete", "non_gapped", "sufficient", "frozen_second", "ready"):
        value = getattr(view, key, None)
        if value is not None:
            result[key] = bool(value)
    nested = getattr(view, "market_view_ref", None) or getattr(view, "shared_market_view_ref", None) or getattr(view, "shared_view_ref", None)
    if isinstance(nested, Mapping):
        result["market_view_ref"] = copy.deepcopy(dict(nested))
    return result


@dataclass(frozen=True)
class StrategySpec:
    """Registration input for one executable strategy."""

    strategy_id: str
    profile_ref: Mapping[str, Any] | None = None
    packet: Mapping[str, Any] | None = None
    enabled: bool = True
    request: Mapping[str, Any] | None = None
    exhausted: bool = False


@dataclass(frozen=True)
class AnalysisJob:
    """A typed, externally claimable model-analysis work item."""

    job_id: str
    idempotency_key: str
    strategy_id: str
    generation: int
    market_view_id: str
    closed_snapshot_id: str | None
    created_at: datetime
    market_view: Mapping[str, Any]
    profile_ref: Mapping[str, Any] | None = None
    request: Mapping[str, Any] | None = None
    kind: str = "analysis"

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", utc_instant(self.created_at))
        object.__setattr__(self, "market_view", copy.deepcopy(dict(self.market_view)))
        if self.profile_ref is not None:
            object.__setattr__(self, "profile_ref", copy.deepcopy(dict(self.profile_ref)))
        if self.request is not None:
            object.__setattr__(self, "request", copy.deepcopy(dict(self.request)))

    @property
    def generation_token(self) -> str:
        return f"{self.strategy_id}:{self.generation}:{self.job_id}"

    @property
    def view_id(self) -> str:
        return self.market_view_id

    @property
    def id(self) -> str:
        return self.job_id

    @property
    def snapshot_id(self) -> str | None:
        return self.closed_snapshot_id

    @property
    def idempotency_id(self) -> str:
        return self.idempotency_key

    @property
    def strategy_profile_ref(self) -> Mapping[str, Any] | None:
        return self.profile_ref

    def to_dict(self) -> dict[str, Any]:
        # This projection is deliberately model-facing and account-free.
        return {
            "job_id": self.job_id,
            "idempotency_key": self.idempotency_key,
            "strategy_id": self.strategy_id,
            "generation": self.generation,
            "market_view_id": self.market_view_id,
            "closed_snapshot_id": self.closed_snapshot_id,
            "created_at": self.created_at.isoformat().replace("+00:00", "Z"),
            "market_view": copy.deepcopy(dict(self.market_view)),
            "profile_ref": copy.deepcopy(dict(self.profile_ref)) if self.profile_ref is not None else None,
            "request": copy.deepcopy(dict(self.request)) if self.request is not None else None,
            "kind": self.kind,
        }

    as_dict = to_dict


@dataclass
class TimerToken:
    token_id: str
    strategy_id: str
    generation: int
    kind: str
    due_at: datetime
    active: bool = True

    def __post_init__(self) -> None:
        self.due_at = utc_instant(self.due_at)


@dataclass
class StrategyRecord:
    strategy_id: str
    profile_ref: Mapping[str, Any] | None = None
    packet: Mapping[str, Any] | None = None
    enabled: bool = True
    state: StrategyState = StrategyState.ANALYSIS_DUE
    generation: int = 0
    exhausted: bool = False
    intended_resume_state: StrategyState | None = None
    in_flight_job_id: str | None = None
    in_flight_generation: int | None = None
    pending_id: str | None = None
    open_position_id: str | None = None
    # A disabled strategy increments its analysis generation, but an already
    # open paper trade remains monitor-only and must still settle against the
    # generation that accepted that trade.
    open_position_generation: int | None = None
    timer_tokens: dict[str, TimerToken] = field(default_factory=dict)
    provider_retry_count: int = 0
    semantic_retry_key: tuple[str | None, str | None] | None = None
    semantic_retry_count: int = 0
    rejected_immediate_view_id: str | None = None
    current_view_id: str | None = None
    last_closed_snapshot_id: str | None = None
    transition_count: int = 0
    last_transition_at: datetime | None = None
    transition_log: list[dict[str, Any]] = field(default_factory=list)
    latest_instruction: Mapping[str, Any] | None = None
    blocked_reason: str | None = None
    decision_accepted_at: datetime | None = None
    next_due_at: datetime | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.profile_ref is not None:
            self.profile_ref = copy.deepcopy(dict(self.profile_ref))
        if self.packet is not None:
            self.packet = copy.deepcopy(dict(self.packet))
        if self.state == StrategyState.EXHAUSTED:
            self.exhausted = True

    @property
    def strategy_profile_ref(self) -> Mapping[str, Any] | None:
        return self.profile_ref

    @property
    def has_trade(self) -> bool:
        return self.pending_id is not None or self.open_position_id is not None

    @property
    def monitor_only(self) -> bool:
        return not self.enabled and self.open_position_id is not None

    def snapshot(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "profile_ref": copy.deepcopy(dict(self.profile_ref)) if self.profile_ref is not None else None,
            "state": self.state.value,
            "enabled": self.enabled,
            "exhausted": self.exhausted,
            "generation": self.generation,
            "intended_resume_state": self.intended_resume_state.value if self.intended_resume_state else None,
            "in_flight_job_id": self.in_flight_job_id,
            "pending_id": self.pending_id,
            "open_position_id": self.open_position_id,
            "open_position_generation": self.open_position_generation,
            "provider_retry_count": self.provider_retry_count,
            "semantic_retry_key": list(self.semantic_retry_key) if self.semantic_retry_key else None,
            "semantic_retry_count": self.semantic_retry_count,
            "rejected_immediate_view_id": self.rejected_immediate_view_id,
            "current_view_id": self.current_view_id,
            "last_closed_snapshot_id": self.last_closed_snapshot_id,
            "transition_count": self.transition_count,
            "latest_instruction": copy.deepcopy(dict(self.latest_instruction)) if self.latest_instruction else None,
            "blocked_reason": self.blocked_reason,
            "decision_accepted_at": self.decision_accepted_at.isoformat().replace("+00:00", "Z") if self.decision_accepted_at else None,
            "next_due_at": self.next_due_at.isoformat().replace("+00:00", "Z") if self.next_due_at else None,
            "expires_at": self.expires_at.isoformat().replace("+00:00", "Z") if self.expires_at else None,
        }

    to_dict = snapshot
    as_dict = snapshot

    @property
    def pending_expires_at(self) -> datetime | None:
        return self.expires_at

    @property
    def next_wake_at(self) -> datetime | None:
        return self.next_due_at

    @property
    def retry_count(self) -> int:
        return self.provider_retry_count

    @property
    def malformed_grounding_retry_count(self) -> int:
        return self.semantic_retry_count


__all__ = [
    "AnalysisJob",
    "MarketReadiness",
    "MarketView",
    "StrategyRecord",
    "StrategySpec",
    "StrategyState",
    "TERMINAL_STATES",
    "TimerToken",
    "closed_snapshot_id",
    "market_ready",
    "stable_view_id",
    "view_to_mapping",
    "view_ref",
]


SchedulerState = StrategyState
State = StrategyState
PerStrategyState = StrategyRecord
Timer = TimerToken
AnalysisWorkItem = AnalysisJob
