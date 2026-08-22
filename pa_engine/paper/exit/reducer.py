"""Deterministic one-position stop/target monitor and settlement reducer."""

from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from collections.abc import Iterable, Mapping
from typing import Any, Callable

from pa_engine.market import ALPACA_IEX_FEED
from pa_engine.market_sources.alpaca.stream import timestamp_ns
from pa_engine.contracts import canonical_json_bytes

from .boundary import (
    PATH_PRECISION,
    canonical_timestamp,
    event_mapping,
    exact_decimal,
    fill_interval,
    fill_timestamp,
    freeze,
    invoke_compatible,
    recovery_mapping,
    thaw,
    trade_mapping,
    utc_instant,
)
from .models import ExitEvent, ExitState, ExitTransition, SchedulerCloseNotification
from .scheduler_bridge import ExitSchedulerBridge, SchedulerBindingError


UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class _PendingSettlementAttempt:
    """Minimal immutable evidence for an unknown G024 close attempt."""

    key: str
    selected_exit: Decimal
    reason: str
    source: Mapping[str, Any]
    ambiguity_reason: str | None
    recovery: bool
    at: datetime | str | None
    commit_kind: str
    commit_context: Any

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", freeze(self.source))
        object.__setattr__(self, "commit_context", freeze(self.commit_context))


def _field(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def _ns(value: datetime | str) -> int:
    if isinstance(value, datetime):
        instant = utc_instant(value)
        return int(instant.timestamp()) * 1_000_000_000 + instant.microsecond * 1_000
    return timestamp_ns(str(value))


def _stamp(value: datetime | str) -> str:
    # Gap boundaries are a wire boundary, not display text.  Never preserve
    # an unparseable value merely so a later ordering comparison can fail in a
    # less useful place; callers normalize this exception to ``invalid_gap``.
    return canonical_timestamp(value)


def _accepted(value: Any) -> bool:
    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, Mapping):
        if value.get("accepted") is False:
            return False
        if value.get("accepted") is True or value.get("changed") is True:
            return True
        return str(value.get("status", "")).lower() in {"accepted", "closed", "exhausted", "replay", "success", "settled"}
    accepted = getattr(value, "accepted", None)
    if accepted is False:
        return False
    if accepted is True or bool(getattr(value, "changed", False)):
        return True
    return str(getattr(value, "status", "")).lower() in {"accepted", "closed", "exhausted", "replay", "success", "settled"}


def _result_mapping(value: Any) -> Any:
    if isinstance(value, Mapping):
        return copy.deepcopy(dict(value))
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            mapped = to_dict()
            if isinstance(mapped, Mapping):
                return copy.deepcopy(dict(mapped))
        except Exception:
            return {"status": str(getattr(value, "status", "unknown")), "accepted": _accepted(value)}
    return value


def _normal_values(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        # A real G019 HistoryResult carries an intentionally empty generic
        # ``bars`` field alongside populated ``recovery_bars``.  Prefer the
        # recovery-specific collection so an empty placeholder cannot hide a
        # valid multi-minute batch.
        for key in ("recovery_bars", "bars", "values", "data"):
            if key in value:
                nested = value[key]
                return tuple(nested) if isinstance(nested, (list, tuple)) else (nested,)
        if "start_at" in value or value.get("artifact_kind") == "recovery_bar":
            return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(value)
    for key in ("recovery_bars", "bars", "values", "data"):
        nested = getattr(value, key, None)
        if nested is not None:
            return tuple(nested) if isinstance(nested, (list, tuple)) else (nested,)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return tuple(value)
    return (value,)


def _source_ref(mapped: Mapping[str, Any], *, recovery: bool = False) -> dict[str, Any]:
    keys = (
        "recovery_bar_id",
        "recovery_bar_hash",
        "source_request_hash",
        "recovery_cursor",
        "event_id",
        "revision_id",
        "content_hash",
        "event_hash",
        "start_at",
        "end_at",
        "execution_watermark",
    )
    result = {key: copy.deepcopy(mapped.get(key)) for key in keys if mapped.get(key) is not None}
    result["source"] = "recovery_bar" if recovery else "second_aggregate_event"
    if recovery:
        result["path_precision"] = PATH_PRECISION
    else:
        result["path_precision"] = "second_ohlcv"
    return result


def _uncertainty(*, partial: bool = False, ambiguous: bool = False, recovery: bool = False) -> dict[str, Any] | None:
    codes: list[str] = []
    if partial:
        codes.append("partial_data")
    if ambiguous or recovery:
        # G014's vocabulary is closed; this is the existing uncertainty code,
        # not a new downstream contract code.
        codes.append("high_uncertainty")
    if not codes:
        return None
    if recovery:
        details = "minute-bar path is unavailable"
    elif ambiguous:
        details = "same-second OHLC has no intrasecond path"
    else:
        details = "execution data is partial"
    return {
        "level": "high",
        "codes": list(dict.fromkeys(codes)),
        "details": details,
    }


def _trade_lineage(value: Any) -> dict[str, Any]:
    mapped = trade_mapping(value)
    return {key: mapped[key] for key in sorted(mapped)}


def _entry_lineage(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return copy.deepcopy(dict(value))
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        mapped = to_dict()
        return copy.deepcopy(dict(mapped)) if isinstance(mapped, Mapping) else {}
    return {}


def make_exit_state(
    trade: Any,
    *,
    decision_lineage: Mapping[str, Any] | Any | None = None,
    entry_event: Any | None = None,
    entry_fill_event: Any | None = None,
    entry_second: Any | None = None,
    entry_fill_second: Any | None = None,
    fill_event: Any | None = None,
    entry_fill_reference: Any | None = None,
    entry_filled_at: datetime | str | None = None,
    entry_fill_at: datetime | str | None = None,
    entry_fill_interval: tuple[str, str] | None = None,
    generation: int = 0,
    market_feed: Mapping[str, Any] | Any | None = None,
) -> ExitState:
    """Construct a monitor state from a validated G024 trade projection."""

    mapped = trade_mapping(trade)
    if type(generation) is not int or generation < 0:
        raise ValueError("generation must be a non-negative integer")
    source_event = None
    for candidate in (entry_event, entry_fill_event, entry_second, entry_fill_second, fill_event):
        if candidate is not None:
            source_event = candidate
            break
    if source_event is None and isinstance(entry_fill_reference, Mapping):
        source_event = entry_fill_reference
    source_mapped: Mapping[str, Any] | None = None
    if source_event is not None:
        try:
            if isinstance(source_event, Mapping):
                source_mapped = copy.deepcopy(dict(source_event))
            else:
                source_mapped = copy.deepcopy(dict(source_event.to_dict()))
        except Exception:
            source_mapped = None
    interval = entry_fill_interval if entry_fill_interval is not None else fill_interval(source_event)
    if source_mapped is not None and interval is None and ("start_at" in source_mapped or "end_at" in source_mapped or "interval" in source_mapped):
        raise ValueError("entry event interval is malformed")
    explicit_fill_at = entry_filled_at if entry_filled_at is not None else entry_fill_at
    fill_at = fill_timestamp(explicit_fill_at)
    if explicit_fill_at is not None and fill_at is None:
        raise ValueError("entry_filled_at must be a valid timezone-aware timestamp")
    if fill_at is None and source_mapped is not None:
        fill_at = fill_timestamp(source_mapped.get("filled_at") or source_mapped.get("fill_at"))
    if market_feed is None:
        feed = ALPACA_IEX_FEED.to_dict()
    elif hasattr(market_feed, "to_dict"):
        feed = copy.deepcopy(dict(market_feed.to_dict()))
    elif isinstance(market_feed, Mapping):
        feed = copy.deepcopy(dict(market_feed))
    else:
        raise ValueError("market_feed must be a mapping")
    if feed != ALPACA_IEX_FEED.to_dict():
        raise ValueError("G026 only accepts the frozen Alpaca IEX feed identity")
    if source_mapped is not None:
        source_symbol = source_mapped.get("symbol")
        if source_symbol is None and isinstance(source_mapped.get("instrument"), Mapping):
            source_symbol = source_mapped["instrument"].get("symbol")
        if source_symbol is not None and source_symbol != mapped["symbol"]:
            raise ValueError("entry event symbol differs from trade")
        source_feed = source_mapped.get("market_feed")
        if hasattr(source_feed, "to_dict"):
            source_feed = source_feed.to_dict()
        if source_feed is not None and isinstance(source_feed, Mapping) and dict(source_feed) != feed:
            raise ValueError("entry event feed differs from trade")
    if interval is not None:
        if not isinstance(interval, (list, tuple)) or len(interval) != 2:
            raise ValueError("entry_fill_interval must be a two-item interval")
        interval = (_stamp(interval[0]), _stamp(interval[1]))
        if _ns(interval[1]) <= _ns(interval[0]):
            raise ValueError("entry_fill_interval must be increasing")
    else:
        # A fill timestamp alone cannot prove which half-open second was the
        # entry interval.  Without that boundary, evaluating the first
        # accepted second could incorrectly turn an entry touch into an exit.
        raise ValueError("entry_fill_interval is required for executable exit monitoring")
    # Pending-entry state is accepted as a convenient source of decision
    # lineage; it remains copied, never mutated.
    if decision_lineage is None and hasattr(trade, "decision_lineage"):
        decision_lineage = getattr(trade, "decision_lineage")
    if decision_lineage is None and isinstance(trade, Mapping):
        decision_lineage = trade.get("decision_lineage") or trade.get("lineage")
    return ExitState(
        strategy_id=mapped["strategy_id"],
        trade_id=mapped["trade_id"],
        symbol=mapped["symbol"],
        direction=mapped["direction"],
        entry_price=mapped["entry_price"],
        stop_price=mapped["stop_price"],
        target_price=mapped["target_price"],
        quantity=mapped["quantity"],
        trade_lineage=mapped,
        decision_lineage=_entry_lineage(decision_lineage),
        entry_event=source_mapped,
        entry_fill_interval=interval,
        entry_filled_at=fill_at,
        generation=generation,
        market_feed=feed,
    )


class ExitReducer:
    """One open-position reducer with no clock, model, storage or network."""

    def __init__(
        self,
        state: ExitState,
        *,
        settle_trade: Callable[..., Any] | None = None,
        close_trade: Callable[..., Any] | None = None,
        on_position_close: Callable[..., Any] | None = None,
        scheduler: Any = None,
        scheduler_bridge: ExitSchedulerBridge | None = None,
        paper_account_book: Any = None,
        book: Any = None,
        recovery_callback: Callable[..., Any] | None = None,
        recovery_request: Mapping[str, Any] | None = None,
        expected_source_request_hash: str | None = None,
        enabled: bool = True,
        exhausted: bool = False,
    ) -> None:
        if not isinstance(state, ExitState):
            raise TypeError("state must be ExitState")
        self._state = state
        self._paper_account_book = paper_account_book if paper_account_book is not None else book
        supplied_settle = settle_trade if settle_trade is not None else close_trade
        if self._paper_account_book is not None:
            authoritative_close = getattr(self._paper_account_book, "close_trade", None)
            if not callable(authoritative_close):
                raise ValueError("paper_account_book must expose the authoritative close_trade boundary")
            if supplied_settle is not None:
                same_bound_method = (
                    supplied_settle is authoritative_close
                    or (
                        getattr(supplied_settle, "__self__", None) is self._paper_account_book
                        and getattr(supplied_settle, "__func__", None) is getattr(authoritative_close, "__func__", None)
                    )
                )
                if not same_bound_method:
                    raise ValueError("paper_account_book cannot be combined with a distinct settlement override")
            self._settle_trade = authoritative_close
        else:
            if supplied_settle is not None:
                raise ValueError("standalone settlement callbacks lack an authoritative G024 account boundary")
            self._settle_trade = None
        self._on_position_close = on_position_close
        self._scheduler = scheduler
        self._scheduler_bridge = scheduler_bridge
        self._recovery_callback = recovery_callback
        self._recovery_callback_called = False
        self._enabled = bool(enabled)
        self._exhausted = bool(exhausted)
        self._expected_source_request_hash = expected_source_request_hash
        self._expected_recovery_symbol: str | None = None
        self._expected_recovery_feed: Mapping[str, Any] | None = None
        self._expected_recovery_feed_name: str | None = None
        self._expected_recovery_cursor: str | None = None
        self._expected_recovery_start: str | None = None
        self._expected_recovery_end: str | None = None
        request_payload: Mapping[str, Any] | None = recovery_request if isinstance(recovery_request, Mapping) else None
        if request_payload is None and recovery_request is not None:
            try:
                candidate = recovery_request.to_dict()  # type: ignore[union-attr]
            except Exception:
                candidate = None
            if isinstance(candidate, Mapping):
                request_payload = dict(candidate)
        if request_payload is not None:
            request = request_payload.get("request") if isinstance(request_payload.get("request"), Mapping) else request_payload
            instrument = request.get("instrument")
            if isinstance(instrument, Mapping):
                self._expected_recovery_symbol = instrument.get("symbol")
            self._expected_recovery_symbol = self._expected_recovery_symbol or request.get("symbol")
            feed = request.get("market_feed") or request.get("feed")
            if isinstance(feed, Mapping):
                self._expected_recovery_feed = dict(feed)
            elif isinstance(feed, str):
                # G019 HistoryRequest carries the provider feed name ("iex")
                # while RecoveryBar carries the full frozen MarketFeedRef.
                # Bind the request's provider name without treating it as a
                # per-bar cursor or silently accepting another feed.
                self._expected_recovery_feed_name = feed
            self._expected_source_request_hash = self._expected_source_request_hash or request.get("source_request_hash") or request.get("request_hash") or request.get("content_hash")
            self._expected_recovery_cursor = request.get("recovery_cursor") or request.get("cursor")
            recovery_range = request.get("range") if isinstance(request.get("range"), Mapping) else request
            self._expected_recovery_start = recovery_range.get("start_at") or recovery_range.get("start") or recovery_range.get("from_at")
            self._expected_recovery_end = recovery_range.get("end_at") or recovery_range.get("end") or recovery_range.get("to_at")
        self._callback_sent = False
        # A settlement call whose outcome was unknown (for example a
        # transport exception after G024 may have committed) leaves the
        # complete deterministic attempt and its commit context in memory.
        # Only the same reducer instance may use it to retry before consuming
        # later inputs; a fresh monitor intentionally remains callback-free on
        # replay.
        self._pending_settlement: _PendingSettlementAttempt | None = None
        self._scheduler_binding_error: str | None = None
        if self._scheduler_bridge is None and scheduler is not None:
            try:
                self._scheduler_bridge = ExitSchedulerBridge.bind(scheduler, state)
            except SchedulerBindingError as exc:
                raise ValueError(f"scheduler binding rejected: {exc}") from exc
        elif self._scheduler_bridge is not None:
            if type(self._scheduler_bridge) is not ExitSchedulerBridge:
                raise ValueError("scheduler binding rejected: scheduler_bridge must be ExitSchedulerBridge")
            if not self._scheduler_bridge.bound_to(state):
                raise ValueError("scheduler binding rejected: bridge identity differs from open trade")
            if scheduler is not None and self._scheduler_bridge.scheduler is not scheduler:
                raise ValueError("scheduler binding rejected: bridge scheduler differs from supplied scheduler")
            prepared = self._scheduler_bridge.prepare_close()
            if not prepared.accepted:
                raise ValueError(f"scheduler binding rejected: {prepared.error or prepared.status}")

    @classmethod
    def from_trade(cls, trade: Any, **kwargs: Any) -> "ExitReducer":
        if "on_close" in kwargs and "on_position_close" not in kwargs:
            kwargs["on_position_close"] = kwargs.pop("on_close")
        if "settle" in kwargs and "settle_trade" not in kwargs:
            kwargs["settle_trade"] = kwargs.pop("settle")
        if "account_book" in kwargs and "paper_account_book" not in kwargs:
            kwargs["paper_account_book"] = kwargs.pop("account_book")
        state_keys = {
            "decision_lineage",
            "entry_event",
            "entry_fill_event",
            "entry_second",
            "entry_fill_second",
            "fill_event",
            "entry_fill_reference",
            "entry_filled_at",
            "entry_fill_at",
            "entry_fill_interval",
            "generation",
            "market_feed",
        }
        state_kwargs = {key: kwargs.pop(key) for key in tuple(kwargs) if key in state_keys}
        state = make_exit_state(trade, **state_kwargs)
        return cls(state, **kwargs)

    @classmethod
    def create(cls, trade: Any, **kwargs: Any) -> "ExitReducer":
        return cls.from_trade(trade, **kwargs)

    @property
    def state(self) -> ExitState:
        return self._state

    @property
    def snapshot(self) -> ExitState:
        return self._state

    @property
    def trade(self) -> Mapping[str, Any]:
        return self._state.trade_lineage

    @property
    def strategy_id(self) -> str:
        return self._state.strategy_id

    @property
    def trade_id(self) -> str:
        return self._state.trade_id

    @property
    def position_id(self) -> str:
        return self._state.trade_id

    @property
    def terminal(self) -> bool:
        return self._state.terminal

    @property
    def closed(self) -> bool:
        return self._state.terminal

    @property
    def episode_id(self) -> str:
        return self._state.episode_id

    def _transition(self, status: str, *, changed: bool = False, accepted: bool = False, replayed: bool = False, reason: str | None = None, code: str | None = None, event: ExitEvent | None = None, lineage: Mapping[str, Any] | None = None, settlement_result: Any = None, scheduler_callback: SchedulerCloseNotification | None = None, selected_exit: Decimal | None = None, exit_reason: str | None = None, ambiguity_reason: str | None = None, outcome_status: str | None = None, not_evaluated: bool = False, partial_data: bool = False, gap_overlap: bool = False) -> ExitTransition:
        return ExitTransition(
            status=status,
            state=self._state,
            changed=changed,
            accepted=accepted,
            replayed=replayed,
            reason=reason,
            code=code,
            event=event,
            lineage=lineage,
            settlement_result=settlement_result,
            scheduler_callback=scheduler_callback,
            selected_exit=selected_exit,
            exit_reason=exit_reason,
            ambiguity_reason=ambiguity_reason,
            outcome_status=outcome_status,
            not_evaluated=not_evaluated,
            partial_data=partial_data,
            gap_overlap=gap_overlap,
        )

    def _replayed(self, reason: str = "terminal state is immutable") -> ExitTransition:
        return self._transition("replayed", accepted=True, replayed=True, reason=reason, code="idempotent_replay", outcome_status=self._state.outcome_status)

    def _same_feed(self, mapped: Mapping[str, Any]) -> bool:
        feed = mapped.get("market_feed")
        if hasattr(feed, "to_dict"):
            feed = feed.to_dict()
        return isinstance(feed, Mapping) and dict(feed) == dict(self._state.market_feed)

    def _gap_overlap(self, start: str, end: str | None) -> bool:
        start_ns, end_ns = _ns(start), _ns(end) if end is not None else 2**63 - 1
        for gap_start, gap_end in self._state.gap_intervals:
            gap_s, gap_e = _ns(gap_start), _ns(gap_end) if gap_end is not None else 2**63 - 1
            if start_ns < gap_e and end_ns > gap_s:
                return True
        return False

    def _append_second(self, mapped: Mapping[str, Any]) -> None:
        interval = (str(mapped["start_at"]), str(mapped["end_at"]))
        event_id = str(mapped.get("revision_id") or mapped.get("event_id") or mapped.get("content_hash"))
        intervals = list(self._state.processed_intervals)
        ids = list(self._state.processed_event_ids)
        if interval not in intervals:
            intervals.append(interval)
        if event_id not in ids:
            ids.append(event_id)
        watermark = str(mapped.get("execution_watermark"))
        if self._state.processed_watermark is None or _ns(watermark) > _ns(self._state.processed_watermark):
            processed_watermark = watermark
        else:
            processed_watermark = self._state.processed_watermark
        self._state = replace(self._state, processed_intervals=tuple(intervals), processed_event_ids=tuple(ids), processed_watermark=processed_watermark)

    def _append_recovery(self, mapped: Mapping[str, Any], lineage: Mapping[str, Any]) -> None:
        key = str(mapped.get("recovery_bar_id") or mapped.get("content_hash"))
        ids = list(self._state.processed_event_ids)
        if key not in ids:
            ids.append(key)
        lineages = list(self._state.recovery_lineage)
        ident = (mapped.get("recovery_bar_id"), mapped.get("content_hash"))
        if not any((item.get("recovery_bar_id"), item.get("recovery_bar_hash")) == ident for item in lineages if isinstance(item, Mapping)):
            lineages.append(freeze(lineage))
        wm = mapped.get("end_at")
        processed = self._state.processed_watermark
        if processed is None or _ns(wm) > _ns(processed):
            processed = str(wm)
        self._state = replace(self._state, processed_event_ids=tuple(ids), recovery_lineage=tuple(lineages), processed_watermark=processed)

    def _set_partial(self, *, gap: bool = True, reason: str = "partial execution data") -> None:
        uncertainty = _uncertainty(partial=True)
        self._state = replace(self._state, gap_active=gap or self._state.gap_active, partial_data=True, outcome_status="not_evaluated", uncertainty=uncertainty)

    def _settlement_key(self, *, selected_exit: Decimal, reason: str, source: Mapping[str, Any]) -> str:
        projection = {
            "operation": "settle_exit",
            "strategy_id": self.strategy_id,
            "trade_id": self.trade_id,
            "episode_id": self._state.episode_id,
            "selected_exit": format(selected_exit, "f"),
            "exit_reason": reason,
            "source_event": thaw(source),
        }
        return "exit-settle:" + hashlib.sha256(canonical_json_bytes(projection)).hexdigest()

    def _settlement_attempt(
        self,
        *,
        selected_exit: Decimal,
        reason: str,
        source: Mapping[str, Any],
        ambiguity_reason: str | None,
        recovery: bool,
        at: datetime | str | None,
        commit_kind: str,
        commit_context: Any,
    ) -> _PendingSettlementAttempt:
        return _PendingSettlementAttempt(
            key=self._settlement_key(selected_exit=selected_exit, reason=reason, source=source),
            selected_exit=selected_exit,
            reason=reason,
            source=source,
            ambiguity_reason=ambiguity_reason,
            recovery=recovery,
            at=at,
            commit_kind=commit_kind,
            commit_context=commit_context,
        )

    @staticmethod
    def _unknown_settlement_failure(transition: ExitTransition) -> bool:
        return transition.status == "rejected" and transition.code in {
            "settlement_boundary_error",
            "settlement_result_invalid",
            "settlement_authority_mismatch",
        }

    def _commit_settlement_attempt(self, attempt: _PendingSettlementAttempt, transition: ExitTransition) -> ExitTransition:
        """Commit the original trigger only after terminal G024 acceptance."""

        if not transition.accepted or not transition.state.terminal:
            return transition
        if attempt.commit_kind == "second":
            self._append_second(thaw(attempt.commit_context))
        else:
            for item in thaw(attempt.commit_context):
                self._append_recovery(item["mapped"], item["lineage"])
        return replace(transition, state=self._state)

    def _finish_settlement_attempt(self, attempt: _PendingSettlementAttempt, transition: ExitTransition) -> ExitTransition:
        if transition.accepted and transition.state.terminal:
            self._pending_settlement = None
            return self._commit_settlement_attempt(attempt, transition)
        if self._unknown_settlement_failure(transition):
            self._pending_settlement = attempt
        return transition

    def _retry_pending_settlement(self) -> ExitTransition | None:
        """Retry an unknown close before consuming any newer reducer input."""

        attempt = self._pending_settlement
        if attempt is None:
            return None
        transition = self._settle(
            selected_exit=attempt.selected_exit,
            reason=attempt.reason,
            source=thaw(attempt.source),
            ambiguity_reason=attempt.ambiguity_reason,
            recovery=attempt.recovery,
            at=attempt.at,
        )
        if transition.accepted and transition.state.terminal:
            self._pending_settlement = None
            return self._commit_settlement_attempt(attempt, transition)
        # Keep the exact original context for another future retry; the
        # current driver must not advance its own watermark or lineage.
        return transition

    def _trade_binding(self) -> tuple[bool, bool]:
        """Return ``(binding_ok, no_open_trade)`` for the G024 boundary.

        A live open trade must match the complete frozen G024 projection, not
        merely strategy/trade IDs.  A strategy account with no open trade is
        tolerated only so the exact deterministic idempotency key can reach
        G024 and return a replay for a second monitor instance.
        """

        book = self._paper_account_book
        if book is None:
            return True, False
        getter = getattr(book, "get_account", None)
        if not callable(getter):
            return False, False
        try:
            account = getter(self.strategy_id)
        except Exception:
            return False, False
        if account is None:
            return False, False
        if _field(account, "strategy_id", None) != self.strategy_id:
            return False, False
        if isinstance(account, Mapping):
            if "open_trade" not in account:
                return False, False
            open_trade = account.get("open_trade")
        else:
            open_trade = getattr(account, "open_trade", None)
        if open_trade is None:
            return True, True
        try:
            actual = trade_mapping(open_trade)
        except Exception:
            return False, False
        expected = thaw(self._state.trade_lineage)
        if not isinstance(expected, Mapping):
            return False, False
        # Full semantic equality covers symbol/direction/levels/quantity/R,
        # requested-vs-filled entry, allocation and G024 math-version flags.
        if dict(actual) != dict(expected):
            return False, False
        return True, False

    def _trade_binding_ok(self) -> bool:
        return self._trade_binding()[0]

    def _notify_scheduler(self, *, exhausted: bool, reason: str) -> SchedulerCloseNotification:
        if self._scheduler_bridge is not None:
            return self._scheduler_bridge.notify(exhausted=exhausted or self._exhausted, reason=reason)
        if self._scheduler_binding_error is not None and self._on_position_close is None:
            return SchedulerCloseNotification(
                attempted=False,
                accepted=False,
                status="binding_rejected",
                strategy_id=self.strategy_id,
                trade_id=self.trade_id,
                generation=self._state.generation,
                error=self._scheduler_binding_error,
                callback_count=0,
            )
        if self._callback_sent:
            return SchedulerCloseNotification(attempted=True, accepted=False, status="duplicate_suppressed", strategy_id=self.strategy_id, trade_id=self.trade_id, generation=self._state.generation, error="close callback was already attempted", callback_count=1)
        self._callback_sent = True
        if not self._enabled:
            return SchedulerCloseNotification(status="suppressed_disabled", strategy_id=self.strategy_id, trade_id=self.trade_id, generation=self._state.generation)
        if exhausted or self._exhausted:
            return SchedulerCloseNotification(status="suppressed_exhausted", strategy_id=self.strategy_id, trade_id=self.trade_id, generation=self._state.generation)
        callback = self._on_position_close
        if callback is None:
            return SchedulerCloseNotification(status="not_configured", strategy_id=self.strategy_id, trade_id=self.trade_id, generation=self._state.generation)
        try:
            value = invoke_compatible(callback, self.strategy_id, self.trade_id, generation=self._state.generation, exhausted=exhausted, reason=reason)
        except Exception as exc:
            return SchedulerCloseNotification(attempted=True, accepted=False, status="callback_error", strategy_id=self.strategy_id, trade_id=self.trade_id, generation=self._state.generation, error=type(exc).__name__, callback_count=1)
        accepted = value is True or bool(getattr(value, "accepted", getattr(value, "closed", False))) or str(getattr(value, "status", "")).lower() in {"accepted", "closed", "success", "settled"} or (isinstance(value, Mapping) and bool(value.get("accepted", value.get("changed", value.get("status") in {"accepted", "closed", "success", "settled"}))))
        return SchedulerCloseNotification(attempted=True, accepted=accepted, status="accepted" if accepted else "callback_rejected", strategy_id=self.strategy_id, trade_id=self.trade_id, generation=self._state.generation, error=None if accepted else "close callback returned false", callback_count=1)

    def _scheduler_preflight(self) -> ExitTransition | None:
        """Ensure one live typed G023 close boundary before G024 mutation."""

        if self._scheduler_bridge is not None:
            prepared = self._scheduler_bridge.prepare_close()
            if not prepared.accepted:
                code = "missing_scheduler_callback" if prepared.status == "callback_missing" else "scheduler_binding_rejected"
                return self._transition("rejected", reason=prepared.error or prepared.status, code=code)
            return None
        if self._scheduler is not None:
            return self._transition("rejected", reason="G023 scheduler binding is unavailable", code="scheduler_binding_rejected")
        if not callable(self._on_position_close):
            return self._transition("rejected", reason="G023 close callback is not configured", code="missing_scheduler_callback")
        return None

    def _validate_settlement_result(self, result: Any, selected: Decimal) -> tuple[bool, bool, Mapping[str, Any] | None, bool]:
        """Validate the typed G024 close result before advancing G026 state.

        Returns ``(valid, replayed, settlement_mapping, exhausted)``.  The
        accepted/change/replay flags, closed-trade identity, account-open
        status and exact settlement projection are all checked at this edge;
        a forged ``accepted`` mapping therefore cannot close the monitor.
        """

        accepted = _field(result, "accepted", None)
        changed = _field(result, "changed", None)
        replayed = _field(result, "replayed", None)
        if type(accepted) is not bool or not accepted or type(changed) is not bool or type(replayed) is not bool or changed == replayed:
            return False, False, None, False
        closed_trade = _field(result, "closed_trade", None)
        try:
            closed_mapping = trade_mapping(closed_trade)
        except Exception:
            return False, False, None, False
        expected = thaw(self._state.trade_lineage)
        if not isinstance(expected, Mapping) or dict(closed_mapping) != dict(expected):
            return False, False, None, False
        account = _field(result, "account", None)
        if account is None:
            return False, False, None, False
        if _field(account, "strategy_id", None) != self.strategy_id:
            return False, False, None, False
        if isinstance(account, Mapping):
            if "open_trade" not in account or account.get("open_trade") is not None:
                return False, False, None, False
        elif not hasattr(account, "open_trade") or getattr(account, "open_trade") is not None:
            return False, False, None, False
        settlement = _field(result, "settlement", None)
        if not isinstance(settlement, Mapping):
            return False, False, None, False
        required = {"entry_price", "exit_price", "quantity", "applied_pnl", "closing_balance", "balance_after", "exhausted"}
        if not required.issubset(set(settlement)):
            return False, False, None, False
        try:
            if exact_decimal(settlement.get("exit_price"), "settlement.exit_price", positive=True) != selected:
                return False, False, None, False
        except Exception:
            return False, False, None, False
        exhausted = bool(_field(result, "exhausted", False) or settlement.get("exhausted", False))
        return True, replayed, settlement, exhausted

    def _validate_authoritative_account(self, settlement: Mapping[str, Any]) -> bool:
        """Confirm the attached G024 book actually reflects the result."""

        book = self._paper_account_book
        if book is None:
            return True
        getter = getattr(book, "get_account", None)
        if not callable(getter):
            return False
        try:
            account = getter(self.strategy_id)
        except Exception:
            return False
        if account is None or _field(account, "strategy_id", None) != self.strategy_id:
            return False
        if isinstance(account, Mapping):
            if "open_trade" not in account or account.get("open_trade") is not None or "balance" not in account:
                return False
        elif not hasattr(account, "open_trade") or getattr(account, "open_trade") is not None or not hasattr(account, "balance"):
            return False
        try:
            actual_balance = exact_decimal(_field(account, "balance"), "account.balance")
            expected_balance = exact_decimal(settlement.get("closing_balance"), "settlement.closing_balance")
        except Exception:
            return False
        return actual_balance == expected_balance

    def _settle(self, *, selected_exit: Decimal, reason: str, source: Mapping[str, Any], ambiguity_reason: str | None = None, recovery: bool = False, at: datetime | str | None = None) -> ExitTransition:
        if self._state.terminal:
            return self._replayed("duplicate settlement cannot resurrect a settled trade")
        if self._settle_trade is None:
            return self._transition("rejected", reason="G024 settlement boundary is not configured", code="missing_settlement_boundary")
        scheduler_preflight = self._scheduler_preflight()
        if scheduler_preflight is not None:
            return scheduler_preflight
        binding_ok, no_open_trade = self._trade_binding()
        if not binding_ok:
            return self._transition("rejected", reason="G024 open trade identity differs from monitor", code="trade_identity_mismatch")
        selected = exact_decimal(selected_exit, "selected_exit", positive=True)
        key = self._settlement_key(selected_exit=selected, reason=reason, source=source)
        try:
            result = invoke_compatible(
                self._settle_trade,
                self.strategy_id,
                selected,
                idempotency_key=key,
                reason=reason,
                position_id=self.trade_id,
                trade_id=self.trade_id,
                event_reference=source,
            )
        except Exception:
            return self._transition("rejected", reason="G024 settlement transition failed", code="settlement_boundary_error")
        if not _accepted(result):
            return self._transition("rejected", reason="G024 rejected exact exit settlement", code="settlement_rejected", settlement_result=result)
        valid_result, result_replayed, validated_settlement, validated_exhausted = self._validate_settlement_result(result, selected)
        if not valid_result:
            return self._transition("rejected", reason="G024 returned an invalid accepted settlement", code="settlement_result_invalid", settlement_result=result)
        if no_open_trade and not result_replayed:
            return self._transition("rejected", reason="G024 has no live trade; only an idempotent replay is admissible", code="trade_identity_mismatch", settlement_result=result)
        settlement = validated_settlement
        if settlement is None or not self._validate_authoritative_account(settlement):
            return self._transition("rejected", reason="G024 account state disagrees with settlement result", code="settlement_authority_mismatch", settlement_result=result)
        settlement_projection = _result_mapping(settlement)
        exhausted = validated_exhausted
        g024_replayed = result_replayed
        pending_replay = g024_replayed and self._pending_settlement is not None and self._pending_settlement.key == key and not self._callback_sent
        if g024_replayed and not pending_replay:
            # An idempotent G024 replay proves that the paper settlement was
            # already accepted for this exact key.  A fresh monitor (or a
            # monitor whose callback was already attempted) must not notify
            # G023 a second time.
            callback = SchedulerCloseNotification(
                attempted=False,
                accepted=False,
                status="not_attempted",
                strategy_id=self.strategy_id,
                trade_id=self.trade_id,
                generation=self._state.generation,
                error="G024 settlement was an idempotent replay",
                callback_count=0,
            )
        else:
            callback = self._notify_scheduler(exhausted=exhausted, reason=reason)
        event_source = dict(source)
        payload = {
            "status": "settled",
            "outcome_status": reason,
            "strategy_id": self.strategy_id,
            "trade_id": self.trade_id,
            "episode_id": self._state.episode_id,
            "selected_exit": format(selected, "f"),
            "exit_reason": reason,
            "ambiguity_reason": ambiguity_reason,
            "uncertainty": _uncertainty(ambiguous=bool(ambiguity_reason), recovery=recovery),
            "source": event_source,
            "settlement": settlement_projection,
            "scheduler_callback": callback.to_dict(),
            "g024_replayed": g024_replayed,
            "trade_lineage": thaw(self._state.trade_lineage),
            "decision_lineage": thaw(self._state.decision_lineage),
        }
        event = ExitEvent("paper_trade_exit_settled", self.strategy_id, self.trade_id, payload)
        closed_at = utc_instant(at or source.get("execution_watermark") or source.get("end_at") or self._state.entry_filled_at or "1970-01-01T00:00:00Z")
        uncertainty = _uncertainty(ambiguous=bool(ambiguity_reason), recovery=recovery)
        self._state = replace(
            self._state,
            status="settled",
            outcome_status=reason,
            terminal_reason=reason,
            selected_exit=selected,
            exit_reason=reason,
            ambiguity_reason=ambiguity_reason,
            uncertainty=uncertainty,
            settlement=settlement_projection if isinstance(settlement_projection, Mapping) else {"result": settlement_projection},
            scheduler_notification=callback.to_dict(),
            closed_at=closed_at,
            gap_active=False,
            partial_data=False if recovery else self._state.partial_data,
        )
        return self._transition(
            "replayed" if g024_replayed else ("settled" if not exhausted else "exhausted"),
            changed=not g024_replayed,
            accepted=True,
            replayed=g024_replayed,
            event=event,
            lineage=event_source,
            settlement_result=result,
            scheduler_callback=callback,
            selected_exit=selected,
            exit_reason=reason,
            ambiguity_reason=ambiguity_reason,
            outcome_status=reason,
            partial_data=self._state.partial_data,
            gap_overlap=self._state.gap_active,
        )

    def _touch(self, low: Decimal, high: Decimal) -> tuple[Decimal | None, str | None, str | None]:
        if self._state.direction == "long":
            stop_hit = low <= self._state.stop_price
            target_hit = high >= self._state.target_price
        else:
            stop_hit = high >= self._state.stop_price
            target_hit = low <= self._state.target_price
        if stop_hit and target_hit:
            return self._state.stop_price, "stop_loss", "dual_touch_stop_first"
        if stop_hit:
            return self._state.stop_price, "stop_loss", None
        if target_hit:
            return self._state.target_price, "take_profit", None
        return None, None, None

    def settle(self, exit_price: Any = None, *_args: Any, reason: str = "manual", source_event: Mapping[str, Any] | None = None, source: Mapping[str, Any] | None = None, ambiguity_reason: str | None = None, recovery: bool = False, at: datetime | str | None = None, **_kwargs: Any) -> ExitTransition:
        """Reject direct caller-selected settlement.

        Runtime settlement is intentionally reachable only through a validated
        frozen G020 second or an eligible G019 recovery batch.  Keeping this
        compatibility method typed-but-non-executable prevents a caller from
        selecting a favorable configured level without source evidence.
        """

        return self._transition(
            "rejected",
            reason="direct settlement requires a validated frozen market event",
            code="direct_settlement_unsupported",
            outcome_status=self._state.outcome_status,
            not_evaluated=self._state.outcome_status == "not_evaluated",
            partial_data=self._state.partial_data,
        )

    close_position = settle
    close_trade = settle

    def _event_eligible_after_fill(self, start: str, end: str) -> tuple[bool, str | None]:
        interval = self._state.entry_fill_interval
        if interval is not None:
            fill_end = _ns(interval[1])
            start_ns, end_ns = _ns(start), _ns(end)
            if end_ns <= fill_end:
                return False, "entry_fill_second"
            if start_ns < fill_end < end_ns:
                return False, "entry_fill_straddle"
            return start_ns >= fill_end, None
        if self._state.entry_filled_at is not None:
            fill_ns = _ns(self._state.entry_filled_at)
            start_ns, end_ns = _ns(start), _ns(end)
            if end_ns <= fill_ns:
                return False, "before_entry_fill"
            if start_ns <= fill_ns < end_ns:
                return False, "entry_fill_second"
        return True, None

    def _recovery_identity_matches(self, mapped: Mapping[str, Any]) -> bool:
        instrument = mapped.get("instrument")
        if not isinstance(instrument, Mapping) or instrument.get("symbol") != self._state.symbol:
            return False
        if self._expected_recovery_symbol is not None and self._expected_recovery_symbol != self._state.symbol:
            return False
        if dict(mapped.get("market_feed", {})) != dict(self._state.market_feed):
            return False
        if self._expected_recovery_feed is not None and dict(self._expected_recovery_feed) != dict(self._state.market_feed):
            return False
        if self._expected_recovery_feed_name is not None and self._expected_recovery_feed_name != "iex":
            return False
        if self._expected_source_request_hash is not None and mapped.get("source_request_hash") != self._expected_source_request_hash:
            return False
        # ``recovery_cursor`` is a per-bar G019 cursor and is already
        # validated as that bar's own ``end_at``.  A HistoryRequest/Result
        # envelope may carry one batch cursor, but it must never be compared
        # to every minute bar here; batch-level validation handles it below.
        return instrument.get("venue") == "IEX" and instrument.get("asset_class") == "us_equity" and instrument.get("currency") == "USD"

    def _recovery_batch_envelope_matches(self, mapped_values: list[Mapping[str, Any]]) -> bool:
        """Bind a G019 request envelope to the complete recovery batch.

        G019's ``HistoryRequest.start_at/end_at`` describe one half-open
        request window, whereas each RecoveryBar is one minute and carries
        its own end cursor.  The envelope must therefore be covered by the
        batch edges, not repeated on every bar.  Edge extension is allowed
        only when the first/last bars still intersect the request; the
        contiguous active-gap coverage check below remains authoritative.
        """

        if not mapped_values:
            return False
        ordered = mapped_values
        try:
            first_start_ns = _ns(str(ordered[0]["start_at"]))
            last_end_ns = _ns(str(ordered[-1]["end_at"]))
        except Exception:
            return False
        if self._expected_recovery_start is not None or self._expected_recovery_end is not None:
            if self._expected_recovery_start is None or self._expected_recovery_end is None:
                return False
            try:
                request_start_ns = _ns(self._expected_recovery_start)
                request_end_ns = _ns(self._expected_recovery_end)
            except Exception:
                return False
            if request_end_ns <= request_start_ns:
                return False
            if first_start_ns > request_start_ns or last_end_ns < request_end_ns:
                return False
            request_cursor_ns = request_start_ns
            previous_start_ns: int | None = None
            previous_end_ns: int | None = None
            for mapped in ordered:
                try:
                    start_ns, end_ns = _ns(str(mapped["start_at"])), _ns(str(mapped["end_at"]))
                except Exception:
                    return False
                if end_ns <= request_start_ns or start_ns >= request_end_ns:
                    return False
                if previous_start_ns is not None and start_ns < previous_start_ns:
                    return False
                if previous_end_ns is not None and start_ns < previous_end_ns:
                    return False
                if start_ns > request_cursor_ns:
                    return False
                request_cursor_ns = max(request_cursor_ns, end_ns)
                previous_start_ns, previous_end_ns = start_ns, end_ns
            if request_cursor_ns < request_end_ns:
                return False
        if self._expected_recovery_cursor is not None:
            # A timestamp cursor supplied by an outer result is a batch
            # cursor.  Only the final bar may satisfy it; intermediate bars
            # retain their own G019 end cursor.  Opaque provider/page cursors
            # are envelope metadata and are deliberately not compared with
            # any RecoveryBar cursor.
            try:
                _ns(str(self._expected_recovery_cursor))
            except Exception:
                pass
            else:
                if str(ordered[-1].get("recovery_cursor")) != str(self._expected_recovery_cursor):
                    return False
        return True

    def on_second(self, event: Any, *, now: datetime | str | None = None, received_at: datetime | str | None = None, generation: int | None = None) -> ExitTransition:
        if self._state.terminal:
            return self._replayed("late or duplicate second cannot resurrect a settled trade")
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale exit monitor generation", code="generation_mismatch")
        pending = self._retry_pending_settlement()
        if pending is not None:
            return pending
        try:
            mapped = event_mapping(event)
        except Exception:
            return self._transition("rejected", reason="second event failed strict G020 validation", code="invalid_second_event")
        if mapped.get("symbol") != self._state.symbol or not self._same_feed(mapped):
            return self._transition("rejected", reason="second event identity differs from open trade", code="identity_mismatch")
        start, end = str(mapped["start_at"]), str(mapped["end_at"])
        revision_id = str(mapped.get("revision_id") or mapped.get("event_id") or mapped.get("content_hash"))
        interval = (start, end)
        if revision_id in self._state.processed_event_ids or interval in self._state.processed_intervals:
            return self._replayed("duplicate or already-processed second")
        try:
            watermark = str(mapped["execution_watermark"])
            watermark_ns = _ns(watermark)
        except Exception:
            return self._transition("rejected", reason="second execution watermark is malformed", code="invalid_watermark")
        clock_value = now if now is not None else received_at
        if clock_value is not None:
            try:
                if _ns(clock_value) < watermark_ns:
                    return self._transition("not_evaluated", reason="second is not execution-frozen at the supplied clock", code="not_frozen", not_evaluated=True, outcome_status=self._state.outcome_status)
            except Exception:
                return self._transition("rejected", reason="supplied monitor clock is malformed", code="invalid_clock")
        if self._state.processed_watermark is not None and watermark_ns <= _ns(self._state.processed_watermark):
            return self._transition("not_evaluated", reason="second revision is older than processed watermark", code="late_revision", not_evaluated=True, outcome_status=self._state.outcome_status)
        if self._state.processed_intervals:
            latest_end = max(_ns(item[1]) for item in self._state.processed_intervals)
            if _ns(start) < latest_end:
                return self._transition("not_evaluated", reason="out-of-order second precedes processed interval watermark", code="late_revision", not_evaluated=True, outcome_status=self._state.outcome_status)
        if self._state.gap_active:
            quality = mapped.get("quality") if isinstance(mapped.get("quality"), Mapping) else {}
            quality_codes = quality.get("codes") if isinstance(quality.get("codes"), (list, tuple)) else ()
            if bool(quality.get("connection_gap")) or quality.get("status") == "partial" or "partial_data" in quality_codes or self._gap_overlap(start, end) or self._state.gap_active:
                self._set_partial(gap=True)
                return self._transition("not_evaluated", accepted=True, reason="open position waits for gap recovery", code="gap_overlap", not_evaluated=True, partial_data=True, gap_overlap=True)
        fill_ok, fill_reason = self._event_eligible_after_fill(start, end)
        if not fill_ok:
            self._append_second(mapped)
            return self._transition("not_evaluated", changed=True, accepted=True, reason="entry-fill second is not an exit interval", code=fill_reason, not_evaluated=True, outcome_status=self._state.outcome_status)
        quality = mapped.get("quality") if isinstance(mapped.get("quality"), Mapping) else {}
        quality_codes = quality.get("codes") if isinstance(quality.get("codes"), (list, tuple)) else ()
        if bool(quality.get("connection_gap")) or quality.get("status") == "partial" or "partial_data" in quality_codes:
            self._append_second(mapped)
            if (start, end) not in self._state.gap_intervals:
                self._state = replace(self._state, gap_intervals=self._state.gap_intervals + ((start, end),))
            self._set_partial(gap=True)
            return self._transition("not_evaluated", changed=True, accepted=True, reason="confirmed/partial gap cannot invent an exit", code="gap_overlap", not_evaluated=True, partial_data=True, gap_overlap=True)
        try:
            low, high = exact_decimal(mapped["low"], "low", positive=True), exact_decimal(mapped["high"], "high", positive=True)
        except Exception:
            return self._transition("rejected", reason="second OHLC is malformed", code="invalid_ohlc")
        selected, reason, ambiguity = self._touch(low, high)
        if selected is None:
            self._append_second(mapped)
            self._state = replace(self._state, outcome_status="open")
            return self._transition("open", changed=True, accepted=True, reason="frozen second did not touch stop or target", code="no_touch", outcome_status="open")
        source = _source_ref(mapped)
        exit_reason = reason or "exit"
        at = mapped.get("execution_watermark")
        attempt = self._settlement_attempt(
            selected_exit=selected,
            reason=exit_reason,
            source=source,
            ambiguity_reason=ambiguity,
            recovery=False,
            at=at,
            commit_kind="second",
            commit_context=mapped,
        )
        transition = self._settle(selected_exit=selected, reason=exit_reason, source=source, ambiguity_reason=ambiguity, recovery=False, at=at)
        return self._finish_settlement_attempt(attempt, transition)

    consume_second = on_second
    process_second = on_second
    on_event = on_second
    process_event = on_second
    handle_market_event = on_second

    def on_gap(self, gap: Any = None, *, start_at: str | None = None, end_at: str | None = None, reason: str | None = None, generation: int | None = None) -> ExitTransition:
        if self._state.terminal:
            return self._replayed("late gap cannot resurrect a settled trade")
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale exit monitor generation", code="generation_mismatch")
        pending = self._retry_pending_settlement()
        if pending is not None:
            return pending
        if gap is not None:
            try:
                start_at = _field(gap, "start_at", start_at)
                end_at = _field(gap, "end_at", end_at)
                reason = _field(gap, "reason", reason)
            except Exception:
                return self._transition("rejected", reason="gap mapping is malformed", code="invalid_gap")
        if not isinstance(start_at, str):
            return self._transition("rejected", reason="gap start_at is required", code="invalid_gap")
        open_ended = end_at is None
        if open_ended:
            try:
                start = _stamp(start_at)
            except Exception:
                return self._transition("rejected", reason="gap timestamps are invalid", code="invalid_gap")
            item = (start, None)
            if item in self._state.gap_intervals:
                return self._replayed("duplicate confirmed gap")
            gap_reason = reason or "confirmed feed gap"
            payload = {
                "gap_interval": [start, None],
                "start_at": start,
                "end_at": None,
                "reason": gap_reason,
                "outcome_status": "not_evaluated",
                "partial_data": True,
                "source": "confirmed_feed_gap",
            }
            self._state = replace(self._state, gap_intervals=self._state.gap_intervals + (item,), gap_active=True, partial_data=True, outcome_status="not_evaluated", uncertainty=_uncertainty(partial=True))
            event = ExitEvent("paper_trade_gap_detected", self.strategy_id, self.trade_id, payload)
            return self._transition("not_evaluated", changed=True, accepted=True, reason=gap_reason, code="gap_overlap", event=event, lineage=payload, not_evaluated=True, partial_data=True, gap_overlap=True, outcome_status="not_evaluated")
        if not isinstance(end_at, str):
            return self._transition("rejected", reason="finite gap end_at is required", code="invalid_gap")
        try:
            start = _stamp(start_at)
            end = _stamp(end_at)
            if _ns(end) <= _ns(start):
                raise ValueError
        except Exception:
            return self._transition("rejected", reason="gap timestamps are invalid", code="invalid_gap")
        item = (start, end)
        if item in self._state.gap_intervals:
            return self._replayed("duplicate confirmed gap")
        intervals = self._state.gap_intervals + (item,)
        fill_end_ns = _ns(self._state.entry_fill_interval[1])
        # Recompute from the complete immutable gap worklist.  A newly
        # recorded pre-entry interval must never clear an older post-entry
        # gap that is still waiting for recovery.
        overlap = any(gap_end is None or _ns(gap_end) > fill_end_ns for _gap_start, gap_end in intervals)
        gap_reason = reason or "confirmed feed gap"
        if overlap:
            outcome_status = "not_evaluated"
            partial_data = True
            uncertainty = _uncertainty(partial=True)
        else:
            # A gap entirely before the entry-fill interval is retained for
            # lineage but does not change an otherwise executable monitor.
            outcome_status = self._state.outcome_status
            partial_data = self._state.partial_data
            uncertainty = self._state.uncertainty
        self._state = replace(self._state, gap_intervals=intervals, gap_active=overlap, partial_data=partial_data, outcome_status=outcome_status, uncertainty=uncertainty)
        payload = {
            "gap_interval": [start, end],
            "start_at": start,
            "end_at": end,
            "reason": gap_reason,
            "outcome_status": outcome_status,
            "partial_data": partial_data,
            "source": "confirmed_feed_gap",
        }
        event = ExitEvent("paper_trade_gap_detected", self.strategy_id, self.trade_id, payload)
        return self._transition("not_evaluated" if overlap else "gap", changed=True, accepted=True, reason=gap_reason, code="gap_overlap" if overlap else "gap_outside_window", event=event, lineage=payload, not_evaluated=overlap, partial_data=partial_data, gap_overlap=overlap, outcome_status=outcome_status)

    def close_gap(self, start_at: str, end_at: str, *, reason: str | None = None, generation: int | None = None) -> ExitTransition:
        if self._state.terminal:
            return self._replayed("late gap update cannot rewrite a settled trade")
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale exit monitor generation", code="generation_mismatch")
        pending = self._retry_pending_settlement()
        if pending is not None:
            return pending
        if not isinstance(start_at, str) or not isinstance(end_at, str):
            return self._transition("rejected", reason="finite gap start_at and end_at are required", code="invalid_gap")
        try:
            start, end = _stamp(start_at), _stamp(end_at)
            if _ns(end) <= _ns(start):
                raise ValueError
        except Exception:
            return self._transition("rejected", reason="gap timestamps are invalid", code="invalid_gap")
        if (start, None) not in self._state.gap_intervals:
            return self._transition("rejected", reason="no matching open-ended gap", code="gap_update_mismatch")
        intervals = tuple((start, end) if item == (start, None) else item for item in self._state.gap_intervals)
        # Recompute execution overlap from the complete immutable worklist.
        # Closing a pre-entry open gap must clear the temporary partial-data
        # poison when no independent post-entry/open gap remains, while a
        # separate active gap must continue to pause evaluation.
        fill_end_ns = _ns(self._state.entry_fill_interval[1])
        overlap = any(item[1] is None or _ns(item[1]) > fill_end_ns for item in intervals)
        if overlap:
            outcome_status = "not_evaluated"
            partial_data = True
            uncertainty = _uncertainty(partial=True)
            transition_status = "not_evaluated"
            transition_code = "gap_overlap"
        else:
            outcome_status = "open"
            partial_data = False
            uncertainty = None
            transition_status = "gap"
            transition_code = "gap_outside_window"
        gap_reason = reason or "confirmed feed gap closed"
        payload = {
            "gap_interval": [start, end],
            "start_at": start,
            "end_at": end,
            "reason": gap_reason,
            "outcome_status": outcome_status,
            "not_evaluated": overlap,
            "partial_data": partial_data,
            "gap_overlap": overlap,
            "source": "confirmed_feed_gap_update",
        }
        self._state = replace(self._state, gap_intervals=intervals, gap_active=overlap, partial_data=partial_data, outcome_status=outcome_status, uncertainty=uncertainty)
        event = ExitEvent("paper_trade_gap_updated", self.strategy_id, self.trade_id, payload)
        return self._transition(transition_status, changed=True, accepted=True, reason=gap_reason, code=transition_code, event=event, lineage=payload, not_evaluated=overlap, partial_data=partial_data, gap_overlap=overlap, outcome_status=outcome_status)

    submit_gap = on_gap
    mark_gap = on_gap

    def _process_recovery(self, values: Iterable[Any], *, now: datetime | str | None = None) -> ExitTransition:
        # Recovery is a batch boundary.  Validate every candidate (including
        # chronology and entry-fill eligibility) before reading any OHLC for
        # touch evaluation or mutating processed lineage.  A later favorable
        # bar must never bypass an earlier malformed/incomplete candidate.
        self._state = replace(self._state, recovery_attempted=True)
        mapped_values: list[Mapping[str, Any]] = []
        saw_duplicate = False
        batch_source_request_hash: str | None = None
        previous_end_ns: int | None = None
        fill_end_ns = _ns(self._state.entry_fill_interval[1])
        # Only finite intervals that reach beyond the entry-fill boundary are
        # exit-recovery work.  Older pre-entry gaps remain in immutable
        # lineage, but must not force an otherwise complete post-entry batch
        # to fetch irrelevant bars.
        finite_gaps = [item for item in self._state.gap_intervals if item[1] is not None and _ns(item[1]) > fill_end_ns]
        if any(item[1] is None for item in self._state.gap_intervals):
            return self._transition("not_evaluated", accepted=True, reason="open-ended gap requires an explicit close update", code="recovery_not_evaluated", not_evaluated=True, partial_data=True, gap_overlap=True)
        if not finite_gaps:
            return self._transition("not_evaluated", accepted=True, reason="recovery has no finite active gap", code="recovery_not_evaluated", not_evaluated=True, partial_data=True, gap_overlap=True)
        merged_gaps: list[tuple[int, int]] = []
        for gap_start, gap_end in sorted(finite_gaps, key=lambda item: _ns(item[0])):
            start_ns, end_ns = _ns(gap_start), _ns(gap_end)
            if merged_gaps and start_ns <= merged_gaps[-1][1]:
                merged_gaps[-1] = (merged_gaps[-1][0], max(merged_gaps[-1][1], end_ns))
            else:
                merged_gaps.append((start_ns, end_ns))
        coverage_start_ns, coverage_end_ns = merged_gaps[0][0], merged_gaps[-1][1]
        if self._state.processed_intervals:
            previous_end_ns = max(_ns(item[1]) for item in self._state.processed_intervals)
        if self._state.processed_watermark is not None:
            watermark_ns = _ns(self._state.processed_watermark)
            previous_end_ns = max(previous_end_ns or watermark_ns, watermark_ns)

        def reject_batch(reason: str, code: str) -> ExitTransition:
            self._set_partial(gap=True)
            return self._transition(
                "not_evaluated",
                accepted=True,
                reason=reason,
                code=code,
                not_evaluated=True,
                partial_data=True,
                gap_overlap=True,
            )

        for value in values:
            try:
                mapped = recovery_mapping(value, expected_source_request_hash=self._expected_source_request_hash)
            except Exception:
                return reject_batch("recovery batch contains a malformed bar", "recovery_not_evaluated")
            if not self._recovery_identity_matches(mapped):
                return reject_batch("recovery bar identity differs from open trade", "recovery_identity_mismatch")
            source_hash = mapped.get("source_request_hash")
            if batch_source_request_hash is None:
                batch_source_request_hash = str(source_hash)
            elif source_hash != batch_source_request_hash:
                return reject_batch("recovery batch mixes source request hashes", "recovery_identity_mismatch")
            identity = str(mapped.get("recovery_bar_id") or mapped.get("content_hash"))
            if identity in self._state.processed_event_ids:
                saw_duplicate = True
                continue

            start, end = str(mapped["start_at"]), str(mapped["end_at"])
            fill_ok, _fill_reason = self._event_eligible_after_fill(start, end)
            if not fill_ok:
                return reject_batch("recovery bar straddles or precedes the entry-fill interval", "recovery_not_evaluated")
            start_ns, end_ns = _ns(start), _ns(end)
            if start_ns > coverage_start_ns or end_ns < coverage_end_ns:
                # Individual bars may extend outside the range, but the
                # complete batch must prove full union coverage below.
                pass
            if mapped.get("complete") is not True or mapped.get("coverage_state") != "complete" or mapped.get("trigger_eligible") is not True:
                return reject_batch("recovery batch contains an incomplete bar", "recovery_not_evaluated")
            # A recovery minute may begin before an execution watermark while
            # still extending beyond it; reject only candidates wholly older
            # than the processed watermark.
            if previous_end_ns is not None and end_ns <= previous_end_ns:
                return reject_batch("recovery batch contains a late or out-of-order bar", "recovery_not_evaluated")
            previous_end_ns = end_ns
            mapped_values.append(mapped)

        if not mapped_values:
            if saw_duplicate:
                return self._replayed("duplicate recovery bar")
            return reject_batch("recovery batch is empty", "recovery_not_evaluated")

        # Validate the whole outer G019 request envelope before reading any
        # OHLC.  Its start/end describe the complete half-open request, not
        # each individual RecoveryBar; the final bar may satisfy a batch
        # cursor while intermediate bars retain their own end cursors.
        if not self._recovery_batch_envelope_matches(mapped_values):
            return reject_batch("recovery batch does not match its request envelope", "recovery_identity_mismatch")

        # Validate contiguous active-gap coverage before reading OHLC or
        # evaluating a favorable touch.  This keeps the batch atomic even if a
        # later candidate would otherwise settle the trade.
        batch_start_ns = _ns(str(mapped_values[0]["start_at"]))
        batch_end_ns = _ns(str(mapped_values[-1]["end_at"]))
        if batch_start_ns > coverage_start_ns or batch_end_ns < coverage_end_ns:
            return reject_batch("recovery batch does not fully cover all active gaps", "recovery_not_evaluated")
        cursor_ns = coverage_start_ns
        for mapped in mapped_values:
            start_ns, end_ns = _ns(str(mapped["start_at"])), _ns(str(mapped["end_at"]))
            if start_ns > cursor_ns:
                return reject_batch("recovery batch contains a coverage hole", "recovery_not_evaluated")
            cursor_ns = max(cursor_ns, end_ns)
        if cursor_ns < coverage_end_ns:
            return reject_batch("recovery batch does not reach the end of all active gaps", "recovery_not_evaluated")

        # The strict G019 validator already checks canonical OHLC, but keep
        # this conversion inside the all-or-nothing preflight so a forged
        # typed mapping cannot partially advance recovery state.
        evaluated: list[tuple[Mapping[str, Any], dict[str, Any], Decimal, Decimal, Decimal | None, str | None, str | None]] = []
        for mapped in mapped_values:
            try:
                low = exact_decimal(mapped["low"], "low", positive=True)
                high = exact_decimal(mapped["high"], "high", positive=True)
            except Exception:
                return reject_batch("recovery batch contains malformed OHLC", "recovery_not_evaluated")
            lineage = _source_ref(mapped, recovery=True)
            lineage["recovery_bar_hash"] = mapped.get("content_hash")
            lineage["recovery_bar_id"] = mapped.get("recovery_bar_id")
            lineage["uncertainty"] = _uncertainty(ambiguous=True, recovery=True)
            selected, reason, ambiguity = self._touch(low, high)
            evaluated.append((mapped, lineage, low, high, selected, reason, ambiguity))

        first_touch = next((index for index, item in enumerate(evaluated) if item[4] is not None), None)
        apply_count = first_touch + 1 if first_touch is not None else len(evaluated)

        if first_touch is not None:
            _mapped, lineage, _low, _high, selected, reason, ambiguity = evaluated[first_touch]
            exit_reason = reason or "exit"
            at = str(_mapped["end_at"])
            attempt = self._settlement_attempt(
                selected_exit=selected,
                reason=exit_reason,
                source=lineage,
                ambiguity_reason=ambiguity,
                recovery=True,
                at=at,
                commit_kind="recovery",
                commit_context=tuple(
                    {"mapped": mapped, "lineage": item_lineage}
                    for mapped, item_lineage, _low, _high, _selected, _reason, _ambiguity in evaluated[:apply_count]
                ),
            )
            transition = self._settle(selected_exit=selected, reason=exit_reason, source=lineage, ambiguity_reason=ambiguity, recovery=True, at=at)
            return self._finish_settlement_attempt(attempt, transition)

        # Recovery lineage is retained separately; clear only the active gap
        # worklist so a later, unrelated gap does not inherit an old range.
        resolved = set(finite_gaps)
        remaining_gaps = tuple(item for item in self._state.gap_intervals if item not in resolved)
        remaining_active = any(item[1] is None or _ns(item[1]) > fill_end_ns for item in remaining_gaps)
        self._state = replace(self._state, gap_intervals=remaining_gaps, gap_active=remaining_active, partial_data=remaining_active, outcome_status="not_evaluated" if remaining_active else "open", uncertainty=_uncertainty(partial=True) if remaining_active else None)
        return self._transition("recovered", changed=True, accepted=True, reason="complete recovery data consumed", code="recovered", outcome_status="open")

    def on_recovery(self, values: Any = None, *, now: datetime | str | None = None, generation: int | None = None) -> ExitTransition:
        if self._state.terminal:
            return self._replayed("late recovery cannot resurrect a settled trade")
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale exit monitor generation", code="generation_mismatch")
        pending = self._retry_pending_settlement()
        if pending is not None:
            return pending
        if not self._state.gap_active:
            if self._state.recovery_attempted:
                return self._replayed("duplicate recovery delivery")
            return self._transition("not_evaluated", accepted=True, reason="recovery is only consumed for an overlapping gap", code="recovery_without_gap", not_evaluated=True)
        if values is None and self._recovery_callback is not None and not self._recovery_callback_called:
            self._recovery_callback_called = True
            try:
                values = invoke_compatible(self._recovery_callback, self._state, generation=self._state.generation)
            except Exception:
                values = ()
        try:
            normalized = _normal_values(values)
        except Exception:
            return self._transition("rejected", reason="recovery response is malformed", code="invalid_recovery")
        return self._process_recovery(normalized, now=now)

    recover = on_recovery
    reconcile = on_recovery

    def on_timer(self, now: datetime | str | None = None, **_: Any) -> ExitTransition:
        # Open positions have no X expiry.  This explicit no-op keeps a timer
        # callback from accidentally inventing a favorable exit.
        if self._state.terminal:
            return self._replayed("timer after settlement is a duplicate")
        pending = self._retry_pending_settlement()
        if pending is not None:
            return pending
        return self._transition("open", accepted=True, reason="open position remains monitored", code="no_expiry", outcome_status=self._state.outcome_status, not_evaluated=self._state.outcome_status == "not_evaluated", partial_data=self._state.partial_data)

    process_timer = on_timer
    handle_timer = on_timer

    def disable(self, *, reason: str = "disabled") -> ExitTransition:
        if self._state.terminal:
            return self._replayed("duplicate disable")
        self._enabled = False
        return self._transition("open", changed=True, accepted=True, reason=reason, code="disabled", outcome_status=self._state.outcome_status, partial_data=self._state.partial_data)

    deactivate = disable

    def reduce(self, event: Any = None, *, now: datetime | str | None = None, gap: Any = None, recovery: Any = None, generation: int | None = None) -> ExitTransition:
        if gap is not None:
            result = self.on_gap(gap, generation=generation)
            if result.status == "rejected":
                return result
        if event is not None:
            result = self.on_second(event, now=now, generation=generation)
            if result.status in {"settled", "exhausted", "rejected"} or self._state.terminal:
                return result
        if recovery is not None:
            result = self.on_recovery(recovery, now=now, generation=generation)
            if result.status in {"settled", "exhausted", "rejected"} or self._state.terminal:
                return result
        return self._transition("open", accepted=True, reason="no state change", code="no_state_change", outcome_status=self._state.outcome_status, not_evaluated=self._state.outcome_status == "not_evaluated", partial_data=self._state.partial_data)

    handle_second = on_second
    consume_event = on_second
    process_event = on_second
    on_frozen_second = on_second
    handle_frozen_second = on_second
    on_exit_event = on_second


class ExitMonitor(ExitReducer):
    """Public G026 monitor facade."""

    def __init__(self, state_or_trade: ExitState | Any, **kwargs: Any) -> None:
        # Ergonomic construction accepts either the already validated state or
        # a G024 PaperTrade/mapping. Invalid inputs are normalized to a
        # ValueError by make_exit_state rather than leaking KeyError/AttributeError.
        if "on_close" in kwargs and "on_position_close" not in kwargs:
            kwargs["on_position_close"] = kwargs.pop("on_close")
        if "settle" in kwargs and "settle_trade" not in kwargs:
            kwargs["settle_trade"] = kwargs.pop("settle")
        if "account_book" in kwargs and "paper_account_book" not in kwargs:
            kwargs["paper_account_book"] = kwargs.pop("account_book")
        if isinstance(state_or_trade, ExitState):
            state = state_or_trade
        else:
            state_keys = {"decision_lineage", "entry_event", "entry_fill_event", "entry_second", "entry_fill_second", "fill_event", "entry_fill_reference", "entry_filled_at", "entry_fill_at", "entry_fill_interval", "generation", "market_feed"}
            state_kwargs = {key: kwargs.pop(key) for key in tuple(kwargs) if key in state_keys}
            state = make_exit_state(state_or_trade, **state_kwargs)
        super().__init__(state, **kwargs)

    @classmethod
    def from_trade(cls, trade: Any, **kwargs: Any) -> "ExitMonitor":
        if "on_close" in kwargs and "on_position_close" not in kwargs:
            kwargs["on_position_close"] = kwargs.pop("on_close")
        if "settle" in kwargs and "settle_trade" not in kwargs:
            kwargs["settle_trade"] = kwargs.pop("settle")
        if "account_book" in kwargs and "paper_account_book" not in kwargs:
            kwargs["paper_account_book"] = kwargs.pop("account_book")
        state_keys = {"decision_lineage", "entry_event", "entry_fill_event", "entry_second", "entry_fill_second", "fill_event", "entry_fill_reference", "entry_filled_at", "entry_fill_at", "entry_fill_interval", "generation", "market_feed"}
        state_kwargs = {key: kwargs.pop(key) for key in tuple(kwargs) if key in state_keys}
        state = make_exit_state(trade, **state_kwargs)
        return cls(state, **kwargs)

    @classmethod
    def from_position(cls, trade: Any, **kwargs: Any) -> "ExitMonitor":
        return cls.from_trade(trade, **kwargs)

    from_open_trade = from_trade
    from_paper_trade = from_trade

    @classmethod
    def create(cls, trade: Any, **kwargs: Any) -> "ExitMonitor":
        return cls.from_trade(trade, **kwargs)


PositionExitMonitor = ExitMonitor
StopTargetMonitor = ExitMonitor
LevelMonitor = ExitMonitor
OpenPositionMonitor = ExitMonitor
OpenTradeMonitor = ExitMonitor
ExitSettlementReducer = ExitReducer
StopTargetReducer = ExitReducer


def create_exit_monitor(trade: Any, **kwargs: Any) -> ExitMonitor:
    return ExitMonitor.from_trade(trade, **kwargs)


def create_monitor(trade: Any, **kwargs: Any) -> ExitMonitor:
    return ExitMonitor.from_trade(trade, **kwargs)


def reduce_exit(state: ExitState, event: Any = None, **kwargs: Any) -> ExitTransition:
    event_kwargs = {key: kwargs[key] for key in ("now", "received_at", "generation") if key in kwargs}
    constructor_kwargs = {key: value for key, value in kwargs.items() if key not in event_kwargs}
    reducer = ExitReducer(state, **constructor_kwargs)
    if event is not None:
        return reducer.on_second(event, **event_kwargs)
    return reducer._transition("open", accepted=True, code="no_state_change", outcome_status=state.outcome_status)


reduce = reduce_exit
settle_exit = reduce_exit
monitor_open_position = create_exit_monitor


__all__ = [
    "ExitMonitor",
    "ExitReducer",
    "LevelMonitor",
    "OpenPositionMonitor",
    "OpenTradeMonitor",
    "ExitSettlementReducer",
    "StopTargetReducer",
    "PositionExitMonitor",
    "StopTargetMonitor",
    "create_exit_monitor",
    "create_monitor",
    "make_exit_state",
    "monitor_open_position",
    "reduce",
    "reduce_exit",
    "settle_exit",
]
