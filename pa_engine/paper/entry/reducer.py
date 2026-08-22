"""Single in-memory reducer for G025 pending price-trigger entries."""

from __future__ import annotations

import copy
import inspect
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping

from pa_engine.market import ALPACA_IEX_FEED
from pa_engine.market_sources.alpaca.history import RecoveryBar
from pa_engine.market_sources.alpaca.stream import canonical_timestamp as event_timestamp, timestamp_ns

from .boundary import (
    canonical_timestamp,
    event_mapping,
    invoke_compatible,
    make_pending_state,
    recovery_mapping,
    utc_instant,
)
from .models import EntryEvent, PendingEntryState, PendingEntryTransition, freeze, thaw
from .scheduler_bridge import PendingEntrySchedulerBridge


UTC = timezone.utc
_NS = 1_000_000_000


def _instant_ns(value: datetime | str) -> int:
    if isinstance(value, datetime):
        instant = value.astimezone(UTC)
        return int(instant.timestamp()) * _NS + instant.microsecond * 1000
    return timestamp_ns(str(value))


def _stamp(value: datetime | str) -> str:
    if isinstance(value, str):
        try:
            return event_timestamp(value)
        except Exception:
            pass
    return canonical_timestamp(value)


def _field(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def _accepted(value: Any, *, statuses: set[str] | None = None) -> bool:
    if value is True:
        return True
    if isinstance(value, Mapping):
        if value.get("accepted") is False:
            return False
        if value.get("accepted") is True or value.get("changed") is True:
            return True
        status = str(value.get("status", "")).lower()
    else:
        if hasattr(value, "accepted") and getattr(value, "accepted") is False:
            return False
        if bool(getattr(value, "accepted", False)) or bool(getattr(value, "changed", False)):
            return True
        status = str(getattr(value, "status", "")).lower()
    return status in (statuses or {"accepted", "pending", "filled", "expired", "replay", "open", "success"})


def _position_id(value: Any) -> str | None:
    return _field(value, "position_id") or _field(value, "trade_id")


def _episode_projection(state: PendingEntryState) -> dict[str, Any]:
    """Stable identity for one pending episode (not merely its G023 ID)."""

    return {
        "pending_id": state.pending_id,
        "generation": state.generation,
        "decision_accepted_at": _stamp(state.decision_accepted_at),
        "expires_at": _stamp(state.expires_at),
    }


def _episode_id(state: PendingEntryState) -> str:
    import hashlib
    from pa_engine.contracts import canonical_json_bytes

    return hashlib.sha256(canonical_json_bytes(_episode_projection(state))).hexdigest()


def _cancel_idempotency_key(state: PendingEntryState, _reason: str) -> str:
    """Stable cancellation identity scoped to one pending episode."""

    import hashlib
    from pa_engine.contracts import canonical_json_bytes

    projection = {
        "operation": "cancel_pending_entry",
        "strategy_id": state.strategy_id,
        "episode": _episode_projection(state),
        "episode_id": _episode_id(state),
    }
    return "entry-cancel:" + hashlib.sha256(canonical_json_bytes(projection)).hexdigest()


def _normal_recovery_values(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        for key in ("bars", "recovery_bars", "values", "data"):
            if key in value:
                nested = value[key]
                if isinstance(nested, (list, tuple)):
                    return tuple(nested)
                return (nested,)
        if "start_at" in value or value.get("artifact_kind") == "recovery_bar":
            return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(value)
    for key in ("recovery_bars", "bars", "values", "data"):
        nested = getattr(value, key, None)
        if nested is not None:
            if isinstance(nested, (list, tuple)):
                return tuple(nested)
            return (nested,)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return tuple(value)
    return (value,)


class PendingEntryReducer:
    """Deterministic one-pending-entry reducer.

    The reducer owns no clock, network, storage or model calls.  Market/timer
    inputs are fed into :meth:`on_second`, :meth:`on_gap` and
    :meth:`on_expiry`; both races converge through this one transition path.
    """

    def __init__(
        self,
        state: PendingEntryState,
        *,
        fill_pending: Callable[..., Any] | None = None,
        expire_pending: Callable[..., Any] | None = None,
        cancel_pending: Callable[..., Any] | None = None,
        on_pending_fill: Callable[..., Any] | None = None,
        on_pending_expiry: Callable[..., Any] | None = None,
        recovery_callback: Callable[..., Any] | None = None,
        scheduler: Any = None,
        scheduler_bridge: PendingEntrySchedulerBridge | None = None,
        paper_account_book: Any = None,
        recovery_request: Mapping[str, Any] | None = None,
        expected_source_request_hash: str | None = None,
        expected_recovery_cursor: str | None = None,
        enabled: bool = True,
        exhausted: bool = False,
    ) -> None:
        if not isinstance(state, PendingEntryState):
            raise TypeError("state must be PendingEntryState")
        self._state = state
        self._fill_pending = fill_pending
        self._expire_pending = expire_pending
        self._cancel_pending = cancel_pending
        self._on_pending_fill = on_pending_fill
        self._on_pending_expiry = on_pending_expiry
        self._recovery_callback = recovery_callback
        self._scheduler = scheduler
        self._scheduler_bridge = scheduler_bridge
        self._secondary_fill_callback: Callable[..., Any] | None = None
        self._secondary_expiry_callback: Callable[..., Any] | None = None
        self._recovery_request = copy.deepcopy(dict(recovery_request)) if isinstance(recovery_request, Mapping) else recovery_request
        self._expected_source_request_hash = expected_source_request_hash
        self._expected_recovery_cursor = expected_recovery_cursor
        self._expected_recovery_symbol: str | None = None
        self._expected_recovery_feed: Mapping[str, Any] | None = None
        if self._recovery_request is not None:
            nested_request = _field(self._recovery_request, "request")
            if nested_request is not None:
                self._recovery_request = nested_request
            request_instrument = _field(self._recovery_request, "instrument")
            if isinstance(request_instrument, Mapping):
                self._expected_recovery_symbol = request_instrument.get("symbol")
            self._expected_recovery_symbol = self._expected_recovery_symbol or _field(self._recovery_request, "symbol")
            request_feed = _field(self._recovery_request, "market_feed") or _field(self._recovery_request, "feed")
            if isinstance(request_feed, Mapping):
                self._expected_recovery_feed = dict(request_feed)
            self._expected_source_request_hash = self._expected_source_request_hash or (
                _field(self._recovery_request, "source_request_hash")
                or _field(self._recovery_request, "request_hash")
                or _field(self._recovery_request, "content_hash")
            )
            self._expected_recovery_cursor = self._expected_recovery_cursor or (
                _field(self._recovery_request, "recovery_cursor")
                or _field(self._recovery_request, "cursor")
            )
        self._callback_fill_sent = False
        self._callback_expiry_sent = False
        self._callback_cancel_sent = False
        self._enabled = bool(enabled)
        self._exhausted = bool(exhausted)
        if paper_account_book is not None:
            self._fill_pending = self._fill_pending or getattr(paper_account_book, "fill_pending_entry", None)
            self._expire_pending = self._expire_pending or getattr(paper_account_book, "expire_pending_entry", None)
            self._cancel_pending = self._cancel_pending or getattr(paper_account_book, "expire_pending_entry", None)
        if self._scheduler_bridge is None and scheduler is not None:
            # A real G023 scheduler has a live record and the private timer
            # cancellation seam.  Bind it here for direct reducer use; the
            # monitor facade binds before creating the G024 pending record.
            self._scheduler_bridge = PendingEntrySchedulerBridge.bind(scheduler, state)
        if scheduler is not None:
            if self._scheduler_bridge is not None:
                self._secondary_fill_callback = self._on_pending_fill
                self._secondary_expiry_callback = self._on_pending_expiry
            self._on_pending_fill = self._on_pending_fill or getattr(scheduler, "on_pending_fill", None)
            self._on_pending_expiry = self._on_pending_expiry or getattr(scheduler, "on_pending_expiry", None)
            self._cancel_pending = self._cancel_pending or getattr(scheduler, "cancel_pending", None)
        if self._scheduler_bridge is not None:
            self._on_pending_fill = self._scheduler_bridge.on_fill
            self._on_pending_expiry = self._scheduler_bridge.on_expiry

    def _scheduler_generation_live(self) -> bool:
        if self._scheduler_bridge is not None:
            return self._scheduler_bridge.is_live()
        if self._scheduler is None:
            return True
        getter = getattr(self._scheduler, "get", None)
        if not callable(getter):
            return True
        try:
            record = getter(self._state.strategy_id)
        except Exception:
            return False
        if record is None:
            return False
        if getattr(record, "generation", self._state.generation) != self._state.generation:
            return False
        if getattr(record, "pending_id", self._state.pending_id) != self._state.pending_id:
            return False
        record_expiry = getattr(record, "expires_at", None)
        if record_expiry is None or _instant_ns(record_expiry) != _instant_ns(self._state.expires_at):
            return False
        record_state = str(getattr(getattr(record, "state", None), "value", getattr(record, "state", ""))).lower()
        if record_state == "market_wait":
            intended = str(getattr(getattr(record, "intended_resume_state", None), "value", getattr(record, "intended_resume_state", ""))).lower()
            if intended != "waiting_entry":
                return False
        elif record_state != "waiting_entry":
            return False
        if getattr(record, "exhausted", False) or getattr(record, "enabled", True) is False:
            return False
        return True

    @classmethod
    def from_instruction(cls, instruction: Mapping[str, Any], **kwargs: Any) -> "PendingEntryReducer":
        if "accepted_at" in kwargs and "decision_accepted_at" not in kwargs:
            kwargs["decision_accepted_at"] = kwargs.pop("accepted_at")
        if "submitted_time" in kwargs and "submitted_at" not in kwargs:
            kwargs["submitted_at"] = kwargs.pop("submitted_time")
        if "book" in kwargs and "paper_account_book" not in kwargs:
            kwargs["paper_account_book"] = kwargs.pop("book")
        state_keys = {"symbol", "decision_accepted_at", "submitted_at", "generation", "pending_id", "market_feed", "expected_profile_ref", "validator"}
        state_kwargs = {key: kwargs.pop(key) for key in tuple(kwargs) if key in state_keys}
        state = make_pending_state(instruction, **state_kwargs)
        return cls(state, **kwargs)

    @property
    def state(self) -> PendingEntryState:
        return self._state

    @property
    def pending(self) -> PendingEntryState:
        return self._state

    @property
    def pending_state(self) -> PendingEntryState:
        return self._state

    @property
    def pending_id(self) -> str:
        return self._state.pending_id

    @property
    def strategy_id(self) -> str:
        return self._state.strategy_id

    @property
    def generation(self) -> int:
        return self._state.generation

    @property
    def expires_at(self) -> datetime:
        return self._state.expires_at

    @property
    def expire_at(self) -> datetime:
        return self._state.expires_at

    @property
    def resolution_at(self) -> datetime:
        return self._state.resolution_at

    @property
    def resolution_deadline(self) -> datetime:
        return self._state.resolution_at

    @property
    def decision_accepted_at(self) -> datetime:
        return self._state.decision_accepted_at

    @property
    def entry_price(self) -> Decimal:
        return self._state.entry_price

    @property
    def terminal(self) -> bool:
        return self._state.terminal

    def snapshot(self) -> PendingEntryState:
        return self._state

    def _transition(self, status: str, *, changed: bool = False, accepted: bool = False, replayed: bool = False, terminal: bool | None = None, reason: str | None = None, code: str | None = None, event: EntryEvent | None = None, lineage: Mapping[str, Any] | None = None, fill_result: Any = None, expiry_result: Any = None, filled_price: Decimal | None = None, exit_eligible_after: datetime | None = None, not_evaluated: bool = False, partial_data: bool = False, gap_overlap: bool = False) -> PendingEntryTransition:
        return PendingEntryTransition(
            status=status,
            state=self._state,
            changed=changed,
            accepted=accepted,
            replayed=replayed,
            terminal=self._state.terminal if terminal is None else terminal,
            strategy_id=self._state.strategy_id,
            pending_id=self._state.pending_id,
            generation=self._state.generation,
            reason=reason,
            code=code,
            event=event,
            lineage=lineage,
            fill_result=fill_result,
            expiry_result=expiry_result,
            filled_price=filled_price,
            exit_eligible_after=exit_eligible_after,
            not_evaluated=not_evaluated,
            partial_data=partial_data,
            gap_overlap=gap_overlap,
        )

    def _replayed(self, reason: str = "terminal state is immutable") -> PendingEntryTransition:
        return self._transition("replayed", replayed=True, accepted=True, reason=reason, code="idempotent_replay")

    def _append_event(self, mapped: Mapping[str, Any]) -> None:
        interval = (str(mapped["start_at"]), str(mapped["end_at"]))
        ids = list(self._state.processed_event_ids)
        event_id = str(mapped.get("revision_id") or mapped.get("event_id") or mapped.get("content_hash"))
        intervals = list(self._state.processed_intervals)
        if interval not in intervals:
            intervals.append(interval)
        if event_id not in ids:
            ids.append(event_id)
        self._state = replace(self._state, processed_intervals=tuple(intervals), processed_event_ids=tuple(ids))

    def _gap_overlap(self, start_at: str, end_at: str) -> bool:
        start, end = _instant_ns(start_at), _instant_ns(end_at)
        decision, expiry = _instant_ns(self._state.decision_accepted_at), _instant_ns(self._state.expires_at)
        if end <= decision or start >= expiry:
            return False
        for gap_start, gap_end in self._state.gap_intervals:
            gs = _instant_ns(gap_start)
            ge = _instant_ns(gap_end) if gap_end is not None else 2**63 - 1
            if start < ge and end > gs:
                return True
        return False

    def _record_gap(self, start_at: str, end_at: str | None, reason: str | None = None) -> PendingEntryTransition:
        start = _stamp(start_at)
        end = _stamp(end_at) if end_at is not None else None
        intervals = list(self._state.gap_intervals)
        item = (start, end)
        if item in intervals:
            return self._transition("replayed", replayed=True, accepted=True, reason="duplicate gap", code="idempotent_replay")
        intervals.append(item)
        overlap = self._gap_overlap(start, end or _stamp(self._state.resolution_at)) or (
            _instant_ns(start) < _instant_ns(self._state.expires_at) and (end is None or _instant_ns(end) > _instant_ns(self._state.decision_accepted_at))
        )
        self._state = replace(self._state, gap_intervals=tuple(intervals), gap_overlap=self._state.gap_overlap or overlap, partial_data=self._state.partial_data or overlap)
        return self._transition("gap", changed=True, accepted=True, reason=reason or "confirmed feed gap", code="gap_overlap" if overlap else "gap_outside_window", partial_data=overlap, gap_overlap=overlap, not_evaluated=overlap)

    def on_gap(self, gap: Any = None, *, start_at: str | None = None, end_at: str | None = None, reason: str | None = None, generation: int | None = None) -> PendingEntryTransition:
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale pending generation", code="generation_mismatch")
        if not self._scheduler_generation_live():
            return self.disable(reason="scheduler generation is no longer active")
        if self._state.terminal:
            return self._replayed("late gap cannot resurrect a terminal entry")
        if gap is not None:
            start_at = _field(gap, "start_at", start_at)
            end_at = _field(gap, "end_at", end_at)
            reason = _field(gap, "reason", reason)
        if not isinstance(start_at, str):
            return self._transition("rejected", reason="gap start_at is required", code="invalid_gap")
        try:
            _instant_ns(start_at)
            if end_at is not None:
                _instant_ns(end_at)
        except Exception:
            return self._transition("rejected", reason="gap timestamps are invalid", code="invalid_gap")
        return self._record_gap(start_at, end_at, reason)

    submit_gap = on_gap
    mark_gap = on_gap

    def _same_feed(self, mapped: Mapping[str, Any]) -> bool:
        feed = mapped.get("market_feed")
        if hasattr(feed, "to_dict"):
            feed = feed.to_dict()
        return isinstance(feed, Mapping) and dict(feed) == dict(self._state.market_feed)

    def _fill(self, *, reference: Mapping[str, Any], lineage: Mapping[str, Any], filled_at: datetime, exit_eligible_after: datetime, recovery: bool = False) -> PendingEntryTransition:
        if self._fill_pending is None:
            return self._transition("rejected", reason="G024 fill boundary is not configured", code="missing_fill_boundary")
        if self._scheduler_bridge is not None and not self._scheduler_bridge.prepare_fill():
            return self._transition("rejected", reason="G023 pending fill guard rejected", code="scheduler_callback_rejected")
        key_projection = {
            "operation": "fill_pending_entry",
            "strategy_id": self._state.strategy_id,
            "episode": _episode_projection(self._state),
            "episode_id": _episode_id(self._state),
            "entry_price": format(self._state.entry_price, "f"),
            "reference": thaw(reference),
            "source": thaw(lineage),
        }
        import hashlib
        from pa_engine.contracts import canonical_json_bytes

        idempotency_key = "entry-fill:" + hashlib.sha256(canonical_json_bytes(key_projection)).hexdigest()
        try:
            result = invoke_compatible(
                self._fill_pending,
                self._state.strategy_id,
                self._state.pending_id,
                reference,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            if self._scheduler_bridge is not None:
                self._scheduler_bridge.abort_fill()
            return self._transition("rejected", reason="G024 fill transition failed", code="fill_boundary_error")
        if not _accepted(result, statuses={"filled", "accepted", "open", "success", "replay"}):
            if self._scheduler_bridge is not None:
                self._scheduler_bridge.abort_fill()
            return self._transition("rejected", reason="G024 rejected the exact pending fill", code="fill_rejected", fill_result=result)
        position_id = _position_id(result)
        event_payload = {
            "status": "filled",
            "strategy_id": self._state.strategy_id,
            "pending_id": self._state.pending_id,
            "direction": self._state.direction,
            "requested_entry_price": format(self._state.entry_price, "f"),
            "fill_price": format(self._state.entry_price, "f"),
            "filled_at": _stamp(filled_at),
            "exit_eligible_after": _stamp(exit_eligible_after),
            "source": thaw(lineage),
            "position_id": position_id,
            "recovery": recovery,
        }
        event = EntryEvent("pending_entry_filled", self._state.strategy_id, self._state.pending_id, event_payload)
        self._state = replace(
            self._state,
            status="filled",
            terminal_reason="price_touch",
            partial_data=False if recovery else self._state.partial_data,
            gap_overlap=False if recovery else self._state.gap_overlap,
            filled_at=filled_at,
            filled_price=self._state.entry_price,
            exit_eligible_after=exit_eligible_after,
        )
        callback_result = None
        if not self._callback_fill_sent:
            self._callback_fill_sent = True
            if self._on_pending_fill is not None:
                try:
                    callback_result = invoke_compatible(
                        self._on_pending_fill,
                        self._state.strategy_id,
                        self._state.pending_id,
                        generation=self._state.generation,
                        position_id=position_id,
                    )
                except Exception:
                    callback_result = None
            if self._secondary_fill_callback is not None:
                try:
                    invoke_compatible(
                        self._secondary_fill_callback,
                        self._state.strategy_id,
                        self._state.pending_id,
                        generation=self._state.generation,
                        position_id=position_id,
                    )
                except Exception:
                    pass
        return self._transition("filled", changed=True, accepted=True, terminal=True, event=event, lineage=lineage, fill_result=result, filled_price=self._state.entry_price, exit_eligible_after=exit_eligible_after)

    def on_second(self, event: Any, *, now: datetime | str | None = None, received_at: datetime | str | None = None, generation: int | None = None) -> PendingEntryTransition:
        if self._state.terminal:
            return self._replayed("late or duplicate second cannot resurrect a terminal entry")
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale pending generation", code="generation_mismatch")
        if not self._scheduler_generation_live():
            return self.disable(reason="scheduler generation is no longer active")
        if now is None and received_at is not None:
            now = received_at
        raw_status = _field(event, "status")
        if raw_status is not None and raw_status != "frozen":
            return self._transition("not_evaluated", reason="second is not execution-frozen", code="not_frozen", not_evaluated=True)
        try:
            mapped = event_mapping(event)
        except Exception:
            return self._transition("rejected", reason="second event failed strict G020 validation", code="invalid_second_event")
        if mapped.get("symbol") != self._state.symbol or not self._same_feed(mapped):
            return self._transition("rejected", reason="second event identity differs from pending entry", code="identity_mismatch")
        start, end = str(mapped["start_at"]), str(mapped["end_at"])
        revision_id = str(mapped.get("revision_id") or mapped.get("event_id") or mapped.get("content_hash"))
        interval = (start, end)
        if revision_id in self._state.processed_event_ids or interval in self._state.processed_intervals:
            return self._transition("replayed", replayed=True, accepted=True, reason="duplicate or late revision", code="idempotent_replay")
        watermark = mapped.get("execution_watermark")
        resolution_ns = _instant_ns(self._state.resolution_at)
        if not isinstance(watermark, str) or _instant_ns(watermark) > resolution_ns:
            self._append_event(mapped)
            return self._transition("not_evaluated", changed=True, accepted=True, reason="event arrived after resolution boundary", code="late_event", not_evaluated=True)
        if now is not None and _instant_ns(now) > resolution_ns:
            self._append_event(mapped)
            return self._transition("not_evaluated", changed=True, accepted=True, reason="event arrival exceeded resolution boundary", code="late_event", not_evaluated=True)
        if now is not None and _instant_ns(now) < _instant_ns(watermark):
            return self._transition("not_evaluated", accepted=False, reason="second is not frozen at the supplied clock", code="not_frozen", not_evaluated=True)
        decision_ns, expiry_ns = _instant_ns(self._state.decision_accepted_at), _instant_ns(self._state.expires_at)
        start_ns, end_ns = _instant_ns(start), _instant_ns(end)
        self._append_event(mapped)
        # Temporal eligibility is whole-interval, half-open and exact.  A
        # decision-mid-second interval is deliberately typed not-evaluated.
        if start_ns < decision_ns < end_ns:
            return self._transition("not_evaluated", changed=True, accepted=True, reason="event straddles decision acceptance", code="decision_straddle", not_evaluated=True)
        if end_ns <= decision_ns:
            return self._transition("not_evaluated", changed=True, accepted=True, reason="event is before decision acceptance", code="before_decision", not_evaluated=True)
        if start_ns >= expiry_ns or end_ns > expiry_ns:
            return self._transition("not_evaluated", changed=True, accepted=True, reason="event is outside the X price window", code="outside_window", not_evaluated=True)
        gap = self._gap_overlap(start, end)
        quality = mapped.get("quality") if isinstance(mapped.get("quality"), Mapping) else {}
        if bool(quality.get("connection_gap")) or quality.get("status") == "partial" or gap:
            self._state = replace(self._state, gap_overlap=True, partial_data=True)
            return self._transition("not_evaluated", changed=True, accepted=True, reason="confirmed/partial gap overlaps possible touch", code="gap_overlap", not_evaluated=True, partial_data=True, gap_overlap=True)
        low = Decimal(str(mapped["low"]))
        high = Decimal(str(mapped["high"]))
        if low <= self._state.entry_price <= high:
            lineage = {
                "source": "second_aggregate_event",
                "second_event_id": mapped.get("event_id"),
                "second_revision_id": mapped.get("revision_id"),
                "second_event_hash": mapped.get("content_hash") or mapped.get("event_hash"),
                "start_at": start,
                "end_at": end,
                "path_precision": "second_ohlcv",
            }
            # The event-time watermark, rather than callback arrival time,
            # anchors identity.  Replaying the same frozen second therefore
            # cannot produce a different hash from volatile telemetry.
            filled_at = utc_instant(watermark)
            return self._fill(reference={"reference_price": format(self._state.entry_price, "f"), "event_id": mapped.get("event_id"), "revision_id": mapped.get("revision_id")}, lineage=lineage, filled_at=filled_at, exit_eligible_after=utc_instant(end), recovery=False)
        return self._transition("pending", changed=True, accepted=True, reason="frozen second did not touch entry", code="no_touch")

    consume_second = on_second
    on_event = on_second
    process_second = on_second
    handle_market_event = on_second

    def _recovery_lineage(self, mapped: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "source": "recovery_bar",
            "recovery_bar_id": mapped.get("recovery_bar_id"),
            "recovery_bar_hash": mapped.get("content_hash"),
            "content_hash": mapped.get("content_hash"),
            "source_request_hash": mapped.get("source_request_hash"),
            "recovery_cursor": mapped.get("recovery_cursor"),
            "instrument": thaw(mapped.get("instrument", {})),
            "market_feed": thaw(mapped.get("market_feed", {})),
            "path_precision": mapped.get("path_precision"),
            "start_at": mapped.get("start_at"),
            "end_at": mapped.get("end_at"),
        }

    def _recovery_identity_matches(self, mapped: Mapping[str, Any]) -> bool:
        """Bind every recovery candidate to this pending entry/request.

        G019 validates that a bar is a well-formed IEX recovery artifact.  It
        does not know which pending symbol requested it, so G025 performs the
        second, target-specific identity check here before retaining lineage or
        considering OHLC for a fill.
        """

        instrument = mapped.get("instrument")
        if not isinstance(instrument, Mapping) or instrument.get("symbol") != self._state.symbol:
            return False
        if self._expected_recovery_symbol is not None and self._expected_recovery_symbol != self._state.symbol:
            return False
        if dict(mapped.get("market_feed", {})) != dict(self._state.market_feed):
            return False
        if self._expected_recovery_feed is not None and dict(self._expected_recovery_feed) != dict(self._state.market_feed):
            return False
        if instrument.get("venue") != "IEX" or instrument.get("asset_class") != "us_equity" or instrument.get("currency") != "USD":
            return False
        expected_source = self._expected_source_request_hash
        if expected_source is not None and mapped.get("source_request_hash") != expected_source:
            return False
        expected_cursor = self._expected_recovery_cursor
        if expected_cursor is not None and mapped.get("recovery_cursor") != expected_cursor:
            return False
        return True

    def _process_recovery_values(self, values: Iterable[Any], *, now: datetime | str | None = None) -> PendingEntryTransition | None:
        # Validate and bind the complete batch before touching any OHLC.  A
        # cross-symbol/feed candidate in one recovery response therefore
        # cannot be followed by a matching bar that accidentally fills the
        # wrong pending entry.
        mapped_values: list[Mapping[str, Any]] = []
        saw_partial = False
        saw_straddle = False
        for value in values:
            try:
                mapped = recovery_mapping(value)
            except Exception:
                saw_partial = True
                continue
            if not self._recovery_identity_matches(mapped):
                return self._transition(
                    "not_evaluated",
                    accepted=True,
                    reason="recovery bar identity differs from pending request",
                    code="recovery_identity_mismatch",
                    not_evaluated=True,
                    partial_data=True,
                    gap_overlap=True,
                )
            mapped_values.append(mapped)

        last_lineage: Mapping[str, Any] | None = None
        for mapped in mapped_values:
            start, end = mapped["start_at"], mapped["end_at"]
            start_ns, end_ns = _instant_ns(start), _instant_ns(end)
            lineage = self._recovery_lineage(mapped)
            lineage_identity = (mapped.get("recovery_bar_id"), mapped.get("content_hash"))
            if not any((item.get("recovery_bar_id"), item.get("recovery_bar_hash")) == lineage_identity for item in self._state.recovery_lineage if isinstance(item, Mapping)):
                self._state = replace(self._state, recovery_lineage=self._state.recovery_lineage + (freeze(lineage),))
            last_lineage = lineage
            if start_ns < _instant_ns(self._state.decision_accepted_at) or end_ns > _instant_ns(self._state.expires_at):
                saw_straddle = True
                continue
            if mapped.get("complete") is not True or mapped.get("coverage_state") != "complete" or mapped.get("trigger_eligible") is not True:
                saw_partial = True
                continue
            low, high = Decimal(str(mapped["low"])), Decimal(str(mapped["high"]))
            if low <= self._state.entry_price <= high:
                return self._fill(reference={"reference_price": format(self._state.entry_price, "f"), "recovery_bar_id": mapped.get("recovery_bar_id"), "recovery_bar_hash": mapped.get("content_hash")}, lineage=lineage, filled_at=utc_instant(end), exit_eligible_after=utc_instant(end), recovery=True)
        if saw_partial or saw_straddle:
            return self._transition("not_evaluated", accepted=True, reason="recovery bar is incomplete or straddles X/decision", code="recovery_not_evaluated", lineage=last_lineage, not_evaluated=True, partial_data=True, gap_overlap=True)
        return None

    def on_recovery(self, values: Any, *, now: datetime | str | None = None, generation: int | None = None) -> PendingEntryTransition:
        if self._state.terminal:
            return self._replayed("late recovery cannot resurrect a terminal entry")
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale pending generation", code="generation_mismatch")
        if not self._scheduler_generation_live():
            return self.disable(reason="scheduler generation is no longer active")
        if not self._state.gap_overlap:
            return self._transition("not_evaluated", accepted=True, reason="recovery is only consumed for an overlapping gap", code="recovery_without_gap", not_evaluated=True)
        if self._state.recovery_attempted:
            return self._transition("replayed", replayed=True, accepted=True, reason="duplicate recovery attempt", code="idempotent_replay")
        self._state = replace(self._state, recovery_attempted=True)
        result = self._process_recovery_values(_normal_recovery_values(values), now=now)
        return result or self._transition("pending", accepted=True, reason="recovery did not establish a complete touch", code="recovery_no_touch", not_evaluated=True, partial_data=True, gap_overlap=True)

    recover = on_recovery
    reconcile = on_recovery

    def _expire(self, *, now: datetime, not_evaluated: bool, partial_data: bool, gap_overlap: bool, reason: str, code: str, recovery_result: Any = None) -> PendingEntryTransition:
        if self._expire_pending is None:
            return self._transition("rejected", reason="G024 expiry boundary is not configured", code="missing_expiry_boundary")
        if self._scheduler_bridge is not None and not self._scheduler_bridge.prepare_expiry():
            return self._transition("rejected", reason="G023 pending expiry guard rejected", code="scheduler_callback_rejected")
        import hashlib
        from pa_engine.contracts import canonical_json_bytes

        payload = {
            "operation": "expire_pending_entry",
            "strategy_id": self._state.strategy_id,
            "episode": _episode_projection(self._state),
            "episode_id": _episode_id(self._state),
            "reason": reason,
            "code": code,
            "partial_data": partial_data,
            "gap_overlap": gap_overlap,
        }
        key = "entry-expiry:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        try:
            result = invoke_compatible(self._expire_pending, self._state.strategy_id, self._state.pending_id, idempotency_key=key, reason=reason)
        except Exception:
            if self._scheduler_bridge is not None:
                self._scheduler_bridge.abort_expiry()
            return self._transition("rejected", reason="G024 expiry transition failed", code="expiry_boundary_error")
        if not _accepted(result, statuses={"expired", "accepted", "replay", "success"}):
            if self._scheduler_bridge is not None:
                self._scheduler_bridge.abort_expiry()
            return self._transition("rejected", reason="G024 rejected pending expiry", code="expiry_rejected", expiry_result=result)
        event_payload = {
            "status": "expired",
            "outcome": "not_evaluated" if not_evaluated else "expired",
            "evaluation": "not_evaluated" if not_evaluated else "no_touch",
            "uncertainty": "partial_data" if partial_data else "none",
            "partial_data": partial_data,
            "gap_overlap": gap_overlap,
            "details": "gap_overlap" if gap_overlap else None,
            "quality": {"status": "partial", "codes": ["partial_data"]} if partial_data else {"status": "complete", "codes": []},
            "error_code": "partial_data" if partial_data else None,
            "uncertainty_code": "partial_data" if partial_data else None,
            "internal_detail": "gap_overlap" if gap_overlap else None,
            "resume_state": "analysis_due" if gap_overlap else None,
            "reason": reason,
            "code": code,
            # Expiry identity is anchored to the frozen resolution boundary,
            # not the wall-clock callback that happened to deliver the timer.
            "expired_at": _stamp(self._state.resolution_at),
            "pending_id": self._state.pending_id,
            "decision_accepted_at": _stamp(self._state.decision_accepted_at),
            "expires_at": _stamp(self._state.expires_at),
            "resolution_at": _stamp(self._state.resolution_at),
            "decision_lineage": self._state.decision_lineage.to_dict(),
        }
        event = EntryEvent("pending_entry_expired", self._state.strategy_id, self._state.pending_id, event_payload)
        self._state = replace(self._state, status="expired", terminal_reason=reason, partial_data=partial_data or self._state.partial_data, gap_overlap=gap_overlap or self._state.gap_overlap, recovery_attempted=self._state.recovery_attempted)
        if not self._callback_expiry_sent and self._enabled and not self._exhausted:
            self._callback_expiry_sent = True
            if self._on_pending_expiry is not None:
                try:
                    invoke_compatible(
                        self._on_pending_expiry,
                        self._state.strategy_id,
                        self._state.pending_id,
                        generation=self._state.generation,
                    )
                except Exception:
                    pass
            if self._secondary_expiry_callback is not None:
                try:
                    invoke_compatible(
                        self._secondary_expiry_callback,
                        self._state.strategy_id,
                        self._state.pending_id,
                        generation=self._state.generation,
                    )
                except Exception:
                    pass
        return self._transition("expired", changed=True, accepted=True, terminal=True, event=event, lineage=event_payload["decision_lineage"], expiry_result=result, not_evaluated=not_evaluated, partial_data=partial_data, gap_overlap=gap_overlap)

    def on_expiry(self, now: datetime | str | None = None, *, events: Iterable[Any] | None = None, event: Any = None, recovery: Any = None, generation: int | None = None) -> PendingEntryTransition:
        """Resolve the timer no later than ``expires_at + 3s``.

        If events are supplied, they are reduced first, making the documented
        event-before-expiry rule explicit even when the caller receives both
        inputs in one callback turn.
        """

        if self._state.terminal:
            return self._replayed("duplicate expiry cannot resurrect a terminal entry")
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale pending generation", code="generation_mismatch")
        if not self._scheduler_generation_live():
            return self.disable(reason="scheduler generation is no longer active")
        if event is not None:
            events = (event,) if events is None else tuple(events) + (event,)
        if events is not None:
            for event in events:
                outcome = self.on_second(event, now=now)
                if outcome.status == "filled" or self._state.terminal:
                    return outcome
        if recovery is not None and self._state.gap_overlap and not self._state.recovery_attempted:
            self._state = replace(self._state, recovery_attempted=True)
            candidate = self._process_recovery_values(_normal_recovery_values(recovery), now=now)
            if candidate is not None and candidate.status == "filled":
                return candidate
        instant = utc_instant(now, default=self._state.resolution_at)
        if instant < self._state.resolution_at:
            return self._transition("pending", accepted=True, reason="expiry waits for frozen-second publication grace", code="resolution_grace")
        if self._state.gap_overlap:
            if not self._state.recovery_attempted:
                self._state = replace(self._state, recovery_attempted=True)
                recovery_result = None
                if self._recovery_callback is not None:
                    try:
                        recovery_result = invoke_compatible(self._recovery_callback, self._state, generation=self._state.generation)
                    except Exception:
                        recovery_result = None
                candidate = self._process_recovery_values(_normal_recovery_values(recovery_result), now=instant)
                if candidate is not None and candidate.status == "filled":
                    return candidate
            return self._expire(now=instant, not_evaluated=True, partial_data=True, gap_overlap=True, reason="pending entry expired with unresolved gap", code="gap_overlap")
        return self._expire(now=instant, not_evaluated=False, partial_data=False, gap_overlap=False, reason="pending entry expired without a price touch", code="no_touch")

    expire = on_expiry
    process_expiry = on_expiry
    on_timer = on_expiry
    handle_expiry = on_expiry
    process_timer = on_expiry
    handle_timer = on_expiry

    def disable(self, *, reason: str = "disabled") -> PendingEntryTransition:
        if self._state.terminal:
            return self._replayed("duplicate disable")
        # Cancellation is a two-boundary commit.  First perform every
        # read-only G023 ownership check, then ask G024 to remove the pending
        # entry, and only after acceptance commit the scheduler disable and
        # terminal monitor state.  A rejected/throwing callback therefore
        # leaves all three owners in the exact pending state and can be
        # retried with the same episode key.
        scheduler_cancel_ready = True
        if self._scheduler_bridge is not None:
            scheduler_cancel_ready = self._scheduler_bridge.prepare_cancel()
            if not scheduler_cancel_ready and reason != "scheduler generation is no longer active":
                return self._transition("rejected", reason="G023 pending cancellation guard rejected", code="scheduler_callback_rejected")
        if self._cancel_pending is None:
            return self._transition("rejected", reason="G024 cancellation boundary is not configured", code="missing_cancel_boundary")
        result = None
        key = _cancel_idempotency_key(self._state, reason)
        try:
            result = invoke_compatible(
                self._cancel_pending,
                self._state.strategy_id,
                self._state.pending_id,
                generation=self._state.generation,
                idempotency_key=key,
                reason=reason,
            )
        except Exception:
            return self._transition("rejected", reason="G024 cancellation transition failed", code="cancel_boundary_error")
        if not _accepted(result, statuses={"expired", "cancelled", "accepted", "replay", "success"}):
            return self._transition("rejected", reason="G024 rejected pending cancellation", code="cancel_rejected")
        if self._scheduler_bridge is not None:
            if scheduler_cancel_ready:
                if not self._scheduler_bridge.cancel_scheduler(reason=reason):
                    return self._transition("rejected", reason="G023 scheduler cancellation was not accepted", code="scheduler_cancel_rejected")
            elif reason == "scheduler generation is no longer active":
                # G024 is already cancelled; clear a record whose generation
                # changed out from under this monitor without recalling the
                # model.  Disabled/exhausted records already have no pending
                # id, so this is a harmless false return.
                self._scheduler_bridge.cancel_stale_scheduler(reason=reason)
        self._callback_cancel_sent = True
        self._enabled = False
        self._state = replace(self._state, status="cancelled", terminal_reason=reason)
        event = EntryEvent("pending_entry_cancelled", self._state.strategy_id, self._state.pending_id, {"reason": reason, "generation": self._state.generation})
        return self._transition("cancelled", changed=True, accepted=True, terminal=True, event=event, reason=reason, code="disabled")

    cancel = disable
    disable_pending = disable
    deactivate = disable

    def reduce(self, event: Any = None, *, now: datetime | str | None = None, timer: bool = False, gap: Any = None, recovery: Any = None, generation: int | None = None) -> PendingEntryTransition:
        if generation is not None and generation != self._state.generation:
            return self._transition("rejected", reason="stale pending generation", code="generation_mismatch")
        if gap is not None:
            self.on_gap(gap, generation=generation)
        if event is not None:
            result = self.on_second(event, now=now, generation=generation)
            if result.status == "filled" or self._state.terminal:
                return result
        if recovery is not None:
            result = self.on_recovery(recovery, now=now, generation=generation)
            if result.status == "filled" or self._state.terminal:
                return result
        if timer:
            return self.on_expiry(now, generation=generation)
        return self._transition("pending", accepted=True, reason="no state change")

    handle_second = on_second
    consume_event = on_second
    process_event = on_second


class PendingEntryMonitor(PendingEntryReducer):
    """Public monitor facade with instruction-to-pending construction."""

    @classmethod
    def from_instruction(cls, instruction: Mapping[str, Any], **kwargs: Any) -> "PendingEntryMonitor":
        if "accepted_at" in kwargs and "decision_accepted_at" not in kwargs:
            kwargs["decision_accepted_at"] = kwargs.pop("accepted_at")
        if "submitted_time" in kwargs and "submitted_at" not in kwargs:
            kwargs["submitted_at"] = kwargs.pop("submitted_time")
        if "book" in kwargs and "paper_account_book" not in kwargs:
            kwargs["paper_account_book"] = kwargs.pop("book")
        if "fill_callback" in kwargs and "on_pending_fill" not in kwargs:
            kwargs["on_pending_fill"] = kwargs.pop("fill_callback")
        if "expiry_callback" in kwargs and "on_pending_expiry" not in kwargs:
            kwargs["on_pending_expiry"] = kwargs.pop("expiry_callback")
        if "fill_pending_entry" in kwargs and "fill_pending" not in kwargs:
            kwargs["fill_pending"] = kwargs.pop("fill_pending_entry")
        if "expire_pending_entry" in kwargs and "expire_pending" not in kwargs:
            kwargs["expire_pending"] = kwargs.pop("expire_pending_entry")
        state = make_pending_state(instruction, **{key: kwargs.pop(key) for key in tuple(kwargs) if key in {"symbol", "decision_accepted_at", "submitted_at", "generation", "pending_id", "market_feed", "expected_profile_ref", "validator"}})
        paper_book = kwargs.get("paper_account_book")
        create_pending = kwargs.pop("create_pending", None)
        if create_pending is None and paper_book is not None:
            create_pending = getattr(paper_book, "create_pending_entry", None)
        if not callable(create_pending):
            raise ValueError("G024 create_pending boundary is required")

        # Validate the complete monitor constructor surface before claiming
        # the G023 timer or invoking G024.  In particular, a typo/unknown
        # keyword must not create a paper pending entry that cannot be wrapped
        # by a monitor.
        allowed = {
            "fill_pending",
            "expire_pending",
            "cancel_pending",
            "on_pending_fill",
            "on_pending_expiry",
            "recovery_callback",
            "scheduler",
            "scheduler_bridge",
            "paper_account_book",
            "recovery_request",
            "expected_source_request_hash",
            "expected_recovery_cursor",
            "enabled",
            "exhausted",
        }
        unknown = sorted(set(kwargs) - allowed)
        if unknown:
            raise TypeError("unknown pending-entry monitor keyword(s): " + ", ".join(unknown))

        scheduler = kwargs.get("scheduler")
        if scheduler is not None and kwargs.get("scheduler_bridge") is None:
            # Claim G023's X timer only after the G024 create boundary and
            # every constructor argument have been resolved.  Missing/invalid
            # configuration therefore leaves the scheduler record and token
            # completely untouched.
            kwargs["scheduler_bridge"] = PendingEntrySchedulerBridge.bind(scheduler, state)
        scheduler_bridge = kwargs.get("scheduler_bridge")
        idempotency_key = "entry-create:" + _episode_id(state)
        account_before = paper_book.get_account(state.strategy_id) if paper_book is not None and callable(getattr(paper_book, "get_account", None)) else None
        pending_before = getattr(getattr(account_before, "pending_trade", None), "pending_id", None)

        def cleanup_created_pending() -> None:
            cleanup = getattr(paper_book, "expire_pending_entry", None) if paper_book is not None else None
            if not callable(cleanup):
                return
            account_after = paper_book.get_account(state.strategy_id) if callable(getattr(paper_book, "get_account", None)) else None
            pending_after = getattr(getattr(account_after, "pending_trade", None), "pending_id", None)
            # Never clean up a pending trade that predated this create call;
            # a position-exists rejection belongs to that existing owner.
            if pending_after is None or pending_before is not None:
                return
            try:
                invoke_compatible(
                    cleanup,
                    state.strategy_id,
                    state.pending_id,
                    idempotency_key="entry-create-cleanup:" + _episode_id(state),
                    reason="pending_monitor_construction_failed",
                )
            except Exception:
                pass

        try:
            result = invoke_compatible(
                create_pending,
                state.strategy_id,
                state.symbol,
                state.direction,
                state.entry_price,
                state.stop_price,
                state.target_price,
                pending_id=state.pending_id,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            cleanup_created_pending()
            if scheduler_bridge is not None:
                scheduler_bridge.cancel_scheduler(reason="pending_create_failed")
            raise ValueError("G024 pending creation failed") from exc
        if not _accepted(result, statuses={"pending", "accepted", "replay", "success"}):
            cleanup_created_pending()
            if scheduler_bridge is not None:
                scheduler_bridge.cancel_scheduler(reason="pending_create_rejected")
            raise ValueError("G024 rejected pending entry creation")
        kwargs["fill_pending"] = kwargs.pop("fill_pending", None)
        kwargs["expire_pending"] = kwargs.pop("expire_pending", None)
        kwargs["paper_account_book"] = paper_book
        try:
            monitor = cls(state, **kwargs)
        except Exception as exc:
            # A constructor failure after an accepted G024 create must not
            # strand an account-side pending trade.  Best-effort expiry is
            # idempotent and uses a cleanup key distinct from the episode
            # create key; scheduler ownership is then closed fail-closed.
            cleanup_created_pending()
            if scheduler_bridge is not None:
                scheduler_bridge.cancel_scheduler(reason="pending_monitor_construction_failed")
            raise
        monitor.creation_result = result
        monitor.pending_creation_result = result
        return monitor

    create_from_instruction = from_instruction
    from_scheduler_instruction = from_instruction
    create_pending = from_instruction


def create_pending_entry(instruction: Mapping[str, Any], **kwargs: Any) -> PendingEntryMonitor:
    return PendingEntryMonitor.from_instruction(instruction, **kwargs)


monitor_pending_entry = create_pending_entry
create_monitor = create_pending_entry


def reduce_pending_entry(state: PendingEntryState, event: Any = None, **kwargs: Any) -> PendingEntryTransition:
    monitor = PendingEntryReducer(state, **{key: kwargs.pop(key) for key in tuple(kwargs) if key in {"fill_pending", "expire_pending", "cancel_pending", "on_pending_fill", "on_pending_expiry", "recovery_callback", "scheduler", "scheduler_bridge", "paper_account_book", "recovery_request", "expected_source_request_hash", "expected_recovery_cursor", "enabled", "exhausted"}})
    return monitor.reduce(event, **kwargs)


reduce_pending = reduce_pending_entry


__all__ = [
    "PendingEntryMonitor",
    "PendingEntryReducer",
    "create_pending_entry",
    "create_monitor",
    "monitor_pending_entry",
    "reduce_pending",
    "reduce_pending_entry",
]
