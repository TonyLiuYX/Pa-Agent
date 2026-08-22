"""One-shot G023 close notification bridge for the G026 reducer."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable

from .boundary import invoke_compatible
from .models import ExitState, SchedulerCloseNotification


class SchedulerBindingError(ValueError):
    """Raised when an open position cannot be bound to the live scheduler."""


def _name(value: Any) -> str:
    return str(getattr(value, "value", value)).lower()


def _field(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


class ExitSchedulerBridge:
    """Validate one G023 open-position record and notify it at most once.

    Notification is deliberately committed after G024 accepts settlement.  A
    false/throwing callback is recorded in a typed result and never rolls back
    the already-accepted paper settlement.
    """

    __slots__ = ("scheduler", "strategy_id", "trade_id", "generation", "_callback", "_bound", "_called", "_identity")

    def __init__(self, scheduler: Any, state: ExitState, callback: Callable[..., Any] | None = None) -> None:
        self.scheduler = scheduler
        self.strategy_id = state.strategy_id
        self.trade_id = state.trade_id
        self.generation = state.generation
        self._callback = callback
        self._bound = False
        self._called = False
        self._identity = (state.strategy_id, state.trade_id, state.generation)

    @classmethod
    def bind(cls, scheduler: Any, state: ExitState, callback: Callable[..., Any] | None = None) -> "ExitSchedulerBridge":
        bridge = cls(scheduler, state, callback)
        record = bridge._record()
        if record is None:
            raise SchedulerBindingError("G023 scheduler record is missing")
        if _field(record, "strategy_id", state.strategy_id) != state.strategy_id:
            raise SchedulerBindingError("G023 strategy identity differs from open trade")
        if _field(record, "open_position_id", None) != state.trade_id:
            raise SchedulerBindingError("G023 open-position identity differs from G024 trade")
        expected_generation = _field(record, "open_position_generation", None)
        if expected_generation is None:
            expected_generation = _field(record, "generation", state.generation)
        if state.generation is not None and expected_generation != state.generation:
            raise SchedulerBindingError("G023 open-position generation differs from trade")
        state_name = _name(_field(record, "state", ""))
        intended = _name(_field(record, "intended_resume_state", ""))
        allowed = {"open_position", "disabled", "market_wait"}
        if state_name not in allowed or (state_name == "market_wait" and intended not in {"", "open_position", "analysis_due"}):
            raise SchedulerBindingError("G023 record is not in an open-position closeable state")
        bridge._bound = True
        return bridge

    @classmethod
    def from_scheduler(cls, scheduler: Any, state: ExitState, callback: Callable[..., Any] | None = None) -> "ExitSchedulerBridge":
        return cls.bind(scheduler, state, callback)

    def _record(self) -> Any | None:
        getter = getattr(self.scheduler, "get", None)
        if not callable(getter):
            return None
        try:
            return getter(self.strategy_id)
        except Exception:
            return None

    def bound_to(self, state: ExitState) -> bool:
        """Return whether this bridge remains bound to exactly this state."""

        return (
            isinstance(state, ExitState)
            and self._bound
            and self._identity == (state.strategy_id, state.trade_id, state.generation)
            and (self.strategy_id, self.trade_id, self.generation) == self._identity
        )

    def _identity_intact(self) -> bool:
        return (self.strategy_id, self.trade_id, self.generation) == self._identity

    def _live(self) -> bool:
        if not self._identity_intact():
            return False
        record = self._record()
        if record is None or _field(record, "open_position_id", None) != self.trade_id:
            return False
        if _field(record, "strategy_id", self.strategy_id) != self.strategy_id:
            return False
        expected = _field(record, "open_position_generation", None)
        if expected is None:
            expected = _field(record, "generation", self.generation)
        if expected != self.generation:
            return False
        state_name = _name(_field(record, "state", ""))
        intended = _name(_field(record, "intended_resume_state", ""))
        return state_name in {"open_position", "disabled", "market_wait"} and (state_name != "market_wait" or intended in {"", "open_position", "analysis_due"})

    @property
    def live(self) -> bool:
        """Side-effect-free current binding check."""

        return self._bound and self._live()

    def prepare_close(self) -> SchedulerCloseNotification:
        """Preflight a close without mutating scheduler state."""

        if not self._bound or not self._identity_intact() or not self._live():
            return SchedulerCloseNotification(
                attempted=False,
                accepted=False,
                status="binding_rejected",
                strategy_id=self.strategy_id,
                trade_id=self.trade_id,
                generation=self.generation,
                error="G023 open-position binding is not live",
                callback_count=0,
            )
        if self._callback is None and self._owned_scheduler_callback() is None:
            return SchedulerCloseNotification(
                attempted=False,
                accepted=False,
                status="callback_missing",
                strategy_id=self.strategy_id,
                trade_id=self.trade_id,
                generation=self.generation,
                error="G023 on_position_close callback is unavailable",
                callback_count=0,
            )
        return SchedulerCloseNotification(
            attempted=False,
            accepted=True,
            status="ready",
            strategy_id=self.strategy_id,
            trade_id=self.trade_id,
            generation=self.generation,
            callback_count=0,
        )

    def _owned_scheduler_callback(self) -> Callable[..., Any] | None:
        callback = getattr(self.scheduler, "on_position_close", None)
        if callable(callback) and getattr(callback, "__self__", None) is self.scheduler:
            return callback
        return None

    @property
    def called(self) -> bool:
        return self._called

    def notify(self, *, exhausted: bool = False, reason: str | None = None) -> SchedulerCloseNotification:
        if self._called:
            return SchedulerCloseNotification(
                attempted=True,
                accepted=False,
                status="duplicate_suppressed",
                strategy_id=self.strategy_id,
                trade_id=self.trade_id,
                generation=self.generation,
                error="close callback was already attempted",
                callback_count=1,
            )
        self._called = True
        prepared = self.prepare_close()
        if not prepared.accepted:
            return prepared
        callback = self._callback or self._owned_scheduler_callback()
        try:
            value = invoke_compatible(
                callback,
                self.strategy_id,
                self.trade_id,
                exhausted=bool(exhausted),
                reason=reason,
                generation=self.generation,
            )
        except Exception as exc:
            return SchedulerCloseNotification(
                attempted=True,
                accepted=False,
                status="callback_error",
                strategy_id=self.strategy_id,
                trade_id=self.trade_id,
                generation=self.generation,
                error=type(exc).__name__,
                callback_count=1,
            )
        accepted = value is True
        if isinstance(value, dict):
            accepted = value.get("accepted", value.get("changed", value.get("status") in {"accepted", "closed", "success"})) is True
        else:
            accepted = bool(getattr(value, "accepted", getattr(value, "closed", accepted))) if value is not None else accepted
            if value is not None and str(getattr(value, "status", "")).lower() in {"accepted", "closed", "success", "settled"}:
                accepted = True
        return SchedulerCloseNotification(
            attempted=True,
            accepted=accepted,
            status="accepted" if accepted else "callback_rejected",
            strategy_id=self.strategy_id,
            trade_id=self.trade_id,
            generation=self.generation,
            error=None if accepted else "G023 close callback returned false",
            callback_count=1,
        )

    on_close = notify
    notify_close = notify


PositionCloseSchedulerBridge = ExitSchedulerBridge
CloseSchedulerBridge = ExitSchedulerBridge


__all__ = ["CloseSchedulerBridge", "ExitSchedulerBridge", "PositionCloseSchedulerBridge", "SchedulerBindingError"]
