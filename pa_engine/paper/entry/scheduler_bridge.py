"""Fail-closed wiring between G025 and the live G023 scheduler.

G023 owns a normal ``pending_expiry`` timer for all conditional entries.  A
G025 monitor must claim that timer before it can be constructed: otherwise the
normal scheduler timer would recall the strategy at ``X`` while the monitor
is still inside the publication grace window.  This adapter uses only the
small runtime record/timer seam already exposed by G023 and keeps the
conditional-entry reducer as the sole expiry owner.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


UTC = timezone.utc


class SchedulerBindingError(ValueError):
    """Raised when a live scheduler record cannot be claimed safely."""


def _instant(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return None
        return value.astimezone(UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(UTC)
    return None


def _same_instant(left: Any, right: Any) -> bool:
    left_value, right_value = _instant(left), _instant(right)
    return left_value is not None and right_value is not None and left_value == right_value


def _state_name(value: Any) -> str:
    return str(getattr(value, "value", value)).lower()


class PendingEntrySchedulerBridge:
    """Claim and guard one G023 pending-entry scheduler record.

    ``prepare_fill`` and ``prepare_expiry`` are strictly read-only guards.
    They never invoke a scheduler callback or mutate a record.  The matching
    callback is committed only after G024 has accepted its account transition.
    """

    __slots__ = (
        "scheduler",
        "strategy_id",
        "pending_id",
        "generation",
        "expires_at",
        "_bound",
        "_claimed_token",
        "_fill_claimed",
        "_expiry_claimed",
    )

    def __init__(self, scheduler: Any, state: Any) -> None:
        self.scheduler = scheduler
        self.strategy_id = state.strategy_id
        self.pending_id = state.pending_id
        self.generation = state.generation
        self.expires_at = state.expires_at
        self._bound = False
        self._claimed_token = None
        self._fill_claimed = False
        self._expiry_claimed = False

    @classmethod
    def bind(cls, scheduler: Any, state: Any) -> "PendingEntrySchedulerBridge":
        bridge = cls(scheduler, state)
        record = bridge._record()
        if record is None:
            raise SchedulerBindingError("G023 scheduler record is missing")
        if not bridge._record_matches(record):
            raise SchedulerBindingError("G023 pending identity/generation/expiry does not match")
        tokens = getattr(record, "timer_tokens", None)
        token = tokens.get("pending_expiry") if isinstance(tokens, dict) else None
        if token is None or not bool(getattr(token, "active", False)):
            raise SchedulerBindingError("G023 pending expiry timer is not claimable")
        if getattr(token, "generation", None) != bridge.generation or not _same_instant(getattr(token, "due_at", None), bridge.expires_at):
            raise SchedulerBindingError("G023 pending expiry timer identity does not match")
        cancel = getattr(scheduler, "_cancel_timer_kind", None)
        if not callable(cancel):
            raise SchedulerBindingError("G023 pending timer cancellation seam is unavailable")
        cancel(record, "pending_expiry")
        tokens_after = getattr(record, "timer_tokens", None)
        if isinstance(tokens_after, dict) and "pending_expiry" in tokens_after:
            raise SchedulerBindingError("G023 pending expiry timer was not cancelled")
        if not bridge._record_matches(record):
            raise SchedulerBindingError("G023 record changed while claiming pending timer")
        # Keep the exact token so a pre-create failure can restore the live
        # scheduler record without disabling the strategy.  This is strictly
        # a local ownership claim; no scheduler transition/log is emitted by
        # the private timer seam.
        bridge._claimed_token = token
        bridge._bound = True
        return bridge

    def release(self) -> bool:
        """Undo a timer claim when no G024 owner was created.

        The monitor facade uses this only on a failed pre-create path.  It
        restores the same token object and leaves the G023 record in its
        original WAITING_ENTRY/MARKET_WAIT state.  If the record changed in
        the meantime, fail closed rather than restoring a stale timer.
        """

        if not self._bound or self._claimed_token is None:
            return False
        record = self._record()
        tokens = getattr(record, "timer_tokens", None) if record is not None else None
        if not self._record_matches(record) or not isinstance(tokens, dict) or "pending_expiry" in tokens:
            return False
        token = self._claimed_token
        token.active = True
        tokens["pending_expiry"] = token
        self._bound = False
        self._claimed_token = None
        return True

    def _record(self) -> Any | None:
        getter = getattr(self.scheduler, "get", None)
        if not callable(getter):
            return None
        try:
            return getter(self.strategy_id)
        except Exception:
            return None

    def _record_matches(self, record: Any) -> bool:
        if record is None:
            return False
        if getattr(record, "pending_id", None) != self.pending_id:
            return False
        if getattr(record, "generation", None) != self.generation:
            return False
        if not _same_instant(getattr(record, "expires_at", None), self.expires_at):
            return False
        if bool(getattr(record, "enabled", True)) is False or bool(getattr(record, "exhausted", False)):
            return False
        state = _state_name(getattr(record, "state", ""))
        if state == "waiting_entry":
            return True
        if state == "market_wait":
            return _state_name(getattr(record, "intended_resume_state", "")) == "waiting_entry"
        return False

    def is_live(self) -> bool:
        """Return whether the exact scheduler record still owns this monitor."""

        return bool(self._bound and self._record_matches(self._record()))

    def _owned_method(self, name: str) -> Any | None:
        """Return only a scheduler-bound method, never a replaced callback."""

        callback = getattr(self.scheduler, name, None)
        if not callable(callback) or getattr(callback, "__self__", None) is not self.scheduler:
            return None
        return callback

    def prepare_fill(self) -> bool:
        """Read-only G023 guard before G024 can open the paper trade."""

        if self._fill_claimed:
            return self.is_live()
        record = self._record()
        if not self._bound or not self._record_matches(record):
            return False
        if self._owned_method("on_pending_fill") is None:
            return False
        self._fill_claimed = True
        return True

    def complete_fill(self, position_id: str | None = None) -> bool:
        if not self._fill_claimed:
            return False
        record = self._record()
        if not self._record_matches(record):
            self._fill_claimed = False
            return False
        callback = self._owned_method("on_pending_fill")
        if callback is None:
            self._fill_claimed = False
            return False
        # Consume the one commit claim before crossing into G023.  A callback
        # that returns false/raises therefore cannot be invoked a second time
        # by a duplicate delivery.
        self._fill_claimed = False
        try:
            accepted = callback(self.strategy_id, self.pending_id, position_id, generation=self.generation)
        except Exception:
            accepted = False
        if accepted is not True:
            return False
        after = self._record()
        if after is None or getattr(after, "pending_id", None) is not None:
            return False
        return True

    def abort_fill(self) -> None:
        self._fill_claimed = False

    def on_fill(self, strategy_id: str, pending_id: str, *, generation: int | None = None, position_id: str | None = None, **_: Any) -> bool:
        """Complete a preflighted fill; never invoke G023 twice."""

        if strategy_id != self.strategy_id or pending_id != self.pending_id or generation not in {None, self.generation}:
            return False
        return self.complete_fill(position_id)

    def prepare_expiry(self) -> bool:
        """Preflight G023 expiry before G024 removes the pending record.

        Unlike fill, the scheduler callback is intentionally deferred until
        after G024 accepts the expiry.  G023's callback may enqueue analysis
        work, so invoking it before the account boundary would violate the
        single reducer ordering (and could queue work for an expiry that G024
        rejected).
        """

        if self._expiry_claimed:
            return self.is_live()
        record = self._record()
        if not self._bound or not self._record_matches(record):
            return False
        if self._owned_method("on_pending_expiry") is None:
            return False
        self._expiry_claimed = True
        return True

    def complete_expiry(self) -> bool:
        if not self._expiry_claimed:
            return False
        record = self._record()
        if not self._record_matches(record):
            self._expiry_claimed = False
            return False
        callback = self._owned_method("on_pending_expiry")
        if callback is None:
            self._expiry_claimed = False
            return False
        # Expiry is one scheduler commit.  Do not call the callback once in
        # ``on_expiry`` and then again here: G023's first call already clears
        # the pending record and may enqueue a model job.
        self._expiry_claimed = False
        try:
            accepted = callback(self.strategy_id, self.pending_id, generation=self.generation)
        except Exception:
            accepted = False
        if accepted is not True:
            return False
        after = self._record()
        return after is not None and getattr(after, "pending_id", None) is None

    def abort_expiry(self) -> None:
        self._expiry_claimed = False

    def prepare_cancel(self) -> bool:
        """Read-only ownership guard before a G024 cancellation/expiry."""

        if not self._bound or not self._record_matches(self._record()):
            return False
        return self._owned_method("disable") is not None

    def cancel_scheduler(self, *, reason: str = "disabled") -> bool:
        """Cancel the live G023 pending record without a model recall."""

        record = self._record()
        if not self._bound or not self._record_matches(record):
            return False
        disable = self._owned_method("disable")
        if disable is None:
            return False
        try:
            return bool(disable(self.strategy_id, reason=reason))
        except Exception:
            return False

    def cancel_stale_scheduler(self, *, reason: str = "disabled") -> bool:
        """Clear this pending record after an external generation invalidation.

        This path is used only after G025 has already accepted the G024
        cancellation and the live record no longer matches the claimed
        generation.  It never performs a model recall; G023's disable path
        clears the stale pending id and timer atomically.
        """

        record = self._record()
        if record is None or getattr(record, "pending_id", None) != self.pending_id:
            return False
        disable = self._owned_method("disable")
        if disable is None:
            return False
        try:
            return bool(disable(self.strategy_id, reason=reason))
        except Exception:
            return False

    def on_expiry(self, strategy_id: str, pending_id: str, *, generation: int | None = None, **_: Any) -> bool:
        if strategy_id != self.strategy_id or pending_id != self.pending_id or generation not in {None, self.generation}:
            return False
        # ``complete_expiry`` is the sole commit path.  Keeping this adapter
        # as a thin validator prevents the historical callback-then-complete
        # double invocation and makes the return value reflect that one call.
        return self.complete_expiry()


__all__ = ["PendingEntrySchedulerBridge", "SchedulerBindingError"]
