"""G028 runtime persistence facade: write sink and restart hydration seam.

This module is the durable-state layer owned by the automatic runtime.  It
sits on top of the accepted G027 store and never redefines it:

- :class:`RuntimeSink` is the write-side facade for every automatic paper
  transition.  All envelopes are built in ``mode="automatic"`` with the
  content-bound ``*_key`` idempotency helpers, so re-delivery after a
  restart is a store-level replay.  Each commit method returns
  ``"appended"`` when a new event was stored and ``"replayed"`` when the
  exact event already existed.
- :class:`RuntimeHydrator` is the restart restore seam: it rebuilds the
  in-memory G024 account book, the G025 pending state and the G026 exit
  state exclusively from durable rows.  It never calls G024 math
  operations, so restored balances and positions are never recomputed or
  reset.

Failure vocabulary (:class:`RuntimePersistenceError`): ``invalid_sink_input``
for malformed caller input, ``hydrate_account_mismatch`` for contradictory
account rows, ``pending_state_corrupt`` / ``exit_state_corrupt`` for
unreconstructable reducer states and ``monitor_bind_failed`` for scheduler
bridge failures.  Upstream
:class:`~pa_engine.storage.StorageConflictError`,
:class:`~pa_engine.storage.EnvelopeValidationError` and
:class:`~pa_engine.storage.InvalidStorageInputError` always propagate
unchanged; they are deliberate typed outcomes, not input bugs.
"""

from __future__ import annotations

import functools
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping

from pa_engine.contracts import canonical_json
from pa_engine.market import ALPACA_IEX_FEED
from pa_engine.paper.core import PaperAccountBook, PaperTrade, PendingTrade
from pa_engine.paper.core.account import _ManagedAccount
from pa_engine.paper.entry import (
    EntryLineage,
    PendingEntryReducer,
    PendingEntrySchedulerBridge,
    PendingEntryState,
)
from pa_engine.paper.exit import ExitReducer, ExitSchedulerBridge, ExitState, trade_mapping
from pa_engine.storage import (
    EnvelopeValidationError,
    InvalidStorageInputError,
    RuntimeStore,
    StorageConflictError,
    account_change_key,
    account_exhausted_key,
    close_key,
    decision_key,
    envelope_for_account_change,
    envelope_for_account_exhausted,
    envelope_for_close,
    envelope_for_decision,
    envelope_for_fill,
    envelope_for_fill_rejected,
    envelope_for_market_recovery,
    envelope_for_pending_entry,
    envelope_for_schedule,
    envelope_for_state_transition,
    fill_key,
    fill_rejected_key,
    instant_from_text,
    instant_to_text,
    is_canonical_instant,
    is_recovery_bar_ref,
    is_second_ref,
    make_recovery_bar_ref,
    make_second_ref,
    pending_key,
    recovery_key,
    schedule_key,
    state_transition_key,
)

PAPER_MATH_VERSION = "0.1.0"

_UPSTREAM_ERRORS = (
    StorageConflictError,
    EnvelopeValidationError,
    InvalidStorageInputError,
)


class RuntimePersistenceError(ValueError):
    """Typed failure raised by the G028 persistence facade.

    ``code`` is a stable machine vocabulary: ``invalid_sink_input``,
    ``hydrate_account_mismatch``, ``pending_state_corrupt``,
    ``exit_state_corrupt`` and ``monitor_bind_failed``.  Upstream storage
    errors always propagate unchanged instead of being wrapped.
    """

    def __init__(self, message: str, *, code: str = "invalid_sink_input") -> None:
        super().__init__(message)
        self.code = code


def _sink_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """Convert non-upstream input errors into :class:`RuntimePersistenceError`."""

    @functools.wraps(func)
    def wrapper(self: "RuntimeSink", *args: Any, **kwargs: Any) -> Any:
        try:
            return func(self, *args, **kwargs)
        except _UPSTREAM_ERRORS:
            raise
        except RuntimePersistenceError:
            raise
        except (ValueError, TypeError, KeyError) as exc:
            raise RuntimePersistenceError(str(exc), code="invalid_sink_input") from exc

    return wrapper


def _decimal(value: Any, field: str, code: str) -> Decimal:
    """Convert a canonical decimal text or Decimal into an exact Decimal."""

    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise RuntimePersistenceError(
            f"{field} is not canonical decimal text", code=code
        ) from exc
    if not parsed.is_finite():
        raise RuntimePersistenceError(f"{field} is not finite", code=code)
    return parsed


def _optional_decimal(value: Any, field: str, code: str) -> Decimal | None:
    if value is None:
        return None
    return _decimal(value, field, code)


def _compact_profile_ref(record: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the envelope-compatible compact profile ref for a snapshot.

    The frozen envelope validator accepts exactly ``{profile_ref_id,
    profile_ref_hash, strategy_id}``, while a G016 profile ref carries the
    full packet/snapshot lineage.  The durable strategy row keeps the full
    ref (the engine re-seeds scheduler records from it); only the envelope
    carries the compact identity projection.
    """

    value = record.get("profile_ref")
    if not isinstance(value, Mapping):
        return None
    compact = {
        key: value.get(key)
        for key in ("profile_ref_id", "profile_ref_hash", "strategy_id")
    }
    if not isinstance(compact["profile_ref_id"], str) or not isinstance(
        compact["profile_ref_hash"], str
    ):
        return None
    return compact


def _ref(value: Any, *, bar: bool = False) -> dict[str, Any]:
    """Normalize an upstream market value into a validated ref mapping.

    An already-valid ``second_event``/``recovery_bar`` mapping is returned
    unchanged; a G020/G019 value object or raw mapping is projected through
    :func:`~pa_engine.storage.make_second_ref` /
    :func:`~pa_engine.storage.make_recovery_bar_ref`.  Invalid output is
    rejected later by the envelope builder with the upstream validation
    error.
    """

    if isinstance(value, Mapping) and (is_second_ref(value) or is_recovery_bar_ref(value)):
        return dict(value)
    if not isinstance(value, Mapping):
        to_dict = getattr(value, "to_dict", None)
        if not callable(to_dict):
            raise RuntimePersistenceError(
                "market ref must be a mapping or typed upstream value",
                code="invalid_sink_input",
            )
        value = to_dict()
    if bar:
        return make_recovery_bar_ref(value)
    return make_second_ref(value)


class RuntimeSink:
    """Write-side facade for one automatic runtime over a G027 store.

    Every method builds one content-bound automatic-mode envelope, commits it
    atomically with its materialized row through the store and returns
    ``"appended"`` or ``"replayed"``.  ``occurred_at`` accepts an aware
    :class:`datetime` or canonical UTC text; when omitted the store's own
    database clock is used so tests and the runtime share one deterministic
    clock.
    """

    def __init__(self, store: RuntimeStore) -> None:
        self._store = store

    @property
    def store(self) -> RuntimeStore:
        return self._store

    def _now_text(self) -> str:
        return instant_to_text(self._store._db._clock())

    def _occurred(self, occurred_at: datetime | str | None) -> str:
        if occurred_at is None:
            return self._now_text()
        if isinstance(occurred_at, datetime):
            return instant_to_text(occurred_at)
        if isinstance(occurred_at, str) and is_canonical_instant(occurred_at):
            return occurred_at
        raise RuntimePersistenceError(
            "occurred_at must be a datetime or canonical UTC text",
            code="invalid_sink_input",
        )

    def _event_verdict(self, envelope: Mapping[str, Any]) -> str | None:
        """Return ``"replayed"`` when the exact event is already stored.

        The store's mutation methods return ``None``, so the sink checks the
        content-bound event id before committing.  A pre-existing event with
        identical content means the store will replay it; different content
        is left to the store, which raises :class:`StorageConflictError`.
        """

        document = dict(envelope)
        row = self._store._db.conn.execute(
            "SELECT idempotency_key, envelope_json FROM events WHERE event_id = ?",
            (document["event_id"],),
        ).fetchone()
        if row is None:
            return None
        if row[0] == document["idempotency_key"] and row[1] == canonical_json(document):
            return "replayed"
        return "conflict"

    def _commit(self, envelope: Mapping[str, Any], apply: Callable[[], None]) -> str:
        """Commit *apply* and return the appended/replayed verdict.

        *apply* runs unconditionally: for row-bearing commits the row upsert
        is the payload of a replayed event, and for event-only commits the
        store's replay is a no-op.  A conflict verdict means *apply* raises
        the store's :class:`StorageConflictError` inside its transaction.
        """

        verdict = self._event_verdict(envelope)
        apply()
        if verdict == "replayed":
            return "replayed"
        if verdict == "conflict":
            raise StorageConflictError(
                f"event_id {envelope['event_id']} already exists with different content"
            )
        return "appended"

    @_sink_errors
    def commit_state(
        self,
        snapshot: Mapping[str, Any],
        *,
        transition: Mapping[str, Any] | None = None,
        timers: Iterable[Mapping[str, Any]] = (),
        clear_timers: Iterable[str] = (),
        occurred_at: datetime | str | None = None,
    ) -> str:
        """Persist one scheduler snapshot, optionally with its transition event.

        When *transition* is given, ``resume_state`` is taken from the
        transition or falls back to ``snapshot["intended_resume_state"]`` in
        both the envelope and the store's transition payload.  A
        transition-less commit stores no event and returns ``"appended"``.
        """

        if transition is None:
            self._store.commit_strategy_state(snapshot)
            return "appended"
        effective = dict(transition)
        if effective.get("resume_state") is None and snapshot.get("intended_resume_state") is not None:
            effective["resume_state"] = snapshot.get("intended_resume_state")
        occurred = self._occurred(occurred_at)
        envelope_record = dict(snapshot)
        envelope_record["profile_ref"] = _compact_profile_ref(snapshot)
        envelope = envelope_for_state_transition(
            record=envelope_record,
            from_state=effective["from"],
            to_state=effective["to"],
            reason=effective["reason"],
            resume_state=effective.get("resume_state"),
            occurred_at=occurred,
            idempotency_key=state_transition_key(
                snapshot["strategy_id"], snapshot["generation"], effective["to"], effective["reason"]
            ),
            mode="automatic",
        )
        return self._commit(
            envelope,
            lambda: self._store.commit_strategy_state(
                snapshot,
                transition=effective,
                timers=timers,
                clear_timers=clear_timers,
                envelope=envelope,
            ),
        )

    @_sink_errors
    def commit_decision(
        self,
        snapshot: Mapping[str, Any],
        instruction: Mapping[str, Any],
        *,
        occurred_at: datetime | str | None = None,
    ) -> str:
        """Persist one accepted G018 decision envelope."""

        occurred = self._occurred(occurred_at)
        envelope_record = dict(snapshot)
        envelope_record["profile_ref"] = _compact_profile_ref(snapshot)
        envelope = envelope_for_decision(
            record=envelope_record,
            instruction=instruction,
            occurred_at=occurred,
            idempotency_key=decision_key(
                snapshot["strategy_id"], snapshot["generation"], instruction["instruction_hash"]
            ),
            mode="automatic",
        )
        return self._store.append_event(envelope)

    @_sink_errors
    def commit_schedule(
        self,
        snapshot: Mapping[str, Any],
        *,
        timer_kind: str,
        due_at: datetime | str,
        decision_accepted_at: datetime | str,
        occurred_at: datetime | str | None = None,
    ) -> str:
        """Persist one schedule_set event and its timer row."""

        occurred = self._occurred(occurred_at)
        envelope_record = dict(snapshot)
        envelope_record["profile_ref"] = _compact_profile_ref(snapshot)
        envelope = envelope_for_schedule(
            record=envelope_record,
            timer_kind=timer_kind,
            due_at=due_at,
            decision_accepted_at=decision_accepted_at,
            occurred_at=occurred,
            idempotency_key=schedule_key(
                snapshot["strategy_id"], snapshot["generation"], timer_kind, due_at
            ),
            mode="automatic",
        )
        return self._commit(
            envelope,
            lambda: self._store.commit_schedule_set(snapshot["strategy_id"], envelope=envelope),
        )

    @_sink_errors
    def prepare_pending(
        self,
        state: Mapping[str, Any],
        *,
        event: str,
        market_ref: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Build (without committing) one pending-entry envelope.

        The created envelope is content-bound to the episode.  When the
        episode later fills, pass the exact original created envelope back to
        :meth:`commit_pending` with the terminal filled state: the event
        replays byte-identically while the store's row upsert moves the
        episode to ``status="filled"`` without fabricating a second event.
        """

        occurred = self._occurred(occurred_at)
        return envelope_for_pending_entry(
            state,
            event,
            market_ref=market_ref,
            occurred_at=occurred,
            idempotency_key=pending_key(state["strategy_id"], state["pending_id"], event),
            mode="automatic",
        )

    @_sink_errors
    def commit_pending(
        self,
        state: Mapping[str, Any],
        *,
        event: str,
        market_ref: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
        envelope: Mapping[str, Any] | None = None,
    ) -> str:
        """Persist one G025 pending snapshot with its linked event.

        *state* is the reducer's ``PendingEntryState.to_dict()`` projection
        (or its terminal variant); *event* is one of ``created``, ``expired``
        or ``cancelled``.  For the fill transition pass the original created
        *envelope* (see :meth:`prepare_pending`) so the row upsert can move
        the episode to ``status="filled"`` while the event stream replays the
        identical episode-created event.
        """

        occurred = self._occurred(occurred_at)
        if envelope is None:
            envelope = self.prepare_pending(
                state, event=event, market_ref=market_ref, occurred_at=occurred
            )
        return self._commit(
            envelope,
            lambda: self._store.commit_pending_entry(state, event=event, envelope=envelope),
        )

    @_sink_errors
    def commit_fill(
        self,
        trade_state: Mapping[str, Any],
        *,
        market_ref: Any,
        occurred_at: datetime | str | None = None,
    ) -> str:
        """Persist one G026 open-position snapshot with its ``paper_fill`` event.

        *market_ref* is a ``second_event`` ref mapping or an upstream G020
        aggregate value normalized through
        :func:`~pa_engine.storage.make_second_ref`.
        """

        occurred = self._occurred(occurred_at)
        envelope = envelope_for_fill(
            trade_state,
            market_ref=_ref(market_ref),
            occurred_at=occurred,
            idempotency_key=fill_key(trade_state["strategy_id"], trade_state["trade_id"]),
            mode="automatic",
        )
        return self._commit(
            envelope,
            lambda: self._store.commit_trade(trade_state, event="filled", envelope=envelope),
        )

    @_sink_errors
    def commit_close(
        self,
        trade_state: Mapping[str, Any],
        *,
        market_ref: Any,
        occurred_at: datetime | str | None = None,
    ) -> str:
        """Persist one settled G026 snapshot with its ``paper_close`` event."""

        occurred = self._occurred(occurred_at)
        envelope = envelope_for_close(
            trade_state,
            market_ref=_ref(market_ref),
            occurred_at=occurred,
            idempotency_key=close_key(
                trade_state["strategy_id"],
                trade_state["trade_id"],
                trade_state.get("exit_reason"),
                trade_state.get("closed_at"),
            ),
            mode="automatic",
        )
        return self._commit(
            envelope,
            lambda: self._store.commit_trade(trade_state, event="closed", envelope=envelope),
        )

    @_sink_errors
    def commit_fill_rejected(
        self,
        strategy_id: str,
        generation: int,
        *,
        reason: str,
        pending_id: str | None = None,
        trade_id: str | None = None,
        occurred_at: datetime | str | None = None,
    ) -> str:
        """Persist one ``paper_fill_rejected`` event without touching funds."""

        occurred = self._occurred(occurred_at)
        envelope = envelope_for_fill_rejected(
            strategy_id,
            generation,
            reason=reason,
            pending_id=pending_id,
            trade_id=trade_id,
            occurred_at=occurred,
            idempotency_key=fill_rejected_key(strategy_id, reason, pending_id, trade_id),
            mode="automatic",
        )
        return self._commit(
            envelope,
            lambda: self._store.commit_fill_rejected(strategy_id, envelope=envelope),
        )

    @_sink_errors
    def commit_account_change(
        self,
        strategy_id: str,
        *,
        balance_before: Decimal | str,
        balance_after: Decimal | str,
        occurred_at: datetime | str | None = None,
        generation: int = 0,
    ) -> str:
        """Persist one account balance change with its ``account_changed`` event.

        The envelope requires a strategy generation; callers may pass the
        current scheduler generation, and it defaults to ``0``.
        """

        occurred = self._occurred(occurred_at)
        envelope = envelope_for_account_change(
            strategy_id,
            generation,
            balance_before=balance_before,
            balance_after=balance_after,
            occurred_at=occurred,
            idempotency_key=account_change_key(strategy_id, balance_before, balance_after),
            mode="automatic",
        )
        return self._commit(
            envelope,
            lambda: self._store.commit_account_event(
                strategy_id,
                balance_before=balance_before,
                balance_after=balance_after,
                envelope=envelope,
            ),
        )

    @_sink_errors
    def commit_account_exhausted(
        self,
        strategy_id: str,
        *,
        balance: Decimal | str,
        reason: str,
        occurred_at: datetime | str | None = None,
        generation: int = 0,
    ) -> str:
        """Mark one account exhausted with its ``account_exhausted`` event."""

        occurred = self._occurred(occurred_at)
        envelope = envelope_for_account_exhausted(
            strategy_id,
            generation,
            balance=balance,
            reason=reason,
            occurred_at=occurred,
            idempotency_key=account_exhausted_key(strategy_id, balance, reason),
            mode="automatic",
        )
        return self._commit(
            envelope,
            lambda: self._store.commit_account_exhausted(
                strategy_id, balance=balance, reason=reason, envelope=envelope
            ),
        )

    @_sink_errors
    def commit_market_recovery(
        self,
        symbol: str,
        *,
        bars: Iterable[Any],
        recovery_cursor: datetime | str | None,
        occurred_at: datetime | str | None = None,
    ) -> str:
        """Persist one G019 recovery batch with its cursor advancement.

        *bars* are ``recovery_bar`` ref mappings or G019 recovery bar values;
        both are normalized into validated refs before the envelope is built.
        """

        refs = [_ref(bar, bar=True) for bar in bars]
        occurred = self._occurred(occurred_at)
        envelope = envelope_for_market_recovery(
            symbol,
            bars=refs,
            recovery_cursor=recovery_cursor,
            occurred_at=occurred,
            idempotency_key=recovery_key(symbol, recovery_cursor, len(refs)),
            mode="automatic",
        )
        return self._commit(
            envelope,
            lambda: self._store.commit_recovery(
                symbol, bars=refs, recovery_cursor=recovery_cursor, envelope=envelope
            ),
        )

    @_sink_errors
    def advance_cursor(self, symbol: str, second_ref: Any) -> bool:
        """Advance the stored last-processed second, idempotently per second."""

        return self._store.advance_cursor(symbol, _ref(second_ref))

    @_sink_errors
    def mark_live_blocked(self, symbol: str, *, reason: str) -> None:
        """Record that the live feed for one symbol is blocked pending recovery."""

        self._store.mark_live_blocked(symbol, reason=reason)

    @_sink_errors
    def is_live_blocked(self, symbol: str) -> bool:
        """Return whether the live feed for one symbol is currently blocked."""

        return self._store.is_live_blocked(symbol)

    @_sink_errors
    def get_cursor(self, symbol: str) -> Mapping[str, Any] | None:
        """Return the stored market cursor for one symbol, if any."""

        return self._store.get_cursor(symbol)

    @_sink_errors
    def list_cursors(self) -> list[Mapping[str, Any]]:
        """Return all stored market cursor rows."""

        return self._store.list_cursors()


class RuntimeHydrator:
    """Restart restore seam over one G027 store.

    The hydrator rebuilds in-memory G024/G025/G026 state exclusively from
    durable rows and never calls G024 math operations, so a restored balance,
    position or pending episode is never recomputed or reset.  *registry* is
    the optional G016 registry used by the engine to re-seed scheduler
    records; the hydrator only carries it for the caller.
    """

    def __init__(self, store: RuntimeStore, *, registry: Any = None) -> None:
        self._store = store
        self.registry = registry

    @property
    def store(self) -> RuntimeStore:
        return self._store

    # -- shared field helpers ---------------------------------------------

    @staticmethod
    def _pick(*mappings: Mapping[str, Any], names: tuple[str, ...], default: Any = None) -> Any:
        for mapping in mappings:
            for name in names:
                value = mapping.get(name)
                if value is not None:
                    return value
        return default

    @staticmethod
    def _require_text(mapping: Mapping[str, Any], key: str, code: str) -> str:
        value = mapping.get(key)
        if not isinstance(value, str) or not value:
            raise RuntimePersistenceError(
                f"stored state is missing non-empty {key}", code=code
            )
        return value

    def _instant(self, mapping: Mapping[str, Any], key: str, code: str) -> datetime:
        value = mapping.get(key)
        if not isinstance(value, str) or not is_canonical_instant(value):
            raise RuntimePersistenceError(
                f"stored state {key} is not canonical UTC text", code=code
            )
        return instant_from_text(value)

    def _optional_instant(self, mapping: Mapping[str, Any], key: str, code: str) -> datetime | None:
        value = mapping.get(key)
        if value is None:
            return None
        return self._instant({key: value}, key, code)

    def _int(self, mapping: Mapping[str, Any], key: str, code: str) -> int:
        value = mapping.get(key)
        if type(value) is not int:
            raise RuntimePersistenceError(
                f"stored state {key} is not an integer", code=code
            )
        return value

    def _mapping(self, mapping: Mapping[str, Any], key: str, code: str) -> dict[str, Any]:
        value = mapping.get(key)
        if not isinstance(value, Mapping):
            raise RuntimePersistenceError(
                f"stored state {key} is not a mapping", code=code
            )
        return dict(value)

    # -- account book hydration -------------------------------------------

    def hydrate_account_book(self, strategy_ids: Iterable[str]) -> PaperAccountBook:
        """Rebuild one :class:`~pa_engine.paper.core.PaperAccountBook`.

        For each strategy the open trade is reconstructed from the
        authoritative trade row ``state_json`` (the G026 ExitState projection
        whose ``trade_lineage`` holds the full G024 trade) and the active
        pending entry from the pending row ``state_json``, with column and
        embedded account-row values as fallbacks.  The fixed USD 1,000.00
        reset is never applied: the stored balance is authoritative.
        """

        book = PaperAccountBook()
        for strategy_id in strategy_ids:
            row = self._store.get_account(strategy_id)
            if row is None:
                raise RuntimePersistenceError(
                    f"account row for strategy_id {strategy_id!r} is missing",
                    code="hydrate_account_mismatch",
                )
            open_rows = self._store.list_trades(strategy_id=strategy_id, status="open")
            pending_rows = self._store.list_pending_entries(
                strategy_id=strategy_id, status="pending"
            )
            if len(open_rows) > 1:
                raise RuntimePersistenceError(
                    f"strategy {strategy_id} has more than one open trade",
                    code="hydrate_account_mismatch",
                )
            if len(pending_rows) > 1:
                raise RuntimePersistenceError(
                    f"strategy {strategy_id} has more than one active pending entry",
                    code="hydrate_account_mismatch",
                )
            if bool(row.get("pending", 0)) and not pending_rows:
                raise RuntimePersistenceError(
                    f"account {strategy_id} is flagged pending but no pending row exists",
                    code="hydrate_account_mismatch",
                )
            if open_rows and pending_rows:
                raise RuntimePersistenceError(
                    f"strategy {strategy_id} has both an open trade and an active "
                    "pending entry",
                    code="hydrate_account_mismatch",
                )
            try:
                balance = Decimal(str(row.get("balance", "")))
            except Exception as exc:
                raise RuntimePersistenceError(
                    f"account {strategy_id} balance is not canonical decimal text",
                    code="hydrate_account_mismatch",
                ) from exc
            open_trade = (
                self._paper_trade_from_row(open_rows[0], row) if open_rows else None
            )
            pending_trade = (
                self._pending_trade_from_row(pending_rows[0], row) if pending_rows else None
            )
            book._accounts[strategy_id] = _ManagedAccount(
                strategy_id=strategy_id,
                balance=balance,
                exhausted=bool(row.get("exhausted", 0)),
                open_trade=open_trade,
                pending_trade=pending_trade,
                pending=bool(pending_rows),
            )
        return book

    def _paper_trade_from_row(
        self, trade_row: Mapping[str, Any], account_row: Mapping[str, Any]
    ) -> PaperTrade:
        state = trade_row.get("state_json")
        lineage = state.get("trade_lineage") if isinstance(state, Mapping) else None
        lineage = lineage if isinstance(lineage, Mapping) else {}
        embedded = account_row.get("open_trade")
        embedded = embedded if isinstance(embedded, Mapping) else {}
        code = "hydrate_account_mismatch"
        strategy_id = trade_row.get("strategy_id")
        if isinstance(state, Mapping) and state.get("strategy_id") is not None:
            if state["strategy_id"] != strategy_id:
                raise RuntimePersistenceError(
                    "trade state strategy_id differs from the trade row",
                    code=code,
                )
        embedded_id = embedded.get("trade_id")
        if embedded_id is not None and embedded_id != trade_row.get("trade_id"):
            raise RuntimePersistenceError(
                "account open_trade trade_id differs from the trade row", code=code
            )
        try:
            return PaperTrade(
                strategy_id=strategy_id,
                trade_id=self._pick(state, trade_row, names=("trade_id",)),
                symbol=self._pick(lineage, state, trade_row, names=("symbol",)),
                direction=self._pick(lineage, state, trade_row, names=("direction",)),
                entry_price=_decimal(
                    self._pick(lineage, state, trade_row, names=("entry_price",)),
                    "entry_price",
                    code,
                ),
                stop_price=_decimal(
                    self._pick(lineage, state, trade_row, names=("stop_price",)),
                    "stop_price",
                    code,
                ),
                target_price=_decimal(
                    self._pick(lineage, state, trade_row, names=("target_price",)),
                    "target_price",
                    code,
                ),
                quantity=_decimal(
                    self._pick(lineage, state, trade_row, names=("quantity",)),
                    "quantity",
                    code,
                ),
                planned_r=_decimal(
                    self._pick(lineage, state, trade_row, names=("planned_r",), default="0"),
                    "planned_r",
                    code,
                ),
                allocation_fraction=_decimal(
                    self._pick(
                        lineage, embedded, names=("allocation_fraction",), default="1"
                    ),
                    "allocation_fraction",
                    code,
                ),
                paper_math_version=str(
                    self._pick(
                        lineage, embedded, names=("paper_math_version",),
                        default=PAPER_MATH_VERSION,
                    )
                ),
                immediate=bool(
                    self._pick(lineage, embedded, names=("immediate",), default=False)
                ),
                requested_entry_price=_optional_decimal(
                    self._pick(lineage, embedded, names=("requested_entry_price",)),
                    "requested_entry_price",
                    code,
                ),
                opened=bool(self._pick(lineage, embedded, names=("opened",), default=True)),
            )
        except RuntimePersistenceError:
            raise
        except Exception as exc:
            raise RuntimePersistenceError(
                f"trade row is not hydratable: {exc}", code=code
            ) from exc

    def _pending_trade_from_row(
        self, pending_row: Mapping[str, Any], account_row: Mapping[str, Any]
    ) -> PendingTrade:
        state = pending_row.get("state_json")
        state = state if isinstance(state, Mapping) else {}
        embedded = account_row.get("pending_trade")
        embedded = embedded if isinstance(embedded, Mapping) else {}
        code = "hydrate_account_mismatch"
        strategy_id = pending_row.get("strategy_id")
        if state.get("pending_id") is not None and state["pending_id"] != pending_row.get(
            "pending_id"
        ):
            raise RuntimePersistenceError(
                "pending state pending_id differs from the pending row", code=code
            )
        embedded_id = embedded.get("pending_id")
        if embedded_id is not None and embedded_id != pending_row.get("pending_id"):
            raise RuntimePersistenceError(
                "account pending_trade pending_id differs from the pending row", code=code
            )
        try:
            entry_price = _decimal(
                self._pick(state, pending_row, names=("entry_price",)),
                "entry_price",
                code,
            )
            requested = _optional_decimal(
                self._pick(state, embedded, names=("requested_entry_price",)),
                "requested_entry_price",
                code,
            )
            return PendingTrade(
                strategy_id=strategy_id,
                pending_id=pending_row.get("pending_id"),
                symbol=self._pick(state, pending_row, names=("symbol",)),
                direction=self._pick(state, pending_row, names=("direction",)),
                entry_price=entry_price,
                stop_price=_decimal(
                    self._pick(state, pending_row, names=("stop_price",)),
                    "stop_price",
                    code,
                ),
                target_price=_decimal(
                    self._pick(state, pending_row, names=("target_price",)),
                    "target_price",
                    code,
                ),
                planned_r=_decimal(
                    self._pick(state, embedded, names=("planned_r",), default="0"),
                    "planned_r",
                    code,
                ),
                requested_entry_price=requested if requested is not None else entry_price,
                paper_math_version=str(
                    self._pick(
                        state, embedded, names=("paper_math_version",),
                        default=PAPER_MATH_VERSION,
                    )
                ),
            )
        except RuntimePersistenceError:
            raise
        except Exception as exc:
            raise RuntimePersistenceError(
                f"pending row is not hydratable: {exc}", code=code
            ) from exc

    # -- reducer state restoration ----------------------------------------

    def restore_state_rows(self) -> dict[str, Mapping[str, Any]]:
        """Return ``strategy_id -> stored strategy state row`` for all states.

        The engine uses these rows to seed scheduler records before wrapping
        restored monitors.
        """

        return {row["strategy_id"]: row for row in self._store.list_strategy_states()}

    def restore_pending_state(self, pending_row: Mapping[str, Any]) -> PendingEntryState:
        """Reconstruct the G025 pending state from the stored ``state_json``.

        The stored projection is the complete ``PendingEntryState.to_dict()``
        written at commit time; any missing or inconsistent required field
        raises :class:`RuntimePersistenceError` with
        ``code="pending_state_corrupt"``.
        """

        state = pending_row.get("state_json")
        if not isinstance(state, Mapping):
            raise RuntimePersistenceError(
                "pending row state_json is missing or not a mapping",
                code="pending_state_corrupt",
            )
        code = "pending_state_corrupt"
        try:
            status = str(state.get("status", "pending"))
            if status not in ("pending", "filled", "expired", "cancelled"):
                raise RuntimePersistenceError(
                    f"pending state status {status!r} is not in the closed vocabulary",
                    code=code,
                )
            lineage_value = state.get("decision_lineage")
            if not isinstance(lineage_value, Mapping):
                raise RuntimePersistenceError(
                    "pending state decision_lineage is not a mapping", code=code
                )
            lineage = EntryLineage(
                instruction_id=lineage_value.get("instruction_id"),
                instruction_hash=lineage_value.get("instruction_hash"),
                result_id=lineage_value.get("result_id"),
                result_hash=lineage_value.get("result_hash"),
                evidence_package_id=lineage_value.get("evidence_package_id"),
                evidence_package_hash=lineage_value.get("evidence_package_hash"),
                profile_ref_id=lineage_value.get("profile_ref_id"),
                profile_ref_hash=lineage_value.get("profile_ref_hash"),
                market_view_ref=lineage_value.get("market_view_ref"),
                instruction=lineage_value.get("instruction") or {},
            )
            return PendingEntryState(
                strategy_id=self._require_text(state, "strategy_id", code),
                pending_id=self._require_text(state, "pending_id", code),
                symbol=self._require_text(state, "symbol", code),
                direction=self._require_text(state, "direction", code),
                entry_price=_decimal(state.get("entry_price"), "entry_price", code),
                stop_price=_decimal(state.get("stop_price"), "stop_price", code),
                target_price=_decimal(state.get("target_price"), "target_price", code),
                decision_lineage=lineage,
                decision_accepted_at=self._instant(state, "decision_accepted_at", code),
                submitted_at=self._instant(state, "submitted_at", code),
                expires_at=self._instant(state, "expires_at", code),
                resolution_at=self._instant(state, "resolution_at", code),
                generation=self._int(state, "generation", code),
                market_feed=self._mapping(state, "market_feed", code),
                status=status,
                terminal_reason=state.get("terminal_reason"),
                gap_overlap=bool(state.get("gap_overlap", False)),
                partial_data=bool(state.get("partial_data", False)),
                gap_intervals=tuple(tuple(item) for item in state.get("gap_intervals", ())),
                recovery_attempted=bool(state.get("recovery_attempted", False)),
                processed_intervals=tuple(
                    tuple(item) for item in state.get("processed_intervals", ())
                ),
                processed_event_ids=tuple(state.get("processed_event_ids", ())),
                recovery_lineage=tuple(
                    dict(item) for item in state.get("recovery_lineage", ())
                ),
                filled_at=self._optional_instant(state, "filled_at", code),
                filled_price=_optional_decimal(
                    state.get("filled_price"), "filled_price", code
                ),
                exit_eligible_after=self._optional_instant(state, "exit_eligible_after", code),
            )
        except RuntimePersistenceError:
            raise
        except Exception as exc:
            raise RuntimePersistenceError(
                f"pending state_json is not reconstructable: {exc}", code=code
            ) from exc

    def restore_exit_state(self, trade_row: Mapping[str, Any]) -> ExitState:
        """Reconstruct the G026 exit state from a stored open trade row.

        The stored ``state_json`` is the complete ``ExitState.to_dict()``
        projection.  ``trade_lineage`` is re-normalized through G026's strict
        trade mapping so the restored monitor's identity check against the
        hydrated G024 book holds exactly.  If ``entry_fill_interval`` is
        absent it is derived from the row ``opened_at`` instant (the
        immediately preceding second); other missing required fields raise
        :class:`RuntimePersistenceError` with ``code="exit_state_corrupt"``.
        """

        state = trade_row.get("state_json")
        if not isinstance(state, Mapping):
            raise RuntimePersistenceError(
                "trade row state_json is missing or not a mapping",
                code="exit_state_corrupt",
            )
        code = "exit_state_corrupt"
        try:
            trade_lineage = self._restored_trade_lineage(state, trade_row, code)
            interval = state.get("entry_fill_interval")
            if interval is None:
                opened_at = trade_row.get("opened_at")
                if not isinstance(opened_at, str) or not is_canonical_instant(opened_at):
                    raise RuntimePersistenceError(
                        "exit state entry_fill_interval is missing", code=code
                    )
                fill_instant = instant_from_text(opened_at)
                interval = (instant_to_text(fill_instant - timedelta(seconds=1)), opened_at)
            entry_filled_at = state.get("entry_filled_at")
            if entry_filled_at is None:
                entry_filled_at = trade_row.get("opened_at")
            return ExitState(
                strategy_id=self._require_text(state, "strategy_id", code),
                trade_id=self._require_text(state, "trade_id", code),
                symbol=self._require_text(state, "symbol", code),
                direction=self._require_text(state, "direction", code),
                entry_price=_decimal(state.get("entry_price"), "entry_price", code),
                stop_price=_decimal(state.get("stop_price"), "stop_price", code),
                target_price=_decimal(state.get("target_price"), "target_price", code),
                quantity=_decimal(state.get("quantity"), "quantity", code),
                trade_lineage=trade_lineage,
                decision_lineage=self._mapping(state, "decision_lineage", code)
                if state.get("decision_lineage") is not None
                else {},
                entry_event=state.get("entry_event"),
                entry_fill_interval=tuple(interval),
                entry_filled_at=self._optional_instant(
                    {"entry_filled_at": entry_filled_at}, "entry_filled_at", code
                ),
                generation=self._int(state, "generation", code),
                market_feed=self._mapping(state, "market_feed", code)
                if state.get("market_feed") is not None
                else ALPACA_IEX_FEED.to_dict(),
                status=str(state.get("status", "open")),
                outcome_status=str(
                    state.get("outcome_status", state.get("status", "open"))
                ),
                terminal_reason=state.get("terminal_reason"),
                partial_data=bool(state.get("partial_data", False)),
                gap_active=bool(state.get("gap_active", False)),
                gap_intervals=tuple(tuple(item) for item in state.get("gap_intervals", ())),
                processed_intervals=tuple(
                    tuple(item) for item in state.get("processed_intervals", ())
                ),
                processed_event_ids=tuple(state.get("processed_event_ids", ())),
                processed_watermark=state.get("processed_watermark"),
                recovery_lineage=tuple(
                    dict(item) for item in state.get("recovery_lineage", ())
                ),
                recovery_attempted=bool(state.get("recovery_attempted", False)),
                selected_exit=_optional_decimal(
                    state.get("selected_exit"), "selected_exit", code
                ),
                exit_reason=state.get("exit_reason"),
                ambiguity_reason=state.get("ambiguity_reason"),
                uncertainty=state.get("uncertainty"),
                settlement=state.get("settlement"),
                scheduler_notification=state.get("scheduler_notification"),
                closed_at=self._optional_instant(state, "closed_at", code),
            )
        except RuntimePersistenceError:
            raise
        except Exception as exc:
            raise RuntimePersistenceError(
                f"exit state_json is not reconstructable: {exc}", code=code
            ) from exc

    def _restored_trade_lineage(
        self,
        state: Mapping[str, Any],
        trade_row: Mapping[str, Any],
        code: str,
    ) -> dict[str, Any]:
        value = state.get("trade_lineage")
        if not isinstance(value, Mapping):
            value = {
                "strategy_id": trade_row.get("strategy_id"),
                "trade_id": trade_row.get("trade_id"),
                "symbol": trade_row.get("symbol"),
                "direction": trade_row.get("direction"),
                "entry_price": trade_row.get("entry_price"),
                "stop_price": trade_row.get("stop_price"),
                "target_price": trade_row.get("target_price"),
                "quantity": trade_row.get("quantity"),
                "planned_r": trade_row.get("planned_r"),
            }
        try:
            mapped = trade_mapping(value)
            # G026's strict mapping converts only the required price fields to
            # Decimal; the remaining PaperTrade money fields come back from
            # the database as canonical text and must be re-normalized so the
            # restored trade_lineage is byte-equivalent to the live in-memory
            # projection the reducer compares against the hydrated book.
            for key in ("allocation_fraction", "requested_entry_price"):
                if mapped.get(key) is not None:
                    mapped[key] = Decimal(str(mapped[key]))
            return mapped
        except Exception as exc:
            raise RuntimePersistenceError(
                f"exit state trade_lineage is invalid: {exc}", code=code
            ) from exc

    # -- monitor wrapping --------------------------------------------------

    def wrap_pending_monitor(
        self,
        state: PendingEntryState,
        *,
        scheduler: Any,
        book: PaperAccountBook,
        on_pending_fill: Callable[..., Any] | None = None,
        on_pending_expiry: Callable[..., Any] | None = None,
        recovery_request: Mapping[str, Any] | None = None,
        expected_source_request_hash: str | None = None,
        expected_recovery_cursor: str | None = None,
        enabled: bool = True,
        exhausted: bool = False,
    ) -> PendingEntryReducer:
        """Wrap a restored pending state into a live G025 reducer.

        The G023 ``pending_expiry`` timer is claimed through the scheduler
        bridge exactly like the live path; a missing or mismatched scheduler
        record raises :class:`RuntimePersistenceError` with
        ``code="monitor_bind_failed"``.
        """

        if not isinstance(state, PendingEntryState):
            raise RuntimePersistenceError(
                "restored pending state must be a PendingEntryState",
                code="invalid_sink_input",
            )
        try:
            bridge = PendingEntrySchedulerBridge.bind(scheduler, state)
        except Exception as exc:
            raise RuntimePersistenceError(
                f"pending scheduler binding failed: {exc}", code="monitor_bind_failed"
            ) from exc
        try:
            return PendingEntryReducer(
                state,
                fill_pending=book.fill_pending_entry,
                expire_pending=book.expire_pending_entry,
                cancel_pending=self._pending_cancel(book),
                on_pending_fill=on_pending_fill,
                on_pending_expiry=on_pending_expiry,
                scheduler=scheduler,
                scheduler_bridge=bridge,
                paper_account_book=book,
                recovery_request=recovery_request,
                expected_source_request_hash=expected_source_request_hash,
                expected_recovery_cursor=expected_recovery_cursor,
                enabled=enabled,
                exhausted=exhausted,
            )
        except Exception as exc:
            raise RuntimePersistenceError(
                f"pending monitor construction failed: {exc}", code="monitor_bind_failed"
            ) from exc

    @staticmethod
    def _pending_cancel(book: PaperAccountBook) -> Callable[..., Any]:
        def cancel(
            strategy_id: str,
            pending_id: str,
            *,
            idempotency_key: str | None = None,
            reason: str = "cancelled",
            **_kwargs: Any,
        ) -> Any:
            return book.expire_pending_entry(
                strategy_id, pending_id, idempotency_key=idempotency_key, reason=reason
            )

        return cancel

    def wrap_exit_monitor(
        self,
        state: ExitState,
        *,
        scheduler: Any,
        book: PaperAccountBook,
        on_position_close: Callable[..., Any] | None = None,
        recovery_request: Mapping[str, Any] | None = None,
        expected_source_request_hash: str | None = None,
        enabled: bool = True,
        exhausted: bool = False,
    ) -> ExitReducer:
        """Wrap a restored open-position state into a live G026 reducer.

        The hydrated book is the authoritative G024 settlement boundary and
        no settlement override is supplied.  A missing or mismatched
        scheduler record raises :class:`RuntimePersistenceError` with
        ``code="monitor_bind_failed"``.
        """

        if not isinstance(state, ExitState):
            raise RuntimePersistenceError(
                "restored exit state must be an ExitState", code="invalid_sink_input"
            )
        try:
            bridge = ExitSchedulerBridge.bind(scheduler, state)
        except Exception as exc:
            raise RuntimePersistenceError(
                f"exit scheduler binding failed: {exc}", code="monitor_bind_failed"
            ) from exc
        try:
            return ExitReducer(
                state,
                paper_account_book=book,
                scheduler=scheduler,
                scheduler_bridge=bridge,
                on_position_close=on_position_close,
                recovery_request=recovery_request,
                expected_source_request_hash=expected_source_request_hash,
                enabled=enabled,
                exhausted=exhausted,
            )
        except Exception as exc:
            raise RuntimePersistenceError(
                f"exit monitor construction failed: {exc}", code="monitor_bind_failed"
            ) from exc

    # -- restart helpers ---------------------------------------------------

    def cursors(self) -> list[Mapping[str, Any]]:
        """Return all stored market cursor rows after a restore."""

        return self._store.list_cursors()

    def live_blocked(self) -> bool:
        """Return whether any cursor is live-blocked or already advanced.

        Mirrors :class:`~pa_engine.storage.RuntimeRecovery`'s
        ``RestoreSnapshot.live_blocked`` semantics: any stored last-second
        cursor blocks the live feed until the engine's recovery backfill
        commits.
        """

        return any(
            cursor.get("live_blocked") == 1 or cursor.get("last_second_end_at") is not None
            for cursor in self._store.list_cursors()
        )


__all__ = [
    "RuntimeHydrator",
    "RuntimePersistenceError",
    "RuntimeSink",
]
