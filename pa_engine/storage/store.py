"""Repository layer for the G027 SQLite runtime database.

This module owns materialized-state writes (accounts, strategy states,
timers, pending entries, trades), the append-only event stream and the
market cursor/recovery boundary.  Every public mutation runs inside exactly
one :class:`~pa_engine.storage.database.StorageDatabase` transaction, so a
state row and its linked runtime event commit atomically or not at all.

Module-level ``envelope_for_*`` helpers build validated
:class:`~pa_engine.storage.envelope.RuntimeEventEnvelope` dicts from
upstream state/instruction mappings; money arrives as ``Decimal`` or
canonical fixed-point text and is normalized with
:func:`pa_engine.storage.times.decimal_to_text`.  Idempotency keys are
derived deterministically from business content so re-delivery after a
restart is a no-op.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable, Iterator, Mapping

from pa_engine.contracts import canonical_json, canonical_json_bytes
from pa_engine.storage.database import StorageDatabase
from pa_engine.storage.envelope import RuntimeEventEnvelope, assert_durable_json_safe
from pa_engine.storage.errors import (
    EnvelopeValidationError,
    InvalidStorageInputError,
    RecoveryStateError,
    StorageConflictError,
)
from pa_engine.storage.refs import is_recovery_bar_ref, is_second_ref
from pa_engine.storage.schema import FEED_KEY, STRATEGY_STATES, TIMER_KINDS
from pa_engine.storage.times import (
    decimal_from_text,
    decimal_to_text,
    instant_from_text,
    instant_to_text,
    is_canonical_instant,
    is_decimal_text,
)

INITIAL_BALANCE_TEXT = "1000.00"
ACCOUNT_ID_PREFIX = "paper:"
PAPER_MATH_VERSION = "0.1.0"
IDEMPOTENCY_KEY_PREFIX = "rev:"

_ACCOUNT_COLUMNS = (
    "strategy_id",
    "account_id",
    "balance",
    "initial_balance",
    "exhausted",
    "pending",
    "open_trade",
    "pending_trade",
    "paper_math_version",
    "created_at",
    "updated_at",
)

_STRATEGY_STATE_COLUMNS = (
    "strategy_id",
    "state",
    "enabled",
    "exhausted",
    "generation",
    "intended_resume_state",
    "in_flight_job_id",
    "in_flight_generation",
    "pending_id",
    "open_position_id",
    "open_position_generation",
    "provider_retry_count",
    "semantic_retry_profile_hash",
    "semantic_retry_snapshot_id",
    "semantic_retry_count",
    "rejected_immediate_view_id",
    "current_view_id",
    "last_closed_snapshot_id",
    "transition_count",
    "last_transition_at",
    "blocked_reason",
    "decision_accepted_at",
    "next_due_at",
    "expires_at",
    "profile_ref",
    "packet",
    "latest_instruction",
    "created_at",
    "updated_at",
)

_PENDING_COLUMNS = (
    "pending_id",
    "strategy_id",
    "generation",
    "status",
    "symbol",
    "direction",
    "entry_price",
    "stop_price",
    "target_price",
    "decision_accepted_at",
    "submitted_at",
    "expires_at",
    "resolution_at",
    "episode_id",
    "state_hash",
    "state_json",
    "flags_json",
    "created_at",
    "updated_at",
)

_TRADE_COLUMNS = (
    "trade_id",
    "strategy_id",
    "status",
    "symbol",
    "direction",
    "quantity",
    "entry_price",
    "stop_price",
    "target_price",
    "planned_r",
    "trade_json",
    "settlement_json",
    "exit_reason",
    "opened_at",
    "closed_at",
    "episode_id",
    "state_json",
    "state_hash",
    "flags_json",
    "created_at",
    "updated_at",
)

_EVENT_COLUMNS = (
    "event_id",
    "event_hash",
    "event_type",
    "occurred_at",
    "mode",
    "strategy_id",
    "idempotency_key",
    "envelope",
    "created_at",
)

_CURSOR_COLUMNS = (
    "symbol",
    "feed_key",
    "last_second_event_id",
    "last_second_event_hash",
    "last_second_end_at",
    "recovery_cursor",
    "live_blocked",
    "pending_recovery_json",
    "updated_at",
)

_TIMER_COLUMNS = ("strategy_id", "kind", "generation", "due_at", "active", "fired")

_PENDING_EVENT_TYPES = {
    "created": "pending_entry_created",
    "expired": "pending_entry_expired",
    "cancelled": "pending_entry_cancelled",
}
_TRADE_EVENT_TYPES = {"filled": "paper_fill", "closed": "paper_close"}


def _decimal_text(value: Decimal | str) -> str:
    """Return canonical fixed-point text for a Decimal or canonical text."""

    if isinstance(value, Decimal):
        return decimal_to_text(value)
    if isinstance(value, str) and is_decimal_text(value):
        return value
    raise InvalidStorageInputError(
        "money value must be a Decimal or canonical fixed-point decimal text"
    )


def _instant_text(value: datetime | str) -> str:
    """Return canonical UTC instant text for a datetime or canonical text."""

    if isinstance(value, datetime):
        return instant_to_text(value)
    if isinstance(value, str) and is_canonical_instant(value):
        return value
    raise InvalidStorageInputError("instant must be a datetime or canonical UTC text")


def _json_native(value: Any) -> Any:
    """Convert Decimal and datetime leaves into their canonical JSON forms."""

    if isinstance(value, Decimal):
        return decimal_to_text(value)
    if isinstance(value, datetime):
        return instant_to_text(value)
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise InvalidStorageInputError("JSON object keys must be strings")
        return {key: _json_native(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_native(child) for child in value]
    return value


def _canonical_json(value: Any) -> str:
    return canonical_json(_json_native(value))


def _durable_json(value: Any) -> str:
    """Normalize one materialized JSON value after the durable-boundary scan."""

    native = _json_native(value)
    assert_durable_json_safe(native)
    return canonical_json(native)


def _state_hash(state: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(_json_native(state))).hexdigest()


def _flag(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if type(value) is int and value in (0, 1):
        return value
    raise InvalidStorageInputError("flag must be a boolean or 0/1")


def _idempotency_key(*parts: Any) -> str:
    """Derive one stable short idempotency key from business content."""

    rendered = []
    for part in parts:
        if isinstance(part, Decimal):
            rendered.append(decimal_to_text(part))
        elif isinstance(part, datetime):
            rendered.append(instant_to_text(part))
        else:
            rendered.append(str(part))
    digest = hashlib.sha256("|".join(rendered).encode("utf-8")).hexdigest()
    return IDEMPOTENCY_KEY_PREFIX + digest[:16]


def _record_lineage(record: Mapping[str, Any]) -> tuple[str, int, Mapping[str, Any] | None]:
    if not isinstance(record, Mapping):
        raise InvalidStorageInputError("record must be a mapping")
    strategy_id = record.get("strategy_id")
    generation = record.get("generation")
    if not isinstance(strategy_id, str) or not strategy_id:
        raise InvalidStorageInputError("record must carry a non-empty strategy_id")
    if type(generation) is not int or generation < 0:
        raise InvalidStorageInputError("record must carry a non-negative generation")
    profile_ref = record.get("profile_ref")
    if profile_ref is None:
        return strategy_id, generation, None
    if not isinstance(profile_ref, Mapping):
        raise InvalidStorageInputError("record profile_ref must be a mapping or None")
    compact = {
        key: profile_ref.get(key)
        for key in ("profile_ref_id", "profile_ref_hash")
    }
    if profile_ref.get("strategy_id") is not None:
        compact["strategy_id"] = profile_ref["strategy_id"]
    if not isinstance(compact["profile_ref_id"], str) or not isinstance(
        compact["profile_ref_hash"], str
    ):
        raise InvalidStorageInputError("record profile_ref must carry compact identity")
    if compact.get("strategy_id") is not None and compact["strategy_id"] != strategy_id:
        raise StorageConflictError("record profile_ref strategy_id does not match the record")
    return strategy_id, generation, compact


def _make_envelope(
    event_type: str,
    *,
    strategy_id: str | None,
    generation: int | None,
    profile_ref: Mapping[str, Any] | None,
    decision_lineage: Mapping[str, Any] | None,
    market_ref: Mapping[str, Any] | None,
    mode: str,
    occurred_at: datetime | str,
    idempotency_key: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return RuntimeEventEnvelope.make(
        event_type,
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=profile_ref,
        decision_lineage=decision_lineage,
        market_ref=market_ref,
        mode=mode,
        occurred_at=_instant_text(occurred_at),
        idempotency_key=idempotency_key,
        payload=payload,
    ).to_dict()


def envelope_for_state_transition(
    record: Mapping[str, Any],
    *,
    from_state: str,
    to_state: str,
    reason: str,
    resume_state: str | None = None,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Build a validated ``strategy_state_changed`` envelope from a snapshot."""

    strategy_id, generation, profile_ref = _record_lineage(record)
    payload = {
        "from": from_state,
        "to": to_state,
        "reason": reason,
        "resume_state": resume_state,
    }
    return _make_envelope(
        "strategy_state_changed",
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=profile_ref,
        decision_lineage=None,
        market_ref=None,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_decision(
    record: Mapping[str, Any],
    instruction: Mapping[str, Any],
    *,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Build a validated ``model_decision_accepted`` envelope from a G018 decision."""

    strategy_id, generation, profile_ref = _record_lineage(record)
    branch = instruction.get("branch")
    payload: dict[str, Any] = {
        "branch": branch,
        "instruction_id": instruction.get("instruction_id"),
        "instruction_hash": instruction.get("instruction_hash"),
        "result_id": instruction.get("result_id"),
        "result_hash": instruction.get("result_hash"),
        "evidence_package_id": instruction.get("evidence_package_id"),
        "evidence_package_hash": instruction.get("evidence_package_hash"),
    }
    if branch == "no_entry" and instruction.get("next_call_after_seconds") is not None:
        payload["next_call_after_seconds"] = instruction["next_call_after_seconds"]
    elif branch == "wait_for_price":
        if instruction.get("entry_wait_seconds") is not None:
            payload["entry_wait_seconds"] = instruction["entry_wait_seconds"]
        if instruction.get("entry_price") is not None:
            payload["entry_price"] = _decimal_text(instruction["entry_price"])
    lineage = {
        key: payload[key]
        for key in (
            "instruction_id",
            "instruction_hash",
            "result_id",
            "result_hash",
            "evidence_package_id",
            "evidence_package_hash",
        )
    }
    return _make_envelope(
        "model_decision_accepted",
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=profile_ref,
        decision_lineage=lineage,
        market_ref=None,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_schedule(
    record: Mapping[str, Any],
    *,
    timer_kind: str,
    due_at: datetime | str,
    decision_accepted_at: datetime | str,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Build a validated ``schedule_set`` envelope from a snapshot."""

    strategy_id, generation, profile_ref = _record_lineage(record)
    payload = {
        "timer_kind": timer_kind,
        "due_at": _instant_text(due_at),
        "decision_accepted_at": _instant_text(decision_accepted_at),
    }
    return _make_envelope(
        "schedule_set",
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=profile_ref,
        decision_lineage=None,
        market_ref=None,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_pending_entry(
    state: Mapping[str, Any],
    event: str,
    *,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
    market_ref: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a validated pending-entry envelope for one G025 state snapshot."""

    if event not in _PENDING_EVENT_TYPES:
        raise InvalidStorageInputError("pending entry event must be created, expired or cancelled")
    strategy_id, generation, _ = _record_lineage(state)
    if event == "created":
        payload = {
            "pending_id": state["pending_id"],
            "episode_id": state["episode_id"],
            "symbol": state["symbol"],
            "direction": state["direction"],
            "entry_price": _decimal_text(state["entry_price"]),
            "stop_price": _decimal_text(state["stop_price"]),
            "target_price": _decimal_text(state["target_price"]),
            "decision_accepted_at": _instant_text(state["decision_accepted_at"]),
            "submitted_at": _instant_text(state["submitted_at"]),
            "expires_at": _instant_text(state["expires_at"]),
            "resolution_at": _instant_text(state["resolution_at"]),
            "state_hash": _state_hash(state),
        }
    elif event == "expired":
        payload = {
            "pending_id": state["pending_id"],
            "episode_id": state["episode_id"],
            "resolution_at": _instant_text(state["resolution_at"]),
        }
    else:
        payload = {
            "pending_id": state["pending_id"],
            "episode_id": state["episode_id"],
            "resolution_at": _instant_text(state["resolution_at"]),
            "reason": state.get("terminal_reason") or "cancelled",
        }
    return _make_envelope(
        _PENDING_EVENT_TYPES[event],
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=None,
        decision_lineage=None,
        market_ref=market_ref,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_fill(
    trade_state: Mapping[str, Any],
    *,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
    market_ref: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a validated ``paper_fill`` envelope from one G026 trade snapshot."""

    strategy_id, generation, _ = _record_lineage(trade_state)
    trade_id = trade_state.get("trade_id")
    episode_id = trade_state.get("episode_id")
    if not isinstance(trade_id, str) or not trade_id:
        raise InvalidStorageInputError("trade state must carry a non-empty trade_id")
    if not isinstance(episode_id, str) or not episode_id:
        raise InvalidStorageInputError("trade state must carry a non-empty episode_id")
    filled_at = trade_state.get("entry_filled_at") or trade_state.get("filled_at")
    if filled_at is None:
        raise InvalidStorageInputError("trade state must carry a fill instant")
    trade_lineage = trade_state.get("trade_lineage") or {}
    planned = trade_lineage.get("planned_r")
    if planned is None:
        planned = trade_state.get("planned_r", "0")
    payload = {
        "trade_id": trade_id,
        "episode_id": episode_id,
        "symbol": trade_state["symbol"],
        "direction": trade_state["direction"],
        "entry_price": _decimal_text(trade_state["entry_price"]),
        "quantity": _decimal_text(trade_state["quantity"]),
        "planned_r": _decimal_text(planned),
        "filled_at": _instant_text(filled_at),
    }
    return _make_envelope(
        "paper_fill",
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=None,
        decision_lineage=None,
        market_ref=market_ref,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_fill_rejected(
    strategy_id: str,
    generation: int,
    *,
    reason: str,
    pending_id: str | None = None,
    trade_id: str | None = None,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Build a validated ``paper_fill_rejected`` envelope."""

    payload: dict[str, Any] = {"reason": reason}
    if pending_id is not None:
        payload["pending_id"] = pending_id
    if trade_id is not None:
        payload["trade_id"] = trade_id
    return _make_envelope(
        "paper_fill_rejected",
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=None,
        decision_lineage=None,
        market_ref=None,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_close(
    trade_state: Mapping[str, Any],
    *,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
    market_ref: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a validated ``paper_close`` envelope from one G026 settled snapshot."""

    strategy_id, generation, _ = _record_lineage(trade_state)
    trade_id = trade_state.get("trade_id")
    episode_id = trade_state.get("episode_id")
    closed_at = trade_state.get("closed_at")
    if not isinstance(trade_id, str) or not trade_id:
        raise InvalidStorageInputError("trade state must carry a non-empty trade_id")
    if not isinstance(episode_id, str) or not episode_id:
        raise InvalidStorageInputError("trade state must carry a non-empty episode_id")
    if closed_at is None:
        raise InvalidStorageInputError("trade state must carry a close instant")
    payload: dict[str, Any] = {
        "trade_id": trade_id,
        "episode_id": episode_id,
        "exit_reason": trade_state.get("exit_reason"),
        "closed_at": _instant_text(closed_at),
    }
    if trade_state.get("selected_exit") is not None:
        payload["selected_exit"] = _decimal_text(trade_state["selected_exit"])
    if trade_state.get("settlement") is not None:
        payload["settlement"] = _json_native(trade_state["settlement"])
    return _make_envelope(
        "paper_close",
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=None,
        decision_lineage=None,
        market_ref=market_ref,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_account_change(
    strategy_id: str,
    generation: int,
    *,
    balance_before: Decimal | str,
    balance_after: Decimal | str,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Build a validated ``account_changed`` envelope."""

    payload = {
        "balance_before": _decimal_text(balance_before),
        "balance_after": _decimal_text(balance_after),
    }
    return _make_envelope(
        "account_changed",
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=None,
        decision_lineage=None,
        market_ref=None,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_account_exhausted(
    strategy_id: str,
    generation: int,
    *,
    balance: Decimal | str,
    reason: str,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Build a validated ``account_exhausted`` envelope."""

    payload = {"balance": _decimal_text(balance), "reason": reason}
    return _make_envelope(
        "account_exhausted",
        strategy_id=strategy_id,
        generation=generation,
        profile_ref=None,
        decision_lineage=None,
        market_ref=None,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def envelope_for_market_recovery(
    symbol: str,
    *,
    bars: Iterable[Mapping[str, Any]],
    recovery_cursor: datetime | str | None,
    mode: str = "automatic",
    occurred_at: datetime | str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Build a validated ``market_recovery`` envelope from recovery bar refs."""

    bar_list = [dict(bar) for bar in bars]
    for bar in bar_list:
        if not is_recovery_bar_ref(bar) or bar.get("symbol") != symbol:
            raise EnvelopeValidationError(
                "market recovery bars must be valid recovery_bar refs for the symbol"
            )
    payload = {
        "symbol": symbol,
        "feed_key": FEED_KEY,
        "recovery_cursor": _instant_text(recovery_cursor) if recovery_cursor is not None else None,
        "bars": bar_list,
        "bar_count": len(bar_list),
    }
    return _make_envelope(
        "market_recovery",
        strategy_id=None,
        generation=None,
        profile_ref=None,
        decision_lineage=None,
        market_ref=None,
        mode=mode,
        occurred_at=occurred_at,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def state_transition_key(strategy_id: str, generation: int, to_state: str, reason: str) -> str:
    return _idempotency_key("state_transition", strategy_id, generation, to_state, reason)


def decision_key(strategy_id: str, generation: int, instruction_hash: str) -> str:
    return _idempotency_key("decision", strategy_id, generation, instruction_hash)


def schedule_key(strategy_id: str, generation: int, timer_kind: str, due_at: str) -> str:
    return _idempotency_key("schedule", strategy_id, generation, timer_kind, due_at)


def pending_key(strategy_id: str, pending_id: str, event: str) -> str:
    return _idempotency_key("pending", strategy_id, pending_id, event)


def fill_key(strategy_id: str, trade_id: str) -> str:
    return _idempotency_key("fill", strategy_id, trade_id)


def fill_rejected_key(
    strategy_id: str, reason: str, pending_id: str | None, trade_id: str | None
) -> str:
    return _idempotency_key("fill_rejected", strategy_id, reason, pending_id, trade_id)


def close_key(strategy_id: str, trade_id: str, exit_reason: str, closed_at: str) -> str:
    return _idempotency_key("close", strategy_id, trade_id, exit_reason, closed_at)


def account_change_key(
    strategy_id: str, balance_before: Decimal | str, balance_after: Decimal | str
) -> str:
    return _idempotency_key(
        "account_change", strategy_id, _decimal_text(balance_before), _decimal_text(balance_after)
    )


def account_exhausted_key(strategy_id: str, balance: Decimal | str, reason: str) -> str:
    return _idempotency_key("account_exhausted", strategy_id, _decimal_text(balance), reason)


def recovery_key(symbol: str, recovery_cursor: str | None, bar_count: int) -> str:
    return _idempotency_key("recovery", symbol, recovery_cursor, bar_count)


def _parse_json_columns(
    row: tuple[Any, ...], columns: tuple[str, ...], json_columns: tuple[str, ...]
) -> dict[str, Any]:
    result = dict(zip(columns, row))
    for key in json_columns:
        if result.get(key) is not None:
            result[key] = json.loads(result[key])
    return result


class RuntimeStore:
    """Repository for materialized G027 state, events and market cursors."""

    def __init__(self, db: StorageDatabase) -> None:
        self._db = db

    def _now_text(self) -> str:
        return instant_to_text(self._db._clock())

    def _validated_envelope_dict(self, envelope: Any) -> dict[str, Any]:
        if isinstance(envelope, RuntimeEventEnvelope):
            document = envelope.to_dict()
            RuntimeEventEnvelope.from_dict(document)
            return document
        if isinstance(envelope, Mapping):
            return RuntimeEventEnvelope.from_dict(dict(envelope)).to_dict()
        raise EnvelopeValidationError("envelope must be a RuntimeEventEnvelope or a mapping")

    def _append_event(self, conn: Any, document: Mapping[str, Any]) -> str:
        event_id = document["event_id"]
        idempotency_key = document["idempotency_key"]
        envelope_json = canonical_json(document)
        row = conn.execute(
            "SELECT idempotency_key, envelope_json FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if row is not None:
            if row[0] == idempotency_key and row[1] == envelope_json:
                return "replayed"
            raise StorageConflictError(
                f"event_id {event_id} already exists with different content"
            )
        conflict = conn.execute(
            "SELECT event_id FROM events WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if conflict is not None:
            raise StorageConflictError(
                f"idempotency_key {idempotency_key} already exists with a different event_id"
            )
        conn.execute(
            "INSERT INTO events (event_id, event_hash, event_type, occurred_at, mode, "
            "strategy_id, idempotency_key, envelope_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                document["event_hash"],
                document["event_type"],
                document["occurred_at"],
                document["mode"],
                document["strategy_id"],
                idempotency_key,
                envelope_json,
                self._now_text(),
            ),
        )
        return "appended"

    def ensure_accounts(self, strategy_ids: Iterable[str]) -> None:
        """Ensure one paper account row per strategy id, idempotently."""

        ids = [strategy_id for strategy_id in strategy_ids]
        for strategy_id in ids:
            if not isinstance(strategy_id, str) or not strategy_id:
                raise InvalidStorageInputError("strategy_id must be a non-empty string")
        now_text = self._now_text()
        with self._db.transaction() as conn:
            for strategy_id in ids:
                conn.execute(
                    "INSERT OR IGNORE INTO accounts (strategy_id, account_id, balance, "
                    "initial_balance, exhausted, pending, paper_math_version, created_at, "
                    "updated_at) VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?)",
                    (
                        strategy_id,
                        ACCOUNT_ID_PREFIX + strategy_id,
                        INITIAL_BALANCE_TEXT,
                        INITIAL_BALANCE_TEXT,
                        PAPER_MATH_VERSION,
                        now_text,
                        now_text,
                    ),
                )

    def _update_account(
        self,
        conn: Any,
        strategy_id: str,
        *,
        balance: Decimal | str,
        exhausted: bool | int | None = None,
        open_trade: Mapping[str, Any] | None = None,
        pending_trade: Mapping[str, Any] | None = None,
        pending: bool | int | None = None,
    ) -> None:
        balance_text = _decimal_text(balance)
        if decimal_from_text(balance_text) < 0:
            raise InvalidStorageInputError("account balance must be non-negative")
        if (
            conn.execute("SELECT 1 FROM accounts WHERE strategy_id = ?", (strategy_id,)).fetchone()
            is None
        ):
            raise InvalidStorageInputError(f"account for strategy_id {strategy_id} is not ensured")
        sets = ["balance = ?", "updated_at = ?"]
        params: list[Any] = [balance_text, self._now_text()]
        if exhausted is not None:
            sets.append("exhausted = ?")
            params.append(_flag(exhausted))
        if open_trade is not None:
            sets.append("open_trade_json = ?")
            params.append(_durable_json(open_trade))
        if pending_trade is not None:
            sets.append("pending_trade_json = ?")
            params.append(_durable_json(pending_trade))
        if pending is not None:
            sets.append("pending = ?")
            params.append(_flag(pending))
        params.append(strategy_id)
        conn.execute(f"UPDATE accounts SET {', '.join(sets)} WHERE strategy_id = ?", params)

    def get_account(self, strategy_id: str) -> dict[str, Any] | None:
        row = self._db.conn.execute(
            "SELECT strategy_id, account_id, balance, initial_balance, exhausted, pending, "
            "open_trade_json AS open_trade, pending_trade_json AS pending_trade, "
            "paper_math_version, created_at, updated_at FROM accounts WHERE strategy_id = ?",
            (strategy_id,),
        ).fetchone()
        if row is None:
            return None
        return _parse_json_columns(row, _ACCOUNT_COLUMNS, ("open_trade", "pending_trade"))

    def update_account(
        self,
        strategy_id: str,
        *,
        balance: Decimal | str,
        exhausted: bool | int | None = None,
        open_trade: Mapping[str, Any] | None = None,
        pending_trade: Mapping[str, Any] | None = None,
        pending: bool | int | None = None,
    ) -> None:
        with self._db.transaction() as conn:
            self._update_account(
                conn,
                strategy_id,
                balance=balance,
                exhausted=exhausted,
                open_trade=open_trade,
                pending_trade=pending_trade,
                pending=pending,
            )

    def list_accounts(self) -> list[dict[str, Any]]:
        rows = self._db.conn.execute(
            "SELECT strategy_id, account_id, balance, initial_balance, exhausted, pending, "
            "open_trade_json AS open_trade, pending_trade_json AS pending_trade, "
            "paper_math_version, created_at, updated_at FROM accounts ORDER BY strategy_id"
        ).fetchall()
        return [
            _parse_json_columns(row, _ACCOUNT_COLUMNS, ("open_trade", "pending_trade"))
            for row in rows
        ]

    def _upsert_timer(self, conn: Any, strategy_id: str, timer: Mapping[str, Any]) -> None:
        kind = timer.get("kind")
        generation = timer.get("generation")
        if kind not in TIMER_KINDS:
            raise InvalidStorageInputError(f"timer kind {kind!r} is not in the closed vocabulary")
        if type(generation) is not int or generation < 0:
            raise InvalidStorageInputError("timer generation must be a non-negative integer")
        conn.execute(
            "INSERT INTO timers (strategy_id, kind, generation, due_at, active, fired) "
            "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(strategy_id, kind) DO UPDATE SET "
            "generation = excluded.generation, due_at = excluded.due_at, "
            "active = excluded.active, fired = excluded.fired",
            (
                strategy_id,
                kind,
                generation,
                _instant_text(timer["due_at"]),
                _flag(timer.get("active", 1)),
                _flag(timer.get("fired", 0)),
            ),
        )

    def save_timers(self, strategy_id: str, timers: Iterable[Mapping[str, Any]]) -> None:
        with self._db.transaction() as conn:
            for timer in timers:
                self._upsert_timer(conn, strategy_id, timer)

    def clear_timer(self, strategy_id: str, kind: str) -> None:
        if kind not in TIMER_KINDS:
            raise InvalidStorageInputError(f"timer kind {kind!r} is not in the closed vocabulary")
        with self._db.transaction() as conn:
            conn.execute(
                "DELETE FROM timers WHERE strategy_id = ? AND kind = ?", (strategy_id, kind)
            )

    def get_timers(self, strategy_id: str) -> list[dict[str, Any]]:
        rows = self._db.conn.execute(
            "SELECT strategy_id, kind, generation, due_at, active, fired FROM timers "
            "WHERE strategy_id = ? ORDER BY kind",
            (strategy_id,),
        ).fetchall()
        return [dict(zip(_TIMER_COLUMNS, row)) for row in rows]

    def commit_strategy_state(
        self,
        snapshot: Mapping[str, Any],
        *,
        transition: Mapping[str, Any] | None = None,
        timers: Iterable[Mapping[str, Any]] = (),
        clear_timers: Iterable[str] = (),
        envelope: Mapping[str, Any] | None = None,
    ) -> None:
        """Upsert one strategy snapshot, optionally with its transition event."""

        if not isinstance(snapshot, Mapping):
            raise InvalidStorageInputError("snapshot must be a mapping")
        strategy_id = snapshot.get("strategy_id")
        state = snapshot.get("state")
        generation = snapshot.get("generation")
        if not isinstance(strategy_id, str) or not strategy_id:
            raise InvalidStorageInputError("snapshot must carry a non-empty strategy_id")
        if state not in STRATEGY_STATES:
            raise InvalidStorageInputError(
                "snapshot state is not in the closed strategy vocabulary"
            )
        if type(generation) is not int or generation < 0:
            raise InvalidStorageInputError("snapshot generation must be a non-negative integer")
        enabled = snapshot.get("enabled")
        exhausted = snapshot.get("exhausted")
        if not isinstance(enabled, bool):
            raise InvalidStorageInputError("snapshot enabled must be a boolean")
        if not isinstance(exhausted, bool):
            raise InvalidStorageInputError("snapshot exhausted must be a boolean")
        resume_state = snapshot.get("intended_resume_state")
        if resume_state is not None and resume_state not in STRATEGY_STATES:
            raise InvalidStorageInputError(
                "snapshot intended_resume_state is not in the closed strategy vocabulary"
            )
        semantic_retry_key = snapshot.get("semantic_retry_key")
        retry_profile = None
        retry_snapshot = None
        if semantic_retry_key is not None:
            if not isinstance(semantic_retry_key, (list, tuple)) or len(semantic_retry_key) != 2:
                raise InvalidStorageInputError(
                    "snapshot semantic_retry_key must be a two-item list of nullable strings"
                )
            retry_profile, retry_snapshot = semantic_retry_key
            if retry_profile is not None and not isinstance(retry_profile, str):
                raise InvalidStorageInputError("semantic_retry_profile_hash must be a string")
            if retry_snapshot is not None and not isinstance(retry_snapshot, str):
                raise InvalidStorageInputError("semantic_retry_snapshot_id must be a string")
        for key in ("provider_retry_count", "semantic_retry_count", "transition_count"):
            value = snapshot.get(key, 0)
            if type(value) is not int or value < 0:
                raise InvalidStorageInputError(f"snapshot {key} must be a non-negative integer")
        for key in ("in_flight_generation", "open_position_generation"):
            value = snapshot.get(key)
            if value is not None and type(value) is not int:
                raise InvalidStorageInputError(f"snapshot {key} must be an integer or None")
        instants: dict[str, str | None] = {}
        for key in ("decision_accepted_at", "next_due_at", "expires_at"):
            value = snapshot.get(key)
            instants[key] = _instant_text(value) if value is not None else None
        now_text = self._now_text()
        with self._db.transaction() as conn:
            if transition is not None:
                if envelope is None:
                    raise InvalidStorageInputError(
                        "a state transition requires its strategy_state_changed envelope"
                    )
                validated = RuntimeEventEnvelope.from_dict(envelope)
                if validated.event_type != "strategy_state_changed":
                    raise InvalidStorageInputError(
                        "transition envelope must be strategy_state_changed"
                    )
                if validated.strategy_id != strategy_id:
                    raise StorageConflictError(
                        "transition envelope strategy_id does not match the snapshot strategy_id"
                    )
                if validated.generation != generation:
                    raise StorageConflictError(
                        f"envelope generation {validated.generation} does not match "
                        f"snapshot generation {generation}"
                    )
                snapshot_profile = snapshot.get("profile_ref")
                envelope_profile = validated.profile_ref
                if snapshot_profile is not None and not isinstance(snapshot_profile, Mapping):
                    raise InvalidStorageInputError("snapshot profile_ref must be a mapping or None")
                if snapshot_profile is not None and snapshot_profile.get("strategy_id") not in (
                    None,
                    strategy_id,
                ):
                    raise StorageConflictError(
                        "snapshot profile_ref strategy_id does not match the snapshot strategy_id"
                    )
                if (snapshot_profile is None) != (envelope_profile is None):
                    raise StorageConflictError(
                        "snapshot profile_ref presence does not match envelope profile_ref"
                    )
                if snapshot_profile is not None and envelope_profile is not None:
                    if snapshot_profile.get("profile_ref_hash") != envelope_profile.get(
                        "profile_ref_hash"
                    ):
                        raise StorageConflictError(
                            "snapshot profile_ref hash does not match envelope profile_ref hash"
                        )
                expected_payload = {
                    "from": transition.get("from"),
                    "to": transition.get("to"),
                    "reason": transition.get("reason"),
                    "resume_state": transition.get("resume_state"),
                }
                if dict(validated.payload) != expected_payload:
                    raise InvalidStorageInputError("transition does not match envelope payload")
                event_status = self._append_event(conn, validated.to_dict())
                if event_status == "replayed":
                    return
            last_transition_at = snapshot.get("last_transition_at")
            if last_transition_at is None and transition is not None:
                last_transition_at = now_text
            if last_transition_at is not None:
                last_transition_at = _instant_text(last_transition_at)
            else:
                existing = conn.execute(
                    "SELECT last_transition_at FROM strategy_states WHERE strategy_id = ?",
                    (strategy_id,),
                ).fetchone()
                if existing is not None:
                    last_transition_at = existing[0]
            conn.execute(
                "INSERT INTO strategy_states (strategy_id, state, enabled, exhausted, "
                "generation, intended_resume_state, in_flight_job_id, in_flight_generation, "
                "pending_id, open_position_id, open_position_generation, provider_retry_count, "
                "semantic_retry_profile_hash, semantic_retry_snapshot_id, semantic_retry_count, "
                "rejected_immediate_view_id, current_view_id, last_closed_snapshot_id, "
                "transition_count, last_transition_at, blocked_reason, decision_accepted_at, "
                "next_due_at, expires_at, profile_ref_json, packet_json, latest_instruction_json, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(strategy_id) DO UPDATE "
                "SET state = excluded.state, enabled = excluded.enabled, "
                "exhausted = excluded.exhausted, generation = excluded.generation, "
                "intended_resume_state = excluded.intended_resume_state, "
                "in_flight_job_id = excluded.in_flight_job_id, "
                "in_flight_generation = excluded.in_flight_generation, "
                "pending_id = excluded.pending_id, "
                "open_position_id = excluded.open_position_id, "
                "open_position_generation = excluded.open_position_generation, "
                "provider_retry_count = excluded.provider_retry_count, "
                "semantic_retry_profile_hash = excluded.semantic_retry_profile_hash, "
                "semantic_retry_snapshot_id = excluded.semantic_retry_snapshot_id, "
                "semantic_retry_count = excluded.semantic_retry_count, "
                "rejected_immediate_view_id = excluded.rejected_immediate_view_id, "
                "current_view_id = excluded.current_view_id, "
                "last_closed_snapshot_id = excluded.last_closed_snapshot_id, "
                "transition_count = excluded.transition_count, "
                "last_transition_at = excluded.last_transition_at, "
                "blocked_reason = excluded.blocked_reason, "
                "decision_accepted_at = excluded.decision_accepted_at, "
                "next_due_at = excluded.next_due_at, "
                "expires_at = excluded.expires_at, "
                "profile_ref_json = excluded.profile_ref_json, "
                "packet_json = excluded.packet_json, "
                "latest_instruction_json = excluded.latest_instruction_json, "
                "updated_at = excluded.updated_at",
                (
                    strategy_id,
                    state,
                    int(enabled),
                    int(exhausted),
                    generation,
                    resume_state,
                    snapshot.get("in_flight_job_id"),
                    snapshot.get("in_flight_generation"),
                    snapshot.get("pending_id"),
                    snapshot.get("open_position_id"),
                    snapshot.get("open_position_generation"),
                    snapshot.get("provider_retry_count", 0),
                    retry_profile,
                    retry_snapshot,
                    snapshot.get("semantic_retry_count", 0),
                    snapshot.get("rejected_immediate_view_id"),
                    snapshot.get("current_view_id"),
                    snapshot.get("last_closed_snapshot_id"),
                    snapshot.get("transition_count", 0),
                    last_transition_at,
                    snapshot.get("blocked_reason"),
                    instants["decision_accepted_at"],
                    instants["next_due_at"],
                    instants["expires_at"],
                    _durable_json(snapshot.get("profile_ref"))
                    if snapshot.get("profile_ref") is not None
                    else None,
                    _durable_json(snapshot.get("packet")) if snapshot.get("packet") is not None else None,
                    _durable_json(snapshot.get("latest_instruction"))
                    if snapshot.get("latest_instruction") is not None
                    else None,
                    now_text,
                    now_text,
                ),
            )
            for timer in timers:
                self._upsert_timer(conn, strategy_id, timer)
            for kind in clear_timers:
                if kind not in TIMER_KINDS:
                    raise InvalidStorageInputError(
                        f"timer kind {kind!r} is not in the closed vocabulary"
                    )
                conn.execute(
                    "DELETE FROM timers WHERE strategy_id = ? AND kind = ?", (strategy_id, kind)
                )

    def _strategy_state_from_row(self, row: tuple[Any, ...]) -> dict[str, Any]:
        result = _parse_json_columns(
            row, _STRATEGY_STATE_COLUMNS, ("profile_ref", "packet", "latest_instruction")
        )
        retry_profile = result.pop("semantic_retry_profile_hash")
        retry_snapshot = result.pop("semantic_retry_snapshot_id")
        if retry_profile is not None or retry_snapshot is not None:
            result["semantic_retry_key"] = [retry_profile, retry_snapshot]
        else:
            result["semantic_retry_key"] = None
        return result

    def get_strategy_state(self, strategy_id: str) -> dict[str, Any] | None:
        row = self._db.conn.execute(
            "SELECT strategy_id, state, enabled, exhausted, generation, "
            "intended_resume_state, in_flight_job_id, in_flight_generation, pending_id, "
            "open_position_id, open_position_generation, provider_retry_count, "
            "semantic_retry_profile_hash, semantic_retry_snapshot_id, semantic_retry_count, "
            "rejected_immediate_view_id, current_view_id, last_closed_snapshot_id, "
            "transition_count, last_transition_at, blocked_reason, decision_accepted_at, "
            "next_due_at, expires_at, profile_ref_json AS profile_ref, packet_json AS packet, "
            "latest_instruction_json AS latest_instruction, created_at, updated_at "
            "FROM strategy_states WHERE strategy_id = ?",
            (strategy_id,),
        ).fetchone()
        if row is None:
            return None
        return self._strategy_state_from_row(row)

    def list_strategy_states(self) -> list[dict[str, Any]]:
        rows = self._db.conn.execute(
            "SELECT strategy_id, state, enabled, exhausted, generation, "
            "intended_resume_state, in_flight_job_id, in_flight_generation, pending_id, "
            "open_position_id, open_position_generation, provider_retry_count, "
            "semantic_retry_profile_hash, semantic_retry_snapshot_id, semantic_retry_count, "
            "rejected_immediate_view_id, current_view_id, last_closed_snapshot_id, "
            "transition_count, last_transition_at, blocked_reason, decision_accepted_at, "
            "next_due_at, expires_at, profile_ref_json AS profile_ref, packet_json AS packet, "
            "latest_instruction_json AS latest_instruction, created_at, updated_at "
            "FROM strategy_states ORDER BY strategy_id"
        ).fetchall()
        return [self._strategy_state_from_row(row) for row in rows]

    def commit_pending_entry(
        self, state: Mapping[str, Any], *, event: str, envelope: Mapping[str, Any]
    ) -> None:
        """Upsert one pending-entry snapshot with its linked event atomically."""

        if event not in _PENDING_EVENT_TYPES:
            raise InvalidStorageInputError(
                "pending entry event must be created, expired or cancelled"
            )
        if state.get("status") not in ("pending", "filled", "expired", "cancelled"):
            raise InvalidStorageInputError(
                "pending entry status is not in the closed vocabulary"
            )
        if event == "created" and state.get("status") not in ("pending", "filled"):
            raise InvalidStorageInputError("created pending event requires pending or filled state")
        if event == "expired" and state.get("status") != "expired":
            raise InvalidStorageInputError("expired pending event requires expired state")
        if event == "cancelled" and state.get("status") != "cancelled":
            raise InvalidStorageInputError("cancelled pending event requires cancelled state")
        validated = RuntimeEventEnvelope.from_dict(envelope)
        if validated.event_type != _PENDING_EVENT_TYPES[event]:
            raise InvalidStorageInputError(
                f"envelope type {validated.event_type} does not match pending entry event {event}"
            )
        if validated.strategy_id != state.get("strategy_id"):
            raise StorageConflictError(
                "pending entry envelope strategy_id does not match the state strategy_id"
            )
        if validated.generation != state.get("generation"):
            raise StorageConflictError(
                "pending entry envelope generation does not match the state generation"
            )
        if validated.payload.get("pending_id") != state.get("pending_id"):
            raise InvalidStorageInputError(
                "pending entry envelope pending_id does not match the state pending_id"
            )
        pending_payload = dict(validated.payload)
        pending_payload_expected = {
            "episode_id": state.get("episode_id"),
        }
        if event == "created":
            pending_payload_expected.update(
                {
                    "symbol": state.get("symbol"),
                    "direction": state.get("direction"),
                    "entry_price": _decimal_text(state["entry_price"]),
                    "stop_price": _decimal_text(state["stop_price"]),
                    "target_price": _decimal_text(state["target_price"]),
                    "decision_accepted_at": _instant_text(state["decision_accepted_at"]),
                    "submitted_at": _instant_text(state["submitted_at"]),
                    "expires_at": _instant_text(state["expires_at"]),
                    "resolution_at": _instant_text(state["resolution_at"]),
                }
            )
        else:
            pending_payload_expected["resolution_at"] = _instant_text(state["resolution_at"])
            if event == "cancelled":
                pending_payload_expected["reason"] = state.get("terminal_reason") or "cancelled"
        for key, expected in pending_payload_expected.items():
            if pending_payload.get(key) != expected:
                raise EnvelopeValidationError(
                    f"pending entry envelope payload field {key} does not match the state"
                )
        market_ref = validated.market_ref
        if market_ref is not None and market_ref.get("symbol") != state.get("symbol"):
            raise StorageConflictError(
                "pending entry market ref symbol does not match the state symbol"
            )
        state_hash = _state_hash(state)
        if event == "created" and state["status"] == "pending" and pending_payload.get("state_hash") != state_hash:
            raise StorageConflictError(
                "pending entry envelope state_hash does not match the pending state"
            )
        now_text = self._now_text()
        with self._db.transaction() as conn:
            event_status = self._append_event(conn, validated.to_dict())
            if event_status == "replayed" and not (event == "created" and state["status"] == "filled"):
                return
            state_row = conn.execute(
                "SELECT generation FROM strategy_states WHERE strategy_id = ?",
                (state["strategy_id"],),
            ).fetchone()
            if state_row is not None and state_row[0] != state["generation"]:
                raise StorageConflictError(
                    "pending entry generation does not match the stored strategy generation"
                )
            existing = conn.execute(
                "SELECT strategy_id, generation, status, symbol, direction, entry_price, "
                "stop_price, target_price, decision_accepted_at, submitted_at, expires_at, "
                "resolution_at, episode_id FROM pending_entries WHERE pending_id = ?",
                (state["pending_id"],),
            ).fetchone()
            if existing is not None:
                if event_status == "replayed" and existing[2] != "pending":
                    return
                stable = (
                    ("strategy_id", existing[0], state["strategy_id"]),
                    ("generation", existing[1], state["generation"]),
                    ("symbol", existing[3], state["symbol"]),
                    ("direction", existing[4], state["direction"]),
                    ("entry_price", existing[5], _decimal_text(state["entry_price"])),
                    ("stop_price", existing[6], _decimal_text(state["stop_price"])),
                    ("target_price", existing[7], _decimal_text(state["target_price"])),
                    (
                        "decision_accepted_at",
                        existing[8],
                        _instant_text(state["decision_accepted_at"]),
                    ),
                    ("submitted_at", existing[9], _instant_text(state["submitted_at"])),
                    ("expires_at", existing[10], _instant_text(state["expires_at"])),
                    ("resolution_at", existing[11], _instant_text(state["resolution_at"])),
                    ("episode_id", existing[12], state["episode_id"]),
                )
                mismatch = next((name for name, stored, incoming in stable if stored != incoming), None)
                if mismatch is not None:
                    raise StorageConflictError(
                        f"pending entry {state['pending_id']} identity field {mismatch} changed"
                    )
                if existing[2] != "pending" and state["status"] != existing[2]:
                    raise StorageConflictError(
                        f"pending entry {state['pending_id']} is already {existing[2]}"
                    )
            conn.execute(
                "INSERT INTO pending_entries (pending_id, strategy_id, generation, status, "
                "symbol, direction, entry_price, stop_price, target_price, "
                "decision_accepted_at, submitted_at, expires_at, resolution_at, episode_id, "
                "state_hash, state_json, flags_json, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(pending_id) DO UPDATE SET strategy_id = excluded.strategy_id, "
                "generation = excluded.generation, status = excluded.status, "
                "symbol = excluded.symbol, direction = excluded.direction, "
                "entry_price = excluded.entry_price, stop_price = excluded.stop_price, "
                "target_price = excluded.target_price, "
                "decision_accepted_at = excluded.decision_accepted_at, "
                "submitted_at = excluded.submitted_at, expires_at = excluded.expires_at, "
                "resolution_at = excluded.resolution_at, episode_id = excluded.episode_id, "
                "state_hash = excluded.state_hash, state_json = excluded.state_json, "
                "flags_json = excluded.flags_json, updated_at = excluded.updated_at",
                (
                    state["pending_id"],
                    state["strategy_id"],
                    state["generation"],
                    state["status"],
                    state["symbol"],
                    state["direction"],
                    _decimal_text(state["entry_price"]),
                    _decimal_text(state["stop_price"]),
                    _decimal_text(state["target_price"]),
                    _instant_text(state["decision_accepted_at"]),
                    _instant_text(state["submitted_at"]),
                    _instant_text(state["expires_at"]),
                    _instant_text(state["resolution_at"]),
                    state["episode_id"],
                    state_hash,
                    _durable_json(state),
                    _durable_json(
                        {
                            "gap_overlap": bool(state.get("gap_overlap", False)),
                            "partial_data": bool(state.get("partial_data", False)),
                            "recovery_attempted": bool(state.get("recovery_attempted", False)),
                        }
                    ),
                    now_text,
                    now_text,
                ),
            )

    def _pending_from_row(self, row: tuple[Any, ...]) -> dict[str, Any]:
        return _parse_json_columns(
            row, _PENDING_COLUMNS, ("state_json", "flags_json")
        )

    def get_pending_entry(self, pending_id: str) -> dict[str, Any] | None:
        row = self._db.conn.execute(
            "SELECT pending_id, strategy_id, generation, status, symbol, direction, "
            "entry_price, stop_price, target_price, decision_accepted_at, submitted_at, "
            "expires_at, resolution_at, episode_id, state_hash, state_json, flags_json, "
            "created_at, updated_at FROM pending_entries WHERE pending_id = ?",
            (pending_id,),
        ).fetchone()
        if row is None:
            return None
        return self._pending_from_row(row)

    def list_pending_entries(
        self, *, strategy_id: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if strategy_id is not None:
            clauses.append("strategy_id = ?")
            params.append(strategy_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._db.conn.execute(
            "SELECT pending_id, strategy_id, generation, status, symbol, direction, "
            "entry_price, stop_price, target_price, decision_accepted_at, submitted_at, "
            "expires_at, resolution_at, episode_id, state_hash, state_json, flags_json, "
            "created_at, updated_at FROM pending_entries"
            + where
            + " ORDER BY pending_id",
            params,
        ).fetchall()
        return [self._pending_from_row(row) for row in rows]

    def commit_trade(
        self, state: Mapping[str, Any], *, event: str, envelope: Mapping[str, Any]
    ) -> None:
        """Upsert one trade snapshot with its linked paper event atomically."""

        if event not in _TRADE_EVENT_TYPES:
            raise InvalidStorageInputError("trade event must be filled or closed")
        if event == "filled" and state.get("status") != "open":
            raise InvalidStorageInputError("filled trade event requires open state")
        if event == "closed" and state.get("status") not in ("settled", "exhausted"):
            raise InvalidStorageInputError("closed trade event requires a terminal state")
        validated = RuntimeEventEnvelope.from_dict(envelope)
        if validated.event_type != _TRADE_EVENT_TYPES[event]:
            raise InvalidStorageInputError(
                f"envelope type {validated.event_type} does not match trade event {event}"
            )
        if validated.strategy_id != state.get("strategy_id"):
            raise StorageConflictError(
                "trade envelope strategy_id does not match the state strategy_id"
            )
        if validated.generation != state.get("generation"):
            raise StorageConflictError(
                "trade envelope generation does not match the state generation"
            )
        if validated.payload.get("trade_id") != state.get("trade_id"):
            raise InvalidStorageInputError(
                "trade envelope trade_id does not match the state trade_id"
            )
        if validated.market_ref is not None and validated.market_ref.get("symbol") != state.get("symbol"):
            raise StorageConflictError("trade market ref symbol does not match the state symbol")
        trade_lineage = state.get("trade_lineage") or {}
        if not isinstance(trade_lineage, Mapping):
            raise InvalidStorageInputError("trade_lineage must be a mapping")
        planned = trade_lineage.get("planned_r")
        if planned is None:
            planned = state.get("planned_r", "0")
        trade_payload = dict(validated.payload)
        if trade_payload.get("episode_id") != state.get("episode_id"):
            raise EnvelopeValidationError("trade envelope episode_id does not match the state")
        if event == "filled":
            filled_at = state.get("entry_filled_at") or state.get("filled_at")
            if filled_at is None:
                raise InvalidStorageInputError("filled trade state must carry a fill instant")
            filled_text = _instant_text(filled_at)
            expected_fill = {
                "symbol": state.get("symbol"),
                "direction": state.get("direction"),
                "entry_price": _decimal_text(state["entry_price"]),
                "quantity": _decimal_text(state["quantity"]),
                "planned_r": _decimal_text(planned),
                "filled_at": filled_text,
            }
            for key, expected in expected_fill.items():
                if trade_payload.get(key) != expected:
                    raise EnvelopeValidationError(
                        f"paper_fill envelope payload field {key} does not match the state"
                    )
        opened_at = state.get("entry_filled_at") or state.get("filled_at")
        opened_text = _instant_text(opened_at) if opened_at is not None else self._now_text()
        closed_at = state.get("closed_at")
        closed_text = _instant_text(closed_at) if closed_at is not None else None
        settlement = state.get("settlement")
        if event == "closed":
            if closed_text is None:
                raise InvalidStorageInputError("closed trade state must carry a close instant")
            expected_close = {
                "exit_reason": state.get("exit_reason"),
                "closed_at": closed_text,
                "selected_exit": (
                    _decimal_text(state["selected_exit"])
                    if state.get("selected_exit") is not None
                    else None
                ),
                "settlement": _json_native(settlement) if settlement is not None else None,
            }
            for key, expected in expected_close.items():
                if trade_payload.get(key) != expected:
                    raise EnvelopeValidationError(
                        f"paper_close envelope payload field {key} does not match the state"
                    )
        settlement_json = _durable_json(settlement) if settlement is not None else None
        flags = {
            "gap_active": bool(state.get("gap_active", False)),
            "partial_data": bool(state.get("partial_data", False)),
            "recovery_attempted": bool(state.get("recovery_attempted", False)),
        }
        status = "open" if event == "filled" else "settled"
        now_text = self._now_text()
        with self._db.transaction() as conn:
            event_status = self._append_event(conn, validated.to_dict())
            if event_status == "replayed":
                return
            state_row = conn.execute(
                "SELECT generation FROM strategy_states WHERE strategy_id = ?",
                (state["strategy_id"],),
            ).fetchone()
            if state_row is not None and state_row[0] != state["generation"]:
                raise StorageConflictError(
                    "trade generation does not match the stored strategy generation"
                )
            existing = conn.execute(
                "SELECT strategy_id, status, symbol, direction, quantity, entry_price, "
                "stop_price, target_price, planned_r, opened_at, episode_id FROM trades "
                "WHERE trade_id = ?",
                (state["trade_id"],),
            ).fetchone()
            if existing is not None:
                stable = (
                    ("strategy_id", existing[0], state["strategy_id"]),
                    ("symbol", existing[2], state["symbol"]),
                    ("direction", existing[3], state["direction"]),
                    ("quantity", existing[4], _decimal_text(state["quantity"])),
                    ("entry_price", existing[5], _decimal_text(state["entry_price"])),
                    ("stop_price", existing[6], _decimal_text(state["stop_price"])),
                    ("target_price", existing[7], _decimal_text(state["target_price"])),
                    ("planned_r", existing[8], _decimal_text(planned)),
                    ("opened_at", existing[9], opened_text),
                    ("episode_id", existing[10], state["episode_id"]),
                )
                mismatch = next((name for name, stored, incoming in stable if stored != incoming), None)
                if mismatch is not None:
                    raise StorageConflictError(
                        f"trade {state['trade_id']} identity field {mismatch} changed"
                    )
                if existing[1] != "open" or event != "closed":
                    raise StorageConflictError(
                        f"trade {state['trade_id']} cannot accept {event} from {existing[1]}"
                    )
            conn.execute(
                "INSERT INTO trades (trade_id, strategy_id, status, symbol, direction, "
                "quantity, entry_price, stop_price, target_price, planned_r, trade_json, "
                "settlement_json, exit_reason, opened_at, closed_at, episode_id, state_json, "
                "state_hash, flags_json, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(trade_id) DO UPDATE SET strategy_id = excluded.strategy_id, "
                "status = excluded.status, symbol = excluded.symbol, "
                "direction = excluded.direction, quantity = excluded.quantity, "
                "entry_price = excluded.entry_price, stop_price = excluded.stop_price, "
                "target_price = excluded.target_price, planned_r = excluded.planned_r, "
                "trade_json = excluded.trade_json, "
                "settlement_json = excluded.settlement_json, "
                "exit_reason = excluded.exit_reason, opened_at = excluded.opened_at, "
                "closed_at = excluded.closed_at, episode_id = excluded.episode_id, "
                "state_json = excluded.state_json, state_hash = excluded.state_hash, "
                "flags_json = excluded.flags_json, updated_at = excluded.updated_at",
                (
                    state["trade_id"],
                    state["strategy_id"],
                    status,
                    state["symbol"],
                    state["direction"],
                    _decimal_text(state["quantity"]),
                    _decimal_text(state["entry_price"]),
                    _decimal_text(state["stop_price"]),
                    _decimal_text(state["target_price"]),
                    _decimal_text(planned),
                    _durable_json(trade_lineage),
                    settlement_json,
                    state.get("exit_reason"),
                    opened_text,
                    closed_text,
                    state.get("episode_id"),
                    _durable_json(state),
                    _state_hash(state),
                    _durable_json(flags),
                    now_text,
                    now_text,
                ),
            )

    def _trade_from_row(self, row: tuple[Any, ...]) -> dict[str, Any]:
        return _parse_json_columns(
            row,
            _TRADE_COLUMNS,
            ("trade_json", "settlement_json", "state_json", "flags_json"),
        )

    def get_trade(self, trade_id: str) -> dict[str, Any] | None:
        row = self._db.conn.execute(
            "SELECT trade_id, strategy_id, status, symbol, direction, quantity, entry_price, "
            "stop_price, target_price, planned_r, trade_json, settlement_json, exit_reason, "
            "opened_at, closed_at, episode_id, state_json, state_hash, flags_json, "
            "created_at, updated_at FROM trades WHERE trade_id = ?",
            (trade_id,),
        ).fetchone()
        if row is None:
            return None
        return self._trade_from_row(row)

    def list_trades(
        self, *, strategy_id: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if strategy_id is not None:
            clauses.append("strategy_id = ?")
            params.append(strategy_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._db.conn.execute(
            "SELECT trade_id, strategy_id, status, symbol, direction, quantity, entry_price, "
            "stop_price, target_price, planned_r, trade_json, settlement_json, exit_reason, "
            "opened_at, closed_at, episode_id, state_json, state_hash, flags_json, "
            "created_at, updated_at FROM trades"
            + where
            + " ORDER BY trade_id",
            params,
        ).fetchall()
        return [self._trade_from_row(row) for row in rows]

    def append_event(self, envelope: Any) -> str:
        """Append one validated runtime event, returning appended or replayed."""

        document = self._validated_envelope_dict(envelope)
        with self._db.transaction() as conn:
            return self._append_event(conn, document)

    def _event_from_row(self, row: tuple[Any, ...]) -> dict[str, Any]:
        return _parse_json_columns(row, _EVENT_COLUMNS, ("envelope",))

    def list_events(
        self,
        *,
        strategy_id: str | None = None,
        event_type: str | None = None,
        mode: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if strategy_id is not None:
            clauses.append("strategy_id = ?")
            params.append(strategy_id)
        if event_type is not None:
            clauses.append("event_type = ?")
            params.append(event_type)
        if mode is not None:
            clauses.append("mode = ?")
            params.append(mode)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._db.conn.execute(
            "SELECT event_id, event_hash, event_type, occurred_at, mode, strategy_id, "
            "idempotency_key, envelope_json AS envelope, created_at FROM events"
            + where
            + " ORDER BY occurred_at, created_at",
            params,
        ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def event_stream(self, strategy_id: str | None = None) -> Iterator[dict[str, Any]]:
        yield from self.list_events(strategy_id=strategy_id)

    def _cursor_from_row(self, row: tuple[Any, ...]) -> dict[str, Any]:
        return _parse_json_columns(row, _CURSOR_COLUMNS, ("pending_recovery_json",))

    def get_cursor(self, symbol: str) -> dict[str, Any] | None:
        row = self._db.conn.execute(
            "SELECT symbol, feed_key, last_second_event_id, last_second_event_hash, "
            "last_second_end_at, recovery_cursor, live_blocked, pending_recovery_json, "
            "updated_at FROM market_cursors WHERE symbol = ? AND feed_key = ?",
            (symbol, FEED_KEY),
        ).fetchone()
        if row is None:
            return None
        return self._cursor_from_row(row)

    def list_cursors(self) -> list[dict[str, Any]]:
        rows = self._db.conn.execute(
            "SELECT symbol, feed_key, last_second_event_id, last_second_event_hash, "
            "last_second_end_at, recovery_cursor, live_blocked, pending_recovery_json, "
            "updated_at FROM market_cursors ORDER BY symbol"
        ).fetchall()
        return [self._cursor_from_row(row) for row in rows]

    def advance_cursor(self, symbol: str, second_ref: Mapping[str, Any]) -> bool:
        """Advance the stored last-processed second, idempotently per second."""

        if not is_second_ref(second_ref):
            raise EnvelopeValidationError(
                "cursor advance requires a valid second_event market ref"
            )
        if second_ref.get("symbol") != symbol:
            raise EnvelopeValidationError(
                "cursor advance market ref symbol does not match the requested symbol"
            )
        end_at = second_ref["end_at"]
        event_id = second_ref["second_event_id"]
        event_hash = second_ref["second_event_hash"]
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO market_cursors (symbol, feed_key, updated_at) "
                "VALUES (?, ?, ?)",
                (symbol, FEED_KEY, self._now_text()),
            )
            row = conn.execute(
                "SELECT last_second_event_id, last_second_event_hash, last_second_end_at "
                "FROM market_cursors WHERE symbol = ? AND feed_key = ?",
                (symbol, FEED_KEY),
            ).fetchone()
            stored_end = row[2]
            if stored_end is not None:
                if end_at == stored_end and row[0] == event_id and row[1] == event_hash:
                    return True
                if instant_from_text(end_at) < instant_from_text(stored_end):
                    raise RecoveryStateError(
                        f"cursor end_at {end_at} is earlier than the stored {stored_end}"
                    )
            conn.execute(
                "UPDATE market_cursors SET last_second_event_id = ?, "
                "last_second_event_hash = ?, last_second_end_at = ?, updated_at = ? "
                "WHERE symbol = ? AND feed_key = ?",
                (event_id, event_hash, end_at, self._now_text(), symbol, FEED_KEY),
            )
        return True

    def mark_live_blocked(self, symbol: str, *, reason: str) -> None:
        """Record that the live feed for one symbol is blocked pending recovery."""

        if not isinstance(reason, str) or not reason:
            raise InvalidStorageInputError("live-block reason must be a non-empty string")
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO market_cursors (symbol, feed_key, updated_at) "
                "VALUES (?, ?, ?)",
                (symbol, FEED_KEY, self._now_text()),
            )
            conn.execute(
                "UPDATE market_cursors SET live_blocked = 1, pending_recovery_json = ?, "
                "updated_at = ? WHERE symbol = ? AND feed_key = ?",
                (_canonical_json({"reason": reason}), self._now_text(), symbol, FEED_KEY),
            )

    def is_live_blocked(self, symbol: str) -> bool:
        row = self._db.conn.execute(
            "SELECT live_blocked FROM market_cursors WHERE symbol = ? AND feed_key = ?",
            (symbol, FEED_KEY),
        ).fetchone()
        return row is not None and row[0] == 1

    def commit_recovery(
        self,
        symbol: str,
        *,
        bars: Iterable[Mapping[str, Any]],
        recovery_cursor: datetime | str | None,
        envelope: Mapping[str, Any],
    ) -> None:
        """Commit one market recovery atomically with its event and cursor."""

        bar_list = [dict(bar) for bar in bars]
        if not bar_list:
            raise InvalidStorageInputError("recovery requires at least one bar")
        for bar in bar_list:
            if not is_recovery_bar_ref(bar) or bar.get("symbol") != symbol:
                raise EnvelopeValidationError(
                    "recovery bars must be valid recovery_bar refs for the requested symbol"
                )
        validated = RuntimeEventEnvelope.from_dict(envelope)
        if validated.event_type != "market_recovery":
            raise InvalidStorageInputError("recovery envelope must be market_recovery")
        payload = dict(validated.payload)
        if payload.get("symbol") != symbol:
            raise InvalidStorageInputError(
                "recovery envelope symbol does not match the requested symbol"
            )
        payload_document = validated.to_dict()["payload"]
        if payload_document.get("bars") != bar_list or payload_document.get("bar_count") != len(bar_list):
            raise InvalidStorageInputError(
                "recovery bars do not match the envelope payload"
            )
        cursor_text = _instant_text(recovery_cursor) if recovery_cursor is not None else None
        if cursor_text != payload.get("recovery_cursor"):
            raise InvalidStorageInputError(
                "recovery cursor does not match the envelope payload cursor"
            )
        for previous, current in zip(bar_list, bar_list[1:]):
            if previous["path_precision"] == "minute" and current["path_precision"] == "minute":
                if current["start_at"] != previous["end_at"]:
                    raise RecoveryStateError(
                        "minute recovery bars are not contiguous: "
                        f"{previous['end_at']} != {current['start_at']}"
                    )
            elif instant_from_text(current["start_at"]) < instant_from_text(previous["end_at"]):
                raise RecoveryStateError("recovery bars are not chronological")
        document = validated.to_dict()
        envelope_json = canonical_json(document)
        with self._db.transaction() as conn:
            existing = conn.execute(
                "SELECT envelope_json FROM events WHERE event_id = ?", (document["event_id"],)
            ).fetchone()
            if existing is not None:
                if existing[0] == envelope_json:
                    return
                raise StorageConflictError(
                    "recovery envelope event_id already exists with different content"
                )
            conflict = conn.execute(
                "SELECT event_id FROM events WHERE idempotency_key = ?",
                (document["idempotency_key"],),
            ).fetchone()
            if conflict is not None:
                raise StorageConflictError(
                    "recovery idempotency_key already exists with a different event_id"
                )
            baseline = None
            cursor_row = conn.execute(
                "SELECT last_second_end_at, recovery_cursor FROM market_cursors "
                "WHERE symbol = ? AND feed_key = ?",
                (symbol, FEED_KEY),
            ).fetchone()
            if cursor_row is not None:
                baseline = cursor_row[0]
                stored_recovery = cursor_row[1]
                if stored_recovery is not None and (
                    cursor_text is None
                    or instant_from_text(cursor_text) < instant_from_text(stored_recovery)
                ):
                    raise RecoveryStateError("recovery cursor would move backwards")
            if baseline is not None and instant_from_text(bar_list[0]["start_at"]) < instant_from_text(
                baseline
            ):
                raise RecoveryStateError("first recovery bar starts before the stored second cursor")
            conn.execute(
                "INSERT OR IGNORE INTO market_cursors (symbol, feed_key, updated_at) "
                "VALUES (?, ?, ?)",
                (symbol, FEED_KEY, self._now_text()),
            )
            conn.execute(
                "UPDATE market_cursors SET recovery_cursor = ?, live_blocked = 0, "
                "pending_recovery_json = NULL, updated_at = ? "
                "WHERE symbol = ? AND feed_key = ?",
                (payload.get("recovery_cursor"), self._now_text(), symbol, FEED_KEY),
            )
            self._append_event(conn, document)

    def commit_account_event(
        self,
        strategy_id: str,
        *,
        balance_before: Decimal | str,
        balance_after: Decimal | str,
        envelope: Mapping[str, Any],
    ) -> None:
        """Update the account balance and append its event atomically."""

        before = _decimal_text(balance_before)
        after = _decimal_text(balance_after)
        validated = RuntimeEventEnvelope.from_dict(envelope)
        if validated.event_type != "account_changed":
            raise InvalidStorageInputError("account event envelope must be account_changed")
        if validated.strategy_id != strategy_id:
            raise StorageConflictError(
                "account event envelope strategy_id does not match the requested strategy"
            )
        payload = dict(validated.payload)
        if payload.get("balance_before") != before or payload.get("balance_after") != after:
            raise EnvelopeValidationError(
                "account event envelope payload does not match the balance pair"
            )
        with self._db.transaction() as conn:
            event_status = self._append_event(conn, validated.to_dict())
            if event_status == "replayed":
                return
            state_row = conn.execute(
                "SELECT generation FROM strategy_states WHERE strategy_id = ?",
                (strategy_id,),
            ).fetchone()
            if state_row is not None and state_row[0] != validated.generation:
                raise StorageConflictError(
                    "account event generation does not match the stored strategy generation"
                )
            row = conn.execute(
                "SELECT balance FROM accounts WHERE strategy_id = ?", (strategy_id,)
            ).fetchone()
            if row is None:
                raise InvalidStorageInputError(
                    f"account for strategy_id {strategy_id} is not ensured"
                )
            if row[0] != before:
                raise StorageConflictError(
                    f"account balance_before {before} does not match stored balance {row[0]}"
                )
            self._update_account(conn, strategy_id, balance=after)

    def commit_account_exhausted(
        self,
        strategy_id: str,
        *,
        balance: Decimal | str,
        reason: str,
        envelope: Mapping[str, Any],
    ) -> None:
        """Mark the account exhausted and append its event atomically."""

        balance_text = _decimal_text(balance)
        if not isinstance(reason, str) or not reason:
            raise InvalidStorageInputError("exhausted reason must be a non-empty string")
        validated = RuntimeEventEnvelope.from_dict(envelope)
        if validated.event_type != "account_exhausted":
            raise InvalidStorageInputError("exhausted envelope must be account_exhausted")
        if validated.strategy_id != strategy_id:
            raise StorageConflictError(
                "exhausted envelope strategy_id does not match the requested strategy"
            )
        payload = dict(validated.payload)
        if payload.get("balance") != balance_text or payload.get("reason") != reason:
            raise EnvelopeValidationError(
                "account_exhausted envelope payload does not match balance and reason"
            )
        with self._db.transaction() as conn:
            if (
                conn.execute("SELECT 1 FROM accounts WHERE strategy_id = ?", (strategy_id,)).fetchone()
                is None
            ):
                raise InvalidStorageInputError(f"account for strategy_id {strategy_id} is not ensured")
            event_status = self._append_event(conn, validated.to_dict())
            if event_status == "replayed":
                return
            state_row = conn.execute(
                "SELECT generation FROM strategy_states WHERE strategy_id = ?",
                (strategy_id,),
            ).fetchone()
            if state_row is not None and state_row[0] != validated.generation:
                raise StorageConflictError(
                    "account exhausted event generation does not match the stored strategy generation"
                )
            conn.execute(
                "UPDATE accounts SET balance = ?, exhausted = 1, updated_at = ? "
                "WHERE strategy_id = ?",
                (balance_text, self._now_text(), strategy_id),
            )

    def commit_fill_rejected(
        self, strategy_id: str, *, envelope: Mapping[str, Any]
    ) -> None:
        """Append one paper_fill_rejected event atomically."""

        validated = RuntimeEventEnvelope.from_dict(envelope)
        if validated.event_type != "paper_fill_rejected":
            raise InvalidStorageInputError("fill-rejected envelope must be paper_fill_rejected")
        if validated.strategy_id != strategy_id:
            raise StorageConflictError(
                "fill-rejected envelope strategy_id does not match the requested strategy"
            )
        with self._db.transaction() as conn:
            self._append_event(conn, validated.to_dict())

    def commit_schedule_set(
        self, strategy_id: str, *, envelope: Mapping[str, Any]
    ) -> None:
        """Append one schedule_set event and persist its timer atomically."""

        validated = RuntimeEventEnvelope.from_dict(envelope)
        if validated.event_type != "schedule_set":
            raise InvalidStorageInputError("schedule envelope must be schedule_set")
        if validated.strategy_id != strategy_id:
            raise StorageConflictError(
                "schedule envelope strategy_id does not match the requested strategy"
            )
        if validated.generation is None:
            raise InvalidStorageInputError("schedule envelope must carry a generation")
        payload = dict(validated.payload)
        with self._db.transaction() as conn:
            event_status = self._append_event(conn, validated.to_dict())
            if event_status == "replayed":
                return
            state_row = conn.execute(
                "SELECT generation FROM strategy_states WHERE strategy_id = ?",
                (strategy_id,),
            ).fetchone()
            if state_row is not None and state_row[0] != validated.generation:
                raise StorageConflictError(
                    "schedule generation does not match the stored strategy generation"
                )
            conn.execute(
                "INSERT INTO timers (strategy_id, kind, generation, due_at, active, fired) "
                "VALUES (?, ?, ?, ?, 1, 0) ON CONFLICT(strategy_id, kind) DO UPDATE SET "
                "generation = excluded.generation, due_at = excluded.due_at, "
                "active = 1, fired = 0",
                (strategy_id, payload["timer_kind"], validated.generation, payload["due_at"]),
            )


__all__ = [
    "ACCOUNT_ID_PREFIX",
    "IDEMPOTENCY_KEY_PREFIX",
    "INITIAL_BALANCE_TEXT",
    "PAPER_MATH_VERSION",
    "RuntimeStore",
    "account_change_key",
    "account_exhausted_key",
    "close_key",
    "decision_key",
    "envelope_for_account_change",
    "envelope_for_account_exhausted",
    "envelope_for_close",
    "envelope_for_decision",
    "envelope_for_fill",
    "envelope_for_fill_rejected",
    "envelope_for_market_recovery",
    "envelope_for_pending_entry",
    "envelope_for_schedule",
    "envelope_for_state_transition",
    "fill_key",
    "fill_rejected_key",
    "pending_key",
    "recovery_key",
    "schedule_key",
    "state_transition_key",
]
