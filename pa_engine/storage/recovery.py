"""Restart and recovery orchestration for the G027 SQLite runtime database.

This module owns the deterministic restart semantics: read every
materialized row, derive the minimal post-restart corrections and commit
them in exactly one transaction together with the linked runtime events.
State/timer mutations are atomic with the events they produce, so a
repeated restore is a no-op: overdue work fires exactly once and a lost
in-flight model call becomes one bounded retry instead of a stale replay.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable

from .database import StorageDatabase
from .errors import InvalidStorageInputError, StorageConflictError
from .schema import FEED_KEY
from .store import RuntimeStore, envelope_for_state_transition, state_transition_key
from .times import instant_from_text, instant_to_text, is_canonical_instant

RETRY_DELAYS = (5, 10, 20, 40, 60)

_RETRY_LIMIT = 5
_BLOCK_REASON = "restart_recovery_required"


@dataclass(frozen=True)
class DueWork:
    """One piece of restart-due work for one strategy."""

    strategy_id: str
    kind: str


@dataclass(frozen=True)
class RestoreSnapshot:
    """Read-mostly picture of the database after one restore."""

    run_id: str
    mode: str
    accounts: tuple[dict[str, Any], ...]
    strategy_states: tuple[dict[str, Any], ...]
    timers: tuple[dict[str, Any], ...]
    pending_entries: tuple[dict[str, Any], ...]
    trades: tuple[dict[str, Any], ...]
    cursors: tuple[dict[str, Any], ...]
    due_once: tuple[DueWork, ...]
    retry_conversions: tuple[dict[str, Any], ...]
    market_paused: tuple[str, ...]
    live_blocked: bool


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _resolve_now(now: datetime | str | None, db: StorageDatabase) -> str:
    if now is None:
        return instant_to_text(db._clock())
    if isinstance(now, datetime):
        return instant_to_text(now)
    if isinstance(now, str) and is_canonical_instant(now):
        return now
    raise InvalidStorageInputError("now must be an aware datetime or canonical UTC text")


class RuntimeRecovery:
    """Deterministic restart planner and executor for G027 materialized state."""

    def __init__(self, store: RuntimeStore, db: StorageDatabase) -> None:
        self._store = store
        self._db = db

    def ensure_enabled_accounts(self, strategy_ids: Iterable[str]) -> None:
        """Ensure one account row per enabled strategy id, idempotently."""

        self._store.ensure_accounts(strategy_ids)

    def startup_sequence(
        self, enabled_strategy_ids: Iterable[str], *, now: datetime | str | None = None
    ) -> RestoreSnapshot:
        """Ensure accounts first, then restore; exactly two transactions."""

        self.ensure_enabled_accounts(enabled_strategy_ids)
        return self.restore(now=now)

    def restore(self, *, now: datetime | str | None = None) -> RestoreSnapshot:
        """Read all rows, derive corrections and commit them atomically.

        *now* is an aware datetime or canonical UTC text; when omitted the
        database clock is used.  All mutations and their linked events land
        in exactly one transaction; on any error nothing is persisted.
        """

        now_text = _resolve_now(now, self._db)
        now_dt = instant_from_text(now_text)
        mode = self._db.lease.mode
        run_id = self._db.lease.run_id
        store = self._store

        accounts = tuple(store.list_accounts())
        state_rows = store.list_strategy_states()
        pending_rows = store.list_pending_entries()
        trade_rows = store.list_trades()
        cursor_rows = tuple(store.list_cursors())
        timers_by_strategy = {
            row["strategy_id"]: tuple(store.get_timers(row["strategy_id"]))
            for row in state_rows
        }
        pending_by_id = {row["pending_id"]: row for row in pending_rows}
        trade_by_id = {row["trade_id"]: row for row in trade_rows}
        open_trade_by_strategy = {
            row["strategy_id"]: row for row in trade_rows if row["status"] == "open"
        }

        state_writes: dict[str, dict[str, Any]] = {}
        timer_writes: list[tuple[str, dict[str, Any]]] = []
        event_documents: list[dict[str, Any]] = []
        retry_conversions: list[dict[str, Any]] = []
        due_work: list[tuple[str, str]] = []
        acted: set[str] = set()

        def transition(
            row: dict[str, Any], from_state: str, to_state: str, reason: str, discriminator: str
        ) -> dict[str, Any]:
            return envelope_for_state_transition(
                row,
                from_state=from_state,
                to_state=to_state,
                reason=reason,
                resume_state=None,
                mode=mode,
                occurred_at=now_text,
                idempotency_key=(
                    f"restart:{state_transition_key(row['strategy_id'], row['generation'], to_state, reason)}"
                    f":{discriminator}"
                ),
            )

        for row in state_rows:
            strategy_id = row["strategy_id"]
            if row["state"] != "analyzing" or row.get("in_flight_job_id") is None:
                continue
            if strategy_id in open_trade_by_strategy:
                continue
            retry_count = min(row["provider_retry_count"] + 1, _RETRY_LIMIT)
            delay = RETRY_DELAYS[retry_count - 1]
            due_at = instant_to_text(now_dt + timedelta(seconds=delay))
            state_writes[strategy_id] = {
                "state": "retry_wait",
                "intended_resume_state": None,
                "in_flight_job_id": None,
                "in_flight_generation": None,
                "provider_retry_count": retry_count,
                "next_due_at": due_at,
            }
            timer_writes.append(
                (
                    strategy_id,
                    {
                        "strategy_id": strategy_id,
                        "kind": "retry",
                        "generation": row["generation"],
                        "due_at": due_at,
                        "active": 1,
                        "fired": 0,
                    },
                )
            )
            event_documents.append(
                transition(row, "analyzing", "retry_wait", "lost_in_flight_call", row["in_flight_job_id"])
            )
            retry_conversions.append(
                {
                    "strategy_id": strategy_id,
                    "generation": row["generation"],
                    "provider_retry_count": retry_count,
                    "delay_seconds": delay,
                    "due_at": due_at,
                }
            )
            acted.add(strategy_id)

        for row in state_rows:
            strategy_id = row["strategy_id"]
            if strategy_id in acted or row["state"] not in ("sleeping", "retry_wait"):
                continue
            if strategy_id in open_trade_by_strategy:
                continue
            timer_kind = "sleep" if row["state"] == "sleeping" else "retry"
            timer = next(
                (t for t in timers_by_strategy.get(strategy_id, ()) if t["kind"] == timer_kind),
                None,
            )
            if timer is None or timer["active"] != 1 or timer["fired"] != 0:
                continue
            if instant_from_text(timer["due_at"]) > now_dt:
                continue
            state_writes[strategy_id] = {"state": "analysis_due", "next_due_at": None}
            cleared = dict(timer)
            cleared["fired"] = 1
            timer_writes.append((strategy_id, cleared))
            due_kind = "sleep_due" if row["state"] == "sleeping" else "retry_due"
            due_work.append((strategy_id, due_kind))
            event_documents.append(
                transition(row, row["state"], "analysis_due", "timer_overdue_restart", timer["due_at"])
            )
            acted.add(strategy_id)

        for row in state_rows:
            strategy_id = row["strategy_id"]
            if strategy_id in acted or row["state"] != "waiting_entry":
                continue
            pending_row = pending_by_id.get(row["pending_id"]) if row.get("pending_id") else None
            expiry_timer = next(
                (
                    t
                    for t in timers_by_strategy.get(strategy_id, ())
                    if t["kind"] == "pending_expiry"
                ),
                None,
            )
            open_trade = open_trade_by_strategy.get(strategy_id)
            if open_trade is not None:
                trade_generation = None
                trade_state_json = open_trade.get("state_json")
                if isinstance(trade_state_json, dict) and type(
                    trade_state_json.get("generation")
                ) is int:
                    trade_generation = trade_state_json["generation"]
                if expiry_timer is not None:
                    cleared = dict(expiry_timer)
                    cleared["active"] = 0
                    cleared["fired"] = 1
                    timer_writes.append((strategy_id, cleared))
                state_writes[strategy_id] = {
                    "state": "open_position",
                    "pending_id": None,
                    "open_position_id": open_trade["trade_id"],
                    "open_position_generation": trade_generation,
                }
                event_documents.append(
                    transition(
                        row,
                        "waiting_entry",
                        "open_position",
                        "open_trade_restored",
                        open_trade["trade_id"],
                    )
                )
                acted.add(strategy_id)
                continue
            if pending_row is not None and pending_row["status"] == "pending":
                if (
                    instant_from_text(pending_row["expires_at"]) <= now_dt
                    and (expiry_timer is None or (expiry_timer["active"] == 1 and expiry_timer["fired"] == 0))
                ):
                    due_work.append((strategy_id, "pending_expiry_due"))
                    if expiry_timer is None:
                        timer_writes.append(
                            (
                                strategy_id,
                                {
                                    "strategy_id": strategy_id,
                                    "kind": "pending_expiry",
                                    "generation": pending_row["generation"],
                                    "due_at": pending_row["expires_at"],
                                    "active": 1,
                                    "fired": 1,
                                },
                            )
                        )
                    else:
                        cleared = dict(expiry_timer)
                        cleared["fired"] = 1
                        timer_writes.append((strategy_id, cleared))
                continue
            state_writes[strategy_id] = {
                "state": "analysis_due",
                "next_due_at": None,
                "pending_id": None,
            }
            expiry_timer = next(
                (
                    t
                    for t in timers_by_strategy.get(strategy_id, ())
                    if t["kind"] == "pending_expiry"
                ),
                None,
            )
            if expiry_timer is not None:
                cleared = dict(expiry_timer)
                cleared["fired"] = 1
                timer_writes.append((strategy_id, cleared))
            event_documents.append(
                transition(row, "waiting_entry", "analysis_due", "pending_resolved_restart", str(row["pending_id"]))
            )
            acted.add(strategy_id)

        for row in state_rows:
            strategy_id = row["strategy_id"]
            if strategy_id in acted:
                continue
            state = row["state"]
            if state == "open_position":
                trade = trade_by_id.get(row.get("open_position_id"))
                if trade is None or trade["status"] != "open":
                    state_writes[strategy_id] = {
                        "state": "analysis_due",
                        "next_due_at": None,
                        "open_position_id": None,
                        "open_position_generation": None,
                    }
                    event_documents.append(
                        transition(row, "open_position", "analysis_due", "trade_settled_restart", str(row["open_position_id"]))
                    )
                    acted.add(strategy_id)
            elif state in ("analyzing", "analysis_due", "sleeping", "retry_wait"):
                open_trade = open_trade_by_strategy.get(strategy_id)
                if open_trade is not None:
                    trade_generation = None
                    trade_state_json = open_trade.get("state_json")
                    if isinstance(trade_state_json, dict) and type(
                        trade_state_json.get("generation")
                    ) is int:
                        trade_generation = trade_state_json["generation"]
                    state_writes[strategy_id] = {
                        "state": "open_position",
                        "open_position_id": open_trade["trade_id"],
                        "open_position_generation": trade_generation,
                    }
                    timer_kind = {"sleeping": "sleep", "retry_wait": "retry"}.get(state)
                    if timer_kind is not None:
                        timer = next(
                            (
                                timer
                                for timer in timers_by_strategy.get(strategy_id, ())
                                if timer["kind"] == timer_kind
                            ),
                            None,
                        )
                        if timer is not None:
                            cleared = dict(timer)
                            cleared["active"] = 0
                            cleared["fired"] = 1
                            timer_writes.append((strategy_id, cleared))
                    event_documents.append(
                        transition(row, state, "open_position", "open_trade_restored", open_trade["trade_id"])
                    )
                    acted.add(strategy_id)

        state_by_id = {row["strategy_id"]: row for row in state_rows}
        for strategy_id in state_writes:
            original = state_by_id[strategy_id]
            state_writes[strategy_id]["transition_count"] = original["transition_count"] + 1
            state_writes[strategy_id]["last_transition_at"] = now_text

        blocked_symbols = [
            cursor["symbol"]
            for cursor in cursor_rows
            if cursor["last_second_end_at"] is not None and cursor["live_blocked"] == 0
        ]

        with self._db.transaction() as conn:
            for symbol in blocked_symbols:
                self._mark_live_blocked(conn, symbol, now_text)
            for strategy_id, columns in state_writes.items():
                self._update_state_row(conn, strategy_id, columns)
            for strategy_id, timer in timer_writes:
                self._upsert_timer(conn, strategy_id, timer)
            for document in event_documents:
                self._append_event(conn, document, now_text)

        resolved_states = []
        for row in state_rows:
            resolved = dict(row)
            resolved.update(state_writes.get(row["strategy_id"], {}))
            resolved_states.append(resolved)

        upserted_timers: dict[str, dict[str, dict[str, Any]]] = {}
        for strategy_id, timer in timer_writes:
            upserted_timers.setdefault(strategy_id, {})[timer["kind"]] = dict(timer)
        resolved_timers = []
        for row in state_rows:
            strategy_id = row["strategy_id"]
            by_kind = {
                timer["kind"]: dict(timer) for timer in timers_by_strategy.get(strategy_id, ())
            }
            by_kind.update(upserted_timers.get(strategy_id, {}))
            resolved_timers.extend(by_kind[kind] for kind in sorted(by_kind))

        resolved_cursors = []
        for cursor in cursor_rows:
            resolved = dict(cursor)
            if cursor["last_second_end_at"] is not None and cursor["live_blocked"] == 0:
                resolved["live_blocked"] = 1
                resolved["pending_recovery_json"] = {"reason": _BLOCK_REASON}
                resolved["updated_at"] = now_text
            resolved_cursors.append(resolved)

        paused = {row["strategy_id"] for row in trade_rows if row["status"] == "open"}
        paused |= {row["strategy_id"] for row in pending_rows if row["status"] == "pending"}
        for row in state_rows:
            if row["state"] == "market_wait" and (
                row.get("pending_id") is not None or row.get("open_position_id") is not None
            ):
                paused.add(row["strategy_id"])

        return RestoreSnapshot(
            run_id=run_id,
            mode=mode,
            accounts=accounts,
            strategy_states=tuple(resolved_states),
            timers=tuple(resolved_timers),
            pending_entries=tuple(pending_rows),
            trades=tuple(trade_rows),
            cursors=tuple(resolved_cursors),
            due_once=tuple(
                DueWork(strategy_id=strategy_id, kind=kind)
                for strategy_id, kind in sorted(due_work)
            ),
            retry_conversions=tuple(retry_conversions),
            market_paused=tuple(sorted(paused)),
            live_blocked=any(
                cursor["live_blocked"] == 1 or cursor["last_second_end_at"] is not None
                for cursor in cursor_rows
            ),
        )

    def _mark_live_blocked(self, conn: Any, symbol: str, now_text: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO market_cursors (symbol, feed_key, updated_at) "
            "VALUES (?, ?, ?)",
            (symbol, FEED_KEY, now_text),
        )
        conn.execute(
            "UPDATE market_cursors SET live_blocked = 1, pending_recovery_json = ?, "
            "updated_at = ? WHERE symbol = ? AND feed_key = ?",
            (_canonical_json({"reason": _BLOCK_REASON}), now_text, symbol, FEED_KEY),
        )

    def _update_state_row(
        self, conn: Any, strategy_id: str, columns: dict[str, Any]
    ) -> None:
        sets = ", ".join(f"{key} = ?" for key in columns)
        conn.execute(
            f"UPDATE strategy_states SET {sets} WHERE strategy_id = ?",
            (*columns.values(), strategy_id),
        )

    def _upsert_timer(self, conn: Any, strategy_id: str, timer: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO timers (strategy_id, kind, generation, due_at, active, fired) "
            "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(strategy_id, kind) DO UPDATE SET "
            "generation = excluded.generation, due_at = excluded.due_at, "
            "active = excluded.active, fired = excluded.fired",
            (
                strategy_id,
                timer["kind"],
                timer["generation"],
                timer["due_at"],
                timer["active"],
                timer["fired"],
            ),
        )

    def _append_event(self, conn: Any, document: dict[str, Any], created_at: str) -> None:
        event_id = document["event_id"]
        idempotency_key = document["idempotency_key"]
        envelope_json = _canonical_json(document)
        row = conn.execute(
            "SELECT idempotency_key, envelope_json FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if row is not None:
            if row[0] == idempotency_key and row[1] == envelope_json:
                return
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
                created_at,
            ),
        )


__all__ = ["RETRY_DELAYS", "DueWork", "RestoreSnapshot", "RuntimeRecovery"]
