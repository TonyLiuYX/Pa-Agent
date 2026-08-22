"""Frozen DDL, schema identity and invariant probes for G027 persistence.

This module owns the physical layout of the single v1 SQLite runtime
database.  All instant columns carry canonical UTC text and all
money/price/quantity columns carry canonical fixed-point decimal text as
defined by :mod:`pa_engine.storage.times`; boolean flags are stored as
INTEGER 0/1.

Schema identity is pinned three ways so an incompatible or foreign file
fails explicitly instead of being silently adopted: ``PRAGMA user_version``,
``PRAGMA application_id`` and the ``meta`` row ``schema_version``.  The
:func:`verify_invariants` probes re-check the closed vocabularies and
decimal shapes of materialized rows on every open of an existing database;
any violation is :class:`~pa_engine.storage.errors.StorageCorruptionError`
and never triggers an automatic reset.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3

from pa_engine.contracts import canonical_json_bytes

from .errors import StorageCorruptionError, StorageVersionError
from .times import decimal_from_text, is_canonical_instant, is_decimal_text

SCHEMA_VERSION = 1
APPLICATION_ID = 1345139227  # = 0x502D321B; G027 v1 application identity
FEED_KEY = "alpaca:iex:single_exchange"

STRATEGY_STATES = frozenset(
    {
        "analysis_due",
        "analyzing",
        "sleeping",
        "waiting_entry",
        "open_position",
        "retry_wait",
        "market_wait",
        "blocked",
        "disabled",
        "exhausted",
    }
)
TIMER_KINDS = frozenset({"sleep", "retry", "pending_expiry"})
PENDING_STATUSES = frozenset({"pending", "filled", "expired", "cancelled"})
TRADE_STATUSES = frozenset({"open", "settled"})
EVENT_MODES = frozenset({"automatic", "replay"})
EVENT_TYPES = frozenset(
    {
        "strategy_state_changed",
        "model_decision_accepted",
        "schedule_set",
        "pending_entry_created",
        "pending_entry_expired",
        "pending_entry_cancelled",
        "paper_fill",
        "paper_fill_rejected",
        "paper_close",
        "account_changed",
        "account_exhausted",
        "market_recovery",
    }
)

_SCHEMA_STATEMENTS = (
    """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
""",
    """
CREATE TABLE IF NOT EXISTS accounts (
    strategy_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    balance TEXT NOT NULL,
    initial_balance TEXT NOT NULL,
    exhausted INTEGER NOT NULL DEFAULT 0,
    pending INTEGER NOT NULL DEFAULT 0,
    open_trade_json TEXT,
    pending_trade_json TEXT,
    paper_math_version TEXT NOT NULL DEFAULT '0.1.0',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
""",
    """
CREATE TABLE IF NOT EXISTS strategy_states (
    strategy_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    exhausted INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    intended_resume_state TEXT,
    in_flight_job_id TEXT,
    in_flight_generation INTEGER,
    pending_id TEXT,
    open_position_id TEXT,
    open_position_generation INTEGER,
    provider_retry_count INTEGER NOT NULL DEFAULT 0,
    semantic_retry_profile_hash TEXT,
    semantic_retry_snapshot_id TEXT,
    semantic_retry_count INTEGER NOT NULL DEFAULT 0,
    rejected_immediate_view_id TEXT,
    current_view_id TEXT,
    last_closed_snapshot_id TEXT,
    transition_count INTEGER NOT NULL DEFAULT 0,
    last_transition_at TEXT,
    blocked_reason TEXT,
    decision_accepted_at TEXT,
    next_due_at TEXT,
    expires_at TEXT,
    profile_ref_json TEXT,
    packet_json TEXT,
    latest_instruction_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
""",
    """
CREATE TABLE IF NOT EXISTS timers (
    strategy_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    generation INTEGER NOT NULL,
    due_at TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    fired INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (strategy_id, kind)
)
""",
    """
CREATE TABLE IF NOT EXISTS pending_entries (
    pending_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    status TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_price TEXT NOT NULL,
    stop_price TEXT NOT NULL,
    target_price TEXT NOT NULL,
    decision_accepted_at TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    resolution_at TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    state_hash TEXT NOT NULL,
    state_json TEXT NOT NULL,
    flags_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
""",
    """
CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    status TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    quantity TEXT NOT NULL,
    entry_price TEXT NOT NULL,
    stop_price TEXT NOT NULL,
    target_price TEXT NOT NULL,
    planned_r TEXT NOT NULL,
    trade_json TEXT NOT NULL,
    settlement_json TEXT,
    exit_reason TEXT,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    episode_id TEXT,
    state_json TEXT,
    state_hash TEXT,
    flags_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
""",
    """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_hash TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    mode TEXT NOT NULL,
    strategy_id TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    envelope_json TEXT NOT NULL,
    created_at TEXT NOT NULL
)
""",
    """
CREATE INDEX IF NOT EXISTS events_strategy_idx ON events(strategy_id, occurred_at)
""",
    """
CREATE INDEX IF NOT EXISTS events_type_idx ON events(event_type)
""",
    """
CREATE TABLE IF NOT EXISTS market_cursors (
    symbol TEXT NOT NULL,
    feed_key TEXT NOT NULL,
    last_second_event_id TEXT,
    last_second_event_hash TEXT,
    last_second_end_at TEXT,
    recovery_cursor TEXT,
    live_blocked INTEGER NOT NULL DEFAULT 0,
    pending_recovery_json TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, feed_key)
)
""",
    """
CREATE TABLE IF NOT EXISTS lease (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    run_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    ttl_seconds INTEGER NOT NULL
)
""",
)

SCHEMA_DDL = ";\n\n".join(statement.strip() for statement in _SCHEMA_STATEMENTS) + ";\n"

EXPECTED_TABLES = frozenset(
    {
        "meta",
        "accounts",
        "strategy_states",
        "timers",
        "pending_entries",
        "trades",
        "events",
        "market_cursors",
        "lease",
    }
)

EXPECTED_COLUMNS = {
    "meta": {"key", "value"},
    "accounts": {
        "strategy_id", "account_id", "balance", "initial_balance", "exhausted", "pending",
        "open_trade_json", "pending_trade_json", "paper_math_version", "created_at", "updated_at",
    },
    "strategy_states": {
        "strategy_id", "state", "enabled", "exhausted", "generation", "intended_resume_state",
        "in_flight_job_id", "in_flight_generation", "pending_id", "open_position_id",
        "open_position_generation", "provider_retry_count", "semantic_retry_profile_hash",
        "semantic_retry_snapshot_id", "semantic_retry_count", "rejected_immediate_view_id",
        "current_view_id", "last_closed_snapshot_id", "transition_count", "last_transition_at",
        "blocked_reason", "decision_accepted_at", "next_due_at", "expires_at", "profile_ref_json",
        "packet_json", "latest_instruction_json", "created_at", "updated_at",
    },
    "timers": {"strategy_id", "kind", "generation", "due_at", "active", "fired"},
    "pending_entries": {
        "pending_id", "strategy_id", "generation", "status", "symbol", "direction", "entry_price",
        "stop_price", "target_price", "decision_accepted_at", "submitted_at", "expires_at",
        "resolution_at", "episode_id", "state_hash", "state_json", "flags_json", "created_at",
        "updated_at",
    },
    "trades": {
        "trade_id", "strategy_id", "status", "symbol", "direction", "quantity", "entry_price",
        "stop_price", "target_price", "planned_r", "trade_json", "settlement_json", "exit_reason",
        "opened_at", "closed_at", "episode_id", "state_json", "state_hash", "flags_json",
        "created_at", "updated_at",
    },
    "events": {
        "event_id", "event_hash", "event_type", "occurred_at", "mode", "strategy_id",
        "idempotency_key", "envelope_json", "created_at",
    },
    "market_cursors": {
        "symbol", "feed_key", "last_second_event_id", "last_second_event_hash", "last_second_end_at",
        "recovery_cursor", "live_blocked", "pending_recovery_json", "updated_at",
    },
    "lease": {"id", "run_id", "mode", "acquired_at", "heartbeat_at", "ttl_seconds"},
}

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DIRECTIONS = frozenset({"long", "short"})


def _json_value(value: str, *, table: str, column: str) -> object:
    try:
        parsed = json.loads(value, parse_constant=lambda token: (_ for _ in ()).throw(ValueError(token)))
    except (TypeError, ValueError, json.JSONDecodeError):
        raise StorageCorruptionError(f"{table}.{column} is not valid strict JSON") from None
    return parsed


def _check_json(value: str | None, *, table: str, column: str, optional: bool = True) -> None:
    if value is None:
        if optional:
            return
        raise StorageCorruptionError(f"{table}.{column} is NULL")
    parsed = _json_value(value, table=table, column=column)
    try:
        from .envelope import assert_durable_json_safe

        assert_durable_json_safe(parsed)
    except Exception as exc:
        raise StorageCorruptionError(f"{table}.{column} violates the durable JSON boundary") from exc


def _check_instant(value: str | None, *, table: str, column: str, optional: bool = True) -> None:
    if value is None:
        if optional:
            return
        raise StorageCorruptionError(f"{table}.{column} is NULL")
    if not is_canonical_instant(value):
        raise StorageCorruptionError(f"{table}.{column} is not canonical UTC text")


def _check_decimal(value: str, *, table: str, column: str, nonnegative: bool = False) -> None:
    if not is_decimal_text(value):
        raise StorageCorruptionError(f"{table}.{column} is not canonical decimal text")
    if nonnegative and decimal_from_text(value) < 0:
        raise StorageCorruptionError(f"{table}.{column} is negative")


def _check_flag(value: object, *, table: str, column: str) -> None:
    if type(value) is not int or value not in (0, 1):
        raise StorageCorruptionError(f"{table}.{column} is not an INTEGER 0/1 flag")


def _check_nonnegative_int(value: object, *, table: str, column: str) -> None:
    if type(value) is not int or value < 0:
        raise StorageCorruptionError(f"{table}.{column} is not a non-negative integer")


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all v1 tables/indexes and pin the schema identity.

    The caller owns the surrounding transaction.  ``PRAGMA user_version``
    and ``PRAGMA application_id`` participate in that transaction, so a
    failed bootstrap leaves no half-created database behind.
    """

    for statement in _SCHEMA_STATEMENTS:
        conn.execute(statement)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )


def verify_schema(conn: sqlite3.Connection) -> None:
    """Check schema identity and table presence on an existing database.

    Raises :class:`StorageVersionError` when ``PRAGMA application_id``,
    ``PRAGMA user_version`` or the ``meta`` schema_version row does not
    match this version, and :class:`StorageCorruptionError` when any
    expected table is missing.
    """

    application_id = conn.execute("PRAGMA application_id").fetchone()[0]
    if application_id != APPLICATION_ID:
        raise StorageVersionError(
            f"application_id {application_id} is not {APPLICATION_ID}"
        )
    user_version = conn.execute("PRAGMA user_version").fetchone()[0]
    if user_version != SCHEMA_VERSION:
        raise StorageVersionError(
            f"user_version {user_version} is not {SCHEMA_VERSION}"
        )
    present = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    missing = sorted(EXPECTED_TABLES - present)
    if missing:
        raise StorageCorruptionError(f"missing tables: {', '.join(missing)}")
    for table, expected_columns in EXPECTED_COLUMNS.items():
        columns = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info({table})")
        }
        missing_columns = sorted(expected_columns - columns)
        if missing_columns:
            raise StorageCorruptionError(
                f"missing columns in {table}: {', '.join(missing_columns)}"
            )
    meta_row = conn.execute(
        "SELECT value FROM meta WHERE key = 'schema_version'"
    ).fetchone()
    if meta_row is None or meta_row[0] != str(SCHEMA_VERSION):
        raise StorageVersionError("meta schema_version is missing or mismatched")


def verify_invariants(conn: sqlite3.Connection) -> None:
    """Re-check physical row shapes, identities and closed vocabularies.

    Existing files are untrusted input.  The probes deliberately happen before
    lease acquisition, and a failure is reported as corruption rather than
    allowing a malformed row to reach the runtime hydrator.
    """

    def check_symbol(value: object, *, table: str, column: str) -> None:
        if (
            not isinstance(value, str)
            or not value
            or value != value.upper()
            or any(character.isspace() for character in value)
        ):
            raise StorageCorruptionError(f"{table}.{column} is not an upper-case symbol")

    def check_hash(value: object, *, table: str, column: str) -> None:
        if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
            raise StorageCorruptionError(f"{table}.{column} is not lowercase SHA-256")

    def parsed_json(value: str | None, *, table: str, column: str, optional: bool = True) -> object:
        _check_json(value, table=table, column=column, optional=optional)
        return None if value is None else _json_value(value, table=table, column=column)

    for row in conn.execute(
        "SELECT strategy_id, account_id, balance, initial_balance, exhausted, pending, "
        "open_trade_json, pending_trade_json, paper_math_version, created_at, updated_at "
        "FROM accounts"
    ):
        strategy_id, account_id, balance, initial_balance, exhausted, pending, open_json, pending_json, math_version, created_at, updated_at = row
        if not isinstance(strategy_id, str) or not strategy_id:
            raise StorageCorruptionError("accounts.strategy_id is empty")
        if account_id != "paper:" + strategy_id:
            raise StorageCorruptionError(f"accounts.account_id for strategy_id={strategy_id} is not bound")
        for column, value in (("balance", balance), ("initial_balance", initial_balance)):
            _check_decimal(value, table="accounts", column=column, nonnegative=True)
        for column, value in (("exhausted", exhausted), ("pending", pending)):
            _check_flag(value, table="accounts", column=column)
        if math_version != "0.1.0":
            raise StorageCorruptionError(f"accounts.paper_math_version for strategy_id={strategy_id} is unsupported")
        _check_instant(created_at, table="accounts", column="created_at", optional=False)
        _check_instant(updated_at, table="accounts", column="updated_at", optional=False)
        open_value = parsed_json(open_json, table="accounts", column="open_trade_json")
        pending_value = parsed_json(pending_json, table="accounts", column="pending_trade_json")
        if open_value is not None and not isinstance(open_value, dict):
            raise StorageCorruptionError(
                f"accounts.open_trade_json for strategy_id={strategy_id} is not an object"
            )
        if pending_value is not None and not isinstance(pending_value, dict):
            raise StorageCorruptionError(
                f"accounts.pending_trade_json for strategy_id={strategy_id} is not an object"
            )

    for row in conn.execute(
        "SELECT strategy_id, state, enabled, exhausted, generation, intended_resume_state, "
        "in_flight_generation, open_position_generation, provider_retry_count, "
        "semantic_retry_count, transition_count, last_transition_at, decision_accepted_at, "
        "next_due_at, expires_at, profile_ref_json, packet_json, latest_instruction_json, "
        "created_at, updated_at FROM strategy_states"
    ):
        (
            strategy_id, state, enabled, exhausted, generation, resume_state,
            in_flight_generation, open_position_generation, provider_retry_count,
            semantic_retry_count, transition_count, last_transition_at,
            decision_accepted_at, next_due_at, expires_at, profile_json,
            packet_json, instruction_json, created_at, updated_at,
        ) = row
        if not isinstance(strategy_id, str) or not strategy_id:
            raise StorageCorruptionError("strategy_states.strategy_id is empty")
        if state not in STRATEGY_STATES:
            raise StorageCorruptionError(
                f"strategy_states.state for strategy_id={strategy_id} is not in the closed vocabulary"
            )
        if resume_state is not None and resume_state not in STRATEGY_STATES:
            raise StorageCorruptionError(
                f"strategy_states.intended_resume_state for strategy_id={strategy_id} is not in the closed vocabulary"
            )
        for column, value in (("enabled", enabled), ("exhausted", exhausted)):
            _check_flag(value, table="strategy_states", column=column)
        for column, value in (
            ("generation", generation),
            ("provider_retry_count", provider_retry_count),
            ("semantic_retry_count", semantic_retry_count),
            ("transition_count", transition_count),
        ):
            _check_nonnegative_int(value, table="strategy_states", column=column)
        for column, value in (("in_flight_generation", in_flight_generation), ("open_position_generation", open_position_generation)):
            if value is not None:
                _check_nonnegative_int(value, table="strategy_states", column=column)
        for column, value in (
            ("last_transition_at", last_transition_at),
            ("decision_accepted_at", decision_accepted_at),
            ("next_due_at", next_due_at),
            ("expires_at", expires_at),
            ("created_at", created_at),
            ("updated_at", updated_at),
        ):
            _check_instant(value, table="strategy_states", column=column, optional=column not in {"created_at", "updated_at"})
        profile = parsed_json(profile_json, table="strategy_states", column="profile_ref_json")
        if profile is not None:
            if not isinstance(profile, dict) or not isinstance(profile.get("profile_ref_id"), str) or not isinstance(profile.get("profile_ref_hash"), str):
                raise StorageCorruptionError(f"strategy_states.profile_ref_json for strategy_id={strategy_id} is malformed")
            check_hash(profile["profile_ref_hash"], table="strategy_states", column="profile_ref_hash")
            if profile["profile_ref_id"] != "spr_" + profile["profile_ref_hash"]:
                raise StorageCorruptionError(f"strategy_states.profile_ref_json for strategy_id={strategy_id} is not hash-bound")
            if profile.get("strategy_id") is not None and profile["strategy_id"] != strategy_id:
                raise StorageCorruptionError(f"strategy_states.profile_ref_json for strategy_id={strategy_id} crosses strategy lineage")
        packet = parsed_json(packet_json, table="strategy_states", column="packet_json")
        instruction = parsed_json(
            instruction_json, table="strategy_states", column="latest_instruction_json"
        )
        if packet is not None and not isinstance(packet, dict):
            raise StorageCorruptionError(
                f"strategy_states.packet_json for strategy_id={strategy_id} is not an object"
            )
        if instruction is not None and not isinstance(instruction, dict):
            raise StorageCorruptionError(
                f"strategy_states.latest_instruction_json for strategy_id={strategy_id} is not an object"
            )

    for row in conn.execute("SELECT strategy_id, kind, generation, due_at, active, fired FROM timers"):
        strategy_id, kind, generation, due_at, active, fired = row
        if kind not in TIMER_KINDS:
            raise StorageCorruptionError(f"timers.kind for strategy_id={strategy_id} is not in the closed vocabulary")
        _check_nonnegative_int(generation, table="timers", column="generation")
        _check_instant(due_at, table="timers", column="due_at", optional=False)
        _check_flag(active, table="timers", column="active")
        _check_flag(fired, table="timers", column="fired")

    for row in conn.execute(
        "SELECT pending_id, strategy_id, generation, status, symbol, direction, entry_price, "
        "stop_price, target_price, decision_accepted_at, submitted_at, expires_at, resolution_at, "
        "episode_id, state_hash, state_json, flags_json, created_at, updated_at FROM pending_entries"
    ):
        (
            pending_id, strategy_id, generation, status, symbol, direction,
            entry_price, stop_price, target_price, decision_accepted_at,
            submitted_at, expires_at, resolution_at, episode_id, state_hash,
            state_json, flags_json, created_at, updated_at,
        ) = row
        if not isinstance(pending_id, str) or not pending_id or not isinstance(strategy_id, str) or not strategy_id or not isinstance(episode_id, str) or not episode_id:
            raise StorageCorruptionError("pending_entries lineage identifiers are malformed")
        if status not in PENDING_STATUSES:
            raise StorageCorruptionError(f"pending_entries.status for pending_id={pending_id} is not in the closed vocabulary")
        check_symbol(symbol, table="pending_entries", column="symbol")
        if direction not in _DIRECTIONS:
            raise StorageCorruptionError(f"pending_entries.direction for pending_id={pending_id} is invalid")
        _check_nonnegative_int(generation, table="pending_entries", column="generation")
        for column, value in (("entry_price", entry_price), ("stop_price", stop_price), ("target_price", target_price)):
            _check_decimal(value, table="pending_entries", column=column)
        for column, value in (("decision_accepted_at", decision_accepted_at), ("submitted_at", submitted_at), ("expires_at", expires_at), ("resolution_at", resolution_at), ("created_at", created_at), ("updated_at", updated_at)):
            _check_instant(value, table="pending_entries", column=column, optional=False)
        check_hash(state_hash, table="pending_entries", column="state_hash")
        state_value = parsed_json(state_json, table="pending_entries", column="state_json", optional=False)
        if not isinstance(state_value, dict):
            raise StorageCorruptionError(f"pending_entries.state_json for pending_id={pending_id} is not an object")
        for key, expected in (
            ("pending_id", pending_id),
            ("strategy_id", strategy_id),
            ("generation", generation),
            ("status", status),
            ("symbol", symbol),
            ("direction", direction),
        ):
            if state_value.get(key) != expected:
                raise StorageCorruptionError(
                    f"pending_entries.state_json for pending_id={pending_id} disagrees with {key}"
                )
        flags_value = parsed_json(flags_json, table="pending_entries", column="flags_json")
        if flags_value is not None and not isinstance(flags_value, dict):
            raise StorageCorruptionError(f"pending_entries.flags_json for pending_id={pending_id} is not an object")
        if hashlib.sha256(canonical_json_bytes(state_value)).hexdigest() != state_hash:
            raise StorageCorruptionError(f"pending_entries.state_hash for pending_id={pending_id} does not match state_json")

    for row in conn.execute(
        "SELECT trade_id, strategy_id, status, symbol, direction, quantity, entry_price, stop_price, "
        "target_price, planned_r, trade_json, settlement_json, opened_at, closed_at, episode_id, "
        "state_json, state_hash, flags_json, created_at, updated_at FROM trades"
    ):
        (
            trade_id, strategy_id, status, symbol, direction, quantity,
            entry_price, stop_price, target_price, planned_r, trade_json,
            settlement_json, opened_at, closed_at, episode_id, state_json,
            state_hash, flags_json, created_at, updated_at,
        ) = row
        if not isinstance(trade_id, str) or not trade_id or not isinstance(strategy_id, str) or not strategy_id:
            raise StorageCorruptionError("trades lineage identifiers are malformed")
        if status not in TRADE_STATUSES:
            raise StorageCorruptionError(f"trades.status for trade_id={trade_id} is not in the closed vocabulary")
        check_symbol(symbol, table="trades", column="symbol")
        if direction not in _DIRECTIONS:
            raise StorageCorruptionError(f"trades.direction for trade_id={trade_id} is invalid")
        for column, value in (("quantity", quantity), ("entry_price", entry_price), ("stop_price", stop_price), ("target_price", target_price), ("planned_r", planned_r)):
            _check_decimal(value, table="trades", column=column)
        _check_instant(opened_at, table="trades", column="opened_at", optional=False)
        _check_instant(closed_at, table="trades", column="closed_at")
        _check_instant(created_at, table="trades", column="created_at", optional=False)
        _check_instant(updated_at, table="trades", column="updated_at", optional=False)
        if episode_id is not None and (not isinstance(episode_id, str) or not episode_id):
            raise StorageCorruptionError(f"trades.episode_id for trade_id={trade_id} is malformed")
        trade_value = parsed_json(trade_json, table="trades", column="trade_json", optional=False)
        settlement_value = parsed_json(settlement_json, table="trades", column="settlement_json")
        if not isinstance(trade_value, dict):
            raise StorageCorruptionError(f"trades.trade_json for trade_id={trade_id} is not an object")
        if settlement_value is not None and not isinstance(settlement_value, dict):
            raise StorageCorruptionError(f"trades.settlement_json for trade_id={trade_id} is not an object")
        state_value = parsed_json(state_json, table="trades", column="state_json")
        if state_value is not None and not isinstance(state_value, dict):
            raise StorageCorruptionError(f"trades.state_json for trade_id={trade_id} is not an object")
        if state_value is not None:
            expected_statuses = {"open"} if status == "open" else {"settled", "exhausted"}
            for key, expected in (
                ("trade_id", trade_id),
                ("strategy_id", strategy_id),
                ("symbol", symbol),
                ("direction", direction),
            ):
                if state_value.get(key) != expected:
                    raise StorageCorruptionError(
                        f"trades.state_json for trade_id={trade_id} disagrees with {key}"
                    )
            if state_value.get("status") not in expected_statuses:
                raise StorageCorruptionError(
                    f"trades.state_json for trade_id={trade_id} disagrees with status"
                )
        if state_hash is not None:
            check_hash(state_hash, table="trades", column="state_hash")
            if state_value is None or hashlib.sha256(canonical_json_bytes(state_value)).hexdigest() != state_hash:
                raise StorageCorruptionError(f"trades.state_hash for trade_id={trade_id} does not match state_json")
        flags_value = parsed_json(flags_json, table="trades", column="flags_json")
        if flags_value is not None and not isinstance(flags_value, dict):
            raise StorageCorruptionError(f"trades.flags_json for trade_id={trade_id} is not an object")

    for row in conn.execute(
        "SELECT event_id, event_hash, event_type, occurred_at, mode, strategy_id, idempotency_key, envelope_json, created_at FROM events"
    ):
        event_id, event_hash, event_type, occurred_at, mode, strategy_id, idempotency_key, envelope_json, created_at = row
        if event_type not in EVENT_TYPES:
            raise StorageCorruptionError(f"events.event_type for event_id={event_id} is not in the closed vocabulary")
        if mode not in EVENT_MODES:
            raise StorageCorruptionError(f"events.mode for event_id={event_id} is not in the closed vocabulary")
        check_hash(event_hash, table="events", column="event_hash")
        if not isinstance(event_id, str) or event_id != "rev_" + event_hash:
            raise StorageCorruptionError(f"events.event_id for event_id={event_id} is not hash-bound")
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise StorageCorruptionError(f"events.idempotency_key for event_id={event_id} is empty")
        _check_instant(occurred_at, table="events", column="occurred_at", optional=False)
        _check_instant(created_at, table="events", column="created_at", optional=False)
        document = parsed_json(envelope_json, table="events", column="envelope_json", optional=False)
        if not isinstance(document, dict):
            raise StorageCorruptionError(f"events.envelope_json for event_id={event_id} is not an object")
        try:
            from .envelope import RuntimeEventEnvelope

            envelope = RuntimeEventEnvelope.from_dict(document)
        except Exception as exc:
            raise StorageCorruptionError(f"events.envelope_json for event_id={event_id} failed validation") from exc
        if envelope.event_id != event_id or envelope.event_hash != event_hash or envelope.event_type != event_type or envelope.occurred_at != occurred_at or envelope.mode != mode or envelope.strategy_id != strategy_id or envelope.idempotency_key != idempotency_key:
            raise StorageCorruptionError(f"events row {event_id} disagrees with its envelope")

    for row in conn.execute(
        "SELECT symbol, feed_key, last_second_event_id, last_second_event_hash, last_second_end_at, recovery_cursor, live_blocked, pending_recovery_json, updated_at FROM market_cursors"
    ):
        symbol, feed_key, event_id, event_hash, end_at, recovery_cursor, live_blocked, pending_json, updated_at = row
        check_symbol(symbol, table="market_cursors", column="symbol")
        if feed_key != FEED_KEY:
            raise StorageCorruptionError(f"market_cursors.feed_key for symbol={symbol} is not the frozen feed")
        _check_flag(live_blocked, table="market_cursors", column="live_blocked")
        _check_instant(end_at, table="market_cursors", column="last_second_end_at")
        _check_instant(recovery_cursor, table="market_cursors", column="recovery_cursor")
        _check_instant(updated_at, table="market_cursors", column="updated_at", optional=False)
        if event_id is None and event_hash is not None or event_id is not None and event_hash is None:
            raise StorageCorruptionError(f"market_cursors second identity for symbol={symbol} is incomplete")
        if event_id is not None and (not isinstance(event_id, str) or not re.fullmatch(r"sec_[0-9a-f]{64}", event_id)):
            raise StorageCorruptionError(f"market_cursors.last_second_event_id for symbol={symbol} is malformed")
        if event_hash is not None:
            check_hash(event_hash, table="market_cursors", column="last_second_event_hash")
        pending_recovery = parsed_json(
            pending_json, table="market_cursors", column="pending_recovery_json"
        )
        if pending_recovery is not None and not isinstance(pending_recovery, dict):
            raise StorageCorruptionError(
                f"market_cursors.pending_recovery_json for symbol={symbol} is not an object"
            )

    for row in conn.execute("SELECT id, run_id, mode, acquired_at, heartbeat_at, ttl_seconds FROM lease"):
        lease_id, run_id, mode, acquired_at, heartbeat_at, ttl_seconds = row
        if lease_id != 1 or not isinstance(run_id, str) or not run_id or mode not in EVENT_MODES:
            raise StorageCorruptionError("lease identity or mode is malformed")
        _check_instant(acquired_at, table="lease", column="acquired_at", optional=False)
        _check_instant(heartbeat_at, table="lease", column="heartbeat_at", optional=False)
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 86400:
            raise StorageCorruptionError("lease.ttl_seconds is outside the supported range")


__all__ = [
    "APPLICATION_ID",
    "EVENT_MODES",
    "EVENT_TYPES",
    "EXPECTED_COLUMNS",
    "EXPECTED_TABLES",
    "FEED_KEY",
    "PENDING_STATUSES",
    "SCHEMA_DDL",
    "SCHEMA_VERSION",
    "STRATEGY_STATES",
    "TIMER_KINDS",
    "TRADE_STATUSES",
    "create_schema",
    "verify_invariants",
    "verify_schema",
]
