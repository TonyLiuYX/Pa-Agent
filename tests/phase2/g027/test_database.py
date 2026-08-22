"""Focused tests for the G027 storage scaffold: open/verify/lease/times."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pa_engine.storage import (
    InvalidStorageInputError,
    LeaseConflictError,
    StorageClosedError,
    StorageCorruptionError,
    StorageVersionError,
    decimal_from_text,
    decimal_to_text,
    instant_from_text,
    instant_to_text,
    is_canonical_instant,
    is_decimal_text,
    open_database,
)
from pa_engine.storage.schema import (
    APPLICATION_ID,
    SCHEMA_VERSION,
    create_schema,
    verify_invariants,
    verify_schema,
)
from pa_engine.storage.store import RuntimeStore

UTC = timezone.utc
INSTANT = "2026-08-12T14:00:00Z"


class FakeClock:
    def __init__(self, start: str = INSTANT) -> None:
        self._now = instant_from_text(start)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: int = 1) -> None:
        self._now = self._now + timedelta(seconds=seconds)


def open_handle(path: str, run_id: str = "run_db", clock: FakeClock | None = None, mode: str = "automatic", ttl: int = 30):
    return open_database(path, run_id=run_id, mode=mode, lease_ttl_seconds=ttl, clock=clock or FakeClock())


class TestBootstrapAndVerify:
    def test_fresh_database_gets_full_schema_and_identity(self, tmp_path):
        path = str(tmp_path / "fresh.db")
        handle = open_handle(path)
        tables = {row[0] for row in handle.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert tables == {
            "meta", "accounts", "strategy_states", "timers", "pending_entries",
            "trades", "events", "market_cursors", "lease",
        }
        assert handle.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert handle.conn.execute("PRAGMA application_id").fetchone()[0] == APPLICATION_ID
        assert handle.conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        meta = dict(handle.conn.execute("SELECT key, value FROM meta").fetchall())
        assert meta["schema_version"] == str(SCHEMA_VERSION)
        assert handle.lease.run_id == "run_db"
        assert handle.lease.mode == "automatic"
        handle.close()

    def test_existing_database_reopens_cleanly(self, tmp_path):
        path = str(tmp_path / "reopen.db")
        handle = open_handle(path)
        handle.close()
        reopened = open_handle(path, run_id="run_db_2")
        assert reopened.lease.run_id == "run_db_2"
        reopened.close()

    def test_zero_length_existing_file_fails_without_bootstrap(self, tmp_path):
        path = str(tmp_path / "empty.db")
        path_dir = tmp_path / "empty.db"
        path_dir.write_bytes(b"")
        with pytest.raises(StorageCorruptionError):
            open_handle(path)
        assert path_dir.read_bytes() == b""

    def test_missing_parent_directory_is_created(self, tmp_path):
        path = str(tmp_path / "nested" / "dir" / "run.db")
        handle = open_handle(path)
        assert os.path.exists(path)
        handle.close()

    def test_garbage_file_fails_corruption_without_reset(self, tmp_path):
        path = str(tmp_path / "garbage.db")
        with open(path, "wb") as stream:
            stream.write(b"this is not a sqlite database at all")
        with pytest.raises(StorageCorruptionError):
            open_handle(path)
        with open(path, "rb") as stream:
            assert stream.read().startswith(b"this is not a sqlite database")

    def test_wrong_user_version_is_incompatible(self, tmp_path):
        path = str(tmp_path / "version.db")
        handle = open_handle(path)
        handle.conn.execute("PRAGMA user_version = 99")
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageVersionError):
            open_handle(path)

    def test_wrong_application_id_is_incompatible(self, tmp_path):
        path = str(tmp_path / "appid.db")
        handle = open_handle(path)
        handle.conn.execute("PRAGMA application_id = 424242")
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageVersionError):
            open_handle(path)

    def test_missing_table_is_corruption(self, tmp_path):
        path = str(tmp_path / "missing.db")
        handle = open_handle(path)
        handle.conn.execute("DROP TABLE trades")
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_bad_meta_schema_version_is_incompatible(self, tmp_path):
        path = str(tmp_path / "meta.db")
        handle = open_handle(path)
        handle.conn.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageVersionError):
            open_handle(path)


class TestInvariantProbes:
    def _bootstrap(self, tmp_path):
        path = str(tmp_path / "inv.db")
        handle = open_handle(path)
        return path, handle

    def test_balance_not_canonical_decimal_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO accounts (strategy_id, account_id, balance, initial_balance, created_at, updated_at) "
            "VALUES ('pa:x', 'paper:pa:x', 'banana', '1000.00', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_negative_balance_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO accounts (strategy_id, account_id, balance, initial_balance, created_at, updated_at) "
            "VALUES ('pa:x', 'paper:pa:x', '-1.00', '1000.00', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_unknown_strategy_state_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO strategy_states (strategy_id, state, enabled, exhausted, generation, created_at, updated_at) "
            "VALUES ('pa:x', 'quantum_superposition', 1, 0, 0, '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_unknown_resume_state_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO strategy_states (strategy_id, state, enabled, exhausted, generation, intended_resume_state, created_at, updated_at) "
            "VALUES ('pa:x', 'market_wait', 1, 0, 0, 'teleporting', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_unknown_timer_kind_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO timers (strategy_id, kind, generation, due_at) "
            "VALUES ('pa:x', 'alarm', 0, '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_unknown_pending_status_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO pending_entries (pending_id, strategy_id, generation, status, symbol, direction, entry_price, stop_price, target_price, decision_accepted_at, submitted_at, expires_at, resolution_at, episode_id, state_hash, state_json, created_at, updated_at) "
            "VALUES ('pe_1', 'pa:x', 0, 'maybe', 'SPY', 'long', '100', '99', '102', "
            "'2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z', '2026-08-12T00:00:05Z', '2026-08-12T00:00:05Z', "
            "'ep_1', '" + "a" * 64 + "', '{}', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_unknown_trade_status_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO trades (trade_id, strategy_id, status, symbol, direction, quantity, entry_price, stop_price, target_price, planned_r, trade_json, opened_at, created_at, updated_at) "
            "VALUES ('pt_1', 'pa:x', 'margined', 'SPY', 'long', '1', '100', '99', '102', '1', '{}', "
            "'2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_settled_trade_status_is_valid_vocabulary(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO trades (trade_id, strategy_id, status, symbol, direction, quantity, entry_price, stop_price, target_price, planned_r, trade_json, opened_at, created_at, updated_at) "
            "VALUES ('pt_1', 'pa:x', 'settled', 'SPY', 'long', '1', '100', '99', '102', '1', '{}', "
            "'2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        reopened = open_handle(path)
        reopened.close()

    def test_unknown_event_type_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO events (event_id, event_hash, event_type, occurred_at, mode, strategy_id, idempotency_key, envelope_json, created_at) "
            "VALUES ('rev_x', '" + "b" * 64 + "', 'spontaneous_trade', '2026-08-12T00:00:00Z', 'automatic', 'pa:x', 'k', '{}', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_unknown_event_mode_is_corruption(self, tmp_path):
        path, handle = self._bootstrap(tmp_path)
        handle.conn.execute(
            "INSERT INTO events (event_id, event_hash, event_type, occurred_at, mode, strategy_id, idempotency_key, envelope_json, created_at) "
            "VALUES ('rev_x', '" + "b" * 64 + "', 'account_changed', '2026-08-12T00:00:00Z', 'live_money', 'pa:x', 'k', '{}', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_corruption_never_recreates_fresh_state(self, tmp_path):
        path = str(tmp_path / "preserve.db")
        handle = open_handle(path)
        handle.conn.execute(
            "INSERT INTO accounts (strategy_id, account_id, balance, initial_balance, created_at, updated_at) "
            "VALUES ('pa:x', 'paper:pa:x', '1234.56', '1000.00', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        handle.conn.execute("UPDATE accounts SET balance = 'banana' WHERE strategy_id = 'pa:x'")
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)
        raw = sqlite3.connect(path)
        row = raw.execute("SELECT balance FROM accounts WHERE strategy_id = 'pa:x'").fetchone()
        raw.close()
        assert row is not None and row[0] == "banana"

    def test_materialized_secret_tampering_fails_before_lease(self, tmp_path):
        path = str(tmp_path / "secret-row.db")
        handle = open_handle(path)
        RuntimeStore(handle).ensure_accounts(["pa:x"])
        handle.conn.execute(
            "UPDATE accounts SET open_trade_json = ? WHERE strategy_id = 'pa:x'",
            ('{"nested":{"provider_raw_payload":{"body":"x"}}}',),
        )
        handle.conn.commit()
        handle.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)

    def test_missing_schema_column_fails_explicitly(self, tmp_path):
        path = str(tmp_path / "missing-column.db")
        handle = open_handle(path)
        handle.close()
        raw = sqlite3.connect(path)
        raw.execute("ALTER TABLE accounts DROP COLUMN pending_trade_json")
        raw.commit()
        raw.close()
        with pytest.raises(StorageCorruptionError):
            open_handle(path)


class TestLease:
    def test_second_live_handle_is_rejected(self, tmp_path):
        path = str(tmp_path / "lease1.db")
        first = open_handle(path)
        with pytest.raises(LeaseConflictError):
            open_handle(path, run_id="run_other")
        first.close()

    def test_duplicate_same_run_id_handle_is_rejected(self, tmp_path):
        path = str(tmp_path / "lease2.db")
        first = open_handle(path)
        with pytest.raises(LeaseConflictError):
            open_handle(path, run_id="run_db")
        first.close()

    def test_close_releases_lease_for_next_run(self, tmp_path):
        path = str(tmp_path / "lease3.db")
        first = open_handle(path)
        first.close()
        second = open_handle(path, run_id="run_next")
        second.close()

    def test_stale_lease_is_replaced_after_ttl(self, tmp_path):
        path = str(tmp_path / "lease4.db")
        clock = FakeClock()
        first = open_handle(path, run_id="run_crashed", clock=clock, ttl=5)
        clock.advance(6)
        second = open_handle(path, run_id="run_survivor", clock=clock, ttl=5)
        assert second.lease.run_id == "run_survivor"
        second.close()
        first.close(release_lease=False)

    def test_lease_ttl_boundary_is_takeoverable_at_exact_expiry(self, tmp_path):
        path = str(tmp_path / "lease-boundary.db")
        clock = FakeClock()
        first = open_handle(path, run_id="run_crashed", clock=clock, ttl=5)
        clock.advance(5)
        second = open_handle(path, run_id="run_boundary", clock=clock, ttl=5)
        assert second.lease.run_id == "run_boundary"
        second.close()
        first.close(release_lease=False)

    def test_heartbeat_keeps_lease_alive(self, tmp_path):
        path = str(tmp_path / "lease5.db")
        clock = FakeClock()
        first = open_handle(path, run_id="run_hb", clock=clock, ttl=5)
        for _ in range(4):
            clock.advance(3)
            first.heartbeat()
        clock.advance(4)
        with pytest.raises(LeaseConflictError):
            open_handle(path, run_id="run_other", clock=clock, ttl=5)
        first.close()

    def test_heartbeat_after_lease_lost_raises(self, tmp_path):
        path = str(tmp_path / "lease6.db")
        clock = FakeClock()
        first = open_handle(path, run_id="run_hb", clock=clock, ttl=5)
        clock.advance(6)
        survivor = open_handle(path, run_id="run_survivor", clock=clock, ttl=5)
        with pytest.raises(LeaseConflictError):
            first.heartbeat()
        survivor.close()
        first.close(release_lease=False)

    def test_stale_takeover_with_same_run_id_cannot_be_revived_by_old_handle(self, tmp_path):
        path = str(tmp_path / "lease-same-run.db")
        clock = FakeClock()
        first = open_handle(path, run_id="run_same", clock=clock, ttl=5)
        clock.advance(6)
        survivor = open_handle(path, run_id="run_same", clock=clock, ttl=5)
        with pytest.raises(LeaseConflictError):
            first.heartbeat()
        first.close()
        assert survivor.conn.execute("SELECT run_id FROM lease WHERE id = 1").fetchone()[0] == "run_same"
        survivor.close()

    def test_closed_handle_rejects_operations(self, tmp_path):
        path = str(tmp_path / "lease7.db")
        handle = open_handle(path)
        handle.close()
        with pytest.raises(StorageClosedError):
            handle.conn
        handle.close()


class TestTransaction:
    def test_nested_transaction_rejected(self, tmp_path):
        handle = open_handle(str(tmp_path / "txn.db"))
        with handle.transaction():
            with pytest.raises(InvalidStorageInputError):
                with handle.transaction():
                    pass
        handle.close()

    def test_rollback_leaves_prior_state(self, tmp_path):
        handle = open_handle(str(tmp_path / "txn2.db"))
        handle.conn.execute(
            "INSERT INTO accounts (strategy_id, account_id, balance, initial_balance, created_at, updated_at) "
            "VALUES ('pa:x', 'paper:pa:x', '1000.00', '1000.00', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z')"
        )
        handle.conn.commit()
        with pytest.raises(RuntimeError):
            with handle.transaction() as conn:
                conn.execute("UPDATE accounts SET balance = '1.00' WHERE strategy_id = 'pa:x'")
                raise RuntimeError("boom")
        row = handle.conn.execute("SELECT balance FROM accounts WHERE strategy_id = 'pa:x'").fetchone()
        assert row[0] == "1000.00"
        handle.close()

    def test_heartbeat_inside_transaction_rejected(self, tmp_path):
        handle = open_handle(str(tmp_path / "txn3.db"))
        with handle.transaction():
            with pytest.raises(InvalidStorageInputError):
                handle.heartbeat()
        handle.close()


class TestOpenValidation:
    def test_bad_path_rejected(self, tmp_path):
        for bad in ("", 42, None):
            with pytest.raises(InvalidStorageInputError):
                open_handle(bad)

    def test_bad_run_id_rejected(self, tmp_path):
        for bad in ("", "a", "a b", "x" * 200):
            with pytest.raises(InvalidStorageInputError):
                open_handle(str(tmp_path / "rid.db"), run_id=bad)

    def test_bad_mode_rejected(self, tmp_path):
        with pytest.raises(InvalidStorageInputError):
            open_handle(str(tmp_path / "mode.db"), mode="live_money")

    def test_bad_ttl_rejected(self, tmp_path):
        for bad in (0, -1, 90000, True, "30"):
            with pytest.raises(InvalidStorageInputError):
                open_handle(str(tmp_path / "ttl.db"), ttl=bad)

    def test_bad_clock_rejected(self, tmp_path):
        with pytest.raises(InvalidStorageInputError):
            open_handle(str(tmp_path / "clock.db"), clock="not-callable")


class TestTimes:
    def test_instant_roundtrip(self):
        value = datetime(2026, 8, 12, 14, 30, 45, 123000, tzinfo=UTC)
        text = instant_to_text(value)
        assert text == "2026-08-12T14:30:45.123Z"
        assert instant_from_text(text) == value
        assert is_canonical_instant(text)

    def test_instant_trims_trailing_zeros(self):
        text = instant_to_text(datetime(2026, 8, 12, 14, 30, 45, 100000, tzinfo=UTC))
        assert text == "2026-08-12T14:30:45.1Z"

    def test_instant_requires_aware_datetime(self):
        with pytest.raises(ValueError):
            instant_to_text(datetime(2026, 8, 12, 14, 30, 45))

    def test_canonical_instant_rejections(self):
        for bad in ("2026-08-12 14:00:00", "2026-08-12T14:00:00+00:00", "2026-08-12T14:00:00", "", None, 42, "2026-08-12T14:00:00.Z"):
            assert not is_canonical_instant(bad)
        with pytest.raises(ValueError):
            instant_from_text("2026-08-12T14:00:00+00:00")

    def test_decimal_roundtrip(self):
        for value in ("1000.00", "0.1", "19.6", "1", "0"):
            assert decimal_from_text(value) == Decimal(value)
            assert decimal_to_text(Decimal(value)) == value

    def test_decimal_rejections(self):
        for bad in ("1e3", "+1", "01", "1.", ".5", "", None, 42, "0x10"):
            assert not is_decimal_text(bad)
        with pytest.raises(ValueError):
            decimal_from_text("1e3")
        with pytest.raises(ValueError):
            decimal_to_text(Decimal("NaN"))
        with pytest.raises(ValueError):
            decimal_to_text(Decimal("Infinity"))


class TestEventStreamVocabulary:
    def test_closed_event_types_are_the_frozen_twelve(self):
        from pa_engine.storage.schema import EVENT_TYPES

        assert EVENT_TYPES == frozenset(
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

    def test_closed_strategy_states_are_the_frozen_ten(self):
        from pa_engine.storage.schema import STRATEGY_STATES

        assert STRATEGY_STATES == frozenset(
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


class TestVerifyHelpers:
    def test_create_schema_then_verify_schema_passes(self, tmp_path):
        conn = sqlite3.connect(str(tmp_path / "raw.db"))
        conn.execute("BEGIN IMMEDIATE")
        create_schema(conn)
        conn.commit()
        verify_schema(conn)
        verify_invariants(conn)
        conn.close()

    def test_verify_schema_detects_missing_tables(self, tmp_path):
        conn = sqlite3.connect(str(tmp_path / "raw2.db"))
        conn.execute("BEGIN IMMEDIATE")
        create_schema(conn)
        conn.execute("DROP TABLE events")
        conn.commit()
        with pytest.raises(StorageCorruptionError):
            verify_schema(conn)
        conn.close()
