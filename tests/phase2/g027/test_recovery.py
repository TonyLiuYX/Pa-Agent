"""Focused tests for the G027 restart/recovery orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytest

from pa_engine.storage.database import open_database
from pa_engine.storage.errors import InvalidStorageInputError, LeaseConflictError
from pa_engine.storage.recovery import RETRY_DELAYS, DueWork, RestoreSnapshot, RuntimeRecovery
from pa_engine.storage.schema import FEED_KEY
from pa_engine.storage.store import (
    RuntimeStore,
    account_change_key,
    envelope_for_account_change,
    envelope_for_close,
    envelope_for_fill,
    envelope_for_market_recovery,
    envelope_for_pending_entry,
    fill_key,
    pending_key,
    recovery_key,
)
from pa_engine.storage.times import instant_from_text, instant_to_text

H64 = "a" * 64
H64B = "b" * 64
INSTANT = "2026-08-12T14:00:00Z"


class FakeClock:
    def __init__(self, start: str = INSTANT) -> None:
        self._now = instant_from_text(start)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: int = 1) -> None:
        self._now = self._now + timedelta(seconds=seconds)


@dataclass
class Bundle:
    handle: Any
    store: RuntimeStore
    recovery: RuntimeRecovery
    clock: FakeClock


def open_store(
    path: str, run_id: str = "run_a", clock: FakeClock | None = None, mode: str = "automatic"
) -> Bundle:
    clock = clock if clock is not None else FakeClock()
    handle = open_database(path, run_id=run_id, mode=mode, clock=clock)
    store = RuntimeStore(handle)
    return Bundle(handle, store, RuntimeRecovery(store, handle), clock)


def reopen(
    path: str,
    run_id: str = "run_b",
    clock: FakeClock | None = None,
    mode: str = "automatic",
    now: datetime | str | None = None,
) -> tuple[Bundle, RestoreSnapshot]:
    bundle = open_store(path, run_id=run_id, clock=clock, mode=mode)
    snapshot = bundle.recovery.restore(now=now)
    return bundle, snapshot


def at_shift(seconds: int, base: str = INSTANT) -> str:
    return instant_to_text(instant_from_text(base) + timedelta(seconds=seconds))


def second_ref(symbol: str = "SPY", end_at: str = "2026-08-12T14:00:05Z", **overrides):
    start = instant_from_text(end_at) - timedelta(seconds=1)
    ref = {
        "kind": "second_event",
        "symbol": symbol,
        "second_event_id": "sec_" + H64,
        "second_event_hash": H64,
        "start_at": instant_to_text(start),
        "end_at": end_at,
    }
    ref.update(overrides)
    return ref


def bar_ref(symbol: str = "SPY", start_at: str = "2026-08-12T14:00:05Z", **overrides):
    ref = {
        "kind": "recovery_bar",
        "symbol": symbol,
        "recovery_bar_id": "rbar_" + H64,
        "recovery_bar_hash": H64,
        "path_precision": "minute",
        "start_at": start_at,
        "end_at": instant_to_text(instant_from_text(start_at) + timedelta(minutes=1)),
    }
    ref.update(overrides)
    return ref


def snapshot(strategy_id: str = "pa:rose", **overrides):
    base = {
        "strategy_id": strategy_id,
        "profile_ref": None,
        "state": "sleeping",
        "enabled": True,
        "exhausted": False,
        "generation": 3,
        "intended_resume_state": None,
        "in_flight_job_id": None,
        "in_flight_generation": None,
        "pending_id": None,
        "open_position_id": None,
        "open_position_generation": None,
        "provider_retry_count": 0,
        "semantic_retry_key": None,
        "semantic_retry_count": 0,
        "rejected_immediate_view_id": None,
        "current_view_id": None,
        "last_closed_snapshot_id": None,
        "transition_count": 0,
        "latest_instruction": None,
        "blocked_reason": None,
        "decision_accepted_at": None,
        "next_due_at": None,
        "expires_at": None,
    }
    base.update(overrides)
    return base


def pending_state(strategy_id: str = "pa:rose", pending_id: str = "pe_1", **overrides):
    base = {
        "schema_version": "0.1.0",
        "artifact_kind": "pending_entry_state",
        "strategy_id": strategy_id,
        "pending_id": pending_id,
        "episode_id": "ep_1",
        "symbol": "SPY",
        "direction": "long",
        "entry_price": "510.25",
        "stop_price": "509.00",
        "target_price": "515.00",
        "decision_lineage": {},
        "decision_accepted_at": "2026-08-12T14:00:00Z",
        "submitted_at": "2026-08-12T14:00:01Z",
        "expires_at": "2026-08-12T14:05:00Z",
        "resolution_at": "2026-08-12T14:05:30Z",
        "generation": 3,
        "market_feed": {"feed_key": "alpaca:iex:single_exchange"},
        "status": "pending",
        "terminal_reason": None,
        "gap_overlap": False,
        "partial_data": False,
        "uncertainty": "none",
        "gap_intervals": [],
        "recovery_attempted": False,
        "processed_intervals": [],
        "processed_event_ids": [],
        "recovery_lineage": [],
        "filled_at": None,
        "filled_price": None,
        "exit_eligible_after": None,
        "same_second_exit_allowed": False,
    }
    base.update(overrides)
    return base


def trade_state(strategy_id: str = "pa:rose", trade_id: str = "pt_1", **overrides):
    base = {
        "schema_version": "0.1.0",
        "artifact_kind": "paper_exit_state",
        "strategy_id": strategy_id,
        "trade_id": trade_id,
        "position_id": trade_id,
        "episode_id": "ep_1",
        "symbol": "SPY",
        "direction": "long",
        "entry_price": "510.25",
        "stop_price": "509.00",
        "target_price": "515.00",
        "quantity": "19.6",
        "trade_lineage": {
            "strategy_id": strategy_id,
            "trade_id": trade_id,
            "symbol": "SPY",
            "direction": "long",
            "entry_price": "510.25",
            "stop_price": "509.00",
            "target_price": "515.00",
            "quantity": "19.6",
            "planned_r": "1.2",
            "allocation_fraction": "1",
            "paper_math_version": "0.1.0",
            "immediate": False,
            "requested_entry_price": None,
            "opened": True,
        },
        "decision_lineage": {},
        "entry_event": None,
        "entry_fill_interval": ["2026-08-12T14:00:04Z", "2026-08-12T14:00:05Z"],
        "entry_filled_at": "2026-08-12T14:00:05Z",
        "generation": 3,
        "market_feed": {"feed_key": "alpaca:iex:single_exchange"},
        "status": "open",
        "outcome_status": "open",
        "terminal_reason": None,
        "partial_data": False,
        "gap_active": False,
        "gap_intervals": [],
        "processed_intervals": [],
        "processed_event_ids": [],
        "processed_watermark": None,
        "recovery_lineage": [],
        "recovery_attempted": False,
        "selected_exit": None,
        "exit_reason": None,
        "ambiguity_reason": None,
        "uncertainty": None,
        "settlement": None,
        "scheduler_notification": None,
        "closed_at": None,
    }
    base.update(overrides)
    return base


def commit_pending(store: RuntimeStore, state: dict, event: str) -> None:
    envelope = envelope_for_pending_entry(
        state,
        event,
        occurred_at=INSTANT,
        idempotency_key=pending_key(state["strategy_id"], state["pending_id"], event),
    )
    store.commit_pending_entry(state, event=event, envelope=envelope)


def commit_fill(store: RuntimeStore, state: dict) -> None:
    envelope = envelope_for_fill(
        state,
        occurred_at=INSTANT,
        idempotency_key=fill_key(state["strategy_id"], state["trade_id"]),
        market_ref=second_ref(symbol=state["symbol"]),
    )
    store.commit_trade(state, event="filled", envelope=envelope)


def commit_close(store: RuntimeStore, state: dict) -> None:
    envelope = envelope_for_close(
        state,
        occurred_at=INSTANT,
        idempotency_key="rev:close_test",
        market_ref=second_ref(symbol=state["symbol"]),
    )
    store.commit_trade(state, event="closed", envelope=envelope)


class TestFreshDatabase:
    def test_fresh_db_empty_snapshot(self, tmp_path):
        bundle, snap = reopen(str(tmp_path / "fresh.db"), run_id="run_fresh")
        assert snap.run_id == "run_fresh"
        assert snap.mode == "automatic"
        assert snap.accounts == ()
        assert snap.strategy_states == ()
        assert snap.timers == ()
        assert snap.pending_entries == ()
        assert snap.trades == ()
        assert snap.cursors == ()
        assert snap.due_once == ()
        assert snap.retry_conversions == ()
        assert snap.market_paused == ()
        assert snap.live_blocked is False
        bundle.handle.close()

    def test_fresh_db_restore_appends_no_events(self, tmp_path):
        bundle = open_store(str(tmp_path / "fresh2.db"))
        snap = bundle.recovery.restore()
        assert bundle.store.list_events() == []
        assert snap.due_once == ()
        bundle.handle.close()


class TestAccountsRestore:
    def test_two_accounts_diverge_and_restore_exact_balances(self, tmp_path):
        path = str(tmp_path / "ac1.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose", "pa:caveman"])
        bundle.store.update_account("pa:rose", balance="950.25")
        env = envelope_for_account_change(
            "pa:caveman",
            0,
            balance_before="1000.00",
            balance_after="1150.50",
            occurred_at=INSTANT,
            idempotency_key=account_change_key("pa:caveman", "1000.00", "1150.50"),
        )
        bundle.store.commit_account_event(
            "pa:caveman", balance_before="1000.00", balance_after="1150.50", envelope=env
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_ac1", clock=bundle.clock)
        by_id = {row["strategy_id"]: row for row in snap.accounts}
        assert by_id["pa:rose"]["balance"] == "950.25"
        assert by_id["pa:caveman"]["balance"] == "1150.50"
        after = {row["strategy_id"]: row for row in reopened.store.list_accounts()}
        assert after["pa:rose"]["balance"] == "950.25"
        assert after["pa:caveman"]["balance"] == "1150.50"
        reopened.handle.close()

    def test_restore_keeps_initial_balance_1000(self, tmp_path):
        path = str(tmp_path / "ac2.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose", "pa:caveman"])
        bundle.store.update_account("pa:rose", balance="300.00")
        bundle.store.update_account("pa:caveman", balance="40.10")
        bundle.handle.close()
        _reopened, snap = reopen(path, run_id="run_ac2", clock=bundle.clock)
        assert all(row["initial_balance"] == "1000.00" for row in snap.accounts)

    def test_restore_writes_nothing_to_accounts(self, tmp_path):
        path = str(tmp_path / "ac3.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        bundle.store.update_account(
            "pa:rose", balance="777.00", exhausted=False, pending=True
        )
        before = bundle.store.list_accounts()
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_ac3", clock=bundle.clock)
        assert reopened.store.list_accounts() == before
        assert snap.accounts == tuple(before)
        reopened.handle.close()

    def test_account_flags_preserved_through_restore(self, tmp_path):
        path = str(tmp_path / "ac4.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        bundle.store.update_account("pa:rose", balance="80.00", exhausted=True, pending=True)
        bundle.handle.close()
        reopened, _snap = reopen(path, run_id="run_ac4", clock=bundle.clock)
        account = reopened.store.get_account("pa:rose")
        assert account["balance"] == "80.00"
        assert account["exhausted"] == 1
        assert account["pending"] == 1
        reopened.handle.close()


class TestSleeping:
    def test_sleeping_future_exact_restore_no_due(self, tmp_path):
        path = str(tmp_path / "sleep.db")
        bundle = open_store(path)
        due = at_shift(3600)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_s", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:rose")
        assert row["state"] == "sleeping"
        assert row["next_due_at"] == due
        assert snap.due_once == ()
        assert reopened.store.list_events() == []
        reopened.handle.close()

    def test_sleeping_future_timer_preserved_in_snapshot(self, tmp_path):
        path = str(tmp_path / "sleep2.db")
        bundle = open_store(path)
        due = at_shift(3600)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.handle.close()
        _reopened, snap = reopen(path, run_id="run_s2", clock=bundle.clock)
        assert snap.timers == (
            {
                "strategy_id": "pa:rose",
                "kind": "sleep",
                "generation": 3,
                "due_at": due,
                "active": 1,
                "fired": 0,
            },
        )

    def test_sleeping_due_at_now_is_overdue(self, tmp_path):
        path = str(tmp_path / "sleep3.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=INSTANT),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": INSTANT, "active": True, "fired": False}
            ],
        )
        snap = bundle.recovery.restore(now=INSTANT)
        assert [work.kind for work in snap.due_once] == ["sleep_due"]
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        bundle.handle.close()

    def test_sleeping_overdue_transitions_analysis_due(self, tmp_path):
        path = str(tmp_path / "sleep4.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        snap = bundle.recovery.restore()
        assert snap.strategy_states[0]["state"] == "analysis_due"
        assert snap.strategy_states[0]["next_due_at"] is None
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "analysis_due"
        assert row["next_due_at"] is None
        bundle.handle.close()

    def test_sleeping_overdue_appends_one_event(self, tmp_path):
        path = str(tmp_path / "sleep5.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.recovery.restore()
        events = bundle.store.list_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "strategy_state_changed"
        assert events[0]["occurred_at"] == INSTANT
        assert events[0]["mode"] == "automatic"
        assert events[0]["envelope"]["payload"]["from"] == "sleeping"
        assert events[0]["envelope"]["payload"]["to"] == "analysis_due"
        assert events[0]["envelope"]["payload"]["reason"] == "timer_overdue_restart"
        assert events[0]["idempotency_key"].startswith("restart:")
        bundle.handle.close()

    def test_sleeping_overdue_timer_marked_fired(self, tmp_path):
        path = str(tmp_path / "sleep6.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.recovery.restore()
        timers = bundle.store.get_timers("pa:rose")
        assert len(timers) == 1
        assert timers[0]["kind"] == "sleep"
        assert timers[0]["fired"] == 1
        assert timers[0]["due_at"] == due
        bundle.handle.close()

    def test_sleeping_overdue_due_once_sleep_due(self, tmp_path):
        path = str(tmp_path / "sleep7.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        snap = bundle.recovery.restore()
        assert snap.due_once == (DueWork(strategy_id="pa:rose", kind="sleep_due"),)
        bundle.handle.close()

    def test_sleeping_overdue_second_restore_noop(self, tmp_path):
        path = str(tmp_path / "sleep8.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.recovery.restore()
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_s8", clock=bundle.clock)
        assert snap.due_once == ()
        assert snap.retry_conversions == ()
        assert len(reopened.store.list_events()) == 1
        assert reopened.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        reopened.handle.close()

    def test_restore_envelope_carries_profile_ref(self, tmp_path):
        path = str(tmp_path / "sleep9.db")
        bundle = open_store(path)
        due = at_shift(-60)
        profile = {"profile_ref_id": "spr_" + H64, "profile_ref_hash": H64}
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due, profile_ref=profile),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.recovery.restore()
        event = bundle.store.list_events()[0]
        assert event["envelope"]["profile_ref"] == profile
        assert event["envelope"]["generation"] == 3
        bundle.handle.close()


class TestRetryWait:
    def test_retry_wait_future_exact_restore(self, tmp_path):
        path = str(tmp_path / "retry.db")
        bundle = open_store(path)
        due = at_shift(300)
        bundle.store.commit_strategy_state(
            snapshot(state="retry_wait", intended_resume_state="analysis_due", next_due_at=due),
            timers=[
                {"kind": "retry", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_r", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:rose")
        assert row["state"] == "retry_wait"
        assert row["next_due_at"] == due
        assert snap.due_once == ()
        assert snap.retry_conversions == ()
        assert reopened.store.list_events() == []
        reopened.handle.close()

    def test_retry_wait_overdue_transitions_analysis_due(self, tmp_path):
        path = str(tmp_path / "retry2.db")
        bundle = open_store(path)
        due = at_shift(-30)
        bundle.store.commit_strategy_state(
            snapshot(state="retry_wait", intended_resume_state="analysis_due", next_due_at=due),
            timers=[
                {"kind": "retry", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        snap = bundle.recovery.restore()
        assert snap.due_once == (DueWork(strategy_id="pa:rose", kind="retry_due"),)
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "analysis_due"
        assert row["next_due_at"] is None
        events = bundle.store.list_events()
        assert len(events) == 1
        assert events[0]["envelope"]["payload"]["from"] == "retry_wait"
        assert events[0]["envelope"]["payload"]["to"] == "analysis_due"
        assert events[0]["envelope"]["payload"]["reason"] == "timer_overdue_restart"
        bundle.handle.close()

    def test_retry_wait_overdue_timer_fired(self, tmp_path):
        path = str(tmp_path / "retry3.db")
        bundle = open_store(path)
        due = at_shift(-30)
        bundle.store.commit_strategy_state(
            snapshot(state="retry_wait", next_due_at=due),
            timers=[
                {"kind": "retry", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.recovery.restore()
        timer = bundle.store.get_timers("pa:rose")[0]
        assert timer["kind"] == "retry"
        assert timer["fired"] == 1
        bundle.handle.close()

    def test_retry_wait_overdue_second_restore_noop(self, tmp_path):
        path = str(tmp_path / "retry4.db")
        bundle = open_store(path)
        due = at_shift(-30)
        bundle.store.commit_strategy_state(
            snapshot(state="retry_wait", next_due_at=due),
            timers=[
                {"kind": "retry", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.recovery.restore()
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_r4", clock=bundle.clock)
        assert snap.due_once == ()
        assert len(reopened.store.list_events()) == 1
        reopened.handle.close()

    def test_retry_wait_fired_timer_never_re_fires(self, tmp_path):
        path = str(tmp_path / "retry5.db")
        bundle = open_store(path)
        due = at_shift(-30)
        bundle.store.commit_strategy_state(
            snapshot(state="retry_wait", next_due_at=due),
            timers=[
                {"kind": "retry", "generation": 3, "due_at": due, "active": True, "fired": True}
            ],
        )
        snap = bundle.recovery.restore()
        assert snap.due_once == ()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "retry_wait"
        assert bundle.store.list_events() == []
        bundle.handle.close()


class TestInFlight:
    def test_in_flight_converts_to_retry_wait_delay_5(self, tmp_path):
        path = str(tmp_path / "inflight.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        snap = bundle.recovery.restore(now="2026-08-12T18:00:00Z")
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "retry_wait"
        assert row["provider_retry_count"] == 1
        assert row["next_due_at"] == "2026-08-12T18:00:05Z"
        assert row["in_flight_job_id"] is None
        assert row["in_flight_generation"] is None
        assert row["intended_resume_state"] is None
        assert snap.due_once == ()
        bundle.handle.close()

    def test_in_flight_saves_retry_timer(self, tmp_path):
        path = str(tmp_path / "inflight2.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        bundle.recovery.restore(now="2026-08-12T18:00:00Z")
        timers = bundle.store.get_timers("pa:rose")
        assert len(timers) == 1
        assert timers[0]["kind"] == "retry"
        assert timers[0]["generation"] == 3
        assert timers[0]["due_at"] == "2026-08-12T18:00:05Z"
        assert timers[0]["active"] == 1
        assert timers[0]["fired"] == 0
        bundle.handle.close()

    def test_in_flight_appends_one_event(self, tmp_path):
        path = str(tmp_path / "inflight3.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        bundle.recovery.restore()
        events = bundle.store.list_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "strategy_state_changed"
        assert events[0]["envelope"]["payload"]["from"] == "analyzing"
        assert events[0]["envelope"]["payload"]["to"] == "retry_wait"
        assert events[0]["envelope"]["payload"]["reason"] == "lost_in_flight_call"
        assert events[0]["envelope"]["payload"]["resume_state"] is None
        bundle.handle.close()

    def test_in_flight_retry_conversions_recorded(self, tmp_path):
        path = str(tmp_path / "inflight4.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        snap = bundle.recovery.restore()
        assert snap.retry_conversions == (
            {
                "strategy_id": "pa:rose",
                "generation": 3,
                "provider_retry_count": 1,
                "delay_seconds": 5,
                "due_at": at_shift(5),
            },
        )
        bundle.handle.close()

    def test_in_flight_never_becomes_analysis_due(self, tmp_path):
        path = str(tmp_path / "inflight5.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        snap = bundle.recovery.restore()
        assert all(work.kind != "sleep_due" for work in snap.due_once)
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "retry_wait"
        bundle.handle.close()

    def test_in_flight_second_cycle_count_2_delay_10(self, tmp_path):
        path = str(tmp_path / "inflight6.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        first = bundle.recovery.restore()
        assert first.retry_conversions[0]["delay_seconds"] == 5
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_2",
                in_flight_generation=3,
                provider_retry_count=1,
            )
        )
        second = bundle.recovery.restore()
        assert second.retry_conversions[0]["provider_retry_count"] == 2
        assert second.retry_conversions[0]["delay_seconds"] == 10
        assert bundle.store.get_strategy_state("pa:rose")["next_due_at"] == at_shift(10)
        transitions = bundle.store.list_events(event_type="strategy_state_changed")
        assert len(transitions) == 2
        bundle.handle.close()

    def test_in_flight_retry_count_caps_at_five_delay_60(self, tmp_path):
        path = str(tmp_path / "inflight7.db")
        bundle = open_store(path)
        for step, expected in enumerate(RETRY_DELAYS, start=1):
            bundle.store.commit_strategy_state(
                snapshot(
                    state="analyzing",
                    in_flight_job_id=f"job_{step}",
                    in_flight_generation=step,
                    provider_retry_count=step - 1,
                )
            )
            snap = bundle.recovery.restore()
            assert snap.retry_conversions[-1]["provider_retry_count"] == step
            assert snap.retry_conversions[-1]["delay_seconds"] == expected
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_6",
                in_flight_generation=6,
                provider_retry_count=5,
            )
        )
        snap = bundle.recovery.restore()
        assert snap.retry_conversions[-1]["provider_retry_count"] == 5
        assert snap.retry_conversions[-1]["delay_seconds"] == 60
        assert bundle.store.get_strategy_state("pa:rose")["next_due_at"] == at_shift(60)
        bundle.handle.close()

    def test_in_flight_second_restore_is_noop(self, tmp_path):
        path = str(tmp_path / "inflight8.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        bundle.recovery.restore()
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_i8", clock=bundle.clock)
        assert snap.retry_conversions == ()
        assert snap.due_once == ()
        assert len(reopened.store.list_events()) == 1
        assert reopened.store.get_strategy_state("pa:rose")["state"] == "retry_wait"
        reopened.handle.close()

    def test_analyzing_without_job_restores_exactly(self, tmp_path):
        path = str(tmp_path / "inflight9.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(snapshot(state="analyzing"))
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_i9", clock=bundle.clock)
        assert reopened.store.get_strategy_state("pa:rose")["state"] == "analyzing"
        assert snap.retry_conversions == ()
        assert reopened.store.list_events() == []
        reopened.handle.close()


class TestWaitingEntry:
    def test_waiting_pending_future_exact_restore(self, tmp_path):
        path = str(tmp_path / "wait.db")
        bundle = open_store(path)
        pend = pending_state()
        commit_pending(bundle.store, pend, "created")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at=pend["expires_at"])
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_w", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:rose")
        assert row["state"] == "waiting_entry"
        assert row["pending_id"] == "pe_1"
        assert snap.due_once == ()
        assert len(reopened.store.list_events()) == 1
        assert reopened.store.list_events()[0]["event_type"] == "pending_entry_created"
        reopened.handle.close()

    def test_waiting_pending_future_market_paused(self, tmp_path):
        path = str(tmp_path / "wait2.db")
        bundle = open_store(path)
        pend = pending_state()
        commit_pending(bundle.store, pend, "created")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at=pend["expires_at"])
        )
        snap = bundle.recovery.restore()
        assert snap.market_paused == ("pa:rose",)
        bundle.handle.close()

    def test_waiting_pending_overdue_due_once(self, tmp_path):
        path = str(tmp_path / "wait3.db")
        bundle = open_store(path)
        pend = pending_state()
        commit_pending(bundle.store, pend, "created")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at=pend["expires_at"])
        )
        snap = bundle.recovery.restore(now="2026-08-12T14:06:00Z")
        assert snap.due_once == (DueWork(strategy_id="pa:rose", kind="pending_expiry_due"),)
        assert bundle.store.get_timers("pa:rose") == [
            {
                "strategy_id": "pa:rose",
                "kind": "pending_expiry",
                "generation": 3,
                "due_at": "2026-08-12T14:05:00Z",
                "active": 1,
                "fired": 1,
            }
        ]
        assert bundle.recovery.restore(now="2026-08-12T14:07:00Z").due_once == ()
        bundle.handle.close()

    def test_waiting_pending_overdue_state_unchanged(self, tmp_path):
        path = str(tmp_path / "wait4.db")
        bundle = open_store(path)
        pend = pending_state()
        commit_pending(bundle.store, pend, "created")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at=pend["expires_at"])
        )
        bundle.recovery.restore(now="2026-08-12T14:06:00Z")
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "waiting_entry"
        assert row["pending_id"] == "pe_1"
        assert len(bundle.store.list_events()) == 1
        bundle.handle.close()

    def test_waiting_pending_overdue_row_still_pending(self, tmp_path):
        path = str(tmp_path / "wait5.db")
        bundle = open_store(path)
        pend = pending_state()
        commit_pending(bundle.store, pend, "created")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at=pend["expires_at"])
        )
        bundle.recovery.restore(now="2026-08-12T14:06:00Z")
        assert bundle.store.get_pending_entry("pe_1")["status"] == "pending"
        bundle.handle.close()

    def test_waiting_terminal_pending_reconciled(self, tmp_path):
        path = str(tmp_path / "wait6.db")
        bundle = open_store(path)
        commit_pending(bundle.store, pending_state(status="expired"), "expired")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at="2026-08-12T14:05:00Z")
        )
        snap = bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "analysis_due"
        assert snap.due_once == ()
        events = bundle.store.list_events()
        assert len(events) == 2
        transition = [e for e in events if e["event_type"] == "strategy_state_changed"]
        assert len(transition) == 1
        assert transition[0]["envelope"]["payload"]["from"] == "waiting_entry"
        assert transition[0]["envelope"]["payload"]["to"] == "analysis_due"
        assert transition[0]["envelope"]["payload"]["reason"] == "pending_resolved_restart"
        bundle.handle.close()

    def test_waiting_terminal_reconcile_clears_pending_id(self, tmp_path):
        path = str(tmp_path / "wait7.db")
        bundle = open_store(path)
        commit_pending(bundle.store, pending_state(status="expired"), "expired")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at="2026-08-12T14:05:00Z")
        )
        bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["pending_id"] is None
        assert row["next_due_at"] is None
        bundle.handle.close()

    def test_waiting_terminal_reconcile_clears_expiry_timer(self, tmp_path):
        path = str(tmp_path / "wait8.db")
        bundle = open_store(path)
        commit_pending(bundle.store, pending_state(status="expired"), "expired")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at="2026-08-12T14:05:00Z"),
            timers=[
                {
                    "kind": "pending_expiry",
                    "generation": 3,
                    "due_at": "2026-08-12T14:05:00Z",
                    "active": True,
                    "fired": False,
                }
            ],
        )
        bundle.recovery.restore()
        timer = bundle.store.get_timers("pa:rose")[0]
        assert timer["kind"] == "pending_expiry"
        assert timer["fired"] == 1
        bundle.handle.close()

    def test_waiting_terminal_no_fabricated_pending_event(self, tmp_path):
        path = str(tmp_path / "wait9.db")
        bundle = open_store(path)
        commit_pending(bundle.store, pending_state(status="expired"), "expired")
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at="2026-08-12T14:05:00Z")
        )
        bundle.recovery.restore()
        events = bundle.store.list_events()
        assert sum(1 for e in events if e["event_type"] == "pending_entry_expired") == 1
        assert sum(1 for e in events if e["event_type"] == "strategy_state_changed") == 1
        bundle.handle.close()

    def test_waiting_missing_pending_reconciled(self, tmp_path):
        path = str(tmp_path / "wait10.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_missing", expires_at=at_shift(60))
        )
        bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "analysis_due"
        assert row["pending_id"] is None
        events = bundle.store.list_events()
        assert len(events) == 1
        assert events[0]["envelope"]["payload"]["reason"] == "pending_resolved_restart"
        bundle.handle.close()

    def test_waiting_cancelled_variant_reconciled(self, tmp_path):
        path = str(tmp_path / "wait11.db")
        bundle = open_store(path)
        commit_pending(
            bundle.store,
            pending_state(status="cancelled", terminal_reason="manual"),
            "cancelled",
        )
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at="2026-08-12T14:05:00Z")
        )
        bundle.recovery.restore()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        bundle.handle.close()

    def test_waiting_filled_variant_reconciled(self, tmp_path):
        path = str(tmp_path / "wait12.db")
        bundle = open_store(path)
        bundle.handle.conn.execute(
            "INSERT INTO pending_entries (pending_id, strategy_id, generation, status, symbol, "
            "direction, entry_price, stop_price, target_price, decision_accepted_at, "
            "submitted_at, expires_at, resolution_at, episode_id, state_hash, state_json, "
            "flags_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?)",
            (
                "pe_filled",
                "pa:rose",
                3,
                "filled",
                "SPY",
                "long",
                "510.25",
                "509.00",
                "515.00",
                "2026-08-12T14:00:00Z",
                "2026-08-12T14:00:01Z",
                "2026-08-12T14:05:00Z",
                "2026-08-12T14:00:05Z",
                "ep_1",
                "0" * 64,
                "{}",
                "{}",
                INSTANT,
                INSTANT,
            ),
        )
        bundle.handle.conn.commit()
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_filled", expires_at="2026-08-12T14:05:00Z")
        )
        bundle.recovery.restore()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        bundle.handle.close()

    def test_waiting_filled_pending_with_open_trade_restores_position(self, tmp_path):
        path = str(tmp_path / "wait-filled-open.db")
        bundle = open_store(path)
        pending = pending_state()
        created = envelope_for_pending_entry(
            pending,
            "created",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "created"),
        )
        bundle.store.commit_pending_entry(pending, event="created", envelope=created)
        bundle.store.commit_pending_entry(
            pending_state(status="filled"), event="created", envelope=created
        )
        trade = trade_state()
        bundle.store.commit_trade(
            trade,
            event="filled",
            envelope=envelope_for_fill(
                trade,
                occurred_at=INSTANT,
                idempotency_key=fill_key("pa:rose", "pt_1"),
                market_ref=second_ref(),
            ),
        )
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at=pending["expires_at"]),
            timers=[
                {
                    "kind": "pending_expiry",
                    "generation": 3,
                    "due_at": pending["expires_at"],
                    "active": True,
                    "fired": False,
                }
            ],
        )
        bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "open_position"
        assert row["pending_id"] is None
        assert row["open_position_id"] == "pt_1"
        assert bundle.store.get_timers("pa:rose") == [
            {
                "strategy_id": "pa:rose",
                "kind": "pending_expiry",
                "generation": 3,
                "due_at": pending["expires_at"],
                "active": 0,
                "fired": 1,
            }
        ]
        assert bundle.store.list_events(event_type="strategy_state_changed")[0]["envelope"]["payload"]["reason"] == "open_trade_restored"
        bundle.handle.close()

    def test_waiting_reconcile_second_restore_noop(self, tmp_path):
        path = str(tmp_path / "wait13.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_missing", expires_at=at_shift(60))
        )
        bundle.recovery.restore()
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_w13", clock=bundle.clock)
        assert snap.due_once == ()
        assert len(reopened.store.list_events()) == 1
        reopened.handle.close()


class TestOpenPosition:
    def test_open_position_open_trade_exact_restore(self, tmp_path):
        path = str(tmp_path / "open.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(
            snapshot(state="open_position", open_position_id="pt_1", open_position_generation=3)
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_o", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:rose")
        assert row["state"] == "open_position"
        assert row["open_position_id"] == "pt_1"
        assert row["open_position_generation"] == 3
        assert snap.due_once == ()
        assert snap.retry_conversions == ()
        reopened.handle.close()

    def test_open_position_market_paused(self, tmp_path):
        path = str(tmp_path / "open2.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(
            snapshot(state="open_position", open_position_id="pt_1", open_position_generation=3)
        )
        snap = bundle.recovery.restore()
        assert snap.market_paused == ("pa:rose",)
        bundle.handle.close()

    def test_open_position_appends_no_paper_events(self, tmp_path):
        path = str(tmp_path / "open3.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(
            snapshot(state="open_position", open_position_id="pt_1", open_position_generation=3)
        )
        before = bundle.store.list_events()
        assert len(before) == 1
        bundle.recovery.restore()
        after = bundle.store.list_events()
        assert after == before
        assert all(e["event_type"] == "paper_fill" for e in after)
        assert bundle.store.list_events(event_type="paper_close") == []
        assert bundle.store.list_events(event_type="account_changed") == []
        bundle.handle.close()

    def test_open_position_settled_trade_reconciled(self, tmp_path):
        path = str(tmp_path / "open4.db")
        bundle = open_store(path)
        commit_close(
            bundle.store,
            trade_state(
                status="settled",
                exit_reason="target",
                closed_at="2026-08-12T15:00:00Z",
                settlement={
                    "gross_pnl": "19.60",
                    "applied_pnl": "19.60",
                    "net_pnl": "19.60",
                    "balance_before": "1000.00",
                    "balance_after": "1019.60",
                },
            ),
        )
        bundle.store.commit_strategy_state(
            snapshot(state="open_position", open_position_id="pt_1", open_position_generation=3)
        )
        snap = bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "analysis_due"
        assert snap.due_once == ()
        events = bundle.store.list_events()
        transition = [e for e in events if e["event_type"] == "strategy_state_changed"]
        assert len(transition) == 1
        assert transition[0]["envelope"]["payload"]["from"] == "open_position"
        assert transition[0]["envelope"]["payload"]["to"] == "analysis_due"
        assert transition[0]["envelope"]["payload"]["reason"] == "trade_settled_restart"
        bundle.handle.close()

    def test_open_position_settled_clears_position_ids(self, tmp_path):
        path = str(tmp_path / "open5.db")
        bundle = open_store(path)
        commit_close(
            bundle.store,
            trade_state(
                status="settled",
                exit_reason="stop",
                closed_at="2026-08-12T15:00:00Z",
                settlement={
                    "gross_pnl": "-19.60",
                    "applied_pnl": "-19.60",
                    "net_pnl": "-19.60",
                    "balance_before": "1000.00",
                    "balance_after": "980.40",
                },
            ),
        )
        bundle.store.commit_strategy_state(
            snapshot(state="open_position", open_position_id="pt_1", open_position_generation=3)
        )
        bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["open_position_id"] is None
        assert row["open_position_generation"] is None
        assert row["next_due_at"] is None
        bundle.handle.close()

    def test_open_position_missing_trade_reconciled(self, tmp_path):
        path = str(tmp_path / "open6.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(state="open_position", open_position_id="pt_missing", open_position_generation=3)
        )
        bundle.recovery.restore()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        events = bundle.store.list_events()
        assert len(events) == 1
        assert events[0]["envelope"]["payload"]["reason"] == "trade_settled_restart"
        bundle.handle.close()

    def test_open_position_without_position_id_reconciled(self, tmp_path):
        path = str(tmp_path / "open7.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(snapshot(state="open_position"))
        bundle.recovery.restore()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        bundle.handle.close()

    def test_open_trade_row_analysis_due_reconciled_to_open_position(self, tmp_path):
        path = str(tmp_path / "open8.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(snapshot(state="analysis_due"))
        snap = bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "open_position"
        assert row["open_position_id"] == "pt_1"
        assert row["open_position_generation"] == 3
        assert row["transition_count"] == 1
        events = bundle.store.list_events()
        assert len(events) == 2
        transition = [e for e in events if e["event_type"] == "strategy_state_changed"]
        assert len(transition) == 1
        assert transition[0]["envelope"]["payload"]["from"] == "analysis_due"
        assert transition[0]["envelope"]["payload"]["to"] == "open_position"
        assert transition[0]["envelope"]["payload"]["reason"] == "open_trade_restored"
        assert snap.due_once == ()
        bundle.handle.close()

    def test_open_trade_row_sleeping_reconciled_to_open_position(self, tmp_path):
        path = str(tmp_path / "open9.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=at_shift(600)),
            timers=[
                {
                    "kind": "sleep",
                    "generation": 3,
                    "due_at": at_shift(600),
                    "active": True,
                    "fired": False,
                }
            ],
        )
        bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "open_position"
        assert row["open_position_id"] == "pt_1"
        bundle.handle.close()

    @pytest.mark.parametrize(
        "state_name,timer_kind",
        [("sleeping", "sleep"), ("retry_wait", "retry")],
    )
    def test_open_trade_wins_overdue_timer_without_due_work(
        self, tmp_path, state_name, timer_kind
    ):
        path = str(tmp_path / f"open_overdue_{timer_kind}.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(
            snapshot(state=state_name, next_due_at="2026-08-12T17:59:00Z"),
            timers=[
                {
                    "kind": timer_kind,
                    "generation": 3,
                    "due_at": "2026-08-12T17:59:00Z",
                    "active": True,
                    "fired": False,
                }
            ],
        )
        snap = bundle.recovery.restore(now="2026-08-12T18:00:00Z")
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "open_position"
        assert row["open_position_id"] == "pt_1"
        assert snap.due_once == ()
        assert snap.retry_conversions == ()
        assert bundle.store.get_timers("pa:rose")[0]["active"] == 0
        assert bundle.store.list_events(event_type="strategy_state_changed")[0]["envelope"]["payload"]["reason"] == "open_trade_restored"
        bundle.handle.close()

    def test_open_trade_wins_lost_analyzing_retry(self, tmp_path):
        path = str(tmp_path / "open_analyzing.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_lost",
                in_flight_generation=3,
            )
        )
        snap = bundle.recovery.restore(now="2026-08-12T18:00:00Z")
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "open_position"
        assert snap.retry_conversions == ()
        assert snap.due_once == ()
        assert bundle.store.list_events(event_type="strategy_state_changed")[0]["envelope"]["payload"]["reason"] == "open_trade_restored"
        bundle.handle.close()

    def test_open_trade_row_retry_wait_reconciled_to_open_position(self, tmp_path):
        path = str(tmp_path / "open10.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(
            snapshot(state="retry_wait", next_due_at=at_shift(600)),
            timers=[
                {
                    "kind": "retry",
                    "generation": 3,
                    "due_at": at_shift(600),
                    "active": True,
                    "fired": False,
                }
            ],
        )
        bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "open_position"
        assert row["open_position_id"] == "pt_1"
        assert row["open_position_generation"] == 3
        bundle.handle.close()

    def test_open_trade_reconcile_second_restore_noop(self, tmp_path):
        path = str(tmp_path / "open11.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(snapshot(state="analysis_due"))
        bundle.recovery.restore()
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_o11", clock=bundle.clock)
        assert snap.due_once == ()
        assert len(reopened.store.list_events()) == 2
        assert reopened.store.get_strategy_state("pa:rose")["state"] == "open_position"
        reopened.handle.close()


class TestMarketWait:
    def test_market_wait_exact_restore_preserves_resume_state(self, tmp_path):
        path = str(tmp_path / "mw.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="market_wait",
                intended_resume_state="analysis_due",
                blocked_reason="feed gap",
            )
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_mw", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:rose")
        assert row["state"] == "market_wait"
        assert row["intended_resume_state"] == "analysis_due"
        assert row["blocked_reason"] == "feed gap"
        assert snap.due_once == ()
        assert reopened.store.list_events() == []
        reopened.handle.close()

    def test_market_wait_with_pending_id_market_paused(self, tmp_path):
        path = str(tmp_path / "mw2.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(state="market_wait", intended_resume_state="waiting_entry", pending_id="pe_1")
        )
        snap = bundle.recovery.restore()
        assert snap.market_paused == ("pa:rose",)
        assert bundle.store.get_strategy_state("pa:rose")["pending_id"] == "pe_1"
        bundle.handle.close()

    def test_market_wait_with_open_position_market_paused(self, tmp_path):
        path = str(tmp_path / "mw3.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="market_wait",
                intended_resume_state="open_position",
                open_position_id="pt_1",
                open_position_generation=3,
            )
        )
        snap = bundle.recovery.restore()
        assert snap.market_paused == ("pa:rose",)
        assert snap.due_once == ()
        bundle.handle.close()


class TestBlockedDisabledExhausted:
    def test_blocked_exact_restore_never_due(self, tmp_path):
        path = str(tmp_path / "blocked.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(state="blocked", blocked_reason="bad credentials", next_due_at=at_shift(-60)),
            timers=[
                {
                    "kind": "sleep",
                    "generation": 3,
                    "due_at": at_shift(-60),
                    "active": True,
                    "fired": False,
                }
            ],
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_b1", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:rose")
        assert row["state"] == "blocked"
        assert row["blocked_reason"] == "bad credentials"
        assert snap.due_once == ()
        assert reopened.store.list_events() == []
        reopened.handle.close()

    def test_blocked_with_overdue_timer_not_converted(self, tmp_path):
        path = str(tmp_path / "blocked2.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="blocked", blocked_reason="bad credentials", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        snap = bundle.recovery.restore()
        assert snap.due_once == ()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "blocked"
        assert bundle.store.get_timers("pa:rose")[0]["fired"] == 0
        assert bundle.store.list_events() == []
        bundle.handle.close()

    def test_disabled_exact_restore(self, tmp_path):
        path = str(tmp_path / "disabled.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(snapshot(state="disabled", enabled=False))
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_d1", clock=bundle.clock)
        assert reopened.store.get_strategy_state("pa:rose")["state"] == "disabled"
        assert snap.due_once == ()
        assert snap.retry_conversions == ()
        assert reopened.store.list_events() == []
        reopened.handle.close()

    def test_exhausted_exact_restore(self, tmp_path):
        path = str(tmp_path / "exhausted.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(state="exhausted", exhausted=True, provider_retry_count=5)
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_e1", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:rose")
        assert row["state"] == "exhausted"
        assert snap.due_once == ()
        assert reopened.store.list_events() == []
        reopened.handle.close()

    def test_exhausted_open_trade_not_reconciled(self, tmp_path):
        path = str(tmp_path / "exhausted2.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(snapshot(state="exhausted", exhausted=True))
        snap = bundle.recovery.restore()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "exhausted"
        assert snap.market_paused == ("pa:rose",)
        assert bundle.store.list_events(event_type="strategy_state_changed") == []
        bundle.handle.close()

    def test_disabled_open_trade_not_reconciled(self, tmp_path):
        path = str(tmp_path / "disabled2.db")
        bundle = open_store(path)
        commit_fill(bundle.store, trade_state())
        bundle.store.commit_strategy_state(snapshot(state="disabled", enabled=False))
        bundle.recovery.restore()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "disabled"
        assert bundle.store.list_events(event_type="strategy_state_changed") == []
        bundle.handle.close()


class TestCursorGate:
    def test_restore_blocks_live_cursor(self, tmp_path):
        path = str(tmp_path / "cursor.db")
        bundle = open_store(path)
        bundle.store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        snap = bundle.recovery.restore()
        assert snap.live_blocked is True
        assert bundle.store.is_live_blocked("SPY") is True
        assert snap.cursors[0]["live_blocked"] == 1
        bundle.handle.close()

    def test_restore_sets_pending_recovery_reason(self, tmp_path):
        path = str(tmp_path / "cursor2.db")
        bundle = open_store(path)
        bundle.store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        bundle.recovery.restore()
        cursor = bundle.store.get_cursor("SPY")
        assert cursor["pending_recovery_json"] == {"reason": "restart_recovery_required"}
        bundle.handle.close()

    def test_cursor_without_second_left_untouched(self, tmp_path):
        path = str(tmp_path / "cursor3.db")
        bundle = open_store(path)
        bundle.handle.conn.execute(
            "INSERT INTO market_cursors (symbol, feed_key, updated_at) VALUES (?, ?, ?)",
            ("QQQ", FEED_KEY, INSTANT),
        )
        bundle.handle.conn.commit()
        snap = bundle.recovery.restore()
        assert snap.live_blocked is False
        cursor = bundle.store.get_cursor("QQQ")
        assert cursor["live_blocked"] == 0
        assert cursor["pending_recovery_json"] is None
        bundle.handle.close()

    def test_already_blocked_cursor_stays_blocked(self, tmp_path):
        path = str(tmp_path / "cursor4.db")
        bundle = open_store(path)
        bundle.store.mark_live_blocked("SPY", reason="feed disconnected")
        snap = bundle.recovery.restore()
        assert snap.live_blocked is True
        cursor = bundle.store.get_cursor("SPY")
        assert cursor["live_blocked"] == 1
        assert cursor["pending_recovery_json"] == {"reason": "feed disconnected"}
        bundle.handle.close()

    def test_commit_recovery_clears_then_restart_reblocks(self, tmp_path):
        path = str(tmp_path / "cursor5.db")
        bundle = open_store(path)
        bundle.store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        bundle.recovery.restore()
        assert bundle.store.is_live_blocked("SPY") is True
        bars = [bar_ref(start_at="2026-08-12T14:00:05Z")]
        recovery_event = envelope_for_market_recovery(
            "SPY",
            bars=bars,
            recovery_cursor="2026-08-12T14:01:05Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:01:05Z", 1),
        )
        bundle.store.commit_recovery(
            "SPY",
            bars=bars,
            recovery_cursor="2026-08-12T14:01:05Z",
            envelope=recovery_event,
        )
        assert bundle.store.is_live_blocked("SPY") is False
        bundle.recovery.restore()
        assert bundle.store.is_live_blocked("SPY") is True
        assert bundle.store.get_cursor("SPY")["pending_recovery_json"] == {
            "reason": "restart_recovery_required"
        }
        bundle.handle.close()

    def test_live_blocked_true_with_paused_strategies(self, tmp_path):
        path = str(tmp_path / "cursor6.db")
        bundle = open_store(path)
        bundle.store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        commit_fill(bundle.store, trade_state(strategy_id="pa:caveman", trade_id="pt_2"))
        bundle.store.commit_strategy_state(
            snapshot(strategy_id="pa:caveman", state="open_position", open_position_id="pt_2")
        )
        snap = bundle.recovery.restore()
        assert snap.live_blocked is True
        assert snap.market_paused == ("pa:caveman",)
        bundle.handle.close()


class TestStartupSequence:
    def test_startup_sequence_ensures_accounts_and_restores(self, tmp_path):
        path = str(tmp_path / "startup.db")
        bundle = open_store(path)
        snap = bundle.recovery.startup_sequence(["pa:rose", "pa:caveman"])
        accounts = bundle.store.list_accounts()
        assert {row["strategy_id"] for row in accounts} == {"pa:rose", "pa:caveman"}
        assert all(row["balance"] == "1000.00" for row in accounts)
        assert len(snap.accounts) == 2
        bundle.handle.close()

    def test_startup_sequence_second_call_leaves_accounts_untouched(self, tmp_path):
        path = str(tmp_path / "startup2.db")
        bundle = open_store(path)
        bundle.recovery.startup_sequence(["pa:rose", "pa:caveman"])
        bundle.store.update_account("pa:rose", balance="600.00")
        bundle.recovery.startup_sequence(["pa:rose", "pa:caveman", "pa:gap"])
        accounts = bundle.store.list_accounts()
        assert len(accounts) == 3
        assert bundle.store.get_account("pa:rose")["balance"] == "600.00"
        assert bundle.store.get_account("pa:caveman")["balance"] == "1000.00"
        bundle.handle.close()

    def test_startup_sequence_computes_due_after_accounts(self, tmp_path):
        path = str(tmp_path / "startup3.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        snap = bundle.recovery.startup_sequence(["pa:rose"])
        assert len(bundle.store.list_accounts()) == 1
        assert snap.due_once == (DueWork(strategy_id="pa:rose", kind="sleep_due"),)
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        bundle.handle.close()


class TestIdempotentRestart:
    def test_double_restore_no_duplicate_events(self, tmp_path):
        path = str(tmp_path / "idem.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.recovery.restore()
        bundle.handle.close()
        reopened, _snap = reopen(path, run_id="run_i1", clock=bundle.clock)
        reopened.recovery.restore()
        events = reopened.store.list_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "strategy_state_changed"
        reopened.handle.close()

    def test_double_restore_no_duplicate_due_work(self, tmp_path):
        path = str(tmp_path / "idem2.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.recovery.restore()
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_i2", clock=bundle.clock)
        assert snap.due_once == ()
        assert reopened.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        reopened.handle.close()

    def test_due_work_sorted_by_strategy_then_kind(self, tmp_path):
        path = str(tmp_path / "idem3.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(strategy_id="pa:alpha", state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 1, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.store.commit_strategy_state(
            snapshot(strategy_id="pa:bravo", state="retry_wait", next_due_at=due),
            timers=[
                {"kind": "retry", "generation": 1, "due_at": due, "active": True, "fired": False}
            ],
        )
        pend = pending_state(strategy_id="pa:charlie", pending_id="pe_c")
        commit_pending(bundle.store, pend, "created")
        bundle.store.commit_strategy_state(
            snapshot(
                strategy_id="pa:charlie",
                state="waiting_entry",
                pending_id="pe_c",
                expires_at=pend["expires_at"],
            )
        )
        snap = bundle.recovery.restore(now="2026-08-12T14:06:00Z")
        assert [(work.strategy_id, work.kind) for work in snap.due_once] == [
            ("pa:alpha", "sleep_due"),
            ("pa:bravo", "retry_due"),
            ("pa:charlie", "pending_expiry_due"),
        ]
        bundle.handle.close()


class TestLease:
    def test_second_handle_raises_lease_conflict(self, tmp_path):
        path = str(tmp_path / "lease.db")
        bundle = open_store(path)
        with pytest.raises(LeaseConflictError):
            open_database(path, run_id="run_b", mode="automatic", clock=bundle.clock)
        bundle.handle.close()

    def test_stale_lease_replaced_after_ttl(self, tmp_path):
        path = str(tmp_path / "lease2.db")
        bundle = open_store(path)
        bundle.clock.advance(31)
        second = open_database(path, run_id="run_b", mode="automatic", clock=bundle.clock)
        assert second.lease.run_id == "run_b"
        second.close()
        bundle.handle.close()

    def test_close_releases_lease_then_reopen_works(self, tmp_path):
        path = str(tmp_path / "lease3.db")
        bundle = open_store(path)
        bundle.handle.close()
        second = open_database(path, run_id="run_c", mode="automatic", clock=bundle.clock)
        assert second.lease.run_id == "run_c"
        second.close()


class TestMode:
    def test_replay_mode_restore_events_carry_replay(self, tmp_path):
        path = str(tmp_path / "mode.db")
        bundle = open_store(path, mode="automatic")
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_m1", clock=bundle.clock, mode="replay")
        events = reopened.store.list_events()
        assert len(events) == 1
        assert events[0]["mode"] == "replay"
        assert events[0]["envelope"]["payload"]["to"] == "analysis_due"
        assert snap.mode == "replay"
        reopened.handle.close()

    def test_replay_mode_fresh_snapshot_mode(self, tmp_path):
        path = str(tmp_path / "mode2.db")
        bundle = open_store(path, mode="replay")
        snap = bundle.recovery.restore()
        assert snap.mode == "replay"
        bundle.handle.close()


class TestAccountsAfterRestart:
    def test_disabled_strategy_keeps_account_row(self, tmp_path):
        path = str(tmp_path / "keep.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        bundle.store.update_account("pa:rose", balance="420.10")
        bundle.store.commit_strategy_state(snapshot(state="disabled", enabled=False))
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_k1", clock=bundle.clock)
        account = reopened.store.get_account("pa:rose")
        assert account is not None
        assert account["balance"] == "420.10"
        assert account["initial_balance"] == "1000.00"
        assert [row["state"] for row in snap.strategy_states] == ["disabled"]
        assert snap.due_once == ()
        reopened.handle.close()


class TestAtomicity:
    def test_restore_failure_rolls_back_state_and_events(self, tmp_path, monkeypatch):
        path = str(tmp_path / "atomic.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        bundle.store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))

        def failing(conn, document, created_at):
            raise RuntimeError("simulated crash")

        monkeypatch.setattr(bundle.recovery, "_append_event", failing)
        with pytest.raises(RuntimeError):
            bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "analyzing"
        assert row["in_flight_job_id"] == "job_1"
        assert row["provider_retry_count"] == 0
        assert bundle.store.list_events() == []
        assert bundle.store.get_timers("pa:rose") == []
        assert bundle.store.is_live_blocked("SPY") is False
        bundle.handle.close()

    def test_restore_failure_leaves_old_state_usable(self, tmp_path, monkeypatch):
        path = str(tmp_path / "atomic2.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )

        def failing(conn, document, created_at):
            raise RuntimeError("simulated crash")

        monkeypatch.setattr(bundle.recovery, "_append_event", failing)
        with pytest.raises(RuntimeError):
            bundle.recovery.restore()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "sleeping"
        monkeypatch.undo()
        snap = bundle.recovery.restore()
        assert snap.due_once == (DueWork(strategy_id="pa:rose", kind="sleep_due"),)
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        assert len(bundle.store.list_events()) == 1
        bundle.handle.close()


class TestInputValidation:
    def test_restore_rejects_invalid_now(self, tmp_path):
        bundle = open_store(str(tmp_path / "invalid.db"))
        with pytest.raises(InvalidStorageInputError):
            bundle.recovery.restore(now="not-an-instant")
        bundle.handle.close()


class TestSameGenerationRestartCycles:
    def test_lost_in_flight_conversion_repeats_at_later_restart_time(self, tmp_path):
        path = str(tmp_path / "repeat_inflight.db")
        bundle = open_store(path)
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_1",
                in_flight_generation=3,
                provider_retry_count=0,
            )
        )
        first = bundle.recovery.restore(now="2026-08-12T18:00:00Z")
        assert first.retry_conversions[0]["provider_retry_count"] == 1
        bundle.store.commit_strategy_state(
            snapshot(
                state="analyzing",
                in_flight_job_id="job_2",
                in_flight_generation=3,
                provider_retry_count=1,
            )
        )
        second = bundle.recovery.restore(now="2026-08-12T18:10:00Z")
        assert second.retry_conversions[0]["provider_retry_count"] == 2
        assert second.retry_conversions[0]["delay_seconds"] == 10
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "retry_wait"
        events = bundle.store.list_events(event_type="strategy_state_changed")
        assert len(events) == 2
        bundle.handle.close()

    def test_overdue_timer_fires_again_in_new_cycle_at_same_generation(self, tmp_path):
        path = str(tmp_path / "repeat_timer.db")
        bundle = open_store(path)
        due = at_shift(-60)
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=due),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}
            ],
        )
        first = bundle.recovery.restore(now="2026-08-12T18:00:00Z")
        assert first.due_once == (DueWork(strategy_id="pa:rose", kind="sleep_due"),)
        later = at_shift(-30, base="2026-08-12T18:00:00Z")
        bundle.store.commit_strategy_state(
            snapshot(state="sleeping", next_due_at=later),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": later, "active": True, "fired": False}
            ],
        )
        second = bundle.recovery.restore(now="2026-08-12T18:30:00Z")
        assert second.due_once == (DueWork(strategy_id="pa:rose", kind="sleep_due"),)
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        assert len(bundle.store.list_events(event_type="strategy_state_changed")) == 2
        bundle.handle.close()

    def test_pending_resolved_reconciles_new_episode_at_same_generation(self, tmp_path):
        path = str(tmp_path / "repeat_pending.db")
        bundle = open_store(path)
        first_state = pending_state(pending_id="pe_1", status="expired")
        bundle.store.commit_pending_entry(
            first_state, event="expired", envelope=envelope_for_pending_entry(
                first_state, "expired", occurred_at="2026-08-12T17:00:00Z",
                idempotency_key="k_pe_1",
            )
        )
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_1", expires_at=at_shift(-30))
        )
        bundle.recovery.restore(now="2026-08-12T18:00:00Z")
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        second_state = pending_state(pending_id="pe_2", status="cancelled")
        bundle.store.commit_pending_entry(
            second_state, event="cancelled", envelope=envelope_for_pending_entry(
                second_state, "cancelled", occurred_at="2026-08-12T17:30:00Z",
                idempotency_key="k_pe_2",
            )
        )
        bundle.store.commit_strategy_state(
            snapshot(state="waiting_entry", pending_id="pe_2", expires_at=at_shift(-15))
        )
        bundle.recovery.restore(now="2026-08-12T19:00:00Z")
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        events = bundle.store.list_events(event_type="strategy_state_changed")
        assert len(events) == 2
        bundle.handle.close()

    def test_open_trade_restored_again_for_second_trade_at_same_generation(self, tmp_path):
        path = str(tmp_path / "repeat_trade.db")
        bundle = open_store(path)
        first = trade_state(trade_id="pt_1")
        bundle.store.commit_trade(
            first, event="filled", envelope=envelope_for_fill(
                first, occurred_at="2026-08-12T17:00:00Z", idempotency_key="k_pt_1",
                market_ref=second_ref(),
            )
        )
        bundle.store.commit_strategy_state(snapshot(state="sleeping", next_due_at=at_shift(60)))
        bundle.recovery.restore(now="2026-08-12T18:00:00Z")
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "open_position"
        closed = trade_state(
            trade_id="pt_1", status="settled", outcome_status="settled",
            exit_reason="take_profit", selected_exit="515.00",
            closed_at="2026-08-12T18:05:05Z",
            settlement={
                "balance_before": "1000.00",
                "balance_after": "1093.00",
                "gross_pnl": "93.00",
                "applied_pnl": "93.00",
                "net_pnl": "93.00",
            },
        )
        bundle.store.commit_trade(
            closed, event="closed", envelope=envelope_for_close(
                closed, occurred_at="2026-08-12T18:05:00Z", idempotency_key="k_pt_1_close",
                market_ref=second_ref(end_at="2026-08-12T18:05:05Z"),
            )
        )
        second = trade_state(trade_id="pt_2")
        bundle.store.commit_trade(
            second, event="filled", envelope=envelope_for_fill(
                second, occurred_at="2026-08-12T18:10:00Z", idempotency_key="k_pt_2",
                market_ref=second_ref(end_at="2026-08-12T18:10:05Z"),
            )
        )
        bundle.store.commit_strategy_state(snapshot(state="analysis_due"))
        bundle.recovery.restore(now="2026-08-12T19:00:00Z")
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "open_position"
        assert len(bundle.store.list_events(event_type="strategy_state_changed")) == 2
        bundle.handle.close()
