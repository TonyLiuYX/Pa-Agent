"""Focused tests for the G027 repository layer (pa_engine.storage.store)."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pa_engine.contracts import canonical_json_bytes
from pa_engine.storage.database import open_database
from pa_engine.storage.envelope import RuntimeEventEnvelope
from pa_engine.storage.errors import (
    EnvelopeValidationError,
    InvalidStorageInputError,
    RecoveryStateError,
    SecretLeakError,
    StorageConflictError,
)
from pa_engine.storage.store import (
    RuntimeStore,
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
    pending_key,
    recovery_key,
    schedule_key,
    state_transition_key,
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


@pytest.fixture
def db(tmp_path):
    clock = FakeClock()
    handle = open_database(
        str(tmp_path / "run.db"), run_id="run_store", mode="automatic", clock=clock
    )
    yield handle, clock
    handle.close()


@pytest.fixture
def store(db):
    handle, _clock = db
    return RuntimeStore(handle)


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


def bar_ref(symbol: str = "SPY", start_at: str = "2026-08-12T14:00:00Z", **overrides):
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


def instruction(branch: str = "no_entry", **overrides):
    base = {
        "branch": branch,
        "instruction_id": "sdi_" + H64,
        "instruction_hash": H64,
        "result_id": "par_" + H64,
        "result_hash": H64,
        "evidence_package_id": "evp_" + H64,
        "evidence_package_hash": H64,
    }
    if branch == "no_entry":
        base["next_call_after_seconds"] = 60
    elif branch == "wait_for_price":
        base["entry_wait_seconds"] = 120
        base["entry_price"] = "510.25"
    base.update(overrides)
    return base


class TestIdempotencyKeys:
    def test_keys_are_prefixed_and_non_empty(self):
        key = decision_key("pa:rose", 1, H64)
        assert key.startswith("rev:")
        assert len(key) > len("rev:")

    def test_keys_are_deterministic(self):
        first = state_transition_key("pa:rose", 3, "analyzing", "timer fired")
        second = state_transition_key("pa:rose", 3, "analyzing", "timer fired")
        assert first == second

    def test_transition_key_changes_with_input(self):
        assert state_transition_key("pa:rose", 3, "analyzing", "a") != state_transition_key(
            "pa:rose", 3, "sleeping", "a"
        )
        assert state_transition_key("pa:rose", 3, "analyzing", "a") != state_transition_key(
            "pa:rose", 4, "analyzing", "a"
        )

    def test_account_change_key_accepts_decimal_and_text(self):
        assert account_change_key("pa:rose", Decimal("1000.00"), Decimal("950.00")) == (
            account_change_key("pa:rose", "1000.00", "950.00")
        )

    def test_fill_rejected_key_with_nulls(self):
        assert fill_rejected_key("pa:rose", "no funds", None, None) == fill_rejected_key(
            "pa:rose", "no funds", None, None
        )
        assert fill_rejected_key("pa:rose", "no funds", "pe_1", None) != fill_rejected_key(
            "pa:rose", "no funds", None, None
        )

    def test_recovery_key_depends_on_cursor_and_count(self):
        assert recovery_key("SPY", INSTANT, 1) != recovery_key("SPY", INSTANT, 2)
        assert recovery_key("SPY", INSTANT, 1) != recovery_key("SPY", None, 1)

    def test_schedule_key_depends_on_due_at(self):
        assert schedule_key("pa:rose", 1, "sleep", "2026-08-12T15:00:00Z") != schedule_key(
            "pa:rose", 1, "sleep", "2026-08-12T15:01:00Z"
        )


class TestEnvelopeHelpers:
    def test_state_transition_envelope_type(self):
        record = snapshot()
        envelope = envelope_for_state_transition(
            record,
            from_state="sleeping",
            to_state="analyzing",
            reason="timer fired",
            occurred_at=INSTANT,
            idempotency_key=state_transition_key("pa:rose", 3, "analyzing", "timer fired"),
        )
        assert envelope["event_type"] == "strategy_state_changed"
        assert envelope["strategy_id"] == "pa:rose"
        assert envelope["generation"] == 3
        assert envelope["payload"]["resume_state"] is None

    def test_decision_envelope_no_entry(self):
        record = snapshot()
        envelope = envelope_for_decision(
            record,
            instruction(),
            occurred_at=INSTANT,
            idempotency_key=decision_key("pa:rose", 3, H64),
        )
        assert envelope["event_type"] == "model_decision_accepted"
        assert envelope["payload"]["branch"] == "no_entry"
        assert envelope["payload"]["next_call_after_seconds"] == 60
        assert envelope["decision_lineage"]["instruction_hash"] == H64
        assert "entry_price" not in envelope["payload"]

    def test_decision_envelope_wait_for_price(self):
        envelope = envelope_for_decision(
            snapshot(),
            instruction("wait_for_price", entry_price=Decimal("510.25")),
            occurred_at=INSTANT,
            idempotency_key=decision_key("pa:rose", 3, H64),
        )
        assert envelope["payload"]["branch"] == "wait_for_price"
        assert envelope["payload"]["entry_wait_seconds"] == 120
        assert envelope["payload"]["entry_price"] == "510.25"
        assert "next_call_after_seconds" not in envelope["payload"]

    def test_decision_envelope_enter_immediately(self):
        envelope = envelope_for_decision(
            snapshot(),
            instruction("enter_immediately"),
            occurred_at=INSTANT,
            idempotency_key=decision_key("pa:rose", 3, H64),
        )
        assert envelope["payload"]["branch"] == "enter_immediately"
        assert "next_call_after_seconds" not in envelope["payload"]

    def test_decision_envelope_bad_seconds_raises(self):
        with pytest.raises(EnvelopeValidationError):
            envelope_for_decision(
                snapshot(),
                instruction(next_call_after_seconds=0),
                occurred_at=INSTANT,
                idempotency_key=decision_key("pa:rose", 3, H64),
            )

    def test_schedule_envelope_type(self):
        envelope = envelope_for_schedule(
            snapshot(),
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            occurred_at=INSTANT,
            idempotency_key=schedule_key("pa:rose", 3, "sleep", "2026-08-12T15:00:00Z"),
        )
        assert envelope["event_type"] == "schedule_set"
        assert envelope["payload"]["timer_kind"] == "sleep"

    def test_pending_created_envelope_carries_state_hash(self):
        state = pending_state()
        envelope = envelope_for_pending_entry(
            state,
            "created",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "created"),
        )
        assert envelope["event_type"] == "pending_entry_created"
        expected = hashlib.sha256(canonical_json_bytes(state)).hexdigest()
        assert envelope["payload"]["state_hash"] == expected
        assert envelope["payload"]["entry_price"] == "510.25"

    def test_pending_expired_envelope(self):
        state = pending_state(status="expired")
        envelope = envelope_for_pending_entry(
            state,
            "expired",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "expired"),
        )
        assert envelope["event_type"] == "pending_entry_expired"
        assert envelope["payload"]["resolution_at"] == "2026-08-12T14:05:30Z"

    def test_pending_cancelled_envelope_uses_terminal_reason(self):
        state = pending_state(status="cancelled", terminal_reason="manual override")
        envelope = envelope_for_pending_entry(
            state,
            "cancelled",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "cancelled"),
        )
        assert envelope["event_type"] == "pending_entry_cancelled"
        assert envelope["payload"]["reason"] == "manual override"

    def test_fill_envelope_from_trade_lineage(self):
        state = trade_state()
        state["trade_lineage"]["planned_r"] = Decimal("1.2")
        envelope = envelope_for_fill(
            state,
            occurred_at=INSTANT,
            idempotency_key=fill_key("pa:rose", "pt_1"),
            market_ref=second_ref(),
        )
        assert envelope["event_type"] == "paper_fill"
        assert envelope["payload"]["planned_r"] == "1.2"
        assert envelope["payload"]["quantity"] == "19.6"
        assert envelope["payload"]["filled_at"] == "2026-08-12T14:00:05Z"
        assert envelope["market_ref"]["kind"] == "second_event"

    def test_close_envelope_converts_decimal_settlement(self):
        state = trade_state(
            status="settled",
            exit_reason="target",
            closed_at="2026-08-12T15:00:00Z",
            selected_exit=Decimal("515.00"),
            settlement={
                "gross_pnl": Decimal("19.60"),
                "applied_pnl": Decimal("19.60"),
                "net_pnl": Decimal("19.60"),
                "balance_before": Decimal("1000.00"),
                "balance_after": Decimal("1019.60"),
            },
        )
        envelope = envelope_for_close(
            state,
            occurred_at=INSTANT,
            idempotency_key=close_key("pa:rose", "pt_1", "target", "2026-08-12T15:00:00Z"),
            market_ref=second_ref(),
        )
        assert envelope["event_type"] == "paper_close"
        assert envelope["payload"]["settlement"]["balance_after"] == "1019.60"
        assert envelope["payload"]["selected_exit"] == "515.00"

    def test_fill_rejected_envelope(self):
        envelope = envelope_for_fill_rejected(
            "pa:rose",
            3,
            reason="insufficient balance",
            pending_id="pe_1",
            occurred_at=INSTANT,
            idempotency_key=fill_rejected_key("pa:rose", "insufficient balance", "pe_1", None),
        )
        assert envelope["event_type"] == "paper_fill_rejected"
        assert envelope["payload"]["pending_id"] == "pe_1"

    def test_account_change_envelope_decimal_inputs(self):
        envelope = envelope_for_account_change(
            "pa:rose",
            0,
            balance_before=Decimal("1000.00"),
            balance_after=Decimal("950.00"),
            occurred_at=INSTANT,
            idempotency_key=account_change_key("pa:rose", Decimal("1000.00"), Decimal("950.00")),
        )
        assert envelope["event_type"] == "account_changed"
        assert envelope["payload"] == {"balance_before": "1000.00", "balance_after": "950.00"}

    def test_account_exhausted_envelope(self):
        envelope = envelope_for_account_exhausted(
            "pa:rose",
            0,
            balance=Decimal("0.00"),
            reason="balance exhausted",
            occurred_at=INSTANT,
            idempotency_key=account_exhausted_key("pa:rose", Decimal("0.00"), "balance exhausted"),
        )
        assert envelope["event_type"] == "account_exhausted"
        assert envelope["payload"]["balance"] == "0.00"

    def test_market_recovery_envelope(self):
        bars = [bar_ref(start_at="2026-08-12T14:00:00Z")]
        envelope = envelope_for_market_recovery(
            "SPY",
            bars=bars,
            recovery_cursor="2026-08-12T14:01:00Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:01:00Z", 1),
        )
        assert envelope["event_type"] == "market_recovery"
        assert envelope["strategy_id"] is None
        assert envelope["payload"]["bar_count"] == 1
        assert envelope["payload"]["feed_key"] == "alpaca:iex:single_exchange"

    def test_market_recovery_wrong_symbol_bar_raises(self):
        with pytest.raises(EnvelopeValidationError):
            envelope_for_market_recovery(
                "SPY",
                bars=[bar_ref(symbol="QQQ", start_at="2026-08-12T14:00:00Z")],
                recovery_cursor="2026-08-12T14:01:00Z",
                occurred_at=INSTANT,
                idempotency_key=recovery_key("SPY", "2026-08-12T14:01:00Z", 1),
            )

    def test_all_helpers_produce_validated_envelopes(self):
        for envelope in (
            envelope_for_state_transition(
                snapshot(),
                from_state="sleeping",
                to_state="analyzing",
                reason="r",
                occurred_at=INSTANT,
                idempotency_key="k1",
            ),
            envelope_for_schedule(
                snapshot(),
                timer_kind="sleep",
                due_at="2026-08-12T15:00:00Z",
                decision_accepted_at=INSTANT,
                occurred_at=INSTANT,
                idempotency_key="k2",
            ),
            envelope_for_pending_entry(
                pending_state(),
                "created",
                occurred_at=INSTANT,
                idempotency_key="k3",
            ),
            envelope_for_fill(
                trade_state(),
                occurred_at=INSTANT,
                idempotency_key="k4",
                market_ref=second_ref(),
            ),
            envelope_for_account_change(
                "pa:rose",
                0,
                balance_before="1000.00",
                balance_after="950.00",
                occurred_at=INSTANT,
                idempotency_key="k5",
            ),
        ):
            assert RuntimeEventEnvelope.from_dict(envelope).event_hash == envelope["event_hash"]


class TestEnsureAccounts:
    def test_two_accounts_start_at_exactly_1000(self, store):
        store.ensure_accounts(["pa:rose", "pa:caveman"])
        accounts = store.list_accounts()
        assert {account["strategy_id"] for account in accounts} == {"pa:rose", "pa:caveman"}
        assert all(account["balance"] == "1000.00" for account in accounts)
        assert all(account["initial_balance"] == "1000.00" for account in accounts)
        assert all(account["account_id"] == f"paper:{account['strategy_id']}" for account in accounts)

    def test_ensure_accounts_is_idempotent(self, store):
        store.ensure_accounts(["pa:rose"])
        store.update_account("pa:rose", balance="950.00")
        store.ensure_accounts(["pa:rose"])
        assert store.get_account("pa:rose")["balance"] == "950.00"
        assert len(store.list_accounts()) == 1

    def test_ensure_accounts_ignores_existing_rows(self, store):
        store.ensure_accounts(["pa:rose"])
        store.update_account("pa:rose", balance="500.00", exhausted=True)
        store.ensure_accounts(["pa:rose", "pa:caveman"])
        account = store.get_account("pa:rose")
        assert account["balance"] == "500.00"
        assert account["exhausted"] == 1
        assert account["initial_balance"] == "1000.00"

    def test_ensure_accounts_rejects_empty_id(self, store):
        with pytest.raises(InvalidStorageInputError):
            store.ensure_accounts(["pa:rose", ""])


class TestDurableBoundary:
    def test_materialized_json_rejects_recursive_provider_raw_and_broker_secret(self, store):
        with pytest.raises(SecretLeakError):
            store.commit_strategy_state(
                snapshot(latest_instruction={"nested": {"provider_raw_payload": {"body": "x"}}})
            )
        store.ensure_accounts(["pa:rose"])
        with pytest.raises(SecretLeakError):
            store.update_account(
                "pa:rose", balance="1000.00", open_trade={"nested": {"broker_secret": "x"}}
            )
        assert store.get_strategy_state("pa:rose") is None
        assert store.get_account("pa:rose")["open_trade"] is None

    def test_materialized_json_rejects_non_string_object_keys(self, store):
        with pytest.raises(InvalidStorageInputError):
            store.commit_strategy_state(snapshot(packet={1: "not-json"}))

    def test_legal_reason_value_is_not_a_secret_false_positive(self, store):
        store.ensure_accounts(["pa:rose"])
        envelope = envelope_for_account_exhausted(
            "pa:rose",
            0,
            balance="0.00",
            reason="manual credential rotation reason",
            occurred_at=INSTANT,
            idempotency_key="reason_only",
        )
        store.commit_account_exhausted(
            "pa:rose",
            balance="0.00",
            reason="manual credential rotation reason",
            envelope=envelope,
        )
        assert store.get_account("pa:rose")["exhausted"] == 1


class TestUpdateAccount:
    def test_update_balance_exact_cents_text(self, store):
        store.ensure_accounts(["pa:rose"])
        store.update_account("pa:rose", balance="950.25")
        assert store.get_account("pa:rose")["balance"] == "950.25"

    def test_update_balance_with_decimal(self, store):
        store.ensure_accounts(["pa:rose"])
        store.update_account("pa:rose", balance=Decimal("950.25"))
        assert store.get_account("pa:rose")["balance"] == "950.25"

    def test_bad_balance_rejected_without_change(self, store):
        store.ensure_accounts(["pa:rose"])
        with pytest.raises(InvalidStorageInputError):
            store.update_account("pa:rose", balance="not-money")
        assert store.get_account("pa:rose")["balance"] == "1000.00"

    def test_negative_balance_rejected(self, store):
        store.ensure_accounts(["pa:rose"])
        with pytest.raises(InvalidStorageInputError):
            store.update_account("pa:rose", balance="-5.00")
        assert store.get_account("pa:rose")["balance"] == "1000.00"

    def test_unknown_strategy_rejected(self, store):
        with pytest.raises(InvalidStorageInputError):
            store.update_account("pa:ghost", balance="950.00")

    def test_initial_balance_never_resets(self, store):
        store.ensure_accounts(["pa:rose"])
        store.update_account("pa:rose", balance="300.00")
        store.update_account("pa:rose", balance="120.50")
        account = store.get_account("pa:rose")
        assert account["initial_balance"] == "1000.00"
        assert account["balance"] == "120.50"

    def test_update_other_fields(self, store):
        store.ensure_accounts(["pa:rose"])
        trade = {"trade_id": "pt_1", "symbol": "SPY"}
        store.update_account(
            "pa:rose",
            balance="900.00",
            exhausted=False,
            open_trade=trade,
            pending_trade=None,
            pending=True,
        )
        account = store.get_account("pa:rose")
        assert account["exhausted"] == 0
        assert account["pending"] == 1
        assert account["open_trade"] == trade

    def test_updated_at_follows_clock(self, store, db):
        _handle, clock = db
        store.ensure_accounts(["pa:rose"])
        first = store.get_account("pa:rose")["updated_at"]
        clock.advance(30)
        store.update_account("pa:rose", balance="900.00")
        second = store.get_account("pa:rose")["updated_at"]
        assert second != first

    def test_get_account_missing_returns_none(self, store):
        assert store.get_account("pa:ghost") is None

    def test_list_accounts_parses_json_columns(self, store):
        store.ensure_accounts(["pa:rose"])
        store.update_account("pa:rose", balance="900.00", open_trade={"trade_id": "pt_1"})
        assert store.list_accounts()[0]["open_trade"] == {"trade_id": "pt_1"}


class TestStrategyState:
    def test_commit_insert_then_get(self, store):
        store.commit_strategy_state(snapshot(state="sleeping"))
        saved = store.get_strategy_state("pa:rose")
        assert saved["strategy_id"] == "pa:rose"
        assert saved["state"] == "sleeping"
        assert saved["generation"] == 3
        assert saved["enabled"] == 1

    def test_commit_update_changes_state(self, store):
        store.commit_strategy_state(snapshot(state="sleeping"))
        store.commit_strategy_state(snapshot(state="analyzing", current_view_id="view_1"))
        saved = store.get_strategy_state("pa:rose")
        assert saved["state"] == "analyzing"
        assert saved["current_view_id"] == "view_1"
        assert len(store.list_strategy_states()) == 1

    def test_semantic_retry_key_split_and_roundtrip(self, store):
        store.commit_strategy_state(snapshot(semantic_retry_key=["hash_a", "snap_b"]))
        assert store.get_strategy_state("pa:rose")["semantic_retry_key"] == ["hash_a", "snap_b"]

    def test_semantic_retry_key_none_roundtrip(self, store):
        store.commit_strategy_state(snapshot(semantic_retry_key=None))
        assert store.get_strategy_state("pa:rose")["semantic_retry_key"] is None

    def test_json_columns_roundtrip(self, store):
        packet = {"schema_version": "0.1.0", "strategy_id": "pa:rose"}
        latest = {"instruction_id": "sdi_" + H64, "branch": "no_entry"}
        store.commit_strategy_state(snapshot(packet=packet, latest_instruction=latest))
        saved = store.get_strategy_state("pa:rose")
        assert saved["packet"] == packet
        assert saved["latest_instruction"] == latest

    def test_commit_with_transition_and_envelope(self, store):
        snap = snapshot(state="analyzing")
        envelope = envelope_for_state_transition(
            snap,
            from_state="sleeping",
            to_state="analyzing",
            reason="timer fired",
            occurred_at=INSTANT,
            idempotency_key=state_transition_key("pa:rose", 3, "analyzing", "timer fired"),
        )
        store.commit_strategy_state(
            snap,
            transition={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
            envelope=envelope,
        )
        events = store.list_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "strategy_state_changed"
        assert store.get_strategy_state("pa:rose")["state"] == "analyzing"

    def test_generation_mismatch_conflict(self, store):
        envelope = envelope_for_state_transition(
            snapshot(generation=3),
            from_state="sleeping",
            to_state="analyzing",
            reason="timer fired",
            occurred_at=INSTANT,
            idempotency_key="idem_gen",
        )
        with pytest.raises(StorageConflictError):
            store.commit_strategy_state(
                snapshot(generation=4, state="analyzing"),
                transition={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
                envelope=envelope,
            )
        assert store.list_events() == []

    def test_profile_hash_mismatch_conflict(self, store):
        envelope = RuntimeEventEnvelope.make(
            "strategy_state_changed",
            strategy_id="pa:rose",
            generation=3,
            occurred_at=INSTANT,
            idempotency_key="idem_profile",
            payload={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
            profile_ref={"profile_ref_id": "spr_" + H64B, "profile_ref_hash": H64B},
        ).to_dict()
        snap = snapshot(
            state="analyzing",
            profile_ref={"profile_ref_id": "spr_" + H64, "profile_ref_hash": H64},
        )
        with pytest.raises(StorageConflictError):
            store.commit_strategy_state(
                snap,
                transition={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
                envelope=envelope,
            )
        assert store.list_events() == []

    def test_transition_strategy_and_profile_presence_are_bound(self, store):
        snap = snapshot(state="analyzing")
        other = envelope_for_state_transition(
            snapshot(strategy_id="pa:other"),
            from_state="sleeping",
            to_state="analyzing",
            reason="timer fired",
            occurred_at=INSTANT,
            idempotency_key="idem_other_transition",
        )
        with pytest.raises(StorageConflictError):
            store.commit_strategy_state(
                snap,
                transition={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
                envelope=other,
            )
        profile = {"profile_ref_id": "spr_" + H64, "profile_ref_hash": H64}
        no_profile = envelope_for_state_transition(
            snapshot(),
            from_state="sleeping",
            to_state="analyzing",
            reason="timer fired",
            occurred_at=INSTANT,
            idempotency_key="idem_missing_profile",
        )
        with pytest.raises(StorageConflictError):
            store.commit_strategy_state(
                snapshot(state="analyzing", profile_ref=profile),
                transition={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
                envelope=no_profile,
            )
        assert store.list_events() == []

    def test_transition_requires_envelope(self, store):
        with pytest.raises(InvalidStorageInputError):
            store.commit_strategy_state(
                snapshot(state="analyzing"),
                transition={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
            )

    def test_invalid_state_vocabulary_rejected(self, store):
        with pytest.raises(InvalidStorageInputError):
            store.commit_strategy_state(snapshot(state="flying"))

    def test_transition_timer_failure_rolls_back_everything(self, store):
        store.commit_strategy_state(snapshot(state="sleeping"))
        snap = snapshot(state="analyzing")
        envelope = envelope_for_state_transition(
            snap,
            from_state="sleeping",
            to_state="analyzing",
            reason="timer fired",
            occurred_at=INSTANT,
            idempotency_key="idem_atomic",
        )
        with pytest.raises(InvalidStorageInputError):
            store.commit_strategy_state(
                snap,
                transition={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
                envelope=envelope,
                timers=[{"kind": "bogus", "generation": 1, "due_at": INSTANT}],
            )
        assert store.get_strategy_state("pa:rose")["state"] == "sleeping"
        assert store.list_events() == []
        assert store.get_timers("pa:rose") == []

    def test_commit_state_with_timers_and_clear(self, store):
        store.commit_strategy_state(
            snapshot(),
            timers=[
                {"kind": "sleep", "generation": 3, "due_at": "2026-08-12T15:00:00Z", "active": True, "fired": False},
                {"kind": "retry", "generation": 3, "due_at": "2026-08-12T16:00:00Z", "active": True, "fired": False},
            ],
        )
        assert {timer["kind"] for timer in store.get_timers("pa:rose")} == {"sleep", "retry"}
        store.commit_strategy_state(snapshot(), clear_timers=["sleep"])
        assert {timer["kind"] for timer in store.get_timers("pa:rose")} == {"retry"}

    def test_last_transition_at_preserved_on_plain_update(self, store):
        store.commit_strategy_state(
            snapshot(state="analyzing"),
            transition={"from": "sleeping", "to": "analyzing", "reason": "timer fired", "resume_state": None},
            envelope=envelope_for_state_transition(
                snapshot(state="analyzing"),
                from_state="sleeping",
                to_state="analyzing",
                reason="timer fired",
                occurred_at=INSTANT,
                idempotency_key="idem_preserve",
            ),
        )
        first = store.get_strategy_state("pa:rose")["last_transition_at"]
        store.commit_strategy_state(snapshot(state="analyzing", current_view_id="view_9"))
        second = store.get_strategy_state("pa:rose")["last_transition_at"]
        assert second == first

    def test_invalid_snapshot_flag_rejected(self, store):
        with pytest.raises(InvalidStorageInputError):
            store.commit_strategy_state(snapshot(state="sleeping", enabled=1))


class TestTimers:
    def test_save_and_get_timers(self, store):
        store.save_timers(
            "pa:rose",
            [
                {"kind": "sleep", "generation": 3, "due_at": "2026-08-12T15:00:00Z", "active": True, "fired": False},
                {"kind": "pending_expiry", "generation": 3, "due_at": "2026-08-12T14:05:00Z", "active": True, "fired": False},
            ],
        )
        timers = store.get_timers("pa:rose")
        assert {timer["kind"] for timer in timers} == {"sleep", "pending_expiry"}
        sleep = next(timer for timer in timers if timer["kind"] == "sleep")
        assert sleep["generation"] == 3
        assert sleep["active"] == 1
        assert sleep["fired"] == 0

    def test_save_timers_upserts_by_kind(self, store):
        store.save_timers(
            "pa:rose",
            [{"kind": "sleep", "generation": 2, "due_at": "2026-08-12T14:30:00Z", "active": True, "fired": False}],
        )
        store.save_timers(
            "pa:rose",
            [{"kind": "sleep", "generation": 3, "due_at": "2026-08-12T15:00:00Z", "active": True, "fired": False}],
        )
        timers = store.get_timers("pa:rose")
        assert len(timers) == 1
        assert timers[0]["due_at"] == "2026-08-12T15:00:00Z"
        assert timers[0]["generation"] == 3

    def test_clear_timer_only_removes_one_kind(self, store):
        store.save_timers(
            "pa:rose",
            [
                {"kind": "sleep", "generation": 3, "due_at": "2026-08-12T15:00:00Z", "active": True, "fired": False},
                {"kind": "retry", "generation": 3, "due_at": "2026-08-12T16:00:00Z", "active": True, "fired": False},
            ],
        )
        store.clear_timer("pa:rose", "sleep")
        kinds = {timer["kind"] for timer in store.get_timers("pa:rose")}
        assert kinds == {"retry"}

    def test_due_at_stored_canonical_from_datetime(self, store):
        due = datetime(2026, 8, 12, 15, 0, 0, tzinfo=timezone.utc)
        store.save_timers(
            "pa:rose",
            [{"kind": "sleep", "generation": 3, "due_at": due, "active": True, "fired": False}],
        )
        assert store.get_timers("pa:rose")[0]["due_at"] == "2026-08-12T15:00:00Z"

    def test_invalid_timer_kind_rejected(self, store):
        with pytest.raises(InvalidStorageInputError):
            store.save_timers(
                "pa:rose",
                [{"kind": "bogus", "generation": 3, "due_at": INSTANT, "active": True, "fired": False}],
            )
        assert store.get_timers("pa:rose") == []

    def test_timers_isolated_per_strategy(self, store):
        store.save_timers(
            "pa:rose",
            [{"kind": "sleep", "generation": 3, "due_at": "2026-08-12T15:00:00Z", "active": True, "fired": False}],
        )
        assert store.get_timers("pa:caveman") == []


class TestPendingEntries:
    def test_commit_pending_created(self, store):
        state = pending_state()
        envelope = envelope_for_pending_entry(
            state,
            "created",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "created"),
        )
        store.commit_pending_entry(state, event="created", envelope=envelope)
        saved = store.get_pending_entry("pe_1")
        assert saved["status"] == "pending"
        assert saved["symbol"] == "SPY"
        assert saved["entry_price"] == "510.25"
        assert saved["state_hash"] == envelope["payload"]["state_hash"]
        assert saved["state_json"]["strategy_id"] == "pa:rose"

    def test_commit_pending_expired(self, store):
        state = pending_state(status="expired")
        envelope = envelope_for_pending_entry(
            state,
            "expired",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "expired"),
        )
        store.commit_pending_entry(state, event="expired", envelope=envelope)
        assert store.get_pending_entry("pe_1")["status"] == "expired"

    def test_commit_pending_cancelled(self, store):
        state = pending_state(status="cancelled", terminal_reason="manual")
        envelope = envelope_for_pending_entry(
            state,
            "cancelled",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "cancelled"),
        )
        store.commit_pending_entry(state, event="cancelled", envelope=envelope)
        assert store.get_pending_entry("pe_1")["status"] == "cancelled"

    def test_pending_envelope_type_mismatch_rejected(self, store):
        state = pending_state()
        envelope = envelope_for_pending_entry(
            state,
            "created",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "created"),
        )
        with pytest.raises(InvalidStorageInputError):
            store.commit_pending_entry(state, event="expired", envelope=envelope)
        assert store.get_pending_entry("pe_1") is None

    def test_pending_cross_strategy_envelope_rejected(self, store):
        state = pending_state(strategy_id="pa:rose")
        envelope = envelope_for_pending_entry(
            pending_state(strategy_id="pa:other"),
            "created",
            occurred_at=INSTANT,
            idempotency_key="idem_other",
        )
        with pytest.raises(StorageConflictError):
            store.commit_pending_entry(state, event="created", envelope=envelope)
        assert store.get_pending_entry("pe_1") is None

    def test_pending_state_hash_stable_then_changes(self, store):
        first = pending_state()
        envelope = envelope_for_pending_entry(
            first,
            "created",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "created"),
        )
        store.commit_pending_entry(first, event="created", envelope=envelope)
        saved = store.get_pending_entry("pe_1")
        assert saved["state_hash"] == envelope["payload"]["state_hash"]
        assert saved["state_hash"] == hashlib.sha256(canonical_json_bytes(first)).hexdigest()
        second = pending_state(status="expired", partial_data=True)
        envelope2 = envelope_for_pending_entry(
            second,
            "expired",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "expired"),
        )
        store.commit_pending_entry(second, event="expired", envelope=envelope2)
        updated = store.get_pending_entry("pe_1")
        assert updated["state_hash"] != saved["state_hash"]
        assert updated["state_hash"] == hashlib.sha256(canonical_json_bytes(second)).hexdigest()

    def test_pending_event_payload_must_match_snapshot_hash(self, store):
        state = pending_state()
        envelope = envelope_for_pending_entry(
            pending_state(partial_data=True),
            "created",
            occurred_at=INSTANT,
            idempotency_key="pending_payload_hash_mismatch",
        )
        with pytest.raises(StorageConflictError):
            store.commit_pending_entry(state, event="created", envelope=envelope)
        assert store.get_pending_entry("pe_1") is None

    def test_pending_flags_roundtrip(self, store):
        state = pending_state(gap_overlap=True, partial_data=True, recovery_attempted=True)
        envelope = envelope_for_pending_entry(
            state,
            "created",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "created"),
        )
        store.commit_pending_entry(state, event="created", envelope=envelope)
        assert store.get_pending_entry("pe_1")["flags_json"] == {
            "gap_overlap": True,
            "partial_data": True,
            "recovery_attempted": True,
        }

    def test_list_pending_filters(self, store):
        rose = pending_state()
        cave = pending_state(strategy_id="pa:caveman", pending_id="pe_2", symbol="QQQ")
        for state in (rose, cave):
            envelope = envelope_for_pending_entry(
                state,
                "created",
                occurred_at=INSTANT,
                idempotency_key=pending_key(state["strategy_id"], state["pending_id"], "created"),
            )
            store.commit_pending_entry(state, event="created", envelope=envelope)
        assert {row["pending_id"] for row in store.list_pending_entries(strategy_id="pa:rose")} == {"pe_1"}
        assert len(store.list_pending_entries(status="pending")) == 2
        assert store.list_pending_entries(strategy_id="pa:rose", status="expired") == []

    def test_get_pending_missing_returns_none(self, store):
        assert store.get_pending_entry("pe_none") is None

    def test_commit_pending_upserts_same_row(self, store):
        created = pending_state()
        envelope = envelope_for_pending_entry(
            created,
            "created",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "created"),
        )
        store.commit_pending_entry(created, event="created", envelope=envelope)
        expired = pending_state(status="expired")
        envelope2 = envelope_for_pending_entry(
            expired,
            "expired",
            occurred_at=INSTANT,
            idempotency_key=pending_key("pa:rose", "pe_1", "expired"),
        )
        store.commit_pending_entry(expired, event="expired", envelope=envelope2)
        assert store.get_pending_entry("pe_1")["status"] == "expired"
        assert len(store.list_pending_entries()) == 1

    def test_original_created_event_replay_materializes_filled_pending_once(self, store):
        pending = pending_state()
        envelope = envelope_for_pending_entry(
            pending, "created", occurred_at=INSTANT, idempotency_key="pending_created_once"
        )
        store.commit_pending_entry(pending, event="created", envelope=envelope)
        filled = pending_state(status="filled", filled_at="2026-08-12T14:00:05Z")
        store.commit_pending_entry(filled, event="created", envelope=envelope)
        before = store.get_pending_entry("pe_1")["state_json"]
        store.commit_pending_entry(
            pending_state(status="filled", filled_at="2026-08-12T14:00:05Z", partial_data=True),
            event="created",
            envelope=envelope,
        )
        assert store.get_pending_entry("pe_1")["status"] == "filled"
        assert store.get_pending_entry("pe_1")["state_json"] == before
        assert len(store.list_events(event_type="pending_entry_created")) == 1


class TestTrades:
    def test_commit_trade_filled_is_open(self, store):
        state = trade_state()
        envelope = envelope_for_fill(
            state,
            occurred_at=INSTANT,
            idempotency_key=fill_key("pa:rose", "pt_1"),
            market_ref=second_ref(),
        )
        store.commit_trade(state, event="filled", envelope=envelope)
        saved = store.get_trade("pt_1")
        assert saved["status"] == "open"
        assert saved["symbol"] == "SPY"
        assert saved["quantity"] == "19.6"
        assert saved["opened_at"] == "2026-08-12T14:00:05Z"
        assert saved["trade_json"]["trade_id"] == "pt_1"

    def test_commit_trade_closed_is_settled(self, store):
        state = trade_state(
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
        )
        envelope = envelope_for_close(
            state,
            occurred_at=INSTANT,
            idempotency_key=close_key("pa:rose", "pt_1", "target", "2026-08-12T15:00:00Z"),
            market_ref=second_ref(),
        )
        store.commit_trade(state, event="closed", envelope=envelope)
        saved = store.get_trade("pt_1")
        assert saved["status"] == "settled"
        assert saved["exit_reason"] == "target"
        assert saved["closed_at"] == "2026-08-12T15:00:00Z"
        assert saved["settlement_json"]["balance_after"] == "1019.60"

    def test_replayed_fill_after_close_cannot_reopen_trade(self, store):
        opened = trade_state()
        fill = envelope_for_fill(
            opened, occurred_at=INSTANT, idempotency_key="fill_once", market_ref=second_ref()
        )
        store.commit_trade(opened, event="filled", envelope=fill)
        closed = trade_state(
            status="settled",
            exit_reason="target",
            closed_at="2026-08-12T15:00:00Z",
            settlement={"net_pnl": "19.60", "balance_before": "1000.00", "balance_after": "1019.60"},
        )
        close = envelope_for_close(
            closed, occurred_at=INSTANT, idempotency_key="close_once", market_ref=second_ref()
        )
        store.commit_trade(closed, event="closed", envelope=close)
        store.commit_trade(opened, event="filled", envelope=fill)
        assert store.get_trade("pt_1")["status"] == "settled"
        assert len(store.list_events()) == 2

    def test_trade_planned_r_from_trade_lineage(self, store):
        state = trade_state()
        state["trade_lineage"]["planned_r"] = "2.5"
        envelope = envelope_for_fill(
            state,
            occurred_at=INSTANT,
            idempotency_key=fill_key("pa:rose", "pt_1"),
            market_ref=second_ref(),
        )
        store.commit_trade(state, event="filled", envelope=envelope)
        assert store.get_trade("pt_1")["planned_r"] == "2.5"

    def test_trade_cross_strategy_rejected(self, store):
        state = trade_state(strategy_id="pa:rose")
        envelope = envelope_for_fill(
            trade_state(strategy_id="pa:other"),
            occurred_at=INSTANT,
            idempotency_key="idem_other_fill",
            market_ref=second_ref(),
        )
        with pytest.raises(StorageConflictError):
            store.commit_trade(state, event="filled", envelope=envelope)
        assert store.get_trade("pt_1") is None

    def test_trade_event_mismatch_rejected(self, store):
        state = trade_state()
        envelope = envelope_for_fill(
            state,
            occurred_at=INSTANT,
            idempotency_key=fill_key("pa:rose", "pt_1"),
            market_ref=second_ref(),
        )
        with pytest.raises(InvalidStorageInputError):
            store.commit_trade(state, event="closed", envelope=envelope)

    def test_trade_event_payload_must_match_snapshot(self, store):
        state = trade_state()
        envelope = envelope_for_fill(
            trade_state(quantity="20.0"),
            occurred_at=INSTANT,
            idempotency_key="trade_payload_mismatch",
            market_ref=second_ref(),
        )
        with pytest.raises(EnvelopeValidationError):
            store.commit_trade(state, event="filled", envelope=envelope)
        assert store.get_trade("pt_1") is None

    def test_trade_flags_roundtrip(self, store):
        state = trade_state(gap_active=True, partial_data=True, recovery_attempted=True)
        envelope = envelope_for_fill(
            state,
            occurred_at=INSTANT,
            idempotency_key=fill_key("pa:rose", "pt_1"),
            market_ref=second_ref(),
        )
        store.commit_trade(state, event="filled", envelope=envelope)
        assert store.get_trade("pt_1")["flags_json"] == {
            "gap_active": True,
            "partial_data": True,
            "recovery_attempted": True,
        }

    def test_get_trade_missing_returns_none(self, store):
        assert store.get_trade("pt_none") is None

    def test_list_trades_filters(self, store):
        rose = trade_state()
        cave = trade_state(strategy_id="pa:caveman", trade_id="pt_2", symbol="QQQ")
        for state in (rose, cave):
            envelope = envelope_for_fill(
                state,
                occurred_at=INSTANT,
                idempotency_key=fill_key(state["strategy_id"], state["trade_id"]),
                market_ref=second_ref(symbol=state["symbol"]),
            )
            store.commit_trade(state, event="filled", envelope=envelope)
        assert {row["trade_id"] for row in store.list_trades(strategy_id="pa:rose")} == {"pt_1"}
        assert len(store.list_trades(status="open")) == 2
        assert store.list_trades(strategy_id="pa:rose", status="settled") == []

    def test_commit_trade_upserts_same_row(self, store):
        state = trade_state()
        envelope = envelope_for_fill(
            state,
            occurred_at=INSTANT,
            idempotency_key=fill_key("pa:rose", "pt_1"),
            market_ref=second_ref(),
        )
        store.commit_trade(state, event="filled", envelope=envelope)
        closed = trade_state(
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
        )
        envelope2 = envelope_for_close(
            closed,
            occurred_at=INSTANT,
            idempotency_key=close_key("pa:rose", "pt_1", "stop", "2026-08-12T15:00:00Z"),
            market_ref=second_ref(),
        )
        store.commit_trade(closed, event="closed", envelope=envelope2)
        assert store.get_trade("pt_1")["status"] == "settled"
        assert len(store.list_trades()) == 1


class TestAppendEvent:
    def test_append_event_returns_appended(self, store):
        envelope = envelope_for_schedule(
            snapshot(),
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            occurred_at=INSTANT,
            idempotency_key="idem_1",
        )
        assert store.append_event(envelope) == "appended"
        events = store.list_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "schedule_set"
        assert events[0]["envelope"]["payload"]["timer_kind"] == "sleep"

    def test_append_event_exact_replay_is_noop(self, store):
        envelope = envelope_for_schedule(
            snapshot(),
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            occurred_at=INSTANT,
            idempotency_key="idem_replay",
        )
        assert store.append_event(envelope) == "appended"
        assert store.append_event(envelope) == "replayed"
        assert len(store.list_events()) == 1

    def test_same_key_different_event_conflict(self, store):
        first = envelope_for_schedule(
            snapshot(),
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            occurred_at=INSTANT,
            idempotency_key="idem_conflict",
        )
        second = envelope_for_schedule(
            snapshot(),
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            occurred_at="2026-08-12T14:00:30Z",
            idempotency_key="idem_conflict",
        )
        store.append_event(first)
        with pytest.raises(StorageConflictError):
            store.append_event(second)
        assert len(store.list_events()) == 1

    def test_invalid_envelope_rejected(self, store):
        with pytest.raises(EnvelopeValidationError):
            store.append_event({"event_type": "bogus"})
        with pytest.raises(EnvelopeValidationError):
            store.append_event("not-an-envelope")
        assert store.list_events() == []

    def test_append_event_accepts_envelope_instance(self, store):
        envelope = envelope_for_account_change(
            "pa:rose",
            0,
            balance_before="1000.00",
            balance_after="950.00",
            occurred_at=INSTANT,
            idempotency_key="idem_instance",
        )
        instance = RuntimeEventEnvelope.from_dict(envelope)
        assert store.append_event(instance) == "appended"

    def test_append_event_modes_stored_distinctly(self, store):
        record = snapshot()
        automatic = envelope_for_schedule(
            record,
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            occurred_at=INSTANT,
            idempotency_key="idem_auto",
        )
        replay = envelope_for_schedule(
            record,
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            mode="replay",
            occurred_at=INSTANT,
            idempotency_key="idem_replay_mode",
        )
        store.append_event(automatic)
        store.append_event(replay)
        modes = {event["mode"] for event in store.list_events()}
        assert modes == {"automatic", "replay"}

    def test_market_recovery_event_has_null_strategy(self, store):
        bars = [bar_ref(start_at="2026-08-12T14:00:00Z")]
        envelope = envelope_for_market_recovery(
            "SPY",
            bars=bars,
            recovery_cursor="2026-08-12T14:01:00Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:01:00Z", 1),
        )
        store.append_event(envelope)
        event = store.list_events()[0]
        assert event["strategy_id"] is None
        assert event["event_type"] == "market_recovery"

    def test_list_events_filters(self, store):
        record = snapshot()
        for timer_kind, idem in (("sleep", "idem_f1"), ("retry", "idem_f2")):
            store.append_event(
                envelope_for_schedule(
                    record,
                    timer_kind=timer_kind,
                    due_at="2026-08-12T15:00:00Z",
                    decision_accepted_at=INSTANT,
                    occurred_at=INSTANT,
                    idempotency_key=idem,
                )
            )
        assert len(store.list_events(event_type="schedule_set")) == 2
        assert len(store.list_events(mode="replay")) == 0
        assert len(store.list_events(strategy_id="pa:other")) == 0
        assert len(store.list_events(strategy_id="pa:rose")) == 2

    def test_event_stream_orders_by_occurred_at(self, store):
        record = snapshot()
        for minutes, idem in ((5, "idem_o1"), (1, "idem_o2"), (3, "idem_o3")):
            store.append_event(
                envelope_for_schedule(
                    record,
                    timer_kind="sleep",
                    due_at=instant_to_text(instant_from_text(INSTANT) + timedelta(minutes=minutes)),
                    decision_accepted_at=INSTANT,
                    occurred_at=instant_to_text(
                        instant_from_text(INSTANT) + timedelta(minutes=minutes)
                    ),
                    idempotency_key=idem,
                )
            )
        streamed = [event["occurred_at"] for event in store.event_stream()]
        assert streamed == sorted(streamed)
        assert len(streamed) == 3


class TestAtomicity:
    def test_mid_transaction_crash_keeps_old_state(self, store, monkeypatch):
        store.ensure_accounts(["pa:rose"])
        envelope = envelope_for_account_change(
            "pa:rose",
            0,
            balance_before=Decimal("1000.00"),
            balance_after=Decimal("950.00"),
            occurred_at=INSTANT,
            idempotency_key=account_change_key("pa:rose", Decimal("1000.00"), Decimal("950.00")),
        )
        real = store._append_event

        def failing(conn, document):
            real(conn, document)
            raise RuntimeError("simulated crash")

        monkeypatch.setattr(store, "_append_event", failing)
        with pytest.raises(RuntimeError):
            store.commit_account_event(
                "pa:rose",
                balance_before=Decimal("1000.00"),
                balance_after=Decimal("950.00"),
                envelope=envelope,
            )
        assert store.get_account("pa:rose")["balance"] == "1000.00"
        assert store.list_events() == []

    def test_database_still_usable_after_rollback(self, store, monkeypatch):
        store.ensure_accounts(["pa:rose"])
        envelope = envelope_for_account_change(
            "pa:rose",
            0,
            balance_before=Decimal("1000.00"),
            balance_after=Decimal("950.00"),
            occurred_at=INSTANT,
            idempotency_key=account_change_key("pa:rose", Decimal("1000.00"), Decimal("950.00")),
        )
        real = store._append_event

        def failing(conn, document):
            real(conn, document)
            raise RuntimeError("simulated crash")

        monkeypatch.setattr(store, "_append_event", failing)
        with pytest.raises(RuntimeError):
            store.commit_account_event(
                "pa:rose",
                balance_before=Decimal("1000.00"),
                balance_after=Decimal("950.00"),
                envelope=envelope,
            )
        monkeypatch.undo()
        assert store.commit_account_event(
            "pa:rose",
            balance_before=Decimal("1000.00"),
            balance_after=Decimal("950.00"),
            envelope=envelope,
        ) is None
        assert store.get_account("pa:rose")["balance"] == "950.00"
        assert len(store.list_events()) == 1


class TestCursors:
    def test_advance_cursor_forward(self, store):
        assert store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        cursor = store.get_cursor("SPY")
        assert cursor["last_second_end_at"] == "2026-08-12T14:00:05Z"
        assert cursor["last_second_event_id"] == "sec_" + H64
        assert cursor["feed_key"] == "alpaca:iex:single_exchange"
        assert store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:06Z"))
        assert store.get_cursor("SPY")["last_second_end_at"] == "2026-08-12T14:00:06Z"

    def test_advance_cursor_same_second_idempotent(self, store):
        ref = second_ref(end_at="2026-08-12T14:00:05Z")
        store.advance_cursor("SPY", ref)
        assert store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        assert store.get_cursor("SPY")["last_second_end_at"] == "2026-08-12T14:00:05Z"
        assert len(store.list_cursors()) == 1

    def test_advance_cursor_backward_recovery_error(self, store):
        store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:07Z"))
        with pytest.raises(RecoveryStateError):
            store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        assert store.get_cursor("SPY")["last_second_end_at"] == "2026-08-12T14:00:07Z"

    def test_advance_cursor_rejects_non_second_ref(self, store):
        with pytest.raises(EnvelopeValidationError):
            store.advance_cursor("SPY", bar_ref(start_at="2026-08-12T14:00:00Z"))

    def test_advance_cursor_rejects_symbol_mismatch(self, store):
        with pytest.raises(EnvelopeValidationError):
            store.advance_cursor("QQQ", second_ref(symbol="SPY", end_at="2026-08-12T14:00:05Z"))

    def test_advance_cursor_same_end_at_new_identity_updates(self, store):
        store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        updated = second_ref(
            end_at="2026-08-12T14:00:05Z",
            second_event_id="sec_" + H64B,
            second_event_hash=H64B,
        )
        store.advance_cursor("SPY", updated)
        assert store.get_cursor("SPY")["last_second_event_hash"] == H64B

    def test_get_cursor_missing_returns_none(self, store):
        assert store.get_cursor("QQQ") is None

    def test_mark_live_blocked_and_is_live_blocked(self, store):
        assert store.is_live_blocked("SPY") is False
        store.mark_live_blocked("SPY", reason="feed disconnected")
        assert store.is_live_blocked("SPY") is True
        cursor = store.get_cursor("SPY")
        assert cursor["pending_recovery_json"] == {"reason": "feed disconnected"}
        assert store.is_live_blocked("QQQ") is False

    def test_mark_live_blocked_rejects_empty_reason(self, store):
        with pytest.raises(InvalidStorageInputError):
            store.mark_live_blocked("SPY", reason="")
        assert store.is_live_blocked("SPY") is False


class TestRecovery:
    def test_commit_recovery_clears_block_and_appends_event(self, store):
        store.mark_live_blocked("SPY", reason="feed disconnected")
        store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        bars = [bar_ref(start_at="2026-08-12T14:00:05Z")]
        envelope = envelope_for_market_recovery(
            "SPY",
            bars=bars,
            recovery_cursor="2026-08-12T14:01:05Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:01:05Z", 1),
        )
        store.commit_recovery("SPY", bars=bars, recovery_cursor="2026-08-12T14:01:05Z", envelope=envelope)
        assert store.is_live_blocked("SPY") is False
        cursor = store.get_cursor("SPY")
        assert cursor["recovery_cursor"] == "2026-08-12T14:01:05Z"
        assert cursor["pending_recovery_json"] is None
        events = store.list_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "market_recovery"

    def test_commit_recovery_non_chronological_bars_rejected(self, store):
        bars = [
            bar_ref(start_at="2026-08-12T14:00:05Z"),
            bar_ref(
                start_at="2026-08-12T14:02:05Z",
                recovery_bar_id="rbar_" + H64B,
                recovery_bar_hash=H64B,
            ),
        ]
        envelope = envelope_for_market_recovery(
            "SPY",
            bars=bars,
            recovery_cursor="2026-08-12T14:03:05Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:03:05Z", 2),
        )
        with pytest.raises(RecoveryStateError):
            store.commit_recovery("SPY", bars=bars, recovery_cursor="2026-08-12T14:03:05Z", envelope=envelope)
        assert store.list_events() == []

    def test_commit_recovery_wrong_symbol_rejected(self, store):
        bars = [bar_ref(symbol="QQQ", start_at="2026-08-12T14:00:00Z")]
        envelope = envelope_for_market_recovery(
            "QQQ",
            bars=bars,
            recovery_cursor="2026-08-12T14:01:00Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("QQQ", "2026-08-12T14:01:00Z", 1),
        )
        with pytest.raises(EnvelopeValidationError):
            store.commit_recovery("SPY", bars=bars, recovery_cursor="2026-08-12T14:01:00Z", envelope=envelope)
        assert store.list_events() == []

    def test_commit_recovery_bars_must_match_envelope_payload(self, store):
        envelope_bars = [bar_ref(start_at="2026-08-12T14:00:05Z")]
        supplied_bars = [
            bar_ref(
                start_at="2026-08-12T14:00:05Z",
                recovery_bar_id="rbar_" + H64B,
                recovery_bar_hash=H64B,
            )
        ]
        envelope = envelope_for_market_recovery(
            "SPY",
            bars=envelope_bars,
            recovery_cursor="2026-08-12T14:01:05Z",
            occurred_at=INSTANT,
            idempotency_key="recovery_payload_mismatch",
        )
        with pytest.raises(InvalidStorageInputError):
            store.commit_recovery(
                "SPY",
                bars=supplied_bars,
                recovery_cursor="2026-08-12T14:01:05Z",
                envelope=envelope,
            )
        assert store.list_events() == []

    def test_commit_recovery_identical_replay_noop(self, store):
        bars = [bar_ref(start_at="2026-08-12T14:00:05Z")]
        envelope = envelope_for_market_recovery(
            "SPY",
            bars=bars,
            recovery_cursor="2026-08-12T14:01:05Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:01:05Z", 1),
        )
        store.commit_recovery("SPY", bars=bars, recovery_cursor="2026-08-12T14:01:05Z", envelope=envelope)
        store.commit_recovery("SPY", bars=bars, recovery_cursor="2026-08-12T14:01:05Z", envelope=envelope)
        assert len(store.list_events()) == 1
        assert store.get_cursor("SPY")["recovery_cursor"] == "2026-08-12T14:01:05Z"

    def test_commit_recovery_conflicting_key_rejected(self, store):
        first_bars = [bar_ref(start_at="2026-08-12T14:00:05Z")]
        first = envelope_for_market_recovery(
            "SPY",
            bars=first_bars,
            recovery_cursor="2026-08-12T14:01:05Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:01:05Z", 1),
        )
        store.commit_recovery("SPY", bars=first_bars, recovery_cursor="2026-08-12T14:01:05Z", envelope=first)
        second_bars = [
            bar_ref(
                start_at="2026-08-12T14:00:05Z",
                recovery_bar_id="rbar_" + H64B,
                recovery_bar_hash=H64B,
            )
        ]
        second = envelope_for_market_recovery(
            "SPY",
            bars=second_bars,
            recovery_cursor="2026-08-12T14:01:05Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:01:05Z", 1),
        )
        with pytest.raises(StorageConflictError):
            store.commit_recovery("SPY", bars=second_bars, recovery_cursor="2026-08-12T14:01:05Z", envelope=second)
        assert len(store.list_events()) == 1

    def test_commit_recovery_first_bar_before_stored_second_rejected(self, store):
        store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:10Z"))
        bars = [bar_ref(start_at="2026-08-12T14:00:05Z")]
        envelope = envelope_for_market_recovery(
            "SPY",
            bars=bars,
            recovery_cursor="2026-08-12T14:01:05Z",
            occurred_at=INSTANT,
            idempotency_key=recovery_key("SPY", "2026-08-12T14:01:05Z", 1),
        )
        with pytest.raises(RecoveryStateError):
            store.commit_recovery("SPY", bars=bars, recovery_cursor="2026-08-12T14:01:05Z", envelope=envelope)
        assert store.list_events() == []

    def test_commit_recovery_wrong_envelope_type_rejected(self, store):
        bars = [bar_ref(start_at="2026-08-12T14:00:05Z")]
        envelope = envelope_for_schedule(
            snapshot(),
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            occurred_at=INSTANT,
            idempotency_key="idem_not_recovery",
        )
        with pytest.raises(InvalidStorageInputError):
            store.commit_recovery("SPY", bars=bars, recovery_cursor="2026-08-12T14:01:05Z", envelope=envelope)

    def test_list_cursors_parses_rows(self, store):
        store.advance_cursor("SPY", second_ref(end_at="2026-08-12T14:00:05Z"))
        store.mark_live_blocked("QQQ", reason="gap")
        cursors = store.list_cursors()
        assert {cursor["symbol"] for cursor in cursors} == {"SPY", "QQQ"}


class TestComposites:
    def test_commit_account_event_atomic(self, store):
        store.ensure_accounts(["pa:rose"])
        envelope = envelope_for_account_change(
            "pa:rose",
            0,
            balance_before=Decimal("1000.00"),
            balance_after=Decimal("950.00"),
            occurred_at=INSTANT,
            idempotency_key=account_change_key("pa:rose", Decimal("1000.00"), Decimal("950.00")),
        )
        store.commit_account_event(
            "pa:rose",
            balance_before=Decimal("1000.00"),
            balance_after=Decimal("950.00"),
            envelope=envelope,
        )
        assert store.get_account("pa:rose")["balance"] == "950.00"
        events = store.list_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "account_changed"
        assert events[0]["envelope"]["payload"]["balance_after"] == "950.00"

    def test_account_event_replay_after_later_change_cannot_roll_back(self, store):
        store.ensure_accounts(["pa:rose"])
        first = envelope_for_account_change(
            "pa:rose", 0, balance_before="1000.00", balance_after="950.00",
            occurred_at=INSTANT, idempotency_key="account_first",
        )
        second = envelope_for_account_change(
            "pa:rose", 0, balance_before="950.00", balance_after="900.00",
            occurred_at=INSTANT, idempotency_key="account_second",
        )
        store.commit_account_event("pa:rose", balance_before="1000.00", balance_after="950.00", envelope=first)
        store.commit_account_event("pa:rose", balance_before="950.00", balance_after="900.00", envelope=second)
        store.commit_account_event("pa:rose", balance_before="1000.00", balance_after="950.00", envelope=first)
        assert store.get_account("pa:rose")["balance"] == "900.00"
        assert len(store.list_events(event_type="account_changed")) == 2

    def test_account_event_stale_before_is_rejected_atomically(self, store):
        store.ensure_accounts(["pa:rose"])
        first = envelope_for_account_change(
            "pa:rose", 0, balance_before="1000.00", balance_after="950.00",
            occurred_at=INSTANT, idempotency_key="account_stale_first",
        )
        store.commit_account_event("pa:rose", balance_before="1000.00", balance_after="950.00", envelope=first)
        stale = envelope_for_account_change(
            "pa:rose", 0, balance_before="1000.00", balance_after="900.00",
            occurred_at=INSTANT, idempotency_key="account_stale_second",
        )
        with pytest.raises(StorageConflictError):
            store.commit_account_event("pa:rose", balance_before="1000.00", balance_after="900.00", envelope=stale)
        assert store.get_account("pa:rose")["balance"] == "950.00"
        assert len(store.list_events()) == 1

    def test_commit_account_event_payload_mismatch_rejected(self, store):
        store.ensure_accounts(["pa:rose"])
        envelope = envelope_for_account_change(
            "pa:rose",
            0,
            balance_before="1000.00",
            balance_after="900.00",
            occurred_at=INSTANT,
            idempotency_key="idem_mismatch",
        )
        with pytest.raises(EnvelopeValidationError):
            store.commit_account_event(
                "pa:rose",
                balance_before="1000.00",
                balance_after="950.00",
                envelope=envelope,
            )
        assert store.get_account("pa:rose")["balance"] == "1000.00"
        assert store.list_events() == []

    def test_commit_account_event_unknown_strategy_rejected(self, store):
        envelope = envelope_for_account_change(
            "pa:ghost",
            0,
            balance_before="1000.00",
            balance_after="950.00",
            occurred_at=INSTANT,
            idempotency_key="idem_ghost",
        )
        with pytest.raises(InvalidStorageInputError):
            store.commit_account_event(
                "pa:ghost",
                balance_before="1000.00",
                balance_after="950.00",
                envelope=envelope,
            )

    def test_commit_account_exhausted(self, store):
        store.ensure_accounts(["pa:rose"])
        envelope = envelope_for_account_exhausted(
            "pa:rose",
            0,
            balance=Decimal("0.00"),
            reason="balance exhausted",
            occurred_at=INSTANT,
            idempotency_key=account_exhausted_key("pa:rose", Decimal("0.00"), "balance exhausted"),
        )
        store.commit_account_exhausted(
            "pa:rose",
            balance=Decimal("0.00"),
            reason="balance exhausted",
            envelope=envelope,
        )
        account = store.get_account("pa:rose")
        assert account["exhausted"] == 1
        assert account["balance"] == "0.00"
        assert store.list_events()[0]["event_type"] == "account_exhausted"

    def test_commit_account_exhausted_wrong_strategy_rejected(self, store):
        store.ensure_accounts(["pa:rose"])
        envelope = envelope_for_account_exhausted(
            "pa:other",
            0,
            balance="0.00",
            reason="balance exhausted",
            occurred_at=INSTANT,
            idempotency_key="idem_exhausted_other",
        )
        with pytest.raises(StorageConflictError):
            store.commit_account_exhausted(
                "pa:rose",
                balance="0.00",
                reason="balance exhausted",
                envelope=envelope,
            )
        assert store.get_account("pa:rose")["exhausted"] == 0

    def test_commit_fill_rejected(self, store):
        envelope = envelope_for_fill_rejected(
            "pa:rose",
            3,
            reason="insufficient balance",
            pending_id="pe_1",
            occurred_at=INSTANT,
            idempotency_key=fill_rejected_key("pa:rose", "insufficient balance", "pe_1", None),
        )
        store.commit_fill_rejected("pa:rose", envelope=envelope)
        events = store.list_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "paper_fill_rejected"
        assert events[0]["envelope"]["payload"]["pending_id"] == "pe_1"

    def test_commit_schedule_set_persists_timer_and_event(self, store):
        envelope = envelope_for_schedule(
            snapshot(),
            timer_kind="sleep",
            due_at="2026-08-12T15:00:00Z",
            decision_accepted_at=INSTANT,
            occurred_at=INSTANT,
            idempotency_key=schedule_key("pa:rose", 3, "sleep", "2026-08-12T15:00:00Z"),
        )
        store.commit_schedule_set("pa:rose", envelope=envelope)
        timers = store.get_timers("pa:rose")
        assert len(timers) == 1
        assert timers[0]["kind"] == "sleep"
        assert timers[0]["generation"] == 3
        assert timers[0]["due_at"] == "2026-08-12T15:00:00Z"
        assert timers[0]["active"] == 1
        assert timers[0]["fired"] == 0
        assert store.list_events()[0]["event_type"] == "schedule_set"


class TestIsolation:
    def test_strategy_state_rows_isolated(self, store):
        store.commit_strategy_state(snapshot(strategy_id="pa:rose", state="sleeping"))
        store.commit_strategy_state(snapshot(strategy_id="pa:caveman", state="analyzing"))
        assert {row["strategy_id"] for row in store.list_strategy_states()} == {
            "pa:rose",
            "pa:caveman",
        }
        assert store.get_strategy_state("pa:caveman")["state"] == "analyzing"
        assert store.get_strategy_state("pa:ghost") is None

    def test_pending_entries_isolated_between_strategies(self, store):
        for state in (pending_state(), pending_state(strategy_id="pa:caveman", pending_id="pe_2")):
            envelope = envelope_for_pending_entry(
                state,
                "created",
                occurred_at=INSTANT,
                idempotency_key=pending_key(state["strategy_id"], state["pending_id"], "created"),
            )
            store.commit_pending_entry(state, event="created", envelope=envelope)
        rose_ids = {row["pending_id"] for row in store.list_pending_entries(strategy_id="pa:rose")}
        assert rose_ids == {"pe_1"}

    def test_trades_isolated_between_strategies(self, store):
        for state in (trade_state(), trade_state(strategy_id="pa:caveman", trade_id="pt_2")):
            envelope = envelope_for_fill(
                state,
                occurred_at=INSTANT,
                idempotency_key=fill_key(state["strategy_id"], state["trade_id"]),
                market_ref=second_ref(symbol=state["symbol"]),
            )
            store.commit_trade(state, event="filled", envelope=envelope)
        rose_ids = {row["trade_id"] for row in store.list_trades(strategy_id="pa:rose")}
        assert rose_ids == {"pt_1"}

    def test_accounts_isolated_between_strategies(self, store):
        store.ensure_accounts(["pa:rose", "pa:caveman"])
        store.update_account("pa:rose", balance="900.00")
        assert store.get_account("pa:rose")["balance"] == "900.00"
        assert store.get_account("pa:caveman")["balance"] == "1000.00"
