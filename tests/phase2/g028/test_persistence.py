"""G028 persistence facade tests: real G024/G025/G026/G023 round trips.

The suite proves the sink/hydrator contract against the accepted upstream
components: real G024 account transitions, real G018 wait instructions, real
G020 frozen seconds and the real G023 scheduler.  Every round trip closes
and reopens the SQLite database with a fresh run id so the restore seam is
exercised from durable bytes, never from memory.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pa_engine.market_sources.alpaca.history import RecoveryBar
from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent
from pa_engine.paper.core import PaperAccountBook
from pa_engine.paper.entry import PendingEntryMonitor, PendingEntryReducer, PendingEntryState
from pa_engine.paper.exit import ExitReducer, ExitState, make_exit_state
from pa_engine.runtime.persistence import (
    RuntimeHydrator,
    RuntimePersistenceError,
    RuntimeSink,
)
from pa_engine.storage import (
    RuntimeStore,
    StorageConflictError,
    decimal_to_text,
    instant_to_text,
    open_database,
)
from pa_engine.strategies.knowledge import load_default_registry
from pa_engine.strategy.decision import make_scheduler_instruction
from pa_engine.strategy.runtime import (
    FakeClock,
    StrategyScheduler,
    StrategySpec,
    StrategyState,
    TimerToken,
)

ROOT = Path(__file__).resolve().parents[3]
UTC = timezone.utc
START = datetime(2026, 8, 12, 14, 0, tzinfo=UTC)
H64 = "a" * 64
FEED = {
    "schema_version": "0.1.0",
    "provider": "alpaca",
    "feed": "iex",
    "coverage": "single_exchange",
}
SID = "pa:g028-test"
SYMBOL = "SPY"
PENDING_SID = "pa:nanqiao-range-extreme-limit"


def _stamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _second(
    symbol: str = SYMBOL,
    start: datetime = START,
    *,
    high: str = "100",
    low: str = "100",
    watermark: datetime | None = None,
) -> SecondAggregateEvent:
    end = start + timedelta(seconds=1)
    watermark = watermark or end + timedelta(seconds=3)
    return SecondAggregateEvent(
        symbol,
        _stamp(start),
        _stamp(end),
        low,
        high,
        low,
        high,
        "1",
        contributor_ids=(f"trade-{_stamp(start)}",),
        execution_watermark=_stamp(watermark),
        status="frozen",
        revision_sequence=0,
        quality={"status": "ok", "codes": [], "connection_gap": False},
    )


def _bar(*, symbol: str = SYMBOL, start: datetime = START, end: datetime | None = None, low: str = "99", high: str = "101") -> RecoveryBar:
    end = end or (start + timedelta(minutes=1))
    return RecoveryBar(
        instrument={"asset_class": "us_equity", "symbol": symbol, "venue": "IEX", "currency": "USD"},
        timeframe={"unit": "minute", "multiplier": 1},
        session={"kind": "regular", "timezone": "America/New_York"},
        start_at=_stamp(start),
        end_at=_stamp(end),
        open="100",
        high=high,
        low=low,
        close="100",
        volume="10",
        complete=True,
        coverage_state="complete",
        source_request_hash=H64,
        recovery_cursor=_stamp(end),
        market_feed=FEED,
    )


class StorageClock:
    """Adapter turning the G023 FakeClock into the storage callable clock."""

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock

    def __call__(self) -> datetime:
        return self._clock.now()

    def advance(self, seconds: int = 1) -> None:
        self._clock.advance(seconds)


class Bundle:
    """One open database handle plus the facade under test."""

    def __init__(self, handle: Any, store: RuntimeStore, clock: StorageClock) -> None:
        self.handle = handle
        self.store = store
        self.sink = RuntimeSink(store)
        self.hydrator = RuntimeHydrator(store)
        self.clock = clock


def open_bundle(tmp_path: Path, name: str = "g028.db", *, run_id: str = "run_g028_a") -> Bundle:
    clock = StorageClock(FakeClock(START))
    handle = open_database(str(tmp_path / name), run_id=run_id, mode="automatic", clock=clock)
    return Bundle(handle, RuntimeStore(handle), clock)


def reopen_bundle(tmp_path: Path, name: str = "g028.db", *, run_id: str = "run_g028_b") -> Bundle:
    clock = StorageClock(FakeClock(START))
    handle = open_database(str(tmp_path / name), run_id=run_id, mode="automatic", clock=clock)
    return Bundle(handle, RuntimeStore(handle), clock)


def snapshot(state: StrategyState, *, generation: int = 0, **overrides: Any) -> dict[str, Any]:
    from pa_engine.strategy.runtime import StrategyRecord

    record = StrategyRecord(SID, state=state, generation=generation, **overrides)
    return record.snapshot()


def persist_account_state(store: RuntimeStore, strategy_id: str, account: Any, **updates: Any) -> None:
    """Mirror the engine's account-row update without changing the balance."""

    store.update_account(strategy_id, balance=account.balance, **updates)


def _ready_view(ref: dict, *, view_id: str | None = None, snapshot_id: str | None = None) -> dict:
    return {
        "shared_market_view_id": view_id or ref["shared_market_view_id"],
        "closed_snapshot_id": snapshot_id or ref["market_snapshot_id"],
        "regular_session": True,
        "fresh": True,
        "complete": True,
        "non_gapped": True,
        "sufficient": True,
        "frozen_second": True,
        "market_view_ref": copy.deepcopy(ref),
    }


def _real_instruction() -> tuple[Any, dict, dict, dict]:
    registry = load_default_registry(ROOT / "knowledge")
    loaded = registry.load_packet(PENDING_SID)
    assert loaded.ok
    candidate = json.loads(
        (ROOT / "tests/fixtures/phase2/g018/valid/wait_long.json").read_text(encoding="utf-8")
    )
    instruction = make_scheduler_instruction(
        candidate,
        profile_ref=loaded.profile_ref,
        result={"pa_analysis_result_id": "par_" + "c" * 64, "content_hash": "c" * 64},
        evidence_package=loaded.evidence_package,
    )
    return registry, loaded.profile_ref, loaded.packet, instruction


def _real_scheduler(registry: Any, profile: dict, packet: dict, instruction: dict) -> StrategyScheduler:
    scheduler = StrategyScheduler(
        [StrategySpec(PENDING_SID, profile, packet)],
        clock=FakeClock(START),
        registry=registry,
    )
    scheduler.start(_ready_view(instruction["market_view_ref"]))
    jobs = scheduler.claim_jobs()
    assert len(jobs) == 1
    assert scheduler.complete_job(jobs[0], instruction, decision_accepted_at=START)
    return scheduler


class TestSinkStateAndDecision:
    def test_state_transition_commits_appends_then_replays(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        snap = snapshot(StrategyState.SLEEPING, generation=2)
        transition = {"from": "analysis_due", "to": "sleeping", "reason": "decision_no_entry"}
        first = bundle.sink.commit_state(snap, transition=transition, occurred_at=START)
        assert first == "appended"
        second = bundle.sink.commit_state(snap, transition=transition, occurred_at=START)
        assert second == "replayed"
        events = bundle.store.list_events(event_type="strategy_state_changed")
        assert len(events) == 1
        assert events[0]["envelope"]["mode"] == "automatic"
        assert events[0]["envelope"]["payload"] == {
            "from": "analysis_due",
            "to": "sleeping",
            "reason": "decision_no_entry",
            "resume_state": None,
        }
        bundle.handle.close()

    def test_state_transition_same_key_different_content_conflicts(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        snap = snapshot(StrategyState.SLEEPING, generation=1)
        transition = {"from": "analysis_due", "to": "sleeping", "reason": "decision_no_entry"}
        bundle.sink.commit_state(snap, transition=transition, occurred_at=START)
        with pytest.raises(StorageConflictError):
            bundle.sink.commit_state(snap, transition=transition, occurred_at=START + timedelta(seconds=1))
        assert len(bundle.store.list_events(event_type="strategy_state_changed")) == 1
        bundle.handle.close()

    def test_state_transition_resume_state_falls_back_to_snapshot(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        snap = snapshot(
            StrategyState.MARKET_WAIT,
            intended_resume_state=StrategyState.WAITING_ENTRY,
            generation=3,
        )
        transition = {"from": "analysis_due", "to": "market_wait", "reason": "gap_entered"}
        assert bundle.sink.commit_state(snap, transition=transition, occurred_at=START) == "appended"
        event = bundle.store.list_events(event_type="strategy_state_changed")[0]
        assert event["envelope"]["payload"]["resume_state"] == "waiting_entry"
        assert bundle.store.get_strategy_state(SID)["intended_resume_state"] == "waiting_entry"
        bundle.handle.close()

    def test_plain_state_commit_without_transition_stores_no_event(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        snap = snapshot(StrategyState.SLEEPING, generation=0)
        assert bundle.sink.commit_state(snap) == "appended"
        assert bundle.store.list_events() == []
        assert bundle.store.get_strategy_state(SID)["state"] == "sleeping"
        bundle.handle.close()

    def test_decision_commit_appends_then_replays_then_conflicts(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        snap = snapshot(StrategyState.ANALYZING, generation=1)
        instruction = {
            "strategy_id": SID,
            "branch": "no_entry",
            "next_call_after_seconds": 60,
            "instruction_id": "sdi_" + H64,
            "instruction_hash": H64,
            "result_id": "par_" + H64,
            "result_hash": H64,
            "evidence_package_id": "pkg_" + H64,
            "evidence_package_hash": H64,
        }
        assert bundle.sink.commit_decision(snap, instruction, occurred_at=START) == "appended"
        assert bundle.sink.commit_decision(snap, instruction, occurred_at=START) == "replayed"
        other = dict(instruction)
        other["next_call_after_seconds"] = 120
        with pytest.raises(StorageConflictError):
            bundle.sink.commit_decision(snap, other, occurred_at=START)
        events = bundle.store.list_events(event_type="model_decision_accepted")
        assert len(events) == 1
        assert events[0]["envelope"]["payload"]["branch"] == "no_entry"
        assert events[0]["envelope"]["payload"]["next_call_after_seconds"] == 60
        bundle.handle.close()

    def test_schedule_commit_persists_timer_and_replays(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        snap = snapshot(StrategyState.SLEEPING, generation=0)
        due_at = START + timedelta(seconds=60)
        assert (
            bundle.sink.commit_schedule(
                snap,
                timer_kind="sleep",
                due_at=due_at,
                decision_accepted_at=START,
                occurred_at=START,
            )
            == "appended"
        )
        assert (
            bundle.sink.commit_schedule(
                snap,
                timer_kind="sleep",
                due_at=due_at,
                decision_accepted_at=START,
                occurred_at=START,
            )
            == "replayed"
        )
        timers = bundle.store.get_timers(SID)
        assert [(timer["kind"], timer["due_at"]) for timer in timers] == [
            ("sleep", instant_to_text(due_at))
        ]
        assert len(bundle.store.list_events(event_type="schedule_set")) == 1
        bundle.handle.close()


class TestSinkPending:
    def _state_dict(self, *, status: str = "pending", **overrides: Any) -> dict[str, Any]:
        state = PendingEntryState.from_instruction(
            {
                "branch": "wait_for_price",
                "strategy_id": SID,
                "direction": "long",
                "symbol": SYMBOL,
                "entry_price": "100",
                "stop_price": "99",
                "target_price": "102",
                "entry_wait_seconds": 300,
                "instruction_hash": H64,
            },
            symbol=SYMBOL,
            decision_accepted_at=START,
            submitted_at=START,
            generation=2,
            validator=lambda value: True,
        )
        return state.to_dict() if status == "pending" else state

    def test_pending_created_commit_appends_then_replays(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        state = self._state_dict()
        assert bundle.sink.commit_pending(state, event="created", occurred_at=START) == "appended"
        assert bundle.sink.commit_pending(state, event="created", occurred_at=START) == "replayed"
        row = bundle.store.get_pending_entry(state["pending_id"])
        assert row["status"] == "pending"
        assert row["state_json"]["entry_price"] == "100"
        assert len(bundle.store.list_events(event_type="pending_entry_created")) == 1
        bundle.handle.close()

    def test_pending_expired_materializes_terminal_row(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        pending = self._state_dict()
        bundle.sink.commit_pending(pending, event="created", occurred_at=START)
        expired = dict(pending)
        expired["status"] = "expired"
        expired["terminal_reason"] = "expired at deadline"
        assert (
            bundle.sink.commit_pending(expired, event="expired", occurred_at=START) == "appended"
        )
        row = bundle.store.get_pending_entry(pending["pending_id"])
        assert row["status"] == "expired"
        assert row["state_json"]["terminal_reason"] == "expired at deadline"
        types = {event["event_type"] for event in bundle.store.list_events()}
        assert {"pending_entry_created", "pending_entry_expired"} <= types
        bundle.handle.close()

    def test_pending_cancelled_materializes_terminal_row(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        pending = self._state_dict()
        bundle.sink.commit_pending(pending, event="created", occurred_at=START)
        cancelled = dict(pending)
        cancelled["status"] = "cancelled"
        cancelled["terminal_reason"] = "operator_disabled"
        assert (
            bundle.sink.commit_pending(cancelled, event="cancelled", occurred_at=START)
            == "appended"
        )
        row = bundle.store.get_pending_entry(pending["pending_id"])
        assert row["status"] == "cancelled"
        event = bundle.store.list_events(event_type="pending_entry_cancelled")[0]
        assert event["envelope"]["payload"]["reason"] == "operator_disabled"
        bundle.handle.close()

    def test_pending_filled_uses_original_created_envelope(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        pending = self._state_dict()
        created = bundle.sink.prepare_pending(pending, event="created", occurred_at=START)
        assert bundle.sink.commit_pending(pending, event="created", envelope=created) == "appended"
        filled = dict(pending)
        filled["status"] = "filled"
        filled["terminal_reason"] = "price_touch"
        filled["filled_at"] = _stamp(START + timedelta(seconds=3))
        filled["filled_price"] = "100"
        # A freshly rebuilt created envelope has different content for the
        # same idempotency key and must conflict.
        fresh = bundle.sink.prepare_pending(filled, event="created", occurred_at=START)
        with pytest.raises(StorageConflictError):
            bundle.sink.commit_pending(filled, event="created", envelope=fresh)
        # The original envelope replays byte-identically while the row moves
        # to its terminal filled status.
        assert bundle.sink.commit_pending(filled, event="created", envelope=created) == "replayed"
        assert bundle.store.get_pending_entry(pending["pending_id"])["status"] == "filled"
        assert len(bundle.store.list_events(event_type="pending_entry_created")) == 1
        bundle.handle.close()

    def test_pending_bad_input_wrapped_as_invalid_sink_input(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])

        class ExplodingDict(dict):
            def __getitem__(self, key: str) -> Any:
                if key == "pending_id":
                    raise ValueError("malformed pending identity")
                return super().__getitem__(key)

        bad = ExplodingDict(self._state_dict())
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.sink.commit_pending(bad, event="created", occurred_at=START)
        assert excinfo.value.code == "invalid_sink_input"
        bundle.handle.close()


class TestSinkTradesAndAccounts:
    def _fill_state(self, bundle: Bundle, book: PaperAccountBook) -> tuple[dict[str, Any], SecondAggregateEvent, Any]:
        second = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID,
            SYMBOL,
            "long",
            None,
            "509.00",
            "515.00",
            immediate=True,
            fill_reference="510.25",
            trade_id="pt_1",
        )
        assert opened.accepted
        interval = (second.start_at, second.end_at)
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=interval,
            entry_filled_at=second.end_at,
            generation=0,
            market_feed=FEED,
        )
        return state.to_dict(), second, opened

    def test_fill_commit_then_close_commit_roundtrip(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        state, second, opened = self._fill_state(bundle, book)
        assert bundle.sink.commit_fill(state, market_ref=second, occurred_at=second.end_at) == "appended"
        assert bundle.sink.commit_fill(state, market_ref=second, occurred_at=second.end_at) == "replayed"
        row = bundle.store.get_trade("pt_1")
        assert row["status"] == "open"
        assert row["quantity"] == decimal_to_text(opened.trade.quantity)
        assert row["state_json"]["trade_lineage"]["trade_id"] == "pt_1"
        close_result = book.close_trade(SID, "515.00", position_id="pt_1", reason="target")
        assert close_result.accepted
        closed = make_exit_state(
            opened.trade,
            entry_fill_interval=(second.start_at, second.end_at),
            entry_filled_at=second.end_at,
            generation=0,
            market_feed=FEED,
        )
        closed_at = START + timedelta(seconds=60)
        settled = ExitState(
            strategy_id=closed.strategy_id,
            trade_id=closed.trade_id,
            symbol=closed.symbol,
            direction=closed.direction,
            entry_price=closed.entry_price,
            stop_price=closed.stop_price,
            target_price=closed.target_price,
            quantity=closed.quantity,
            trade_lineage=closed.trade_lineage,
            decision_lineage=closed.decision_lineage,
            entry_event=closed.entry_event,
            entry_fill_interval=closed.entry_fill_interval,
            entry_filled_at=closed.entry_filled_at,
            generation=closed.generation,
            market_feed=closed.market_feed,
            status="settled",
            outcome_status="settled",
            terminal_reason="target",
            selected_exit=Decimal("515.00"),
            exit_reason="target",
            settlement=dict(close_result.settlement),
            closed_at=closed_at,
        )
        close_second = _second(start=START + timedelta(seconds=60), high="515.00", low="515.00")
        assert (
            bundle.sink.commit_close(settled.to_dict(), market_ref=close_second, occurred_at=close_second.end_at)
            == "appended"
        )
        assert (
            bundle.sink.commit_close(settled.to_dict(), market_ref=close_second, occurred_at=close_second.end_at)
            == "replayed"
        )
        assert bundle.store.get_trade("pt_1")["status"] == "settled"
        close_event = bundle.store.list_events(event_type="paper_close")[0]
        assert close_event["envelope"]["payload"]["exit_reason"] == "target"
        assert close_event["envelope"]["market_ref"]["second_event_id"] == close_second.event_id
        bundle.handle.close()

    def test_account_change_commit_replay_and_conflict(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        assert (
            bundle.sink.commit_account_change(
                SID,
                balance_before=Decimal("1000.00"),
                balance_after=Decimal("1020.00"),
                occurred_at=START,
            )
            == "appended"
        )
        assert (
            bundle.sink.commit_account_change(
                SID,
                balance_before=Decimal("1000.00"),
                balance_after=Decimal("1020.00"),
                occurred_at=START,
            )
            == "replayed"
        )
        assert bundle.store.get_account(SID)["balance"] == "1020.00"
        with pytest.raises(StorageConflictError):
            bundle.sink.commit_account_change(
                SID,
                balance_before=Decimal("1000.00"),
                balance_after=Decimal("1020.00"),
                occurred_at=START + timedelta(seconds=1),
            )
        assert len(bundle.store.list_events(event_type="account_changed")) == 1
        bundle.handle.close()

    def test_account_exhausted_commit_flags_row(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        assert (
            bundle.sink.commit_account_exhausted(
                SID, balance=Decimal("0.00"), reason="over_loss_capped", occurred_at=START
            )
            == "appended"
        )
        assert bundle.store.get_account(SID)["exhausted"] == 1
        event = bundle.store.list_events(event_type="account_exhausted")[0]
        assert event["envelope"]["payload"]["balance"] == "0.00"
        assert event["envelope"]["payload"]["reason"] == "over_loss_capped"
        bundle.handle.close()

    def test_fill_rejected_commit_appends_and_replays(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        assert (
            bundle.sink.commit_fill_rejected(
                SID,
                4,
                reason="immediate_price_drift",
                pending_id="ppd_1",
                trade_id="pt_1",
                occurred_at=START,
            )
            == "appended"
        )
        assert (
            bundle.sink.commit_fill_rejected(
                SID,
                4,
                reason="immediate_price_drift",
                pending_id="ppd_1",
                trade_id="pt_1",
                occurred_at=START,
            )
            == "replayed"
        )
        event = bundle.store.list_events(event_type="paper_fill_rejected")[0]
        assert event["envelope"]["payload"]["reason"] == "immediate_price_drift"
        assert event["envelope"]["payload"]["pending_id"] == "ppd_1"
        assert bundle.store.get_account(SID)["balance"] == "1000.00"
        bundle.handle.close()

    def test_market_recovery_commit_with_real_recovery_bars(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        bundle.sink.mark_live_blocked(SYMBOL, reason="restart gap")
        assert bundle.sink.is_live_blocked(SYMBOL) is True
        first = _bar(start=START, end=START + timedelta(minutes=1))
        second = _bar(start=START + timedelta(minutes=1), end=START + timedelta(minutes=2))
        cursor = _stamp(START + timedelta(minutes=2))
        assert (
            bundle.sink.commit_market_recovery(
                SYMBOL, bars=[first, second], recovery_cursor=cursor, occurred_at=START
            )
            == "appended"
        )
        assert (
            bundle.sink.commit_market_recovery(
                SYMBOL, bars=[first, second], recovery_cursor=cursor, occurred_at=START
            )
            == "replayed"
        )
        assert bundle.sink.is_live_blocked(SYMBOL) is False
        stored = bundle.sink.get_cursor(SYMBOL)
        assert stored["recovery_cursor"] == cursor
        event = bundle.store.list_events(event_type="market_recovery")[0]
        assert event["envelope"]["payload"]["bar_count"] == 2
        assert event["envelope"]["payload"]["bars"][0]["kind"] == "recovery_bar"
        bundle.handle.close()


class TestHydratorAccountBook:
    def test_immediate_open_trade_roundtrip_hydrates_exact(self, tmp_path):
        path = str(tmp_path / "hydrate-open.db")
        bundle = open_bundle(tmp_path, name=Path(path).name)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        second = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID,
            SYMBOL,
            "long",
            None,
            "509.00",
            "515.00",
            immediate=True,
            fill_reference="510.25",
            trade_id="pt_1",
        )
        assert opened.accepted
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(second.start_at, second.end_at),
            entry_filled_at=second.end_at,
            generation=0,
            market_feed=FEED,
        )
        assert bundle.sink.commit_fill(state.to_dict(), market_ref=second, occurred_at=second.end_at) == "appended"
        persist_account_state(bundle.store, SID, book.get_account(SID), open_trade=opened.trade.to_dict())
        assert bundle.sink.commit_state(
            snapshot(
                StrategyState.OPEN_POSITION,
                generation=0,
                open_position_id="pt_1",
                open_position_generation=0,
            ),
            transition={"from": "analysis_due", "to": "open_position", "reason": "fill_accepted"},
            occurred_at=second.end_at,
        ) == "appended"
        bundle.handle.close()

        bundle2 = reopen_bundle(tmp_path, name=Path(path).name, run_id="run_g028_b")
        book2 = bundle2.hydrator.hydrate_account_book([SID])
        account = book2.get_account(SID)
        assert account is not None
        assert account.balance == Decimal("1000.00")
        assert account.has_open_position
        trade = account.open_trade
        assert trade.trade_id == "pt_1"
        assert trade.strategy_id == SID
        assert trade.symbol == SYMBOL
        assert trade.direction == "long"
        assert trade.entry_price == Decimal("510.25")
        assert trade.stop_price == Decimal("509.00")
        assert trade.target_price == Decimal("515.00")
        assert trade.quantity == opened.trade.quantity
        assert trade.planned_r == opened.trade.planned_r
        assert trade.immediate is True
        assert trade.paper_math_version == "0.1.0"
        bundle2.handle.close()

    def test_pnl_changed_balance_survives_hydration(self, tmp_path):
        path = str(tmp_path / "hydrate-pnl.db")
        bundle = open_bundle(tmp_path, name=Path(path).name)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        fill = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID, SYMBOL, "long", None, "509.00", "515.00", immediate=True,
            fill_reference="510.25", trade_id="pt_pnl",
        )
        assert opened.accepted
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(fill.start_at, fill.end_at),
            entry_filled_at=fill.end_at,
            generation=0,
            market_feed=FEED,
        )
        bundle.sink.commit_fill(state.to_dict(), market_ref=fill, occurred_at=fill.end_at)
        close_result = book.close_trade(SID, "515.00", position_id="pt_pnl", reason="target")
        assert close_result.accepted
        closed_at = START + timedelta(seconds=60)
        settled = ExitState(
            strategy_id=state.strategy_id,
            trade_id=state.trade_id,
            symbol=state.symbol,
            direction=state.direction,
            entry_price=state.entry_price,
            stop_price=state.stop_price,
            target_price=state.target_price,
            quantity=state.quantity,
            trade_lineage=state.trade_lineage,
            decision_lineage=state.decision_lineage,
            entry_event=state.entry_event,
            entry_fill_interval=state.entry_fill_interval,
            entry_filled_at=state.entry_filled_at,
            generation=state.generation,
            market_feed=state.market_feed,
            status="settled",
            outcome_status="settled",
            terminal_reason="target",
            selected_exit=Decimal("515.00"),
            exit_reason="target",
            settlement=dict(close_result.settlement),
            closed_at=closed_at,
        )
        close_second = _second(start=closed_at, high="515.00", low="515.00")
        bundle.sink.commit_close(settled.to_dict(), market_ref=close_second, occurred_at=close_second.end_at)
        expected = close_result.account.balance
        assert expected > Decimal("1000.00")
        bundle.sink.commit_account_change(
            SID,
            balance_before=close_result.settlement["balance_before"],
            balance_after=expected,
            occurred_at=close_second.end_at,
        )
        bundle.handle.close()

        bundle2 = reopen_bundle(tmp_path, name=Path(path).name, run_id="run_g028_b")
        book2 = bundle2.hydrator.hydrate_account_book([SID])
        restored = book2.get_account(SID)
        assert restored is not None
        assert restored.balance == expected
        assert restored.balance != Decimal("1000.00")
        assert not restored.has_open_position
        assert not restored.pending
        bundle2.handle.close()

    def test_exhausted_close_restores_exhausted_zero_balance(self, tmp_path):
        path = str(tmp_path / "hydrate-exhausted.db")
        bundle = open_bundle(tmp_path, name=Path(path).name)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        fill = _second(high="100", low="100")
        opened = book.open_trade(
            SID, SYMBOL, "short", None, "201.00", "98.00", immediate=True,
            fill_reference="100", trade_id="pt_exh",
        )
        assert opened.accepted
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(fill.start_at, fill.end_at),
            entry_filled_at=fill.end_at,
            generation=0,
            market_feed=FEED,
        )
        bundle.sink.commit_fill(state.to_dict(), market_ref=fill, occurred_at=fill.end_at)
        persist_account_state(bundle.store, SID, book.get_account(SID), open_trade=opened.trade.to_dict())
        stop_second = _second(start=START + timedelta(seconds=1), high="201.00", low="100.00")
        close_result = book.close_trade(SID, "201.00", position_id="pt_exh", reason="stop_loss")
        assert close_result.accepted
        assert close_result.account.balance == Decimal("0.00")
        closed_at = START + timedelta(seconds=1)
        settled = ExitState(
            strategy_id=state.strategy_id,
            trade_id=state.trade_id,
            symbol=state.symbol,
            direction=state.direction,
            entry_price=state.entry_price,
            stop_price=state.stop_price,
            target_price=state.target_price,
            quantity=state.quantity,
            trade_lineage=state.trade_lineage,
            decision_lineage=state.decision_lineage,
            entry_event=state.entry_event,
            entry_fill_interval=state.entry_fill_interval,
            entry_filled_at=state.entry_filled_at,
            generation=state.generation,
            market_feed=state.market_feed,
            status="exhausted",
            outcome_status="exhausted",
            terminal_reason="stop_loss",
            selected_exit=Decimal("201.00"),
            exit_reason="stop_loss",
            settlement=dict(close_result.settlement),
            closed_at=closed_at,
        )
        bundle.sink.commit_close(settled.to_dict(), market_ref=stop_second, occurred_at=stop_second.end_at)
        bundle.sink.commit_account_change(
            SID,
            balance_before=close_result.settlement["balance_before"],
            balance_after=Decimal("0.00"),
            occurred_at=stop_second.end_at,
        )
        bundle.sink.commit_account_exhausted(
            SID, balance=Decimal("0.00"), reason="over_loss_capped", occurred_at=stop_second.end_at
        )
        bundle.handle.close()

        bundle2 = reopen_bundle(tmp_path, name=Path(path).name, run_id="run_g028_b")
        book2 = bundle2.hydrator.hydrate_account_book([SID])
        restored = book2.get_account(SID)
        assert restored is not None
        assert restored.balance == Decimal("0.00")
        assert restored.exhausted is True
        assert not restored.has_open_position
        types = {event["event_type"] for event in bundle2.store.list_events()}
        assert {"account_changed", "account_exhausted"} <= types
        bundle2.handle.close()

    def test_hydrate_missing_account_row_raises(self, tmp_path):
        bundle = open_bundle(tmp_path)
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.hydrator.hydrate_account_book(["pa:missing"])
        assert excinfo.value.code == "hydrate_account_mismatch"
        bundle.handle.close()

    def test_hydrate_pending_flag_without_row_raises(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        bundle.handle.conn.execute(
            "UPDATE accounts SET pending = 1 WHERE strategy_id = ?", (SID,)
        )
        bundle.handle.conn.commit()
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.hydrator.hydrate_account_book([SID])
        assert excinfo.value.code == "hydrate_account_mismatch"
        bundle.handle.close()

    def test_hydrate_both_open_trade_and_active_pending_raises(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        fill = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID, SYMBOL, "long", None, "509.00", "515.00", immediate=True,
            fill_reference="510.25", trade_id="pt_both",
        )
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(fill.start_at, fill.end_at),
            entry_filled_at=fill.end_at,
            generation=0,
            market_feed=FEED,
        )
        bundle.sink.commit_fill(state.to_dict(), market_ref=fill, occurred_at=fill.end_at)
        pending = PendingEntryState.from_instruction(
            {
                "branch": "wait_for_price",
                "strategy_id": SID,
                "direction": "long",
                "symbol": SYMBOL,
                "entry_price": "100",
                "stop_price": "99",
                "target_price": "102",
                "entry_wait_seconds": 300,
                "instruction_hash": H64,
            },
            symbol=SYMBOL,
            decision_accepted_at=START,
            submitted_at=START,
            generation=0,
            validator=lambda value: True,
        )
        bundle.sink.commit_pending(pending.to_dict(), event="created", occurred_at=START)
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.hydrator.hydrate_account_book([SID])
        assert excinfo.value.code == "hydrate_account_mismatch"
        bundle.handle.close()

    def test_hydrate_embedded_open_trade_id_mismatch_raises(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        fill = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID, SYMBOL, "long", None, "509.00", "515.00", immediate=True,
            fill_reference="510.25", trade_id="pt_row",
        )
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(fill.start_at, fill.end_at),
            entry_filled_at=fill.end_at,
            generation=0,
            market_feed=FEED,
        )
        bundle.sink.commit_fill(state.to_dict(), market_ref=fill, occurred_at=fill.end_at)
        tampered = dict(opened.trade.to_dict())
        tampered["trade_id"] = "pt_tampered"
        persist_account_state(bundle.store, SID, book.get_account(SID), open_trade=tampered)
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.hydrator.hydrate_account_book([SID])
        assert excinfo.value.code == "hydrate_account_mismatch"
        bundle.handle.close()


class TestHydratorRestoreStates:
    def test_restore_state_rows_maps_strategy_to_row(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        snap = snapshot(StrategyState.SLEEPING, generation=2, pending_id=None)
        bundle.sink.commit_state(snap)
        rows = bundle.hydrator.restore_state_rows()
        assert SID in rows
        assert rows[SID]["state"] == "sleeping"
        assert rows[SID]["generation"] == 2
        bundle.handle.close()

    def test_restore_pending_state_roundtrip_exact(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        state = PendingEntryState.from_instruction(
            {
                "branch": "wait_for_price",
                "strategy_id": SID,
                "direction": "long",
                "symbol": SYMBOL,
                "entry_price": "100",
                "stop_price": "99",
                "target_price": "102",
                "entry_wait_seconds": 300,
                "instruction_hash": H64,
            },
            symbol=SYMBOL,
            decision_accepted_at=START,
            submitted_at=START,
            generation=3,
            validator=lambda value: True,
        )
        bundle.sink.commit_pending(state.to_dict(), event="created", occurred_at=START)
        row = bundle.store.get_pending_entry(state.pending_id)
        restored = bundle.hydrator.restore_pending_state(row)
        assert isinstance(restored, PendingEntryState)
        assert restored.pending_id == state.pending_id
        assert restored.generation == 3
        assert restored.entry_price == Decimal("100")
        assert restored.expires_at == state.expires_at
        assert restored.episode_id == state.episode_id
        assert restored.market_feed == FEED
        assert restored.decision_lineage.instruction_hash == H64
        assert restored.state_hash == state.state_hash
        bundle.handle.close()

    def test_restore_pending_state_corrupt_json_raises(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        state = PendingEntryState.from_instruction(
            {
                "branch": "wait_for_price",
                "strategy_id": SID,
                "direction": "long",
                "symbol": SYMBOL,
                "entry_price": "100",
                "stop_price": "99",
                "target_price": "102",
                "entry_wait_seconds": 300,
                "instruction_hash": H64,
            },
            symbol=SYMBOL,
            decision_accepted_at=START,
            submitted_at=START,
            generation=0,
            validator=lambda value: True,
        )
        bundle.sink.commit_pending(state.to_dict(), event="created", occurred_at=START)
        bundle.handle.conn.execute(
            "UPDATE pending_entries SET state_json = ? WHERE pending_id = ?",
            ('{"status": "pending"}', state.pending_id),
        )
        bundle.handle.conn.commit()
        row = bundle.store.get_pending_entry(state.pending_id)
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.hydrator.restore_pending_state(row)
        assert excinfo.value.code == "pending_state_corrupt"
        bundle.handle.close()

    def test_restore_exit_state_roundtrip_exact(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        fill = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID, SYMBOL, "long", None, "509.00", "515.00", immediate=True,
            fill_reference="510.25", trade_id="pt_restore",
        )
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(fill.start_at, fill.end_at),
            entry_filled_at=fill.end_at,
            generation=2,
            market_feed=FEED,
        )
        bundle.sink.commit_fill(state.to_dict(), market_ref=fill, occurred_at=fill.end_at)
        row = bundle.store.get_trade("pt_restore")
        restored = bundle.hydrator.restore_exit_state(row)
        assert isinstance(restored, ExitState)
        assert restored.trade_id == "pt_restore"
        assert restored.generation == 2
        assert restored.entry_price == Decimal("510.25")
        assert restored.quantity == opened.trade.quantity
        assert restored.entry_fill_interval == state.entry_fill_interval
        assert restored.episode_id == state.episode_id
        assert restored.trade_lineage["trade_id"] == "pt_restore"
        assert restored.state_hash == state.state_hash
        bundle.handle.close()

    def test_restore_exit_state_corrupt_json_raises(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        fill = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID, SYMBOL, "long", None, "509.00", "515.00", immediate=True,
            fill_reference="510.25", trade_id="pt_corrupt",
        )
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(fill.start_at, fill.end_at),
            entry_filled_at=fill.end_at,
            generation=0,
            market_feed=FEED,
        )
        bundle.sink.commit_fill(state.to_dict(), market_ref=fill, occurred_at=fill.end_at)
        bundle.handle.conn.execute(
            "UPDATE trades SET state_json = ? WHERE trade_id = ?",
            ('{"status": "open"}', "pt_corrupt"),
        )
        bundle.handle.conn.commit()
        row = bundle.store.get_trade("pt_corrupt")
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.hydrator.restore_exit_state(row)
        assert excinfo.value.code == "exit_state_corrupt"
        bundle.handle.close()


class TestRestoredExitMonitor:
    def test_restored_exit_monitor_settles_stop_second_and_persists_close(self, tmp_path):
        path = str(tmp_path / "exit-roundtrip.db")
        bundle = open_bundle(tmp_path, name=Path(path).name)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        fill = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID, SYMBOL, "long", None, "509.00", "515.00", immediate=True,
            fill_reference="510.25", trade_id="pt_rt",
        )
        assert opened.accepted
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(fill.start_at, fill.end_at),
            entry_filled_at=fill.end_at,
            generation=0,
            market_feed=FEED,
        )
        assert bundle.sink.commit_fill(state.to_dict(), market_ref=fill, occurred_at=fill.end_at) == "appended"
        persist_account_state(bundle.store, SID, book.get_account(SID), open_trade=opened.trade.to_dict())
        assert bundle.sink.commit_state(
            snapshot(
                StrategyState.OPEN_POSITION,
                generation=0,
                open_position_id="pt_rt",
                open_position_generation=0,
            ),
            transition={"from": "analysis_due", "to": "open_position", "reason": "fill_accepted"},
            occurred_at=fill.end_at,
        ) == "appended"
        bundle.handle.close()

        bundle2 = reopen_bundle(tmp_path, name=Path(path).name, run_id="run_g028_b")
        book2 = bundle2.hydrator.hydrate_account_book([SID])
        assert book2.get_account(SID).balance == Decimal("1000.00")
        trade_row = bundle2.store.list_trades(strategy_id=SID, status="open")[0]
        exit_state = bundle2.hydrator.restore_exit_state(trade_row)
        assert exit_state.trade_id == "pt_rt"

        scheduler = StrategyScheduler([StrategySpec(SID)], clock=FakeClock(START))
        scheduler.start(
            {
                "shared_market_view_id": "view-1",
                "closed_snapshot_id": "snapshot-1",
                "regular_session": True,
                "fresh": True,
                "complete": True,
                "non_gapped": True,
                "sufficient": True,
                "frozen_second": True,
            }
        )
        record = scheduler.get(SID)
        record.state = StrategyState.OPEN_POSITION
        record.open_position_id = "pt_rt"
        record.open_position_generation = 0
        monitor = bundle2.hydrator.wrap_exit_monitor(exit_state, scheduler=scheduler, book=book2)
        assert isinstance(monitor, ExitReducer)

        stop = _second(start=START + timedelta(seconds=1), high="510.25", low="508.99")
        outcome = monitor.on_second(stop, now=_stamp(START + timedelta(seconds=5)))
        assert outcome.status == "settled"
        assert outcome.exit_reason == "stop_loss"
        assert outcome.selected_exit == Decimal("509.00")
        assert outcome.scheduler_callback is not None and outcome.scheduler_callback.accepted
        assert scheduler.state_for(SID) == StrategyState.ANALYSIS_DUE
        assert scheduler.queue_size == 1
        assert book2.get_account(SID).open_trade is None

        settlement = outcome.state.settlement
        assert bundle2.sink.commit_close(outcome.state.to_dict(), market_ref=stop, occurred_at=stop.end_at) == "appended"
        assert bundle2.sink.commit_close(outcome.state.to_dict(), market_ref=stop, occurred_at=stop.end_at) == "replayed"
        assert (
            bundle2.sink.commit_account_change(
                SID,
                balance_before=settlement["balance_before"],
                balance_after=settlement["balance_after"],
                occurred_at=stop.end_at,
            )
            == "appended"
        )
        close_event = bundle2.store.list_events(event_type="paper_close")[0]
        assert close_event["envelope"]["payload"]["exit_reason"] == "stop_loss"
        assert close_event["envelope"]["payload"]["trade_id"] == "pt_rt"
        assert close_event["envelope"]["market_ref"]["second_event_id"] == stop.event_id
        stored_balance = bundle2.store.get_account(SID)["balance"]
        assert stored_balance == decimal_to_text(book2.get_account(SID).balance)
        assert Decimal(stored_balance) < Decimal("1000.00")
        assert bundle2.store.get_trade("pt_rt")["status"] == "settled"
        bundle2.handle.close()

    def test_restored_exit_monitor_bind_failure_raises(self, tmp_path):
        path = str(tmp_path / "exit-bindfail.db")
        bundle = open_bundle(tmp_path, name=Path(path).name)
        bundle.store.ensure_accounts([SID])
        book = PaperAccountBook()
        fill = _second(high="510.25", low="510.25")
        opened = book.open_trade(
            SID, SYMBOL, "long", None, "509.00", "515.00", immediate=True,
            fill_reference="510.25", trade_id="pt_bf",
        )
        state = make_exit_state(
            opened.trade,
            entry_fill_interval=(fill.start_at, fill.end_at),
            entry_filled_at=fill.end_at,
            generation=0,
            market_feed=FEED,
        )
        bundle.sink.commit_fill(state.to_dict(), market_ref=fill, occurred_at=fill.end_at)
        row = bundle.store.get_trade("pt_bf")
        exit_state = bundle.hydrator.restore_exit_state(row)
        book2 = bundle.hydrator.hydrate_account_book([SID])
        unrelated = StrategyScheduler([StrategySpec("pa:other")], clock=FakeClock(START))
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.hydrator.wrap_exit_monitor(exit_state, scheduler=unrelated, book=book2)
        assert excinfo.value.code == "monitor_bind_failed"
        bundle.handle.close()


class TestRestoredPendingMonitor:
    def test_pending_roundtrip_hydrates_and_fills_touch_second(self, tmp_path):
        registry, profile, packet, instruction = _real_instruction()
        scheduler = _real_scheduler(registry, profile, packet, instruction)
        record = scheduler.get(PENDING_SID)
        assert record is not None

        bundle = open_bundle(tmp_path, name="pending.db", run_id="run_g028_a")
        bundle.store.ensure_accounts([PENDING_SID])
        book = PaperAccountBook()
        monitor = PendingEntryMonitor.from_instruction(
            instruction,
            symbol="AAPL",
            decision_accepted_at=START,
            submitted_at=START,
            generation=record.generation,
            paper_account_book=book,
            scheduler=scheduler,
        )
        pending_dict = monitor.state.to_dict()
        created = bundle.sink.prepare_pending(pending_dict, event="created", occurred_at=START)
        assert bundle.sink.commit_pending(pending_dict, event="created", envelope=created) == "appended"
        account = book.get_account(PENDING_SID)
        assert account.pending is True
        persist_account_state(
            bundle.store, PENDING_SID, account, pending_trade=account.pending_trade.to_dict(), pending=True
        )
        snap = record.snapshot()
        assert (
            bundle.sink.commit_state(
                snap,
                transition={"from": "analysis_due", "to": "waiting_entry", "reason": "pending_created"},
                occurred_at=START,
            )
            == "appended"
        )
        assert (
            bundle.sink.commit_schedule(
                snap,
                timer_kind="pending_expiry",
                due_at=record.expires_at,
                decision_accepted_at=START,
                occurred_at=START,
            )
            == "appended"
        )
        assert (
            bundle.sink.commit_decision(
                snap, instruction, occurred_at=START
            )
            == "appended"
        )
        bundle.handle.close()

        bundle2 = reopen_bundle(tmp_path, name="pending.db", run_id="run_g028_b")
        book2 = bundle2.hydrator.hydrate_account_book([PENDING_SID])
        account2 = book2.get_account(PENDING_SID)
        assert account2 is not None
        assert account2.balance == Decimal("1000.00")
        assert account2.pending is True
        assert account2.pending_trade.pending_id == monitor.pending_id
        assert account2.open_trade is None
        pending_row = bundle2.store.list_pending_entries(strategy_id=PENDING_SID, status="pending")[0]
        restored = bundle2.hydrator.restore_pending_state(pending_row)
        assert restored.pending_id == monitor.pending_id
        assert restored.generation == record.generation
        assert restored.expires_at == monitor.expires_at

        scheduler2 = StrategyScheduler(
            [StrategySpec(PENDING_SID, profile, packet)],
            clock=FakeClock(START),
            registry=registry,
        )
        scheduler2.start(_ready_view(instruction["market_view_ref"]))
        record2 = scheduler2.get(PENDING_SID)
        record2.state = StrategyState.WAITING_ENTRY
        record2.pending_id = restored.pending_id
        record2.generation = restored.generation
        record2.expires_at = restored.expires_at
        record2.timer_tokens["pending_expiry"] = TimerToken(
            "tmr_restore", PENDING_SID, restored.generation, "pending_expiry", restored.expires_at
        )
        monitor2 = bundle2.hydrator.wrap_pending_monitor(
            restored, scheduler=scheduler2, book=book2
        )
        assert isinstance(monitor2, PendingEntryReducer)
        assert monitor2.pending_id == restored.pending_id

        x = instruction["entry_wait_seconds"]
        touch = _second(
            symbol="AAPL",
            start=START + timedelta(seconds=x - 1),
            high="100",
            low="99",
            watermark=START + timedelta(seconds=x + 3),
        )
        outcome = monitor2.on_second(touch, now=_stamp(START + timedelta(seconds=x + 3)))
        assert outcome.status == "filled"
        assert outcome.filled_price == Decimal("100")
        assert book2.get_account(PENDING_SID).pending is False
        assert book2.get_account(PENDING_SID).open_trade is not None
        assert scheduler2.state_for(PENDING_SID) == StrategyState.OPEN_POSITION
        assert scheduler2.get(PENDING_SID).open_position_id == book2.get_account(PENDING_SID).open_trade.trade_id

        assert (
            bundle2.sink.commit_pending(
                monitor2.state.to_dict(), event="created", envelope=created
            )
            == "replayed"
        )
        assert bundle2.store.get_pending_entry(monitor2.pending_id)["status"] == "filled"
        trade = book2.get_account(PENDING_SID).open_trade
        exit_state = make_exit_state(
            trade,
            entry_fill_interval=(touch.start_at, touch.end_at),
            entry_filled_at=_stamp(START + timedelta(seconds=x + 3)),
            generation=restored.generation,
            market_feed=FEED,
            decision_lineage=restored.decision_lineage.to_dict(),
        )
        assert (
            bundle2.sink.commit_fill(exit_state.to_dict(), market_ref=touch, occurred_at=touch.end_at)
            == "appended"
        )
        fill_event = bundle2.store.list_events(event_type="paper_fill")[0]
        assert fill_event["envelope"]["payload"]["trade_id"] == trade.trade_id
        assert fill_event["envelope"]["payload"]["entry_price"] == "100"
        assert fill_event["envelope"]["payload"]["planned_r"] == decimal_to_text(trade.planned_r)
        assert len(bundle2.store.list_events(event_type="pending_entry_created")) == 1
        bundle2.handle.close()

    def test_restored_pending_monitor_bind_failure_raises(self, tmp_path):
        bundle = open_bundle(tmp_path)
        bundle.store.ensure_accounts([SID])
        state = PendingEntryState.from_instruction(
            {
                "branch": "wait_for_price",
                "strategy_id": SID,
                "direction": "long",
                "symbol": SYMBOL,
                "entry_price": "100",
                "stop_price": "99",
                "target_price": "102",
                "entry_wait_seconds": 300,
                "instruction_hash": H64,
            },
            symbol=SYMBOL,
            decision_accepted_at=START,
            submitted_at=START,
            generation=0,
            validator=lambda value: True,
        )
        bundle.sink.commit_pending(state.to_dict(), event="created", occurred_at=START)
        row = bundle.store.get_pending_entry(state.pending_id)
        restored = bundle.hydrator.restore_pending_state(row)
        book = bundle.hydrator.hydrate_account_book([SID])
        unrelated = StrategyScheduler([StrategySpec("pa:other")], clock=FakeClock(START))
        with pytest.raises(RuntimePersistenceError) as excinfo:
            bundle.hydrator.wrap_pending_monitor(restored, scheduler=unrelated, book=book)
        assert excinfo.value.code == "monitor_bind_failed"
        bundle.handle.close()


class TestRestartHelpers:
    def test_cursors_and_live_blocked_helpers(self, tmp_path):
        bundle = open_bundle(tmp_path)
        assert bundle.hydrator.cursors() == []
        assert bundle.hydrator.live_blocked() is False
        first = _second(high="510.25", low="510.25")
        assert bundle.sink.advance_cursor(SYMBOL, first) is True
        assert bundle.hydrator.live_blocked() is True
        assert len(bundle.hydrator.cursors()) == 1
        bar = _bar(start=START + timedelta(minutes=1), end=START + timedelta(minutes=2))
        cursor = _stamp(START + timedelta(minutes=2))
        bundle.sink.commit_market_recovery(SYMBOL, bars=[bar], recovery_cursor=cursor, occurred_at=START)
        # The aggregate helper mirrors RestoreSnapshot.live_blocked: a stored
        # last-second cursor keeps the gate until the engine's reconciliation
        # is complete; the per-symbol column is what unblocks live seconds.
        assert bundle.hydrator.live_blocked() is True
        assert bundle.sink.is_live_blocked(SYMBOL) is False
        assert bundle.hydrator.cursors()[0]["recovery_cursor"] == cursor
        bundle.handle.close()

    def test_sink_passthrough_cursor_methods(self, tmp_path):
        bundle = open_bundle(tmp_path)
        second = _second(high="510.25", low="510.25")
        assert bundle.sink.advance_cursor(SYMBOL, second) is True
        assert bundle.sink.is_live_blocked(SYMBOL) is False
        assert bundle.sink.get_cursor(SYMBOL)["last_second_event_id"] == second.event_id
        bundle.sink.mark_live_blocked(SYMBOL, reason="restart gap")
        assert bundle.sink.is_live_blocked(SYMBOL) is True
        assert bundle.sink.get_cursor(SYMBOL)["pending_recovery_json"] == {"reason": "restart gap"}
        bundle.handle.close()
