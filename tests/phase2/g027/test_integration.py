"""End-to-end G027 integration: the store/recovery layers consume real
G024/G023/G025/G026/G020/G019 shapes (paper account book transitions, real
scheduler records, real pending-entry and exit states, real second aggregates
and recovery bars)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from pa_engine.market_sources.alpaca.history import RecoveryBar
from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent
from pa_engine.paper.core import PaperAccountBook
from pa_engine.paper.entry import PendingEntryState
from pa_engine.paper.exit import ExitState
from pa_engine.storage import (
    DueWork,
    RuntimeRecovery,
    RuntimeStore,
    StorageConflictError,
    account_change_key,
    close_key,
    decimal_to_text,
    envelope_for_account_change,
    envelope_for_close,
    envelope_for_fill,
    envelope_for_market_recovery,
    envelope_for_pending_entry,
    envelope_for_schedule,
    fill_key,
    instant_from_text,
    instant_to_text,
    is_recovery_bar_ref,
    is_second_ref,
    make_recovery_bar_ref,
    make_second_ref,
    open_database,
    pending_key,
    recovery_key,
    schedule_key,
)
from pa_engine.strategy.runtime import FakeClock, StrategyRecord, StrategyScheduler, StrategyState

UTC = timezone.utc
START = "2026-08-12T14:00:00Z"
H64 = "a" * 64
FEED = {
    "schema_version": "0.1.0",
    "provider": "alpaca",
    "feed": "iex",
    "coverage": "single_exchange",
}
FILL_INTERVAL = ("2026-08-12T14:00:04Z", "2026-08-12T14:00:05Z")


@dataclass
class Bundle:
    handle: Any
    store: RuntimeStore
    recovery: RuntimeRecovery
    clock: "StorageClock"


class StorageClock:
    """Adapter turning the G023 FakeClock into the storage callable clock."""

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock

    def __call__(self) -> datetime:
        return self._clock.now()

    def advance(self, seconds: int = 1) -> None:
        self._clock.advance(seconds)


def open_store(
    path: str, run_id: str = "run_a", clock: StorageClock | None = None, mode: str = "automatic"
) -> Bundle:
    clock = clock if clock is not None else StorageClock(FakeClock(instant_from_text(START)))
    handle = open_database(path, run_id=run_id, mode=mode, clock=clock)
    store = RuntimeStore(handle)
    return Bundle(handle, store, RuntimeRecovery(store, handle), clock)


def reopen(
    path: str,
    run_id: str = "run_b",
    clock: StorageClock | None = None,
    mode: str = "automatic",
    now: datetime | str | None = None,
) -> tuple[Bundle, Any]:
    bundle = open_store(path, run_id=run_id, clock=clock, mode=mode)
    snapshot = bundle.recovery.restore(now=now)
    return bundle, snapshot


def at_shift(seconds: int, base: str = START) -> str:
    return instant_to_text(instant_from_text(base) + timedelta(seconds=seconds))


def ready_view(view_id: str = "view-1", snapshot_id: str = "snapshot-1") -> dict[str, Any]:
    return {
        "shared_market_view_id": view_id,
        "closed_snapshot_id": snapshot_id,
        "regular_session": True,
        "fresh": True,
        "complete": True,
        "non_gapped": True,
        "sufficient": True,
        "frozen_second": True,
    }


def second_event(symbol: str = "SPY", end_at: str = "2026-08-12T14:00:05Z") -> SecondAggregateEvent:
    start = instant_from_text(end_at) - timedelta(seconds=1)
    return SecondAggregateEvent(
        symbol,
        instant_to_text(start),
        end_at,
        "510.25",
        "510.25",
        "510.25",
        "510.25",
        "1",
        contributor_ids=(f"trade-{end_at}",),
        execution_watermark=instant_to_text(instant_from_text(end_at) + timedelta(seconds=3)),
        status="frozen",
        quality={"status": "ok", "codes": [], "connection_gap": False},
    )


def recovery_bar(
    symbol: str = "SPY",
    start_at: str = "2026-08-12T14:00:05Z",
    recovery_cursor: str | None = None,
) -> RecoveryBar:
    cursor = recovery_cursor or instant_to_text(instant_from_text(start_at) + timedelta(minutes=1))
    return RecoveryBar(
        instrument={"asset_class": "us_equity", "symbol": symbol, "venue": "IEX", "currency": "USD"},
        timeframe={"unit": "minute", "multiplier": 1},
        session={"kind": "regular", "timezone": "America/New_York"},
        start_at=start_at,
        end_at=instant_to_text(instant_from_text(start_at) + timedelta(minutes=1)),
        open="100",
        high="101",
        low="99",
        close="100",
        volume="10",
        complete=True,
        coverage_state="complete",
        source_request_hash=H64,
        recovery_cursor=cursor,
        market_feed=FEED,
    )


def book_open(book: PaperAccountBook, strategy_id: str, *, direction: str, trade_id: str):
    if direction == "long":
        return book.open_trade(strategy_id, "SPY", "long", "510.25", "509.00", "515.00", trade_id=trade_id)
    return book.open_trade(strategy_id, "QQQ", "short", "300.00", "305.00", "295.00", trade_id=trade_id)


def open_exit_state(book: PaperAccountBook, result, *, generation: int = 0) -> ExitState:
    trade = result.trade
    return ExitState(
        strategy_id=trade.strategy_id,
        trade_id=trade.trade_id,
        symbol=trade.symbol,
        direction=trade.direction,
        entry_price=trade.entry_price,
        stop_price=trade.stop_price,
        target_price=trade.target_price,
        quantity=trade.quantity,
        trade_lineage=trade.to_dict(),
        generation=generation,
        market_feed=FEED,
        status="open",
        outcome_status="open",
        entry_fill_interval=FILL_INTERVAL,
        entry_filled_at=instant_from_text(FILL_INTERVAL[1]),
    )


def settled_exit_state(
    open_state: ExitState, close_result, *, exit_reason: str, closed_at: str, exit_price: str
) -> ExitState:
    return ExitState(
        strategy_id=open_state.strategy_id,
        trade_id=open_state.trade_id,
        symbol=open_state.symbol,
        direction=open_state.direction,
        entry_price=open_state.entry_price,
        stop_price=open_state.stop_price,
        target_price=open_state.target_price,
        quantity=open_state.quantity,
        trade_lineage=open_state.trade_lineage,
        decision_lineage=open_state.decision_lineage,
        entry_event=open_state.entry_event,
        entry_fill_interval=open_state.entry_fill_interval,
        entry_filled_at=open_state.entry_filled_at,
        generation=open_state.generation,
        market_feed=open_state.market_feed,
        status="settled",
        outcome_status="settled",
        terminal_reason=exit_reason,
        selected_exit=Decimal(exit_price),
        exit_reason=exit_reason,
        settlement=dict(close_result.settlement) if close_result.settlement is not None else None,
        closed_at=instant_from_text(closed_at),
    )


def strategy_snapshot(strategy_id: str, state: StrategyState, *, generation: int = 0, **overrides: Any) -> dict[str, Any]:
    record = StrategyRecord(
        strategy_id=strategy_id,
        state=state,
        generation=generation,
        enabled=True,
        **overrides,
    )
    return record.snapshot()


def commit_fill(store: RuntimeStore, state: dict[str, Any], second: SecondAggregateEvent, *, mode: str = "automatic") -> None:
    envelope = envelope_for_fill(
        state,
        mode=mode,
        occurred_at=second.end_at,
        idempotency_key=fill_key(state["strategy_id"], state["trade_id"]),
        market_ref=make_second_ref(second.to_dict()),
    )
    store.commit_trade(state, event="filled", envelope=envelope)


def commit_close(store: RuntimeStore, state: dict[str, Any], second: SecondAggregateEvent, *, mode: str = "automatic") -> None:
    envelope = envelope_for_close(
        state,
        mode=mode,
        occurred_at=second.end_at,
        idempotency_key=close_key(
            state["strategy_id"], state["trade_id"], state["exit_reason"], state["closed_at"]
        ),
        market_ref=make_second_ref(second.to_dict()),
    )
    store.commit_trade(state, event="closed", envelope=envelope)


def commit_account_change(store: RuntimeStore, strategy_id: str, close_result, *, mode: str = "automatic") -> None:
    before = close_result.settlement["balance_before"]
    after = close_result.account.balance
    envelope = envelope_for_account_change(
        strategy_id,
        0,
        mode=mode,
        balance_before=before,
        balance_after=after,
        occurred_at=START,
        idempotency_key=account_change_key(strategy_id, before, after),
    )
    store.commit_account_event(strategy_id, balance_before=before, balance_after=after, envelope=envelope)


def full_lifecycle(
    store: RuntimeStore,
    book: PaperAccountBook,
    result,
    *,
    strategy_id: str,
    exit_price: str,
    exit_reason: str,
    generation: int = 0,
) -> ExitState:
    open_state = open_exit_state(book, result, generation=generation)
    commit_fill(store, open_state.to_dict(), second_event(symbol=open_state.symbol))
    store.commit_strategy_state(
        strategy_snapshot(
            strategy_id,
            StrategyState.OPEN_POSITION,
            generation=generation,
            open_position_id=result.trade.trade_id,
            open_position_generation=generation,
        )
    )
    close_result = book.close_trade(
        strategy_id, exit_price, position_id=result.trade.trade_id, reason=exit_reason
    )
    assert close_result.accepted
    closed_at = at_shift(3600)
    settled = settled_exit_state(
        open_state, close_result, exit_reason=exit_reason, closed_at=closed_at, exit_price=exit_price
    )
    commit_close(store, settled.to_dict(), second_event(symbol=open_state.symbol, end_at=closed_at))
    commit_account_change(store, strategy_id, close_result)
    return settled


class TestG024AccountIntegration:
    def test_long_full_lifecycle_persisted_and_restored_exact(self, tmp_path):
        path = str(tmp_path / "g024-long.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_long")
        assert opened.accepted
        full_lifecycle(
            bundle.store,
            book,
            opened,
            strategy_id="pa:rose",
            exit_price="515.00",
            exit_reason="target",
        )
        expected_balance = decimal_to_text(book.get_account("pa:rose").balance)
        snap = bundle.recovery.restore()
        account = bundle.store.get_account("pa:rose")
        assert account["balance"] == expected_balance
        assert account["initial_balance"] == "1000.00"
        trade = bundle.store.get_trade("pt_long")
        assert trade["symbol"] == "SPY"
        assert trade["direction"] == "long"
        assert trade["status"] == "settled"
        assert trade["quantity"] == decimal_to_text(opened.trade.quantity)
        types = {event["event_type"] for event in bundle.store.list_events()}
        assert {"paper_fill", "paper_close", "account_changed"} <= types
        assert all(event["mode"] == "automatic" for event in bundle.store.list_events())
        assert snap.market_paused == ()
        assert bundle.store.get_strategy_state("pa:rose")["state"] == "analysis_due"
        bundle.handle.close()

    def test_short_full_lifecycle_mirrors_long(self, tmp_path):
        path = str(tmp_path / "g024-short.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="short", trade_id="pt_short")
        assert opened.accepted
        full_lifecycle(
            bundle.store,
            book,
            opened,
            strategy_id="pa:rose",
            exit_price="305.00",
            exit_reason="stop",
        )
        expected_balance = decimal_to_text(book.get_account("pa:rose").balance)
        assert Decimal(expected_balance) < Decimal("1000.00")
        bundle.recovery.restore()
        account = bundle.store.get_account("pa:rose")
        assert account["balance"] == expected_balance
        assert bundle.store.get_trade("pt_short")["direction"] == "short"
        bundle.handle.close()

    def test_restore_never_resets_initial_balance(self, tmp_path):
        path = str(tmp_path / "g024-init.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_1")
        full_lifecycle(bundle.store, book, opened, strategy_id="pa:rose", exit_price="515.00", exit_reason="target")
        snap = bundle.recovery.restore()
        assert all(row["initial_balance"] == "1000.00" for row in snap.accounts)
        assert all(row["balance"] == decimal_to_text(book.get_account("pa:rose").balance) for row in snap.accounts)
        bundle.handle.close()

    def test_long_full_lifecycle_reopen_with_new_run_id(self, tmp_path):
        path = str(tmp_path / "g024-reopen.db")
        bundle = open_store(path, run_id="run_g024_a")
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_long")
        full_lifecycle(bundle.store, book, opened, strategy_id="pa:rose", exit_price="515.00", exit_reason="target")
        expected_balance = decimal_to_text(book.get_account("pa:rose").balance)
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_g024_b", clock=bundle.clock)
        account = reopened.store.get_account("pa:rose")
        assert account["balance"] == expected_balance
        assert account["initial_balance"] == "1000.00"
        assert reopened.store.get_trade("pt_long")["status"] == "settled"
        assert all(event["mode"] == "automatic" for event in reopened.store.list_events())
        assert snap.market_paused == ()
        reopened.handle.close()

    def test_duplicate_fill_delivery_is_replay_noop(self, tmp_path):
        path = str(tmp_path / "g024-replay.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_r")
        state = open_exit_state(book, opened)
        commit_fill(bundle.store, state.to_dict(), second_event())
        commit_fill(bundle.store, state.to_dict(), second_event())
        assert len(bundle.store.list_events(event_type="paper_fill")) == 1
        assert bundle.store.get_account("pa:rose")["balance"] == "1000.00"
        bundle.handle.close()

    def test_duplicate_account_delivery_is_replay_noop_balance_unchanged(self, tmp_path):
        path = str(tmp_path / "g024-ac7.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_ac7")
        full_lifecycle(bundle.store, book, opened, strategy_id="pa:rose", exit_price="515.00", exit_reason="target")
        events_before = len(bundle.store.list_events())
        balance_before = bundle.store.get_account("pa:rose")["balance"]
        bundle.store.commit_account_event(
            "pa:rose",
            balance_before=Decimal("1000.00"),
            balance_after=Decimal(balance_before),
            envelope=envelope_for_account_change(
                "pa:rose",
                0,
                balance_before="1000.00",
                balance_after=balance_before,
                occurred_at=START,
                idempotency_key=account_change_key("pa:rose", "1000.00", balance_before),
            ),
        )
        assert bundle.store.get_account("pa:rose")["balance"] == balance_before
        assert len(bundle.store.list_events()) == events_before
        bundle.handle.close()

    def test_same_idempotency_key_different_content_conflicts(self, tmp_path):
        path = str(tmp_path / "g024-conflict.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_c")
        state = open_exit_state(book, opened)
        commit_fill(bundle.store, state.to_dict(), second_event())
        other = second_event(end_at="2026-08-12T14:00:07Z")
        conflicting = envelope_for_fill(
            state.to_dict(),
            occurred_at=other.end_at,
            idempotency_key=fill_key("pa:rose", "pt_c"),
            market_ref=make_second_ref(other.to_dict()),
        )
        with pytest.raises(StorageConflictError):
            bundle.store.commit_trade(state.to_dict(), event="filled", envelope=conflicting)
        assert len(bundle.store.list_events(event_type="paper_fill")) == 1
        bundle.handle.close()

    def test_open_long_and_short_side_by_side_restore_distinct(self, tmp_path):
        path = str(tmp_path / "g024-pair.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose", "pa:cave"])
        book = PaperAccountBook()
        long_result = book_open(book, "pa:rose", direction="long", trade_id="pt_l")
        short_result = book_open(book, "pa:cave", direction="short", trade_id="pt_s")
        for result in (long_result, short_result):
            state = open_exit_state(book, result)
            commit_fill(bundle.store, state.to_dict(), second_event(symbol=state.symbol))
            bundle.store.commit_strategy_state(
                strategy_snapshot(
                    result.strategy_id,
                    StrategyState.OPEN_POSITION,
                    open_position_id=result.trade.trade_id,
                )
            )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_g024p", clock=bundle.clock)
        assert snap.market_paused == ("pa:cave", "pa:rose")
        assert reopened.store.get_trade("pt_l")["status"] == "open"
        assert reopened.store.get_trade("pt_s")["status"] == "open"
        assert {row["balance"] for row in snap.accounts} == {"1000.00"}
        reopened.handle.close()

    def test_restore_appends_no_extra_paper_events(self, tmp_path):
        path = str(tmp_path / "g024-events.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_e")
        full_lifecycle(bundle.store, book, opened, strategy_id="pa:rose", exit_price="515.00", exit_reason="target")
        paper_before = [e["event_type"] for e in bundle.store.list_events() if e["event_type"].startswith("paper_")]
        bundle.recovery.restore()
        paper_after = [e["event_type"] for e in bundle.store.list_events() if e["event_type"].startswith("paper_")]
        assert paper_after == paper_before
        bundle.handle.close()


class TestG023SchedulerPersistence:
    def _scheduler(self, strategy_id: str = "pa:test") -> tuple[StrategyScheduler, StrategyRecord]:
        scheduler = StrategyScheduler([strategy_id], clock=FakeClock(instant_from_text(START)))
        scheduler._verify_success_instruction = lambda _record, _instruction: True
        scheduler.start(ready_view())
        return scheduler, scheduler.get(strategy_id)

    def test_no_entry_scheduler_snapshot_restores_sleeping_exact(self, tmp_path):
        path = str(tmp_path / "g023-sleep.db")
        scheduler, record = self._scheduler()
        assert scheduler.submit_instruction(
            "pa:test", {"branch": "no_entry", "next_call_after_seconds": 60}, decision_accepted_at=START
        )
        assert record.state is StrategyState.SLEEPING
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        snap = record.snapshot()
        bundle.store.commit_strategy_state(snap)
        envelope = envelope_for_schedule(
            snap,
            timer_kind="sleep",
            due_at=record.next_due_at,
            decision_accepted_at=record.decision_accepted_at,
            occurred_at=START,
            idempotency_key=schedule_key(
                snap["strategy_id"], snap["generation"], "sleep", instant_to_text(record.next_due_at)
            ),
        )
        bundle.store.commit_schedule_set("pa:test", envelope=envelope)
        assert bundle.store.get_account("pa:test")["balance"] == "1000.00"
        assert bundle.store.list_events(event_type="account_changed") == []
        bundle.handle.close()

        reopened, rs = reopen(path, run_id="run_g023s", clock=bundle.clock, now=at_shift(30))
        row = reopened.store.get_strategy_state("pa:test")
        assert row["state"] == "sleeping"
        assert row["next_due_at"] == instant_to_text(record.next_due_at)
        timers = reopened.store.get_timers("pa:test")
        assert len(timers) == 1
        assert timers[0]["kind"] == "sleep"
        assert timers[0]["due_at"] == instant_to_text(record.next_due_at)
        assert timers[0]["fired"] == 0
        assert rs.due_once == ()
        assert reopened.store.list_events(event_type="strategy_state_changed") == []
        reopened.handle.close()

    def test_no_entry_persist_never_mutates_balance(self, tmp_path):
        path = str(tmp_path / "g023-balance.db")
        scheduler, record = self._scheduler()
        assert scheduler.submit_instruction(
            "pa:test", {"branch": "no_entry", "next_call_after_seconds": 60}, decision_accepted_at=START
        )
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        snap = record.snapshot()
        bundle.store.commit_strategy_state(snap)
        bundle.recovery.restore(now=at_shift(30))
        account = bundle.store.get_account("pa:test")
        assert account["balance"] == "1000.00"
        assert account["initial_balance"] == "1000.00"
        assert bundle.store.list_events(event_type="account_changed") == []
        bundle.handle.close()

    def test_overdue_sleep_due_fires_exactly_once(self, tmp_path):
        path = str(tmp_path / "g023-due.db")
        scheduler, record = self._scheduler()
        assert scheduler.submit_instruction(
            "pa:test", {"branch": "no_entry", "next_call_after_seconds": 60}, decision_accepted_at=START
        )
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        bundle.store.commit_strategy_state(record.snapshot())
        bundle.store.commit_schedule_set(
            "pa:test",
            envelope=envelope_for_schedule(
                record.snapshot(),
                timer_kind="sleep",
                due_at=record.next_due_at,
                decision_accepted_at=record.decision_accepted_at,
                occurred_at=START,
                idempotency_key=schedule_key(
                    "pa:test", record.generation, "sleep", instant_to_text(record.next_due_at)
                ),
            ),
        )
        snap = bundle.recovery.restore(now=at_shift(61))
        assert snap.due_once == (DueWork(strategy_id="pa:test", kind="sleep_due"),)
        assert bundle.store.get_strategy_state("pa:test")["state"] == "analysis_due"
        bundle.handle.close()

        reopened, snap2 = reopen(path, run_id="run_g023d", clock=bundle.clock, now=at_shift(120))
        assert snap2.due_once == ()
        assert len(reopened.store.list_events(event_type="strategy_state_changed")) == 1
        assert reopened.store.get_strategy_state("pa:test")["state"] == "analysis_due"
        reopened.handle.close()

    def test_sleep_due_at_now_is_overdue(self, tmp_path):
        path = str(tmp_path / "g023-now.db")
        scheduler, record = self._scheduler()
        assert scheduler.submit_instruction(
            "pa:test", {"branch": "no_entry", "next_call_after_seconds": 60}, decision_accepted_at=START
        )
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        bundle.store.commit_strategy_state(record.snapshot())
        bundle.store.commit_schedule_set(
            "pa:test",
            envelope=envelope_for_schedule(
                record.snapshot(),
                timer_kind="sleep",
                due_at=record.next_due_at,
                decision_accepted_at=record.decision_accepted_at,
                occurred_at=START,
                idempotency_key=schedule_key(
                    "pa:test", record.generation, "sleep", instant_to_text(record.next_due_at)
                ),
            ),
        )
        snap = bundle.recovery.restore(now=instant_to_text(record.next_due_at))
        assert [work.kind for work in snap.due_once] == ["sleep_due"]
        bundle.handle.close()

    def test_wait_for_price_scheduler_with_real_pending_state_restore(self, tmp_path):
        path = str(tmp_path / "g023-pend.db")
        scheduler, record = self._scheduler()
        instruction = {
            "branch": "wait_for_price",
            "strategy_id": "pa:test",
            "direction": "long",
            "symbol": "SPY",
            "entry_price": "100",
            "stop_price": "99",
            "target_price": "102",
            "entry_wait_seconds": 300,
            "instruction_hash": H64,
        }
        assert scheduler.submit_instruction("pa:test", instruction, decision_accepted_at=START)
        assert record.state is StrategyState.WAITING_ENTRY
        pending = PendingEntryState.from_instruction(
            instruction,
            symbol="SPY",
            decision_accepted_at=START,
            submitted_at=START,
            generation=record.generation,
            pending_id=record.pending_id,
            validator=lambda value: True,
        )
        assert pending.pending_id == record.pending_id
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        pending_dict = pending.to_dict()
        bundle.store.commit_pending_entry(
            pending_dict,
            event="created",
            envelope=envelope_for_pending_entry(
                pending_dict,
                "created",
                occurred_at=START,
                idempotency_key=pending_key("pa:test", pending.pending_id, "created"),
            ),
        )
        bundle.store.commit_strategy_state(record.snapshot())
        bundle.handle.close()

        reopened, snap = reopen(path, run_id="run_g023p", clock=bundle.clock, now=at_shift(30))
        row = reopened.store.get_strategy_state("pa:test")
        assert row["state"] == "waiting_entry"
        assert row["pending_id"] == pending.pending_id
        assert row["expires_at"] == instant_to_text(pending.expires_at)
        assert snap.market_paused == ("pa:test",)
        assert snap.due_once == ()
        reopened.handle.close()

    def test_wait_for_price_overdue_pending_expiry_due_once(self, tmp_path):
        path = str(tmp_path / "g023-xdue.db")
        scheduler, record = self._scheduler()
        instruction = {
            "branch": "wait_for_price",
            "strategy_id": "pa:test",
            "direction": "long",
            "symbol": "SPY",
            "entry_price": "100",
            "stop_price": "99",
            "target_price": "102",
            "entry_wait_seconds": 300,
            "instruction_hash": H64,
        }
        assert scheduler.submit_instruction("pa:test", instruction, decision_accepted_at=START)
        pending = PendingEntryState.from_instruction(
            instruction,
            symbol="SPY",
            decision_accepted_at=START,
            submitted_at=START,
            generation=record.generation,
            pending_id=record.pending_id,
            validator=lambda value: True,
        )
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        pending_dict = pending.to_dict()
        bundle.store.commit_pending_entry(
            pending_dict,
            event="created",
            envelope=envelope_for_pending_entry(
                pending_dict,
                "created",
                occurred_at=START,
                idempotency_key=pending_key("pa:test", pending.pending_id, "created"),
            ),
        )
        bundle.store.commit_strategy_state(record.snapshot())
        snap = bundle.recovery.restore(now=at_shift(301))
        assert snap.due_once == (DueWork(strategy_id="pa:test", kind="pending_expiry_due"),)
        assert bundle.store.get_strategy_state("pa:test")["state"] == "waiting_entry"
        assert bundle.store.get_pending_entry(pending.pending_id)["status"] == "pending"
        again = bundle.recovery.restore(now=at_shift(400))
        assert again.due_once == ()
        expired = pending_dict.copy()
        expired["status"] = "expired"
        expired["terminal_reason"] = "expired at deadline"
        bundle.store.commit_pending_entry(
            expired,
            event="expired",
            envelope=envelope_for_pending_entry(
                expired,
                "expired",
                occurred_at=at_shift(301),
                idempotency_key=pending_key("pa:test", pending.pending_id, "expired"),
            ),
        )
        final = bundle.recovery.restore(now=at_shift(400))
        assert final.due_once == ()
        assert bundle.store.get_strategy_state("pa:test")["state"] == "analysis_due"
        bundle.handle.close()

    def test_disabled_scheduler_snapshot_restores_disabled_exact(self, tmp_path):
        path = str(tmp_path / "g023-disabled.db")
        scheduler, record = self._scheduler()
        assert scheduler.disable("pa:test")
        assert record.state is StrategyState.DISABLED
        assert record.enabled is False
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        bundle.store.commit_strategy_state(record.snapshot())
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_g023x2", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:test")
        assert row["state"] == "disabled"
        assert row["enabled"] == 0
        assert snap.due_once == ()
        assert reopened.store.list_events() == []
        reopened.handle.close()


class TestG025PendingIntegration:
    def _real_pending(self, *, x: int = 300, generation: int = 7) -> PendingEntryState:
        return PendingEntryState.from_instruction(
            {
                "branch": "wait_for_price",
                "strategy_id": "pa:test",
                "direction": "long",
                "symbol": "SPY",
                "entry_price": "100",
                "stop_price": "99",
                "target_price": "102",
                "entry_wait_seconds": x,
            },
            symbol="SPY",
            decision_accepted_at=START,
            submitted_at=START,
            generation=generation,
            validator=lambda value: True,
        )

    def test_real_pending_state_created_restore_waiting_entry_market_paused(self, tmp_path):
        path = str(tmp_path / "g025-wait.db")
        pending = self._real_pending()
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        pending_dict = pending.to_dict()
        bundle.store.commit_pending_entry(
            pending_dict,
            event="created",
            envelope=envelope_for_pending_entry(
                pending_dict,
                "created",
                occurred_at=START,
                idempotency_key=pending_key("pa:test", pending.pending_id, "created"),
            ),
        )
        bundle.store.commit_strategy_state(
            strategy_snapshot(
                "pa:test",
                StrategyState.WAITING_ENTRY,
                generation=pending.generation,
                pending_id=pending.pending_id,
                expires_at=pending.expires_at,
            )
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_g025", clock=bundle.clock, now=at_shift(30))
        row = reopened.store.get_strategy_state("pa:test")
        assert row["state"] == "waiting_entry"
        assert row["pending_id"] == pending.pending_id
        assert reopened.store.get_pending_entry(pending.pending_id)["status"] == "pending"
        assert snap.market_paused == ("pa:test",)
        assert snap.due_once == ()
        reopened.handle.close()

    def test_real_pending_state_overdue_due_once_then_noop(self, tmp_path):
        path = str(tmp_path / "g025-due.db")
        pending = self._real_pending(x=300)
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:test"])
        pending_dict = pending.to_dict()
        bundle.store.commit_pending_entry(
            pending_dict,
            event="created",
            envelope=envelope_for_pending_entry(
                pending_dict,
                "created",
                occurred_at=START,
                idempotency_key=pending_key("pa:test", pending.pending_id, "created"),
            ),
        )
        bundle.store.commit_strategy_state(
            strategy_snapshot(
                "pa:test",
                StrategyState.WAITING_ENTRY,
                generation=pending.generation,
                pending_id=pending.pending_id,
                expires_at=pending.expires_at,
            )
        )
        snap = bundle.recovery.restore(now=at_shift(301))
        assert snap.due_once == (DueWork(strategy_id="pa:test", kind="pending_expiry_due"),)
        again = bundle.recovery.restore(now=at_shift(500))
        assert again.due_once == ()
        expired = pending_dict.copy()
        expired["status"] = "expired"
        expired["terminal_reason"] = "expired at deadline"
        bundle.store.commit_pending_entry(
            expired,
            event="expired",
            envelope=envelope_for_pending_entry(
                expired,
                "expired",
                occurred_at=at_shift(301),
                idempotency_key=pending_key("pa:test", pending.pending_id, "expired"),
            ),
        )
        final = bundle.recovery.restore(now=at_shift(500))
        assert final.due_once == ()
        assert len(bundle.store.list_events(event_type="pending_entry_expired")) == 1
        assert bundle.store.get_strategy_state("pa:test")["state"] == "analysis_due"
        bundle.handle.close()


class TestG026ExitIntegration:
    def _open_bundle(self, tmp_path, name: str):
        path = str(tmp_path / name)
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_x")
        assert opened.accepted
        state = open_exit_state(book, opened)
        commit_fill(bundle.store, state.to_dict(), second_event())
        bundle.store.commit_strategy_state(
            strategy_snapshot(
                "pa:rose",
                StrategyState.OPEN_POSITION,
                open_position_id="pt_x",
            )
        )
        return path, bundle, book, opened, state

    def test_real_exit_state_open_restore_exact_and_market_paused(self, tmp_path):
        path, bundle, _book, _opened, state = self._open_bundle(tmp_path, "g026-open.db")
        before = bundle.store.list_events()
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_g026o", clock=bundle.clock)
        row = reopened.store.get_strategy_state("pa:rose")
        assert row["state"] == "open_position"
        assert row["open_position_id"] == "pt_x"
        assert snap.market_paused == ("pa:rose",)
        assert snap.due_once == ()
        assert reopened.store.list_events() == before
        assert state.to_dict()["entry_price"] == "510.25"
        reopened.handle.close()

    def test_real_exit_state_settled_reconciles_analysis_due(self, tmp_path):
        path, bundle, book, opened, state = self._open_bundle(tmp_path, "g026-settled.db")
        close_result = book.close_trade("pa:rose", "515.00", position_id="pt_x", reason="target")
        assert close_result.accepted
        settled = settled_exit_state(
            state, close_result, exit_reason="target", closed_at=at_shift(3600), exit_price="515.00"
        )
        commit_close(bundle.store, settled.to_dict(), second_event(end_at=at_shift(3600)))
        commit_account_change(bundle.store, "pa:rose", close_result)
        snap = bundle.recovery.restore()
        row = bundle.store.get_strategy_state("pa:rose")
        assert row["state"] == "analysis_due"
        assert row["open_position_id"] is None
        assert snap.due_once == ()
        events = bundle.store.list_events(event_type="strategy_state_changed")
        assert len(events) == 1
        assert events[0]["envelope"]["payload"]["reason"] == "trade_settled_restart"
        bundle.handle.close()

    def test_real_exit_state_restore_appends_no_paper_events(self, tmp_path):
        path, bundle, _book, _opened, _state = self._open_bundle(tmp_path, "g026-noextra.db")
        paper_before = [e for e in bundle.store.list_events() if e["event_type"].startswith("paper_")]
        bundle.recovery.restore()
        paper_after = [e for e in bundle.store.list_events() if e["event_type"].startswith("paper_")]
        assert paper_after == paper_before
        bundle.handle.close()


class TestRealMarketRefs:
    def test_second_aggregate_event_ref_drives_fill_envelope(self, tmp_path):
        path = str(tmp_path / "refs-second.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_ref")
        second = second_event()
        ref = make_second_ref(second.to_dict())
        assert is_second_ref(ref)
        assert ref["symbol"] == "SPY"
        assert ref["second_event_id"] == second.event_id
        assert ref["second_event_hash"] == second.content_hash
        state = open_exit_state(book, opened)
        commit_fill(bundle.store, state.to_dict(), second)
        event = bundle.store.list_events(event_type="paper_fill")[0]
        assert event["envelope"]["market_ref"] == ref
        bundle.handle.close()

    def test_recovery_bar_ref_drives_commit_recovery_with_cursor_advance(self, tmp_path):
        path = str(tmp_path / "refs-bar.db")
        bundle = open_store(path)
        bundle.store.mark_live_blocked("SPY", reason="restart gap")
        assert bundle.store.is_live_blocked("SPY") is True
        bundle.store.advance_cursor("SPY", make_second_ref(second_event(end_at="2026-08-12T14:00:05Z").to_dict()))
        bar_one = recovery_bar(start_at="2026-08-12T14:00:05Z")
        bar_two = recovery_bar(start_at="2026-08-12T14:01:05Z", recovery_cursor="2026-08-12T14:02:05Z")
        refs = [make_recovery_bar_ref(bar.to_dict()) for bar in (bar_one, bar_two)]
        assert all(is_recovery_bar_ref(ref) for ref in refs)
        assert [ref["path_precision"] for ref in refs] == ["minute", "minute"]
        assert refs[0]["end_at"] == refs[1]["start_at"]
        cursor = "2026-08-12T14:02:05Z"
        envelope = envelope_for_market_recovery(
            "SPY",
            bars=refs,
            recovery_cursor=cursor,
            occurred_at=START,
            idempotency_key=recovery_key("SPY", cursor, len(refs)),
        )
        bundle.store.commit_recovery("SPY", bars=refs, recovery_cursor=cursor, envelope=envelope)
        assert bundle.store.is_live_blocked("SPY") is False
        stored = bundle.store.get_cursor("SPY")
        assert stored["recovery_cursor"] == cursor
        assert stored["pending_recovery_json"] is None
        event = bundle.store.list_events(event_type="market_recovery")[0]
        assert event["envelope"]["payload"]["bar_count"] == 2
        bundle.store.advance_cursor("SPY", make_second_ref(second_event(end_at="2026-08-12T14:02:05Z").to_dict()))
        assert bundle.store.get_cursor("SPY")["last_second_end_at"] == "2026-08-12T14:02:05Z"
        bundle.handle.close()


class TestEndToEndAcceptance:
    def test_ac1_two_strategies_win_loss_distinct_balances(self, tmp_path):
        path = str(tmp_path / "ac1.db")
        bundle = open_store(path, run_id="run_ac1_a")
        bundle.store.ensure_accounts(["pa:rose", "pa:cave"])
        book = PaperAccountBook()
        long_result = book_open(book, "pa:rose", direction="long", trade_id="pt_rose")
        short_result = book_open(book, "pa:cave", direction="short", trade_id="pt_cave")
        full_lifecycle(
            bundle.store,
            book,
            long_result,
            strategy_id="pa:rose",
            exit_price="515.00",
            exit_reason="target",
        )
        full_lifecycle(
            bundle.store,
            book,
            short_result,
            strategy_id="pa:cave",
            exit_price="305.00",
            exit_reason="stop",
        )
        rose_expected = decimal_to_text(book.get_account("pa:rose").balance)
        cave_expected = decimal_to_text(book.get_account("pa:cave").balance)
        assert Decimal(rose_expected) > Decimal("1000.00")
        assert Decimal(cave_expected) < Decimal("1000.00")
        assert rose_expected != cave_expected
        paper_before = [e["event_type"] for e in bundle.store.list_events() if e["event_type"].startswith("paper_")]
        snap = bundle.recovery.restore()
        by_id = {row["strategy_id"]: row for row in snap.accounts}
        assert by_id["pa:rose"]["balance"] == rose_expected
        assert by_id["pa:cave"]["balance"] == cave_expected
        assert all(row["initial_balance"] == "1000.00" for row in snap.accounts)
        paper_after = [e["event_type"] for e in bundle.store.list_events() if e["event_type"].startswith("paper_")]
        assert paper_after == paper_before
        assert bundle.store.list_events(event_type="paper_fill")
        assert bundle.store.list_events(event_type="paper_close")
        assert bundle.store.list_events(event_type="account_changed")
        bundle.handle.close()

    def test_ac1_reopen_with_different_run_id_restores_distinct_balances(self, tmp_path):
        path = str(tmp_path / "ac1-reopen.db")
        bundle = open_store(path, run_id="run_ac1_a")
        bundle.store.ensure_accounts(["pa:rose", "pa:cave"])
        book = PaperAccountBook()
        long_result = book_open(book, "pa:rose", direction="long", trade_id="pt_rose")
        short_result = book_open(book, "pa:cave", direction="short", trade_id="pt_cave")
        full_lifecycle(
            bundle.store,
            book,
            long_result,
            strategy_id="pa:rose",
            exit_price="515.00",
            exit_reason="target",
        )
        full_lifecycle(
            bundle.store,
            book,
            short_result,
            strategy_id="pa:cave",
            exit_price="305.00",
            exit_reason="stop",
        )
        rose_expected = decimal_to_text(book.get_account("pa:rose").balance)
        cave_expected = decimal_to_text(book.get_account("pa:cave").balance)
        bundle.handle.close()

        reopened, snap = reopen(path, run_id="run_ac1_b", clock=bundle.clock)
        by_id = {row["strategy_id"]: row for row in snap.accounts}
        assert by_id["pa:rose"]["balance"] == rose_expected
        assert by_id["pa:cave"]["balance"] == cave_expected
        assert by_id["pa:rose"]["balance"] != by_id["pa:cave"]["balance"]
        assert all(row["initial_balance"] == "1000.00" for row in snap.accounts)
        paper_before = {e["event_id"] for e in reopened.store.list_events() if e["event_type"].startswith("paper_")}
        reopened.recovery.restore()
        paper_after = {e["event_id"] for e in reopened.store.list_events() if e["event_type"].startswith("paper_")}
        assert paper_after == paper_before
        reopened.handle.close()

    def test_ac7_same_transition_result_persisted_twice_is_replay_noop(self, tmp_path):
        path = str(tmp_path / "ac7.db")
        bundle = open_store(path)
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_ac7")
        state = open_exit_state(book, opened)
        commit_fill(bundle.store, state.to_dict(), second_event())
        bundle.store.commit_strategy_state(
            strategy_snapshot("pa:rose", StrategyState.OPEN_POSITION, open_position_id="pt_ac7")
        )
        close_result = book.close_trade("pa:rose", "515.00", position_id="pt_ac7", reason="target")
        settled = settled_exit_state(
            state, close_result, exit_reason="target", closed_at=at_shift(3600), exit_price="515.00"
        )
        closed_second = second_event(end_at=at_shift(3600))
        close_envelope = envelope_for_close(
            settled.to_dict(),
            occurred_at=closed_second.end_at,
            idempotency_key=close_key("pa:rose", "pt_ac7", "target", settled.to_dict()["closed_at"]),
            market_ref=make_second_ref(closed_second.to_dict()),
        )
        account_envelope = envelope_for_account_change(
            "pa:rose",
            0,
            balance_before=close_result.settlement["balance_before"],
            balance_after=close_result.account.balance,
            occurred_at=START,
            idempotency_key=account_change_key(
                "pa:rose",
                close_result.settlement["balance_before"],
                close_result.account.balance,
            ),
        )
        bundle.store.commit_trade(settled.to_dict(), event="closed", envelope=close_envelope)
        bundle.store.commit_account_event(
            "pa:rose",
            balance_before=close_result.settlement["balance_before"],
            balance_after=close_result.account.balance,
            envelope=account_envelope,
        )
        events_before = len(bundle.store.list_events())
        bundle.store.commit_trade(settled.to_dict(), event="closed", envelope=close_envelope)
        bundle.store.commit_account_event(
            "pa:rose",
            balance_before=close_result.settlement["balance_before"],
            balance_after=close_result.account.balance,
            envelope=account_envelope,
        )
        assert len(bundle.store.list_events()) == events_before
        assert bundle.store.get_account("pa:rose")["balance"] == decimal_to_text(close_result.account.balance)
        assert len(bundle.store.list_trades()) == 1
        bundle.handle.close()

    def test_ac11_replay_mode_events_and_snapshot_mode(self, tmp_path):
        path = str(tmp_path / "ac11.db")
        bundle = open_store(path, run_id="run_ac11_a", mode="replay")
        bundle.store.ensure_accounts(["pa:rose"])
        book = PaperAccountBook()
        opened = book_open(book, "pa:rose", direction="long", trade_id="pt_rp")
        state = open_exit_state(book, opened)
        commit_fill(bundle.store, state.to_dict(), second_event(), mode="replay")
        bundle.store.commit_strategy_state(
            strategy_snapshot("pa:rose", StrategyState.OPEN_POSITION, open_position_id="pt_rp")
        )
        bundle.handle.close()
        reopened, snap = reopen(path, run_id="run_ac11_b", clock=bundle.clock, mode="replay")
        assert snap.mode == "replay"
        events = reopened.store.list_events()
        assert events
        assert all(event["mode"] == "replay" for event in events)
        assert events[0]["event_type"] == "paper_fill"
        reopened.handle.close()
