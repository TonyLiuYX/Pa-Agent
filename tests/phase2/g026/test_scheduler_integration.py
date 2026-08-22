from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent
from pa_engine.paper.core import PaperAccountBook
from pa_engine.paper.exit import ExitMonitor
from pa_engine.strategy.runtime import FakeClock, OpenRequestResult, StrategyScheduler, StrategyState


UTC = timezone.utc
START = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
FEED = {"schema_version": "0.1.0", "provider": "alpaca", "feed": "iex", "coverage": "single_exchange"}


def _view(view_id: str = "view-1", snapshot_id: str = "snapshot-1") -> dict:
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


def _scheduler(model_calls: list[object]) -> StrategyScheduler:
    scheduler = StrategyScheduler(
        ["pa:test"],
        clock=FakeClock(START),
        model_executor=lambda *args, **kwargs: model_calls.append((args, kwargs)),
        open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1"),
    )
    scheduler._verify_success_instruction = lambda _record, _instruction: True
    scheduler.start(_view())
    assert scheduler.submit_instruction(
        "pa:test",
        {
            "strategy_id": "pa:test",
            "branch": "enter_immediately",
            "entry_price": "100",
            "stop_price": "99",
            "target_price": "102",
            "direction": "long",
        },
        decision_accepted_at=START,
    )
    assert scheduler.state_for("pa:test") == StrategyState.OPEN_POSITION
    assert scheduler.get("pa:test").open_position_id == "pos-1"
    return scheduler


def _book_trade(direction: str = "long", *, trade_id: str = "pos-1", stop: str | None = None, target: str | None = None):
    book = PaperAccountBook()
    if direction == "long":
        result = book.open_trade("pa:test", "XYZ", "long", "100", stop or "99", target or "102", trade_id=trade_id)
    else:
        result = book.open_trade("pa:test", "XYZ", "short", "100", stop or "201", target or "98", trade_id=trade_id)
    assert result.accepted
    return book, result.trade


class _MutateThenRaiseBook(PaperAccountBook):
    """Run the real G024 close, then lose the response once."""

    def __init__(self):
        super().__init__()
        self.close_calls = 0

    def close_trade(self, *args, **kwargs):
        self.close_calls += 1
        result = super().close_trade(*args, **kwargs)
        if self.close_calls == 1:
            raise RuntimeError("committed close response was lost")
        return result


def _second(*, high: str, low: str, start: datetime = START + timedelta(seconds=1)) -> SecondAggregateEvent:
    end = start + timedelta(seconds=1)

    def stamp(value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")

    return SecondAggregateEvent(
        "XYZ",
        stamp(start),
        stamp(end),
        low,
        high,
        low,
        high,
        "1",
        contributor_ids=("scheduler-test",),
        execution_watermark=stamp(end + timedelta(seconds=3)),
        status="frozen",
        quality={"status": "ok", "codes": [], "connection_gap": False},
    )


def _monitor(book: PaperAccountBook, trade, scheduler: StrategyScheduler, *, generation: int | None = None) -> ExitMonitor:
    record = scheduler.get("pa:test")
    assert record is not None
    return ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        scheduler=scheduler,
        generation=record.open_position_generation if generation is None else generation,
        entry_fill_interval=("2026-01-02T14:30:00Z", "2026-01-02T14:30:01Z"),
    )


def test_real_scheduler_enabled_close_recalls_once_after_zero_model_calls_open():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book, trade = _book_trade()
    monitor = _monitor(book, trade, scheduler)

    scheduler.on_market_view(_view("view-2", "snapshot-2"))
    scheduler.advance(60)
    assert model_calls == []
    assert scheduler.queue_size == 0

    out = monitor.on_second(_second(high="102", low="100"))
    assert out.status == "settled"
    assert out.scheduler_callback is not None and out.scheduler_callback.accepted
    assert scheduler.state_for("pa:test") == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 1
    assert scheduler.get("pa:test").open_position_id is None

    again = monitor.on_second(_second(high="102", low="100", start=START + timedelta(seconds=2)))
    assert again.replayed
    assert scheduler.queue_size == 1


def test_real_scheduler_disabled_close_settles_without_recall():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book, trade = _book_trade()
    record = scheduler.get("pa:test")
    original_generation = record.open_position_generation
    assert scheduler.disable("pa:test")
    assert scheduler.state_for("pa:test") == StrategyState.OPEN_POSITION
    assert record.open_position_generation == original_generation

    monitor = _monitor(book, trade, scheduler, generation=original_generation)
    out = monitor.on_second(_second(high="102", low="100"))
    assert out.status == "settled" and out.scheduler_callback is not None
    assert out.scheduler_callback.accepted
    assert scheduler.state_for("pa:test") == StrategyState.DISABLED
    assert scheduler.queue_size == 0
    assert model_calls == []
    assert scheduler.get("pa:test").open_position_id is None


def test_real_scheduler_exhausted_g024_close_does_not_recall():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book, trade = _book_trade("short")
    monitor = _monitor(book, trade, scheduler)

    out = monitor.on_second(_second(high="201", low="100"))
    assert out.status == "exhausted"
    assert out.settlement_result.exhausted
    assert out.scheduler_callback is not None and out.scheduler_callback.accepted
    assert scheduler.state_for("pa:test") == StrategyState.EXHAUSTED
    assert scheduler.queue_size == 0
    assert model_calls == []
    assert book.get_account("pa:test").balance == Decimal("0.00")
    assert scheduler.get("pa:test").open_position_id is None


def test_same_monitor_replay_after_mutate_then_raise_recalls_real_scheduler_once():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book = _MutateThenRaiseBook()
    opened = book.open_trade("pa:test", "XYZ", "long", "100", "99", "102", trade_id="pos-1")
    assert opened.accepted
    monitor = _monitor(book, opened.trade, scheduler)
    event = _second(high="102", low="100")

    first = monitor.on_second(event)
    assert first.status == "rejected" and first.code == "settlement_boundary_error"
    assert book.get_account("pa:test").open_trade is None
    assert book.get_account("pa:test").balance == Decimal("1020.00")
    assert scheduler.state_for("pa:test") == StrategyState.OPEN_POSITION
    assert scheduler.get("pa:test").open_position_id == "pos-1"
    assert scheduler.queue_size == 0 and model_calls == []

    second = monitor.on_second(event)
    assert second.status == "replayed" and second.replayed and second.scheduler_callback is not None
    assert second.scheduler_callback.accepted
    assert second.state == monitor.state
    assert scheduler.state_for("pa:test") == StrategyState.ANALYSIS_DUE
    assert scheduler.get("pa:test").open_position_id is None
    assert scheduler.queue_size == 1 and model_calls == []
    assert book.close_calls == 2
    assert book.get_account("pa:test").balance == Decimal("1020.00")

    third = monitor.on_second(_second(high="102", low="100", start=START + timedelta(seconds=2)))
    assert third.replayed
    assert third.scheduler_callback is None
    assert scheduler.queue_size == 1 and model_calls == []


def test_newer_second_first_retries_pending_real_scheduler_settlement():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book = _MutateThenRaiseBook()
    opened = book.open_trade("pa:test", "XYZ", "long", "100", "99", "102", trade_id="pos-1")
    assert opened.accepted
    monitor = _monitor(book, opened.trade, scheduler)
    trigger = _second(high="102", low="100")
    assert monitor.on_second(trigger).code == "settlement_boundary_error"
    later = _second(high="100", low="100", start=START + timedelta(seconds=2))
    recovered = monitor.on_second(later)
    assert recovered.status == "replayed" and recovered.scheduler_callback is not None and recovered.scheduler_callback.accepted
    assert monitor.state.processed_event_ids == (trigger.revision_id,)
    assert later.revision_id not in monitor.state.processed_event_ids
    assert scheduler.state_for("pa:test") == StrategyState.ANALYSIS_DUE
    assert scheduler.get("pa:test").open_position_id is None
    assert scheduler.queue_size == 1 and model_calls == []
    assert book.get_account("pa:test").balance == Decimal("1020.00")


def test_disabled_scheduler_replay_after_mutate_then_raise_clears_without_recall():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    record = scheduler.get("pa:test")
    original_generation = record.open_position_generation
    assert scheduler.disable("pa:test")
    book = _MutateThenRaiseBook()
    opened = book.open_trade("pa:test", "XYZ", "long", "100", "99", "102", trade_id="pos-1")
    assert opened.accepted
    monitor = _monitor(book, opened.trade, scheduler, generation=original_generation)
    event = _second(high="102", low="100")
    assert monitor.on_second(event).code == "settlement_boundary_error"
    replay = monitor.on_second(event)
    assert replay.status == "replayed" and replay.scheduler_callback is not None and replay.scheduler_callback.accepted
    assert scheduler.state_for("pa:test") == StrategyState.DISABLED
    assert scheduler.get("pa:test").open_position_id is None
    assert scheduler.queue_size == 0 and model_calls == []


def test_wrong_scheduler_generation_is_rejected_before_g024_mutation():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book, trade = _book_trade()
    try:
        _monitor(book, trade, scheduler, generation=999)
    except ValueError as exc:
        assert "scheduler binding" in str(exc)
    else:
        raise AssertionError("wrong scheduler generation must fail closed")
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_wrong_scheduler_position_identity_is_rejected_before_g024_mutation():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book, trade = _book_trade(trade_id="pos-2")
    try:
        _monitor(book, trade, scheduler)
    except ValueError as exc:
        assert "scheduler binding" in str(exc)
    else:
        raise AssertionError("wrong scheduler position must fail closed")
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_scheduler_binding_flip_after_construction_rejects_before_g024():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book, trade = _book_trade()
    monitor = _monitor(book, trade, scheduler)
    assert scheduler.on_position_close("pa:test", "pos-1")
    out = monitor.on_second(_second(high="102", low="100"))
    assert out.status == "rejected" and out.code == "scheduler_binding_rejected"
    assert not monitor.state.terminal
    assert monitor.state.processed_event_ids == ()
    assert monitor.state.processed_intervals == ()
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_missing_explicit_callback_rejects_before_g024():
    book, trade = _book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=("2026-01-02T14:30:00Z", "2026-01-02T14:30:01Z"),
    )
    out = monitor.on_second(_second(high="102", low="100"))
    assert out.status == "rejected" and out.code == "missing_scheduler_callback"
    assert not monitor.state.terminal
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def _bound_bridge():
    model_calls: list[object] = []
    scheduler = _scheduler(model_calls)
    book, trade = _book_trade()
    monitor = _monitor(book, trade, scheduler)
    assert monitor._scheduler_bridge is not None
    return scheduler, monitor._scheduler_bridge


@pytest.mark.parametrize("kind", ["strategy", "position", "generation"])
def test_injected_scheduler_bridge_cannot_cross_state_identity(kind):
    scheduler, bridge = _bound_bridge()
    if kind == "strategy":
        book = PaperAccountBook()
        result = book.open_trade("pa:other", "XYZ", "long", "100", "99", "102", trade_id="pos-2")
        assert result.accepted
        trade = result.trade
        generation = 0
    elif kind == "position":
        book, trade = _book_trade(trade_id="pos-2")
        generation = 0
    else:
        book, trade = _book_trade()
        generation = 999

    before_scheduler = scheduler.get("pa:test")
    before_balance = book.get_account(trade.strategy_id).balance
    with pytest.raises(ValueError, match="scheduler binding"):
        ExitMonitor.from_trade(
            trade,
            paper_account_book=book,
            scheduler_bridge=bridge,
            generation=generation,
            entry_fill_interval=("2026-01-02T14:30:00Z", "2026-01-02T14:30:01Z"),
        )
    assert scheduler.get("pa:test") == before_scheduler
    assert book.get_account(trade.strategy_id).open_trade is not None
    assert book.get_account(trade.strategy_id).balance == before_balance


def test_scheduler_and_injected_bridge_must_reference_the_same_scheduler():
    scheduler_a, bridge_a = _bound_bridge()
    model_calls: list[object] = []
    scheduler_b = _scheduler(model_calls)
    book, trade = _book_trade()
    before_scheduler = scheduler_a.get("pa:test")
    with pytest.raises(ValueError, match="scheduler binding"):
        ExitMonitor.from_trade(
            trade,
            paper_account_book=book,
            scheduler=scheduler_b,
            scheduler_bridge=bridge_a,
            entry_fill_interval=("2026-01-02T14:30:00Z", "2026-01-02T14:30:01Z"),
        )
    assert scheduler_a.get("pa:test") == before_scheduler
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")
