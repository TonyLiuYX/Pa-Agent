from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pa_engine.market_sources.alpaca.history import AdapterOutcome, HistoryRequest, HistoryResult, RecoveryBar
from pa_engine.market_sources.alpaca.stream import ConnectionGap, SecondAggregateEvent
from pa_engine.paper.core import PaperAccountBook
from pa_engine.paper.exit import ExitMonitor, reduce_exit


UTC = timezone.utc
START = "2026-01-02T14:30:00Z"
FEED = {"schema_version": "0.1.0", "provider": "alpaca", "feed": "iex", "coverage": "single_exchange"}


def _book_trade(direction: str = "long", *, strategy: str = "pa:test", trade_id: str | None = None):
    book = PaperAccountBook()
    if direction == "long":
        opened = book.open_trade(strategy, "XYZ", "long", "100", "99", "102", trade_id=trade_id)
    else:
        opened = book.open_trade(strategy, "XYZ", "short", "100", "101", "98", trade_id=trade_id)
    assert opened.accepted
    return book, opened.trade


class _FlakyBook(PaperAccountBook):
    """G024 test double that fails once before or after the real close."""

    def __init__(self, *, raise_after: bool = False, failures: int = 1):
        super().__init__()
        self.close_calls = 0
        self.raise_after = raise_after
        self.failures = failures

    def close_trade(self, *args, **kwargs):
        self.close_calls += 1
        if self.close_calls <= self.failures and not self.raise_after:
            raise RuntimeError("transient close boundary failure")
        result = super().close_trade(*args, **kwargs)
        if self.close_calls <= self.failures and self.raise_after:
            raise RuntimeError("close committed before transport failure")
        return result


def _flaky_book_trade(*, raise_after: bool = False, failures: int = 1):
    book = _FlakyBook(raise_after=raise_after, failures=failures)
    opened = book.open_trade("pa:test", "XYZ", "long", "100", "99", "102")
    assert opened.accepted
    return book, opened.trade


def _second(start: str, *, high: str = "100", low: str = "100", symbol: str = "XYZ", quality: dict | None = None, watermark: str | None = None, revision: int = 0):
    instant = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end = instant + timedelta(seconds=1)
    def stamp(value):
        return value.isoformat().replace("+00:00", "Z")
    return SecondAggregateEvent(
        symbol,
        stamp(instant),
        stamp(end),
        low,
        high,
        low,
        high,
        "1",
        contributor_ids=(f"trade-{start}",),
        execution_watermark=watermark or stamp(end + timedelta(seconds=3)),
        status="frozen",
        revision_sequence=revision,
        quality=quality or {"status": "ok", "codes": [], "connection_gap": False},
    )


def _monitor(direction: str = "long", *, strategy: str = "pa:test", trade_id: str | None = None, **kwargs):
    book, trade = _book_trade(direction, strategy=strategy, trade_id=trade_id)
    kwargs.setdefault("on_position_close", lambda *_args, **_kwargs: True)
    monitor = ExitMonitor.from_trade(trade, paper_account_book=book, entry_fill_interval=(START, "2026-01-02T14:30:01Z"), **kwargs)
    return book, trade, monitor


def _recovery(*, symbol: str = "XYZ", start: str = "2026-01-02T14:31:00Z", end: str = "2026-01-02T14:32:00Z", high: str = "101", low: str = "100", complete: bool = True, source_request_hash: str = "a" * 64):
    return RecoveryBar(
        instrument={"asset_class": "us_equity", "symbol": symbol, "venue": "IEX", "currency": "USD"},
        timeframe={"unit": "minute", "multiplier": 1},
        session={"kind": "regular", "timezone": "America/New_York"},
        start_at=start,
        end_at=end,
        open="100",
        high=high,
        low=low,
        close="100",
        volume="10",
        complete=complete,
        coverage_state="complete" if complete else "partial",
        source_request_hash=source_request_hash,
        recovery_cursor=end,
        market_feed=FEED,
    )


def _history_result(bars: list[RecoveryBar], *, start: str = "2026-01-02T14:31:00Z", end: str = "2026-01-02T14:34:00Z") -> HistoryResult:
    request = HistoryRequest(
        "XYZ",
        start,
        end,
        {"unit": "minute", "multiplier": 1},
        recovery=True,
    )
    rebound = [
        _recovery(
            start=bar.start_at,
            end=bar.end_at,
            high=bar.high,
            low=bar.low,
            complete=bar.complete,
            source_request_hash=request.source_request_hash,
        )
        for bar in bars
    ]
    outcome = AdapterOutcome("success", "success", False, False, "recovery bars fetched", pages=1)
    return HistoryResult(request, outcome, recovery_bars=tuple(rebound), coverage=request.coverage)


@pytest.mark.parametrize(
    ("direction", "high", "low", "selected", "pnl"),
    [
        ("long", "100", "99", Decimal("99"), Decimal("-10.00")),
        ("long", "102", "100", Decimal("102"), Decimal("20.00")),
        ("short", "101", "100", Decimal("101"), Decimal("-10.00")),
        ("short", "100", "98", Decimal("98"), Decimal("20.00")),
    ],
)
def test_exact_inclusive_levels_and_g024_math(direction, high, low, selected, pnl):
    book, _trade, monitor = _monitor(direction)
    out = monitor.on_second(_second("2026-01-02T14:30:01Z", high=high, low=low))
    assert out.status == "settled"
    assert out.selected_exit == selected
    assert out.settlement_result.settlement["applied_pnl"] == pnl
    assert out.settlement_result.account.balance == Decimal("1000.00") + pnl
    assert book.get_account("pa:test").open_trade is None


def test_dual_touch_is_stop_first_replay_stable_and_uncertain():
    book, _trade, monitor = _monitor()
    event = _second("2026-01-02T14:30:01Z", high="102", low="99")
    first = monitor.on_second(event)
    again = monitor.on_second(event)
    assert first.status == "settled" and first.ambiguity_reason == "dual_touch_stop_first"
    assert first.selected_exit == Decimal("99")
    assert "high_uncertainty" in first.state.uncertainty_codes
    assert again.replayed and book.get_account("pa:test").balance == Decimal("990.00")


def test_entry_fill_second_is_ignored_then_later_second_settles():
    book, _trade, monitor = _monitor()
    entry_second = _second(START, high="102", low="99")
    ignored = monitor.on_second(entry_second)
    assert ignored.not_evaluated and ignored.code == "entry_fill_second"
    assert book.get_account("pa:test").open_trade is not None
    later = monitor.on_second(_second("2026-01-02T14:30:01Z", high="102", low="100"))
    assert later.status == "settled" and later.selected_exit == Decimal("102")


def test_duplicates_out_of_order_and_late_revision_do_not_double_settle():
    book, _trade, monitor = _monitor()
    first = _second("2026-01-02T14:30:02Z", high="100", low="100")
    assert monitor.on_second(first).status == "open"
    older = _second("2026-01-02T14:30:01Z", high="102", low="100", watermark="2026-01-02T14:30:05Z")
    assert monitor.on_second(older).code == "late_revision"
    assert monitor.on_second(first).replayed
    assert book.get_account("pa:test").open_trade is not None


def test_provisional_delivery_before_execution_watermark_is_not_evaluated():
    _book, _trade, monitor = _monitor()
    event = _second("2026-01-02T14:30:01Z", high="102", low="100")
    out = monitor.on_second(event, now="2026-01-02T14:30:02Z")
    assert out.status == "not_evaluated" and out.code == "not_frozen"


def test_gap_is_not_a_favorable_exit_and_recovers_only_with_complete_bar():
    book, _trade, monitor = _monitor()
    gap = monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    assert gap.outcome_status == "not_evaluated" and gap.partial_data
    paused = monitor.on_second(_second("2026-01-02T14:30:02Z", high="102", low="99"))
    assert paused.not_evaluated and book.get_account("pa:test").open_trade is not None
    incomplete = monitor.on_recovery(_recovery(complete=False))
    assert incomplete.not_evaluated and incomplete.partial_data and monitor.state.gap_active
    recovered = monitor.on_recovery(_recovery())
    assert recovered.status == "recovered" and not monitor.state.gap_active
    assert monitor.on_recovery(_recovery()).replayed
    settled = monitor.on_second(_second("2026-01-02T14:32:00Z", high="102", low="100"))
    assert settled.status == "settled"


def test_complete_recovery_settles_with_minute_no_path_lineage():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    out = monitor.on_recovery(_recovery(high="102", low="100"))
    assert out.status == "settled"
    assert out.path_precision == "minute_bar_no_path"
    assert out.recovery_bar_id and out.recovery_bar_hash
    assert out.lineage["path_precision"] == "minute_bar_no_path"
    assert "high_uncertainty" in out.state.uncertainty_codes
    assert book.get_account("pa:test").balance == Decimal("1020.00")


def test_recovery_callback_is_typed_and_invoked_once_when_values_are_omitted():
    calls = []
    book, _trade = _book_trade()

    def recover(state, **kwargs):
        calls.append((state.trade_id, kwargs["generation"]))
        return [_recovery()]

    monitor = ExitMonitor.from_trade(
        _trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        recovery_callback=recover,
        on_position_close=lambda *_args, **_kwargs: True,
    )
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    assert monitor.on_recovery().status == "recovered"
    assert len(calls) == 1


def test_recovery_identity_mismatch_is_typed_and_keeps_trade_open():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    other = _recovery(symbol="OTHER")
    out = monitor.on_recovery(other)
    assert out.status == "not_evaluated" and out.code == "recovery_identity_mismatch"
    assert book.get_account("pa:test").open_trade is not None


def test_g019_history_result_envelope_binds_whole_batch_not_each_bar():
    bars = [
        _recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"),
        _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z"),
        _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z"),
    ]
    result = _history_result(bars)
    book, trade = _book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        recovery_request=result,
        on_position_close=lambda *_args, **_kwargs: True,
    )
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:34:00Z")
    out = monitor.on_recovery(result)
    assert out.status == "recovered"
    assert not monitor.state.gap_active
    assert book.get_account("pa:test").open_trade is not None


@pytest.mark.parametrize(
    "bars",
    [
        [
            _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z"),
            _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z"),
        ],
        [
            _recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"),
            _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z"),
        ],
        [
            _recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"),
            _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z"),
        ],
        [
            _recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"),
            _recovery(start="2026-01-02T14:34:00Z", end="2026-01-02T14:35:00Z"),
        ],
    ],
)
def test_g019_history_result_missing_or_out_of_range_bar_fails_batch_before_touch(bars):
    result = _history_result(bars)
    book, trade = _book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        recovery_request=result.to_dict(),
        on_position_close=lambda *_args, **_kwargs: True,
    )
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:34:00Z")
    out = monitor.on_recovery(result.to_dict())
    assert out.not_evaluated and out.partial_data
    assert monitor.state.gap_active
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_g019_request_middle_hole_fails_even_when_active_gap_is_subset():
    bars = [
        _recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"),
        _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z"),
    ]
    result = _history_result(bars)
    book, trade = _book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        recovery_request=result,
        on_position_close=lambda *_args, **_kwargs: True,
    )
    # The active gap only covers the first bar; the complete request envelope
    # still must not be accepted with a missing middle bar.
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    out = monitor.on_recovery(result)
    assert out.not_evaluated and out.partial_data
    assert monitor.state.gap_active
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


@pytest.mark.parametrize("order", [("post", "pre"), ("pre", "post")])
def test_pre_entry_gap_cannot_clear_an_existing_post_entry_gap(order):
    callbacks = []
    book, trade = _book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    for name in order:
        if name == "post":
            monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
        else:
            monitor.on_gap(start_at="2026-01-02T14:29:00Z", end_at="2026-01-02T14:30:00Z")
    assert monitor.state.gap_active
    assert monitor.state.partial_data
    assert monitor.state.outcome_status == "not_evaluated"
    assert "partial_data" in monitor.state.uncertainty_codes
    paused = monitor.on_second(_second("2026-01-02T14:30:02Z", high="102", low="100"))
    assert paused.not_evaluated and paused.code == "gap_overlap"
    assert callbacks == []
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_recovery_clears_post_entry_gap_without_demanding_irrelevant_pre_entry_bar():
    _book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:29:00Z", end_at="2026-01-02T14:30:00Z")
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    out = monitor.on_recovery(_recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"))
    assert out.status == "recovered"
    assert not monitor.state.gap_active
    assert monitor.state.gap_intervals == (("2026-01-02T14:29:00Z", "2026-01-02T14:30:00Z"),)


def test_state_lineage_is_recursively_immutable():
    book, trade, monitor = _monitor(decision_lineage={"instruction": {"stop_price": "99"}})
    with pytest.raises(TypeError):
        monitor.state.market_feed["feed"] = "sip"
    projection = monitor.state.to_dict()
    projection["decision_lineage"]["instruction"]["stop_price"] = "1"
    assert monitor.state.decision_lineage["instruction"]["stop_price"] == "99"


def test_callback_is_after_settlement_exactly_once_and_failures_do_not_roll_back():
    callbacks = []
    book, _trade, monitor = _monitor(on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or False)
    out = monitor.on_second(_second("2026-01-02T14:30:01Z", high="102", low="100"))
    assert out.status == "settled" and out.scheduler_callback.status == "callback_rejected"
    assert len(callbacks) == 1 and book.get_account("pa:test").open_trade is None
    assert monitor.on_second(_second("2026-01-02T14:30:02Z", high="102", low="100")).replayed


def test_touch_second_retries_after_transient_g024_exception_without_consuming_input():
    callbacks = []
    book, trade = _flaky_book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    event = _second("2026-01-02T14:30:01Z", high="102", low="100")
    first = monitor.on_second(event)
    assert first.status == "rejected" and first.code == "settlement_boundary_error"
    assert not monitor.state.terminal
    assert monitor.state.processed_event_ids == ()
    assert book.get_account("pa:test").open_trade is not None
    assert callbacks == []

    second = monitor.on_second(event)
    assert second.status == "settled" and second.state == monitor.state
    assert monitor.state.processed_event_ids
    assert len(callbacks) == 1
    assert book.close_calls == 2
    assert book.get_account("pa:test").open_trade is None


def test_newer_second_first_retries_pending_before_advancing_watermark():
    callbacks = []
    book, trade = _flaky_book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    trigger = _second("2026-01-02T14:30:01Z", high="102", low="100")
    assert monitor.on_second(trigger).code == "settlement_boundary_error"
    later = _second("2026-01-02T14:30:02Z", high="100", low="100")
    recovered = monitor.on_second(later)
    assert recovered.status == "settled"
    assert recovered.state == monitor.state
    assert monitor.state.processed_event_ids == (trigger.revision_id,)
    assert monitor.state.processed_watermark == trigger.execution_watermark
    assert later.revision_id not in monitor.state.processed_event_ids
    assert len(callbacks) == 1
    assert book.get_account("pa:test").balance == Decimal("1020.00")


def test_pending_retry_failure_blocks_new_second_without_advancing_state_until_repaired():
    book, trade = _flaky_book_trade(failures=2)
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *_args, **_kwargs: True,
    )
    trigger = _second("2026-01-02T14:30:01Z", high="102", low="100")
    assert monitor.on_second(trigger).code == "settlement_boundary_error"
    later = _second("2026-01-02T14:30:02Z", high="100", low="100")
    still_pending = monitor.on_second(later)
    assert still_pending.status == "rejected" and still_pending.code == "settlement_boundary_error"
    assert monitor.state.processed_event_ids == ()
    assert monitor.state.processed_watermark is None
    assert later.revision_id not in monitor.state.processed_event_ids
    repaired = monitor.on_second(later)
    assert repaired.status == "settled"
    assert monitor.state.processed_event_ids == (trigger.revision_id,)
    assert later.revision_id not in monitor.state.processed_event_ids


def test_touch_second_retries_real_g024_replay_after_book_mutated_then_raised():
    callbacks = []
    book, trade = _flaky_book_trade(raise_after=True)
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    event = _second("2026-01-02T14:30:01Z", high="102", low="100")
    first = monitor.on_second(event)
    assert first.status == "rejected" and first.code == "settlement_boundary_error"
    assert not monitor.state.terminal
    assert monitor.state.processed_event_ids == ()
    assert book.get_account("pa:test").open_trade is None

    replay = monitor.on_second(event)
    assert replay.status == "replayed" and replay.replayed and replay.state == monitor.state
    assert monitor.state.processed_event_ids
    assert len(callbacks) == 1
    assert replay.scheduler_callback is not None and replay.scheduler_callback.accepted
    assert book.close_calls == 2
    assert book.get_account("pa:test").balance == Decimal("1020.00")


def test_touch_recovery_retries_after_transient_g024_exception_without_consuming_bar():
    callbacks = []
    book, trade = _flaky_book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    bar = _recovery(high="102")
    first = monitor.on_recovery(bar)
    assert first.status == "rejected" and first.code == "settlement_boundary_error"
    assert monitor.state.gap_active and monitor.state.partial_data
    assert monitor.state.recovery_lineage == ()
    assert all(bar_id not in monitor.state.processed_event_ids for bar_id in (bar.recovery_bar_id, bar.content_hash))
    assert book.get_account("pa:test").open_trade is not None
    assert callbacks == []

    second = monitor.on_recovery(bar)
    assert second.status == "settled" and second.state == monitor.state
    assert monitor.state.recovery_bar_id == bar.recovery_bar_id
    assert monitor.state.recovery_bar_hash == bar.content_hash
    assert len(callbacks) == 1
    assert book.close_calls == 2


def test_touch_recovery_retries_real_g024_replay_after_book_mutated_then_raised():
    callbacks = []
    book, trade = _flaky_book_trade(raise_after=True)
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    bar = _recovery(high="102")
    first = monitor.on_recovery(bar)
    assert first.status == "rejected" and first.code == "settlement_boundary_error"
    assert monitor.state.gap_active and monitor.state.recovery_lineage == ()
    assert monitor.state.processed_event_ids == ()
    assert book.get_account("pa:test").open_trade is None

    replay = monitor.on_recovery(bar)
    assert replay.status == "replayed" and replay.replayed and replay.state == monitor.state
    assert replay.scheduler_callback is not None and replay.scheduler_callback.accepted
    assert monitor.state.recovery_bar_id == bar.recovery_bar_id
    assert len(callbacks) == 1
    assert book.close_calls == 2
    assert book.get_account("pa:test").balance == Decimal("1020.00")


def test_recovery_pending_retries_before_timer_and_commits_original_lineage():
    callbacks = []
    book, trade = _flaky_book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    bar = _recovery(high="102")
    assert monitor.on_recovery(bar).code == "settlement_boundary_error"
    timer = monitor.on_timer()
    assert timer.status == "settled" and timer.state == monitor.state
    assert monitor.state.recovery_bar_id == bar.recovery_bar_id
    assert not monitor.state.gap_active
    assert len(callbacks) == 1
    assert book.get_account("pa:test").balance == Decimal("1020.00")


def test_opposite_strategies_settle_independently_on_same_second():
    long_book, _long_trade, long_monitor = _monitor(strategy="pa:long")
    short_book, _short_trade, short_monitor = _monitor("short", strategy="pa:short")
    event = _second("2026-01-02T14:30:01Z", high="102", low="98")
    assert long_monitor.on_second(event).selected_exit == Decimal("99")
    assert short_monitor.on_second(event).selected_exit == Decimal("101")
    assert long_book.get_account("pa:long").balance == Decimal("990.00")
    assert short_book.get_account("pa:short").balance == Decimal("990.00")


def test_malformed_event_is_fail_closed_without_traceback():
    _book, _trade, monitor = _monitor()
    out = monitor.on_second({"symbol": "XYZ", "start_at": "bad"})
    assert out.status == "rejected" and out.code == "invalid_second_event"


def test_malformed_explicit_settlement_is_typed_and_fail_closed():
    book, _trade, monitor = _monitor()
    out = monitor.settle({"price": "99"})
    assert out.status == "rejected" and out.code == "direct_settlement_unsupported"
    assert book.get_account("pa:test").open_trade is not None


def test_direct_settlement_without_source_is_typed_and_fail_closed():
    book, _trade, monitor = _monitor()
    out = monitor.settle()
    assert out.status == "rejected" and out.code == "direct_settlement_unsupported"
    assert book.get_account("pa:test").open_trade is not None


@pytest.mark.parametrize(
    ("price", "reason", "source"),
    [("102", "take_profit", None), ("99", "stop_loss", None), ("102", "manual", {"event_id": "forged"})],
)
def test_direct_settlement_never_accepts_caller_selected_level(price, reason, source):
    book, _trade, monitor = _monitor()
    out = monitor.settle(price, reason=reason, source_event=source)
    assert out.status == "rejected" and out.code == "direct_settlement_unsupported"
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


@pytest.mark.parametrize(
    ("field", "value", "event_symbol"),
    [
        ("symbol", "ABC", "ABC"),
        ("entry_price", "101", "XYZ"),
        ("stop_price", "50", "XYZ"),
        ("target_price", "200", "XYZ"),
        ("quantity", "20", "XYZ"),
        ("planned_r", "3", "XYZ"),
        ("allocation_fraction", "0.5", "XYZ"),
        ("requested_entry_price", "100", "XYZ"),
        ("immediate", True, "XYZ"),
        ("paper_math_version", "forged", "XYZ"),
    ],
)
def test_forged_g024_trade_projection_cannot_settle(field, value, event_symbol):
    callbacks = []
    book, trade = _book_trade()
    forged = trade.to_dict()
    forged[field] = value
    monitor = ExitMonitor.from_trade(
        forged,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    out = monitor.on_second(_second("2026-01-02T14:30:01Z", high="200", low="100", symbol=event_symbol))
    assert out.status == "rejected" and out.code == "trade_identity_mismatch"
    assert not monitor.state.terminal and not callbacks
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_forged_direction_is_rejected_at_typed_trade_boundary():
    book, trade = _book_trade()
    forged = trade.to_dict()
    forged["direction"] = "short"
    with pytest.raises(ValueError):
        ExitMonitor.from_trade(forged, paper_account_book=book, entry_fill_interval=(START, "2026-01-02T14:30:01Z"))
    assert book.get_account("pa:test").open_trade is not None


def test_accepted_but_malformed_g024_mapping_cannot_close_or_callback():
    callbacks = []
    book, trade = _book_trade()

    def malformed_close(*_args, **_kwargs):
        return {
            "accepted": True,
            "changed": True,
            "replayed": False,
            "closed_trade": trade,
            "account": {"strategy_id": "pa:test", "open_trade": None, "balance": "1020.00"},
            "settlement": {
                "entry_price": "100",
                "exit_price": "102",
                "quantity": "10",
                "applied_pnl": "20.00",
                "closing_balance": "1020.00",
                "balance_after": "1020.00",
                "exhausted": False,
            },
        }

    class ForgedBoundaryBook:
        def get_account(self, strategy_id):
            return book.get_account(strategy_id)

        def close_trade(self, *args, **kwargs):
            return malformed_close(*args, **kwargs)

    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=ForgedBoundaryBook(),
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
    )
    out = monitor.on_second(_second("2026-01-02T14:30:01Z", high="102", low="100"))
    assert out.status == "rejected" and out.code == "settlement_authority_mismatch"
    assert not monitor.state.terminal and not callbacks
    assert book.get_account("pa:test").open_trade is not None


def test_attached_book_rejects_distinct_settlement_override_before_runtime():
    callbacks = []
    book, trade = _book_trade()
    with pytest.raises(ValueError, match="distinct settlement override"):
        ExitMonitor.from_trade(
            trade,
            paper_account_book=book,
            settle_trade=lambda *_args, **_kwargs: {"accepted": True, "changed": True},
            entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
            on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
        )
    assert not callbacks and book.get_account("pa:test").open_trade is not None and book.get_account("pa:test").balance == Decimal("1000.00")


def test_shape_complete_forged_settlement_cannot_bypass_attached_real_book():
    callbacks = []
    book, trade = _book_trade()

    def forged_close(*_args, **_kwargs):
        return {
            "accepted": True,
            "changed": True,
            "replayed": False,
            "closed_trade": trade,
            "account": {"strategy_id": "pa:test", "open_trade": None, "balance": "2000.00"},
            "settlement": {
                "entry_price": "100",
                "exit_price": "200",
                "quantity": "10",
                "applied_pnl": "1000.00",
                "closing_balance": "2000.00",
                "balance_after": "2000.00",
                "exhausted": False,
            },
        }

    with pytest.raises(ValueError, match="distinct settlement override"):
        ExitMonitor.from_trade(
            trade,
            paper_account_book=book,
            settle_trade=forged_close,
            entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
            on_position_close=lambda *args, **kwargs: callbacks.append((args, kwargs)) or True,
        )
    assert not callbacks and book.get_account("pa:test").open_trade is not None and book.get_account("pa:test").balance == Decimal("1000.00")


def test_missing_entry_fill_interval_is_not_executable():
    book, trade = _book_trade()
    with pytest.raises(ValueError, match="entry_fill_interval"):
        ExitMonitor.from_trade(trade, paper_account_book=book)
    assert book.get_account("pa:test").open_trade is not None


def test_filled_at_without_half_open_interval_is_not_executable():
    book, trade = _book_trade()
    with pytest.raises(ValueError, match="entry_fill_interval"):
        ExitMonitor.from_trade(trade, paper_account_book=book, entry_filled_at=START)
    assert book.get_account("pa:test").open_trade is not None


def test_malformed_explicit_empty_interval_cannot_fall_back_to_entry_source():
    book, trade = _book_trade()
    source = {"start_at": START, "end_at": "2026-01-02T14:30:01Z"}
    with pytest.raises(ValueError, match="entry_fill_interval"):
        ExitMonitor.from_trade(trade, paper_account_book=book, entry_event=source, entry_fill_interval=[])
    assert book.get_account("pa:test").open_trade is not None


def test_entry_fill_straddle_second_cannot_settle():
    book, trade = _book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:00.500000Z"),
    )
    out = monitor.on_second(_second(START, high="102", low="99"))
    assert out.not_evaluated and out.code == "entry_fill_straddle"
    assert book.get_account("pa:test").open_trade is not None


def test_recovery_batch_incomplete_then_touch_is_atomic():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:30:01Z", end_at="2026-01-02T14:31:01Z")
    out = monitor.on_recovery([_recovery(complete=False, high="102"), _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z", high="102")])
    assert out.not_evaluated and out.partial_data
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_recovery_batch_malformed_then_touch_is_atomic():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:30:01Z", end_at="2026-01-02T14:31:01Z")
    malformed = _recovery().to_dict()
    malformed["high"] = "102"
    out = monitor.on_recovery([malformed, _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z", high="102")])
    assert out.not_evaluated and out.partial_data
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_recovery_batch_identity_mismatch_then_touch_is_atomic():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:30:01Z", end_at="2026-01-02T14:31:01Z")
    out = monitor.on_recovery([_recovery(symbol="OTHER", high="102"), _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z", high="102")])
    assert out.not_evaluated and out.partial_data and out.code == "recovery_identity_mismatch"
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_recovery_batch_straddle_then_touch_is_atomic():
    book, trade = _book_trade()
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=("2026-01-02T14:31:30Z", "2026-01-02T14:32:30Z"),
    )
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:34:00Z")
    out = monitor.on_recovery([_recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z", high="102"), _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z", high="102")])
    assert out.not_evaluated and out.partial_data
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_recovery_batch_late_then_touch_is_atomic():
    book, _trade, monitor = _monitor()
    # Advance the processed execution watermark before opening a later gap.
    assert monitor.on_second(_second("2026-01-02T14:30:01Z")).status == "open"
    monitor.on_gap(start_at="2026-01-02T14:33:00Z", end_at="2026-01-02T14:34:00Z")
    out = monitor.on_recovery([_recovery(start="2026-01-02T14:30:00Z", end="2026-01-02T14:31:00Z", high="102"), _recovery(start="2026-01-02T14:34:00Z", end="2026-01-02T14:35:00Z", high="102")])
    assert out.not_evaluated and out.partial_data
    assert book.get_account("pa:test").open_trade is not None
    assert book.get_account("pa:test").balance == Decimal("1000.00")


def test_open_ended_gap_is_rejected_without_recovery_state():
    book, _trade, monitor = _monitor()

    out = monitor.on_gap(ConnectionGap("2026-01-02T14:30:01Z", None, "stream_disconnect"))
    assert out.status == "not_evaluated" and out.not_evaluated and out.partial_data
    assert out.event is not None and out.event.payload["end_at"] is None
    assert monitor.state.gap_active
    paused = monitor.on_second(_second("2026-01-02T14:30:01Z", high="102", low="100"))
    assert paused.not_evaluated and book.get_account("pa:test").open_trade is not None


def test_open_gap_can_be_closed_by_same_start_finite_update():
    _book, _trade, monitor = _monitor()

    opened = monitor.on_gap(ConnectionGap("2026-01-02T14:30:01Z", None, "stream_disconnect"))
    closed = monitor.close_gap("2026-01-02T14:30:01Z", "2026-01-02T14:31:01Z", reason="stream_recovered")
    assert opened.event is not None and closed.status == "not_evaluated"
    assert monitor.state.gap_intervals[-1] == ("2026-01-02T14:30:01Z", "2026-01-02T14:31:01Z")


def test_closing_sole_pre_entry_open_gap_clears_partial_poison():
    book, _trade, monitor = _monitor()

    opened = monitor.on_gap(ConnectionGap("2026-01-02T14:29:00Z", None, "stream_disconnect"))
    assert opened.not_evaluated and opened.partial_data and monitor.state.gap_active

    closed = monitor.close_gap("2026-01-02T14:29:00Z", "2026-01-02T14:30:00Z", reason="stream_recovered")
    assert closed.status == "gap"
    assert closed.code == "gap_outside_window"
    assert not closed.not_evaluated and not closed.partial_data and not closed.gap_overlap
    assert closed.outcome_status == "open"
    assert closed.event is not None
    assert closed.event.payload["outcome_status"] == "open"
    assert closed.event.payload["not_evaluated"] is False
    assert closed.event.payload["partial_data"] is False
    assert closed.event.payload["gap_overlap"] is False
    assert monitor.state.gap_intervals == (("2026-01-02T14:29:00Z", "2026-01-02T14:30:00Z"),)
    assert not monitor.state.gap_active
    assert not monitor.state.partial_data
    assert monitor.state.outcome_status == "open"
    assert monitor.state.uncertainty is None

    later = monitor.on_second(_second("2026-01-02T14:30:02Z"))
    assert later.status == "open"
    assert not later.not_evaluated and not later.partial_data
    assert book.get_account("pa:test").open_trade is not None


def test_closing_pre_entry_open_gap_preserves_post_entry_gap_partial_state():
    _book, _trade, monitor = _monitor()

    post_entry = monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    assert post_entry.not_evaluated and post_entry.partial_data
    monitor.on_gap(ConnectionGap("2026-01-02T14:29:00Z", None, "stream_disconnect"))

    closed = monitor.close_gap("2026-01-02T14:29:00Z", "2026-01-02T14:30:00Z", reason="stream_recovered")
    assert closed.status == "not_evaluated"
    assert closed.code == "gap_overlap"
    assert closed.not_evaluated and closed.partial_data and closed.gap_overlap
    assert closed.outcome_status == "not_evaluated"
    assert monitor.state.gap_active and monitor.state.partial_data
    assert monitor.state.outcome_status == "not_evaluated"
    assert "partial_data" in monitor.state.uncertainty_codes
    assert ("2026-01-02T14:31:00Z", "2026-01-02T14:32:00Z") in monitor.state.gap_intervals


@pytest.mark.parametrize(
    "gap",
    [
        {"start_at": "not-a-time", "end_at": None, "reason": "bad"},
        {"start_at": "2026-01-02T14:30:01", "end_at": None, "reason": "missing-z"},
        {"start_at": "2026-02-30T14:30:01Z", "end_at": None, "reason": "bad-date"},
        ConnectionGap("not-a-time", None, "bad-object"),
        ConnectionGap("2026-01-02T14:30:01", None, "missing-z-object"),
    ],
)
def test_malformed_open_gap_is_typed_and_does_not_mutate_state(gap):
    _book, _trade, monitor = _monitor()
    before = monitor.state
    out = monitor.on_gap(gap)
    assert out.status == "rejected" and out.code == "invalid_gap"
    assert out.event is None
    assert monitor.state == before


def test_malformed_finite_gap_is_typed_and_does_not_mutate_state():
    _book, _trade, monitor = _monitor()
    before = monitor.state
    out = monitor.on_gap(start_at="2026-01-02T14:30:01", end_at="2026-01-02T14:31:01Z")
    assert out.status == "rejected" and out.code == "invalid_gap"
    assert out.event is None
    assert monitor.state == before


def test_close_gap_is_strict_and_late_terminal_update_is_replayed():
    book, _trade, monitor = _monitor()
    monitor.on_gap(ConnectionGap("2026-01-02T14:31:00Z", None, "stream_disconnect"))
    before = monitor.state
    invalid = monitor.close_gap("2026-01-02T14:31:00", "2026-01-02T14:32:00Z")
    assert invalid.status == "rejected" and invalid.code == "invalid_gap"
    assert monitor.state == before

    monitor.close_gap("2026-01-02T14:31:00Z", "2026-01-02T14:32:00Z")
    settled = monitor.on_recovery(_recovery(high="102"))
    assert settled.status == "settled"
    state_hash = monitor.state.state_hash
    late = monitor.close_gap("2026-01-02T14:31:00Z", "2026-01-02T14:33:00Z")
    assert late.replayed and late.code == "idempotent_replay"
    assert monitor.state.state_hash == state_hash
    assert book.get_account("pa:test").open_trade is None


def test_far_future_recovery_bar_cannot_clear_active_gap():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:30:01Z", end_at="2026-01-02T14:31:01Z")
    out = monitor.on_recovery(_recovery(start="2026-01-02T15:00:00Z", end="2026-01-02T15:01:00Z", high="101"))
    assert out.not_evaluated and out.partial_data
    assert monitor.state.gap_active
    assert book.get_account("pa:test").open_trade is not None


def test_before_gap_recovery_bar_cannot_clear_active_gap():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:31:01Z", end_at="2026-01-02T14:32:01Z")
    out = monitor.on_recovery(_recovery(start="2026-01-02T14:29:00Z", end="2026-01-02T14:30:00Z", high="101"))
    assert out.not_evaluated and out.partial_data
    assert monitor.state.gap_active
    assert book.get_account("pa:test").open_trade is not None


def test_multi_minute_recovery_requires_full_contiguous_coverage_and_recovers():
    _book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:34:00Z")
    bars = [
        _recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"),
        _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z"),
        _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z"),
    ]
    out = monitor.on_recovery(bars)
    assert out.status == "recovered" and not monitor.state.gap_active


@pytest.mark.parametrize(
    "bars",
    [
        [_recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z"), _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z")],
        [_recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"), _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z")],
        [_recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"), _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z")],
    ],
)
def test_multi_minute_recovery_missing_head_middle_or_tail_stays_partial(bars):
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:34:00Z")
    out = monitor.on_recovery(bars)
    assert out.not_evaluated and out.partial_data and monitor.state.gap_active
    assert book.get_account("pa:test").open_trade is not None


def test_multi_minute_recovery_full_coverage_settles_first_event_time_touch():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:34:00Z")
    bars = [
        _recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"),
        _recovery(start="2026-01-02T14:32:00Z", end="2026-01-02T14:33:00Z", high="102"),
        _recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z"),
    ]
    out = monitor.on_recovery(bars)
    assert out.status == "settled" and out.selected_exit == Decimal("102")
    assert book.get_account("pa:test").open_trade is None


def test_separated_gaps_require_coverage_of_all_active_ranges():
    book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    monitor.on_gap(start_at="2026-01-02T14:33:00Z", end_at="2026-01-02T14:34:00Z")
    only_last = [_recovery(start="2026-01-02T14:33:00Z", end="2026-01-02T14:34:00Z")]
    out = monitor.on_recovery(only_last)
    assert out.not_evaluated and out.partial_data and monitor.state.gap_active
    assert book.get_account("pa:test").open_trade is not None


def test_recovered_gap_worklist_does_not_poison_a_later_unrelated_gap():
    _book, _trade, monitor = _monitor()
    monitor.on_gap(start_at="2026-01-02T14:31:00Z", end_at="2026-01-02T14:32:00Z")
    first = monitor.on_recovery(_recovery(start="2026-01-02T14:31:00Z", end="2026-01-02T14:32:00Z"))
    assert first.status == "recovered" and not monitor.state.gap_intervals
    monitor.on_gap(start_at="2026-01-02T14:34:00Z", end_at="2026-01-02T14:35:00Z")
    second = monitor.on_recovery(_recovery(start="2026-01-02T14:34:00Z", end="2026-01-02T14:35:00Z"))
    assert second.status == "recovered" and not monitor.state.gap_active


def test_gap_transition_has_deterministic_immutable_internal_event():
    _book, _trade, monitor = _monitor()
    first = monitor.on_gap(start_at="2026-01-02T14:30:01Z", end_at="2026-01-02T14:31:01Z", reason="stream_disconnect")
    assert first.event is not None
    assert first.event.payload["gap_interval"] == ("2026-01-02T14:30:01Z", "2026-01-02T14:31:01Z")
    assert first.event.payload["reason"] == "stream_disconnect"
    assert first.event.payload["outcome_status"] == "not_evaluated"
    assert first.event.payload["partial_data"] is True
    with pytest.raises(TypeError):
        first.event.payload["reason"] = "forged"
    duplicate = monitor.on_gap(start_at="2026-01-02T14:30:01Z", end_at="2026-01-02T14:31:01Z", reason="stream_disconnect")
    assert duplicate.replayed and duplicate.event is None

    _book2, _trade2, monitor2 = _monitor()
    second = monitor2.on_gap(start_at="2026-01-02T14:30:01Z", end_at="2026-01-02T14:31:01Z", reason="stream_disconnect")
    assert second.event is not None and second.event.event_hash == first.event.event_hash


def test_reduce_exit_separates_constructor_and_event_clock_arguments():
    book, _trade, monitor = _monitor()
    out = reduce_exit(
        monitor.state,
        _second("2026-01-02T14:30:01Z", high="102", low="100"),
        paper_account_book=book,
        on_position_close=lambda *_args, **_kwargs: True,
        now="2026-01-02T14:30:05Z",
    )
    assert out.status == "settled"


def test_g024_idempotent_replay_does_not_notify_scheduler_again():
    book, trade = _book_trade()
    first_callbacks = []
    first = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: first_callbacks.append((args, kwargs)) or True,
    )
    event = _second("2026-01-02T14:30:01Z", high="102", low="100")
    first_out = first.on_second(event)
    assert first_out.status == "settled" and len(first_callbacks) == 1

    replay_callbacks = []
    second = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=(START, "2026-01-02T14:30:01Z"),
        on_position_close=lambda *args, **kwargs: replay_callbacks.append((args, kwargs)) or True,
    )
    out = second.on_second(event)
    assert out.status == "replayed" and out.replayed and not out.changed
    assert out.scheduler_callback is not None and out.scheduler_callback.callback_count == 0
    assert not replay_callbacks
    assert book.get_account("pa:test").open_trade is None
