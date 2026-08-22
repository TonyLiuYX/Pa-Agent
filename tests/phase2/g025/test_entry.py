from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pa_engine.market_sources.alpaca.history import RecoveryBar
from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent
from pa_engine.paper.core import PaperAccountBook
from pa_engine.paper.entry import PendingEntryMonitor, PendingEntryState


UTC = timezone.utc
START = "2026-01-02T14:30:00Z"


def instruction(direction="long", *, x=3):
    if direction == "long":
        return {"branch": "wait_for_price", "strategy_id": "pa:test", "direction": "long", "entry_price": "100", "stop_price": "99", "target_price": "102", "entry_wait_seconds": x, "symbol": "XYZ"}
    return {"branch": "wait_for_price", "strategy_id": "pa:test", "direction": "short", "entry_price": "100", "stop_price": "101", "target_price": "98", "entry_wait_seconds": x, "symbol": "XYZ"}


def monitor(*, direction="long", x=3, **kwargs):
    return PendingEntryMonitor.from_instruction(
        instruction(direction, x=x),
        decision_accepted_at=START,
        submitted_at=START,
        generation=7,
        validator=lambda value, **_kwargs: True,
        paper_account_book=kwargs.pop("book", PaperAccountBook()),
        **kwargs,
    )


def second(start="2026-01-02T14:30:00Z", *, high="100", low="100", watermark=None, revision=0, quality=None):
    sec = datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp()
    from datetime import timedelta

    end = datetime.fromtimestamp(sec + 1, tz=UTC).isoformat().replace("+00:00", "Z")
    wm = watermark or datetime.fromtimestamp(sec + 4, tz=UTC).isoformat().replace("+00:00", "Z")
    return SecondAggregateEvent("XYZ", start, end, low, high, low, high, "1", contributor_ids=(f"trade-{start}",), execution_watermark=wm, status="frozen", revision_sequence=revision, quality=quality or {"status": "ok", "codes": [], "connection_gap": False})


def test_long_low_high_equality_fill_exact_requested_level_and_no_same_second_exit():
    book = PaperAccountBook()
    callbacks = []
    m = monitor(book=book, on_pending_fill=lambda *args, **kwargs: callbacks.append((args, kwargs)))
    out = m.on_second(second(high="100", low="99"), now="2026-01-02T14:30:04Z")
    assert out.status == "filled" and out.filled_price == 100
    assert out.exit_eligible_after.isoformat().endswith("14:30:01+00:00")
    assert len(callbacks) == 1 and callbacks[0][1]["generation"] == 7
    assert book.get_account("pa:test").open_trade.entry_price == 100
    assert m.on_second(second(high="102", low="98"), now="2026-01-02T14:30:05Z").replayed


def test_short_touch_and_out_of_order_duplicates_are_idempotent():
    book = PaperAccountBook()
    m = monitor(direction="short", book=book)
    first = second("2026-01-02T14:30:01Z", high="101", low="100")
    assert m.on_second(first, now="2026-01-02T14:30:05Z").status == "filled"
    assert m.on_second(first, now="2026-01-02T14:30:05Z").replayed


def test_decision_straddle_and_after_x_are_not_evaluated():
    m = monitor(x=1)
    # Decision is at 14:30:00; a pre-decision straddle is not eligible.
    straddle = second("2026-01-02T14:29:59Z", high="101", low="99", watermark="2026-01-02T14:30:03Z")
    assert m.on_second(straddle, now="2026-01-02T14:30:03Z").status == "not_evaluated"
    late = second("2026-01-02T14:30:01Z", high="101", low="99", watermark="2026-01-02T14:30:05Z")
    assert m.on_second(late, now="2026-01-02T14:30:05Z").status == "not_evaluated"
    assert m.state.pending


def test_x_one_last_second_arrives_in_publication_grace_without_extending_x():
    book = PaperAccountBook()
    m = monitor(x=1, book=book)
    final = second(high="100", low="99", watermark="2026-01-02T14:30:04Z")
    assert m.on_second(final, now="2026-01-02T14:30:04Z").status == "filled"
    assert m.state.expires_at.isoformat().endswith("14:30:01+00:00")


def test_event_before_expiry_wins_when_timer_and_boundary_second_arrive_together():
    book = PaperAccountBook()
    m = monitor(x=1, book=book)
    boundary = second(high="100", low="99", watermark="2026-01-02T14:30:04Z")
    outcome = m.on_expiry("2026-01-02T14:30:04Z", events=[boundary])
    assert outcome.status == "filled"


def test_expiry_waits_for_resolution_then_calls_g024_and_g023_once():
    book = PaperAccountBook()
    expiry = []
    m = monitor(x=1, book=book, on_pending_expiry=lambda *args, **kwargs: expiry.append((args, kwargs)))
    assert m.on_expiry("2026-01-02T14:30:01Z").status == "pending"
    out = m.on_expiry("2026-01-02T14:30:04Z")
    assert out.status == "expired" and out.event.event_type == "pending_entry_expired"
    assert len(expiry) == 1 and expiry[0][1]["generation"] == 7
    assert m.on_expiry("2026-01-02T14:30:08Z").replayed


def test_gap_recovery_complete_touch_fills_with_lineage():
    book = PaperAccountBook()
    recovery_calls = []

    def recover(state, **kwargs):
        recovery_calls.append((state.pending_id, kwargs))
        return RecoveryBar(
            instrument={"asset_class": "us_equity", "symbol": "XYZ", "venue": "IEX", "currency": "USD"},
            timeframe={"unit": "minute", "multiplier": 1},
            session={"kind": "regular", "timezone": "America/New_York"},
            start_at=START,
            end_at="2026-01-02T14:31:00Z",
            open="100", high="101", low="99", close="100", volume="10",
            complete=True, coverage_state="complete", source_request_hash="a" * 64,
            recovery_cursor="2026-01-02T14:31:00Z", market_feed={"schema_version": "0.1.0", "provider": "alpaca", "feed": "iex", "coverage": "single_exchange"},
        )

    m = monitor(x=60, book=book, recovery_callback=recover)
    m.on_gap(start_at=START, end_at="2026-01-02T14:31:00Z")
    out = m.on_expiry("2026-01-02T14:31:03Z")
    assert out.status == "filled"
    assert out.lineage["path_precision"] == "minute_bar_no_path"
    assert out.lineage["recovery_bar_id"].startswith("rbar_")
    assert len(recovery_calls) == 1


def test_recovery_cross_symbol_is_typed_not_evaluated_and_never_fills():
    book = PaperAccountBook()
    m = monitor(x=60, book=book)
    m.on_gap(start_at=START, end_at="2026-01-02T14:31:00Z")
    other_symbol = RecoveryBar(
        instrument={"asset_class": "us_equity", "symbol": "OTHER", "venue": "IEX", "currency": "USD"},
        timeframe={"unit": "minute", "multiplier": 1},
        session={"kind": "regular", "timezone": "America/New_York"},
        start_at=START,
        end_at="2026-01-02T14:31:00Z",
        open="100", high="101", low="99", close="100", volume="10",
        complete=True, coverage_state="complete", source_request_hash="a" * 64,
        recovery_cursor="2026-01-02T14:31:00Z",
        market_feed={"schema_version": "0.1.0", "provider": "alpaca", "feed": "iex", "coverage": "single_exchange"},
    )
    out = m.on_recovery(other_symbol)
    assert out.status == "not_evaluated" and out.code == "recovery_identity_mismatch"
    assert not m.state.terminal and m.state.pending
    assert m.state.recovery_lineage == ()
    assert book.get_account("pa:test").pending_trade is not None


def test_gap_recovery_incomplete_expires_not_evaluated_partial_once():
    book = PaperAccountBook()
    calls = []
    m = monitor(x=1, book=book, recovery_callback=lambda *args, **kwargs: calls.append(1) or [])
    m.on_gap(start_at=START, end_at=None)
    out = m.on_expiry("2026-01-02T14:30:04Z")
    assert out.status == "expired" and out.not_evaluated and out.partial_data and out.gap_overlap
    assert len(calls) == 1
    assert m.on_expiry("2026-01-02T14:30:09Z").replayed


def test_disable_cancels_without_recall_and_late_second_cannot_resurrect():
    book = PaperAccountBook()
    fills, expiries = [], []
    m = monitor(book=book, on_pending_fill=lambda *a, **k: fills.append(1), on_pending_expiry=lambda *a, **k: expiries.append(1))
    assert m.disable().status == "cancelled"
    assert not fills and not expiries
    assert m.on_second(second(high="100", low="99"), now="2026-01-02T14:30:04Z").replayed


def test_state_is_recursively_immutable_and_content_stable():
    m = monitor()
    with pytest.raises(TypeError):
        m.state.market_feed["feed"] = "sip"
    state_copy = m.state.to_dict()
    state_copy["decision_lineage"]["instruction"]["entry_price"] = "999"
    assert m.state.entry_price == 100
