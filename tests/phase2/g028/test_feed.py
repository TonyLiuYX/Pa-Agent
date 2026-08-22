"""Focused tests for the G028 feed session (``pa_engine/runtime/feed.py``)."""

from __future__ import annotations

import copy
import importlib.util
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pa_engine.market import MarketFeedRef
from pa_engine.market_sources.alpaca.history import AdapterOutcome
from pa_engine.market_sources.alpaca.stream import LocalSecondAggregator, parse_trade
from pa_engine.market_view import validate_shared_market_view
from pa_engine.runtime.feed import FeedSession, FeedSessionError
from pa_engine.strategy.runtime.clock import FakeClock

FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "phase2" / "g028" / "feed"

SYMBOL = "AAPL"
SESSION_OPEN = "2026-03-09T13:30:00Z"
CUTOVER = "2026-03-09T13:33:00Z"
NOW = "2026-03-09T13:34:03Z"
TIMEFRAME = {"unit": "minute", "multiplier": 1}

HISTORY_VENDOR_BARS = [
    {"t": "2026-03-09T13:30:00Z", "o": 100.0, "h": 101.25, "l": 99.5, "c": 100.75, "v": 1000},
    {"t": "2026-03-09T13:31:00Z", "o": 100.75, "h": 101.5, "l": 100.5, "c": 101.25, "v": 1200},
    {"t": "2026-03-09T13:32:00Z", "o": 101.25, "h": 101.75, "l": 100.75, "c": 101.0, "v": 1300},
]


def _builders():
    spec = importlib.util.spec_from_file_location("g028_feed_builders", FIXTURE_ROOT / "builders.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


B = _builders()


def standard_history():
    snapshot = B.history_snapshot(start_at=SESSION_OPEN, end_at=CUTOVER, bars=HISTORY_VENDOR_BARS)
    return B.history_result(snapshot, start_at=SESSION_OPEN, end_at=CUTOVER)


def make_session(*, history_result=None, recovery_result=None, batches=(), timeframe=None, **kwargs):
    adapter = B.FakeHistoryAdapter(history_result=history_result, recovery_result=recovery_result)
    aggregator = B.FakeAggregator(batches=batches)
    session = FeedSession(
        symbol=SYMBOL,
        timeframe=timeframe or TIMEFRAME,
        history_adapter=adapter,
        aggregator=aggregator,
        clock=FakeClock(NOW),
        **kwargs,
    )
    return session, adapter, aggregator


def events():
    first = B.frozen_second(
        start_at="2026-03-09T13:33:00Z", end_at="2026-03-09T13:33:01Z",
        open="10", high="10.1", low="9.95", close="10.05", volume="100", contributor_ids=("1",),
    )
    second = B.frozen_second(
        start_at="2026-03-09T13:33:59Z", end_at="2026-03-09T13:34:00Z",
        open="10.05", high="10.2", low="10", close="10.15", volume="50", contributor_ids=("2",),
    )
    return first, second


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ({"unit": "minute", "multiplier": 1}, "1Min"),
        ({"unit": "minute", "multiplier": 5}, "5Min"),
        ({"unit": "hour", "multiplier": 1}, "1Hour"),
        ({"unit": "day", "multiplier": 1}, "1Day"),
        ({"unit": "week", "multiplier": 1}, "1Week"),
        ("5Min", "5Min"),
        ("1 hour", "1Hour"),
        ("15 minute", "15Min"),
        ("1Hour", "1Hour"),
    ],
)
def test_timeframe_text_mapping(value, expected):
    assert FeedSession(symbol="AAPL", timeframe=value).timeframe_text() == expected


@pytest.mark.parametrize(
    "value",
    [
        "1Month",
        {"unit": "second", "multiplier": 1},
        {"unit": "minute", "multiplier": 60},
        {"unit": "hour", "multiplier": 24},
        {"unit": "day", "multiplier": 2},
        {"unit": "minute", "multiplier": True},
        {"unit": "minute"},
    ],
)
def test_invalid_timeframe_fails_closed(value):
    with pytest.raises(FeedSessionError) as error:
        FeedSession(symbol="AAPL", timeframe=value)
    assert error.value.code == "invalid_timeframe"


def test_constructor_validations():
    with pytest.raises(FeedSessionError) as error:
        FeedSession(symbol="aapl", timeframe="1Min")
    assert error.value.code == "invalid_symbol"
    sip = MarketFeedRef("0.1.0", "alpaca", "sip", "consolidated")
    with pytest.raises(FeedSessionError) as error:
        FeedSession(symbol="AAPL", timeframe="1Min", market_feed=sip)
    assert error.value.code == "feed_scope_mismatch"
    with pytest.raises(FeedSessionError) as error:
        FeedSession(symbol="AAPL", timeframe="1Min", session={"kind": "extended", "timezone": "America/New_York"})
    assert error.value.code == "invalid_session"
    with pytest.raises(FeedSessionError) as error:
        FeedSession(symbol="AAPL", timeframe="1Min", adjustment_policy="adjusted")
    assert error.value.code == "invalid_adjustment_policy"


def test_backfill_ok_and_uses_g019_timeframe_text():
    hist = standard_history()
    session, adapter, _aggregator = make_session(history_result=hist)
    result = session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    assert result is hist
    assert session.history_result is hist
    call = adapter.calls[-1]
    assert call["operation"] == "history" and call["symbol"] == SYMBOL
    assert call["timeframe"] == "1Min" and call["start_at"] == SESSION_OPEN and call["end_at"] == CUTOVER
    assert call["limit"] == 1000
    assert session.cutover_at(result) == datetime(2026, 3, 9, 13, 33, tzinfo=timezone.utc)


def test_backfill_default_end_at_uses_clock():
    session, adapter, _aggregator = make_session(history_result=standard_history())
    session.backfill(start_at=SESSION_OPEN)
    assert adapter.calls[-1]["end_at"] == datetime(2026, 3, 9, 13, 34, 3, tzinfo=timezone.utc)


def test_backfill_missing_snapshot_raises_history_unavailable():
    empty = B.history_result(
        None,
        start_at=SESSION_OPEN,
        end_at=CUTOVER,
        outcome=AdapterOutcome("empty_window", "empty_window", False, False, "no bars"),
    )
    session, _adapter, _aggregator = make_session(history_result=empty)
    with pytest.raises(FeedSessionError) as error:
        session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    assert error.value.code == "history_unavailable"
    bare = FeedSession(symbol="AAPL", timeframe="1Min")
    with pytest.raises(FeedSessionError) as error:
        bare.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    assert error.value.code == "history_unavailable"


def test_backfill_feed_and_adjustment_mismatches_fail_closed():
    hist = standard_history()
    forged_feed = copy.deepcopy(dict(hist.market_snapshot))
    forged_feed["extensions"]["market_feed"]["feed"] = "sip"
    result = B.history_result(forged_feed, start_at=SESSION_OPEN, end_at=CUTOVER)
    session, _adapter, _aggregator = make_session(history_result=result)
    with pytest.raises(FeedSessionError) as error:
        session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    assert error.value.code == "history_feed_mismatch"
    forged_adjustment = copy.deepcopy(dict(hist.market_snapshot))
    forged_adjustment["adjustment_policy"] = "adjusted"
    result = B.history_result(forged_adjustment, start_at=SESSION_OPEN, end_at=CUTOVER)
    session, _adapter, _aggregator = make_session(history_result=result)
    with pytest.raises(FeedSessionError) as error:
        session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    assert error.value.code == "history_adjustment_mismatch"


def test_ingest_dedupe_and_chronological_order():
    first, second = events()
    third = B.frozen_second(
        start_at="2026-03-09T13:34:30Z", end_at="2026-03-09T13:34:31Z",
        open="10.2", high="10.3", low="10.15", close="10.3", volume="1", contributor_ids=("3",),
    )
    session, _adapter, _aggregator = make_session(batches=[[first, second], [first, second, third], [third]])
    assert [event.event_id for event in session.ingest(now="2026-03-09T13:34:31Z")] == [first.event_id, second.event_id]
    assert [event.event_id for event in session.ingest(now="2026-03-09T13:34:31Z")] == [third.event_id]
    assert len(session.skipped_replays) == 2
    assert session.ingest(now="2026-03-09T13:34:31Z") == []
    assert len(session.skipped_replays) == 3
    frozen = session.frozen_events
    assert [event.end_at for event in frozen] == ["2026-03-09T13:33:01Z", "2026-03-09T13:34:00Z", "2026-03-09T13:34:31Z"]
    assert session.latest_second.event_id == third.event_id
    assert session.latest_second.close == "10.3"


def test_ingest_older_interval_replay_is_skipped_and_recorded():
    first, second = events()
    older = B.frozen_second(
        start_at="2026-03-09T13:33:05Z", end_at="2026-03-09T13:33:06Z",
        open="10.1", high="10.2", low="10.05", close="10.2", volume="1", contributor_ids=("9",),
    )
    session, _adapter, _aggregator = make_session(batches=[[first, second], [older]])
    session.ingest(now="2026-03-09T13:34:03Z")
    assert session.ingest(now="2026-03-09T13:34:03Z") == []
    record = session.skipped_replays[0]
    assert record["event_id"] == older.event_id and record["reason"] == "out_of_order_interval"
    assert session.latest_second.event_id == second.event_id


def test_ingest_requires_aggregator():
    session = FeedSession(symbol="AAPL", timeframe="1Min")
    with pytest.raises(FeedSessionError) as error:
        session.ingest(now=NOW)
    assert error.value.code == "aggregator_unavailable"


def test_attach_rejects_wrong_feed_and_wrong_symbol():
    session = FeedSession(symbol="AAPL", timeframe="1Min")
    sip = MarketFeedRef("0.1.0", "alpaca", "sip", "consolidated")
    with pytest.raises(FeedSessionError) as error:
        session.attach(B.FakeAggregator(market_feed=sip))
    assert error.value.code == "feed_scope_mismatch"
    with pytest.raises(FeedSessionError) as error:
        session.attach(B.FakeAggregator(symbol="MSFT"))
    assert error.value.code == "symbol_mismatch"
    good = B.FakeAggregator()
    session.attach(good)
    assert session.aggregator is good


def test_build_view_executable_readiness():
    hist = standard_history()
    first, second = events()
    session, _adapter, _aggregator = make_session(history_result=hist, batches=[[first, second]])
    session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    session.ingest(now=NOW)
    view = session.build_view(now_at=NOW)
    assert view.readiness.executable is True
    assert view.readiness.recovery_required is False
    assert view.readiness.frozen_second is True
    assert view.readiness.session_ready is True
    assert view.latest_second["event_id"] == second.event_id
    assert view.latest_second["close"] == "10.15"
    assert view.current_price == "10.15"
    assert view.observation_frame is not None
    assert session.current_view is view
    assert validate_shared_market_view(view).valid


def test_build_view_requires_history():
    session = FeedSession(symbol="AAPL", timeframe="1Min")
    with pytest.raises(FeedSessionError) as error:
        session.build_view(now_at=NOW)
    assert error.value.code == "history_unavailable"


def test_build_view_surfaces_builder_error_codes():
    hist = standard_history()
    forged = copy.deepcopy(dict(hist.market_snapshot))
    forged["extensions"]["market_feed"]["feed"] = "sip"
    result = B.history_result(forged, start_at=SESSION_OPEN, end_at=CUTOVER)
    session, _adapter, _aggregator = make_session(history_result=hist)
    with pytest.raises(FeedSessionError) as error:
        session.build_view(history_result=result, now_at=NOW)
    assert error.value.code == "invalid_history_snapshot"


def test_off_grid_history_cutover_fails_closed():
    snapshot = B.history_snapshot(
        timeframe="5Min",
        start_at="2026-03-09T13:32:00Z",
        end_at="2026-03-09T13:37:00Z",
        bars=[{"t": "2026-03-09T13:32:00Z", "o": 100, "h": 101, "l": 99, "c": 100, "v": 100}],
    )
    hist = B.history_result(
        snapshot,
        start_at="2026-03-09T13:32:00Z",
        end_at="2026-03-09T13:37:00Z",
        timeframe={"unit": "minute", "multiplier": 5},
    )
    session, _adapter, _aggregator = make_session(history_result=hist, timeframe={"unit": "minute", "multiplier": 5})
    session.backfill(start_at="2026-03-09T13:32:00Z", end_at="2026-03-09T13:37:00Z")
    with pytest.raises(FeedSessionError) as error:
        session.build_view(now_at="2026-03-09T13:37:03Z")
    assert error.value.code == "cutover_mismatch"


def test_cutover_rule_joins_half_open_without_straddle_or_overlap():
    hist = standard_history()
    first, second = events()
    pre_cutover = B.frozen_second(
        start_at="2026-03-09T13:32:59Z", end_at="2026-03-09T13:33:00Z",
        open="9.9", high="9.95", low="9.8", close="9.9", volume="1", contributor_ids=("8",),
    )
    session, _adapter, _aggregator = make_session(history_result=hist, batches=[[pre_cutover, first, second]])
    session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    session.ingest(now=NOW)
    view = session.build_view(now_at=NOW)
    assert session.cutover_at(hist) == datetime(2026, 3, 9, 13, 33, tzinfo=timezone.utc)
    assert view.latest_second["event_id"] == second.event_id
    assert view.readiness.executable is True
    assert [bar["start_at"] for bar in view.closed_snapshot["bars"]] == [
        "2026-03-09T13:30:00Z",
        "2026-03-09T13:31:00Z",
        "2026-03-09T13:32:00Z",
        "2026-03-09T13:33:00Z",
    ]


def test_build_view_gapped_recovery_required():
    hist = standard_history()
    first, second = events()
    session, _adapter, _aggregator = make_session(history_result=hist, batches=[[first, second]])
    session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    session.ingest(now=NOW)
    session.mark_connection_gap(start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:45Z")
    view = session.build_view(now_at=NOW)
    assert view.readiness.connection_gap is True
    assert view.readiness.recovery_required is True
    assert view.executable is False
    assert validate_shared_market_view(view).valid


def test_build_view_accepts_explicit_connection_gaps():
    hist = standard_history()
    first, second = events()
    session, _adapter, _aggregator = make_session(history_result=hist, batches=[[first, second]])
    session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    session.ingest(now=NOW)
    view = session.build_view(
        now_at=NOW,
        connection_gaps=[{"start_at": "2026-03-09T13:33:30Z", "end_at": "2026-03-09T13:33:45Z"}],
    )
    assert view.readiness.recovery_required is True
    assert view.executable is False


def test_recovery_evidence_clears_recovery_required():
    hist = standard_history()
    first, second = events()
    flagged = B.gap_flagged_second(start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:31Z", close="10.1")
    third = B.frozen_second(
        start_at="2026-03-09T13:35:00Z", end_at="2026-03-09T13:35:01Z",
        open="10.3", high="10.4", low="10.25", close="10.4", volume="1", contributor_ids=("4",),
    )
    session, _adapter, _aggregator = make_session(history_result=hist, batches=[[first, flagged, second], [third]])
    session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    session.ingest(now=NOW)
    session.mark_connection_gap(start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:45Z")

    gapped = session.build_view(now_at=NOW)
    assert gapped.readiness.recovery_required is True
    assert gapped.executable is False

    session.close_connection_gap(end_at="2026-03-09T13:33:45Z")
    bars = (
        B.recovery_bar(start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:31Z"),
        B.recovery_bar(start_at="2026-03-09T13:33:31Z", end_at="2026-03-09T13:33:45Z"),
    )
    evidence = session.build_recovery_evidence(bars, generation=1)
    assert evidence["reseeded"] is True
    assert evidence["generation"] == 1 and evidence["recovery_generation"] == 1
    assert evidence["resolved_intervals"] == []
    assert evidence["source_fingerprint"] == hist.market_snapshot["content_hash"]
    assert [item["start_at"] for item in evidence["recovery_bar_intervals"]] == [
        "2026-03-09T13:33:30Z",
        "2026-03-09T13:33:31Z",
    ]

    recovered = session.build_view(now_at=NOW, recovered=True, recovery_generation=1, recovery_evidence=evidence)
    assert recovered.readiness.recovery_required is False
    assert recovered.readiness.connection_gap is False
    assert recovered.executable is True
    assert session.status()["recovery_generation"] == 1

    unproven = session.build_view(now_at=NOW, recovered=True, recovery_generation=1)
    assert unproven.readiness.recovery_required is True
    assert unproven.executable is False

    ignored = session.build_view(now_at=NOW, recovered=False, recovery_generation=1, recovery_evidence=evidence)
    assert ignored.readiness.recovery_required is True
    assert ignored.executable is False

    stale = session.build_view(now_at=NOW, recovered=True, recovery_generation=1, recovery_evidence=evidence)
    assert stale.readiness.recovery_required is True
    assert stale.executable is False

    session.ingest(now="2026-03-09T13:35:04Z")
    reseeded = session.build_view(now_at="2026-03-09T13:35:04Z", recovered=True, recovery_generation=1, recovery_evidence=evidence)
    assert reseeded.readiness.recovery_required is False
    assert reseeded.executable is True


def test_invalid_recovery_evidence_generation_fails_closed():
    session, _adapter, _aggregator = make_session()
    with pytest.raises(FeedSessionError) as error:
        session.build_recovery_evidence((), generation=0)
    assert error.value.code == "invalid_recovery_generation"
    with pytest.raises(FeedSessionError) as error:
        session.build_recovery_evidence((), generation=True)
    assert error.value.code == "invalid_recovery_generation"


def test_real_aggregator_gap_keeps_view_partial():
    hist = standard_history()
    aggregator = LocalSecondAggregator("AAPL")
    session = FeedSession(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        history_adapter=B.FakeHistoryAdapter(history_result=hist),
        aggregator=aggregator,
        clock=FakeClock(NOW),
    )
    session.mark_connection_gap(start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:45Z")
    for trade_id, timestamp, price in ((1, "2026-03-09T13:33:30.1Z", "10.1"), (2, "2026-03-09T13:33:59.1Z", "10.15")):
        payload = {"T": "t", "S": "AAPL", "i": trade_id, "x": "V", "p": price, "s": 1, "c": ["@"], "t": timestamp, "z": "C"}
        aggregator.add_trade(parse_trade(payload, received_at=timestamp))
    session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    session.ingest(now=NOW)

    gapped = session.build_view(now_at=NOW)
    assert gapped.readiness.recovery_required is True
    assert gapped.readiness.partial is True
    assert gapped.executable is False

    session.close_connection_gap(end_at="2026-03-09T13:33:45Z")
    evidence = session.build_recovery_evidence((B.recovery_bar(start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:31Z"),), generation=1)
    recovered = session.build_view(now_at=NOW, recovered=True, recovery_generation=1, recovery_evidence=evidence)
    assert recovered.readiness.recovery_required is False
    assert recovered.readiness.partial is True
    assert recovered.executable is False


def test_same_second_high_low_dual_touch_reaches_g028_feed_session():
    history = standard_history()
    aggregator = LocalSecondAggregator("AAPL")
    for trade_id, price in ((801, "12"), (802, "8"), (803, "10")):
        payload = {
            "T": "t", "S": "AAPL", "i": trade_id, "x": "V", "p": price, "s": 1,
            "c": ["@"], "t": f"2026-03-09T13:33:59.{trade_id - 800}Z", "z": "C",
        }
        aggregator.add_trade(parse_trade(payload, received_at="2026-03-09T13:34:00Z"))

    session = FeedSession(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        history_adapter=B.FakeHistoryAdapter(history_result=history),
        aggregator=aggregator,
        clock=FakeClock(NOW),
    )
    session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    ingested = session.ingest(now=NOW)

    assert len(ingested) == 1
    event = ingested[0]
    assert event.revision_sequence == 0 and event.parent_revision_id is None
    assert event.open == "12" and event.high == "12" and event.low == "8" and event.close == "10"
    assert event.volume == "3" and event.contributor_ids == ("801", "802", "803")
    view = session.build_view(now_at=NOW)
    assert view.executable
    assert view.latest_second is not None and view.latest_second["close"] == "10"
    assert validate_shared_market_view(view).valid


def test_fetch_recovery_passthrough_and_accessors():
    bars = (
        B.recovery_bar(start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:31Z"),
        B.recovery_bar(start_at="2026-03-09T13:33:31Z", end_at="2026-03-09T13:33:45Z"),
    )
    recovery = B.history_result(None, start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:45Z", recovery_bars=bars)
    session, adapter, _aggregator = make_session(history_result=standard_history(), recovery_result=recovery)
    result = session.fetch_recovery(start_at="2026-03-09T13:33:30Z", end_at="2026-03-09T13:33:45Z")
    assert result is recovery
    call = adapter.calls[-1]
    assert call["operation"] == "recovery" and call["start_at"] == "2026-03-09T13:33:30Z" and call["end_at"] == "2026-03-09T13:33:45Z"
    session.fetch_recovery(start_at="2026-03-09T13:33:30Z")
    assert adapter.calls[-1]["end_at"] == datetime(2026, 3, 9, 13, 34, 3, tzinfo=timezone.utc)
    mapped = session.recovery_bars(result)
    assert len(mapped) == 2
    assert {"recovery_bar_id", "content_hash", "start_at", "end_at", "trigger_eligible", "path_precision", "source_request_hash", "recovery_cursor"} <= set(mapped[0])
    assert session.recovery_cursor(result) == "2026-03-09T13:33:30Z"
    assert session.source_request_hash(result) == result.source_request_hash
    assert session.recovery_bars(B.history_result(None, start_at=SESSION_OPEN, end_at=CUTOVER)) == ()


def test_stream_start_stop_noop_and_status():
    session = FeedSession(symbol="AAPL", timeframe="1Min")
    assert session.start_stream() == {"ok": True, "action": "start", "stream": "not_attached", "error": None}
    stop = session.stop_stream()
    assert stop["ok"] is True and stop["stream"] == "not_attached"
    client = B.FakeStreamClient()
    session.attach(stream_client=client)
    assert session.start_stream()["stream"] == "connected"
    assert session.start_stream()["ok"] is True
    assert session.stop_stream()["stream"] == "closed"
    assert session.stop_stream()["ok"] is True
    # Repeated lifecycle calls are idempotent: one connect and one close for
    # this client, with no duplicate consumer.
    assert client.connect_calls == 1 and client.close_calls == 1
    failing = B.FakeStreamClient(fail_on="connect")
    session.attach(stream_client=failing)
    status = session.start_stream()
    assert status["ok"] is False and status["error"] == "RuntimeError"
    failing_close = B.FakeStreamClient(fail_on="close")
    session.attach(stream_client=failing_close)
    status = session.stop_stream()
    assert status["ok"] is False and status["error"] == "RuntimeError"


def test_idle_nonblocking_consumer_yields_and_stops_without_spin():
    class IdleClient:
        supports_receive_timeout = False

        def __init__(self):
            self.receive_calls = 0
            self.closed = False

        def connect(self):
            return None

        def receive_frame_safe(self):
            self.receive_calls += 1
            return None

        def close(self):
            self.closed = True

    client = IdleClient()
    session, _adapter, _aggregator = make_session(stream_client=client)
    assert session.start_stream()["ok"] is True
    deadline = time.monotonic() + 1.0
    while client.receive_calls == 0 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert client.receive_calls > 0
    baseline = client.receive_calls
    time.sleep(0.05)
    # _STREAM_POLL_TIMEOUT is 250ms; an immediate-return transport must not
    # run thousands of receive iterations while it is idle.
    assert client.receive_calls - baseline <= 1
    stopped = session.stop_stream()
    assert stopped["ok"] is True
    assert client.closed is True
    assert session._stream_thread is None


def test_mark_and_close_connection_gap_tracking():
    session, _adapter, aggregator = make_session()
    session.mark_connection_gap(start_at="2026-03-09T13:33:30Z")
    gaps = session.connection_gaps
    assert len(gaps) == 1 and gaps[0]["end_at"] is None
    assert len(session.gap_history) == 1
    session.mark_connection_gap(start_at="2026-03-09T13:33:45Z", end_at="2026-03-09T13:34:00Z")
    assert len(session.connection_gaps) == 2
    assert len(aggregator.gaps) == 2
    closed = session.close_connection_gap(end_at="2026-03-09T13:34:00Z")
    assert closed["end_at"] == "2026-03-09T13:34:00Z"
    assert len(session.connection_gaps) == 1
    assert len(session.gap_history) == 3
    session.mark_connection_gap(start_at="2026-03-09T13:34:01Z")
    resolved = session.close_connection_gap()
    assert resolved["end_at"] == NOW
    resolved_tail = session.close_connection_gap()
    assert resolved_tail["end_at"] == NOW
    assert session.close_connection_gap() is None
    with pytest.raises(FeedSessionError) as error:
        session.mark_connection_gap(start_at="2026-03-09T13:34:02Z", end_at="2026-03-09T13:34:01Z")
    assert error.value.code == "invalid_gap"


def test_reset_seconds_drops_retained_events():
    first, second = events()
    session, _adapter, _aggregator = make_session(batches=[[first, second]])
    session.ingest(now=NOW)
    assert session.reset_seconds() == 2
    assert session.frozen_events == () and session.latest_second is None


def test_status_fields():
    hist = standard_history()
    first, second = events()
    session, _adapter, _aggregator = make_session(history_result=hist, batches=[[first, second]])
    empty = session.status()
    assert empty["symbol"] == SYMBOL
    assert empty["feed"] == "alpaca:iex:single_exchange"
    assert empty["adjustment"] == "raw"
    assert empty["market_data_usage"] == "internal_prototype"
    assert empty["has_view"] is False and empty["view_id"] is None
    assert empty["latest_second_end_at"] is None and empty["frozen_event_count"] == 0
    assert empty["readiness"]["executable"] is False
    session.backfill(start_at=SESSION_OPEN, end_at=CUTOVER)
    session.ingest(now=NOW)
    session.build_view(now_at=NOW)
    status = session.status()
    assert status["frozen_event_count"] == 2
    assert status["latest_second_end_at"] == "2026-03-09T13:34:00Z"
    assert status["has_view"] is True and status["view_id"].startswith("mview_")
    assert status["readiness"]["executable"] is True
    assert status["readiness"]["recovery_required"] is False
    assert status["recovery_generation"] == 0


def test_fixture_index_is_closed():
    index = json.loads((FIXTURE_ROOT / "fixture-index.json").read_text())
    declared = {entry["path"] for entry in index["entries"]}
    actual = {path.name for path in FIXTURE_ROOT.glob("*") if path.name != "fixture-index.json" and not path.name.startswith("__")}
    assert declared == actual
