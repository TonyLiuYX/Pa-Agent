from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from pa_engine.contracts import canonical_json_bytes, finalize_artifact
from pa_engine.market import ALPACA_IEX_FEED
from pa_engine.market_sources.alpaca.stream import LocalSecondAggregator, parse_correction, parse_trade
from pa_engine.market_view import MarketViewError, SharedMarketView, SharedMarketViewBuilder, build_market_view, validate_shared_market_view
from pa_engine.market_view.models import view_hash_projection
from pa_engine.strategy.runtime.state import closed_snapshot_id, market_ready

from .test_market_view import _events, _history, _synthetic_history


FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "phase2" / "g021"


def _rehash_view(document: dict) -> dict:
    digest = hashlib.sha256(canonical_json_bytes(view_hash_projection(document))).hexdigest()
    document["view_hash"] = digest
    document["shared_market_view_hash"] = digest
    document["view_id"] = "mview_" + digest
    document["shared_market_view_id"] = "mview_" + digest
    return document


def _trade(aggregator: LocalSecondAggregator, trade_id: int, price: str, timestamp: str, received_at: str) -> None:
    aggregator.add_trade(
        parse_trade(
            {"T": "t", "S": "AAPL", "i": trade_id, "x": "V", "p": price, "s": 1, "c": ["@"], "t": timestamp, "z": "C"},
            received_at=received_at,
        )
    )


def test_ready_g021_view_is_directly_consumable_by_g023_scheduler_boundary():
    history = _history()
    _aggregator, events = _events()
    view = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z")

    assert view.executable
    assert market_ready(view)
    assert closed_snapshot_id(view) == view.closed_snapshot["market_snapshot_id"]


def test_two_same_second_frozen_trades_enter_g021_builder_without_normalization():
    history = _history()
    aggregator = LocalSecondAggregator("AAPL")
    _trade(aggregator, 701, "10", "2026-03-09T13:33:00.100Z", "2026-03-09T13:33:01Z")
    _trade(aggregator, 702, "12", "2026-03-09T13:33:00.200Z", "2026-03-09T13:33:01Z")

    events = aggregator.freeze(now="2026-03-09T13:34:00Z")
    assert len(events) == 1
    assert events[0].revision_sequence == 0 and events[0].parent_revision_id is None
    assert events[0].contributor_ids == ("701", "702")

    view = SharedMarketViewBuilder("AAPL", {"unit": "minute", "multiplier": 1}).build(
        history,
        events,
        cutover_at="2026-03-09T13:33:00Z",
        now_at="2026-03-09T13:34:00Z",
    )
    assert view.executable
    assert view.latest_second is not None and view.latest_second["close"] == "12"
    assert validate_shared_market_view(view).valid


def test_recovery_requires_reseed_evidence_not_a_truthy_override():
    history = _history()
    _aggregator, events = _events()
    gap = {"start_at": "2026-03-09T13:33:00Z", "end_at": "2026-03-09T13:33:20Z"}

    claimed = build_market_view(
        history,
        events,
        cutover_at="2026-03-09T13:33:00Z",
        now_at="2026-03-09T13:34:00Z",
        connection_gaps=[gap],
        recovered=True,
        recovery_generation=1,
    )
    assert not claimed.executable
    assert claimed.readiness.connection_gap
    assert claimed.readiness.recovery_required


def test_live_data_can_close_a_history_gap_at_or_after_the_cutover():
    history = copy.deepcopy(_history())
    history["analysis_as_of"] = "2026-03-09T13:34:00Z"
    history["generated_at"] = "2026-03-09T13:34:00Z"
    history["coverage"]["expected"]["end_at"] = "2026-03-09T13:34:00Z"
    history["coverage"]["status"] = "partial"
    history["coverage"]["missing_intervals"] = [{"start_at": "2026-03-09T13:33:00Z", "end_at": "2026-03-09T13:34:00Z"}]
    history["quality"] = {"status": "partial", "codes": ["partial_data"], "warnings": ["partial_data"], "details": None}
    history = finalize_artifact(history, "market_snapshot")

    _aggregator, events = _events()
    view = SharedMarketViewBuilder("AAPL", {"unit": "minute", "multiplier": 1}, stale_after_seconds=180).build(
        history,
        events,
        cutover_at="2026-03-09T13:33:00Z",
        now_at="2026-03-09T13:35:00Z",
    )
    assert not view.readiness.partial
    assert view.closed_snapshot["coverage"]["status"] == "complete"


def test_no_trade_candle_interval_is_not_promoted_to_a_confirmed_feed_gap():
    history = _history(end="2026-03-09T13:31:00Z")
    aggregator = LocalSecondAggregator("AAPL")
    _trade(aggregator, 1, "10", "2026-03-09T13:31:00.1Z", "2026-03-09T13:31:01Z")
    _trade(aggregator, 2, "11", "2026-03-09T13:33:00.1Z", "2026-03-09T13:33:01Z")
    events = aggregator.freeze(now="2026-03-09T13:34:10Z")

    view = SharedMarketViewBuilder("AAPL", {"unit": "minute", "multiplier": 1}, stale_after_seconds=120).build(
        history,
        events,
        cutover_at="2026-03-09T13:31:00Z",
        now_at="2026-03-09T13:34:10Z",
    )
    assert not view.readiness.connection_gap
    assert not view.readiness.partial


def test_late_g020_reconciliation_has_a_path_to_a_new_immutable_view():
    history = _history()
    aggregator, frozen = _events(prices=("10",))
    first = build_market_view(history, frozen, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z")
    correction = {
        "T": "c", "S": "AAPL", "x": "V", "oi": 1, "op": "10", "os": 1, "oc": ["@"],
        "ci": 2, "cp": "12", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:34:00Z",
    }
    update = aggregator.apply_correction(parse_correction(correction, received_at="2026-03-09T13:34:01Z"))
    assert update.event is not None and update.event.status == "reconciliation"

    revised = build_market_view(
        history,
        [*frozen, update.event],
        cutover_at="2026-03-09T13:33:00Z",
        now_at="2026-03-09T13:35:00Z",
    )
    assert revised.view_id != first.view_id
    assert first.current_price == "10"


def test_serialized_view_validator_rejects_context_raw_cutover_and_alias_tampering():
    history = _history()
    _aggregator, events = _events()
    view = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z")
    accepted: list[str] = []

    mutations = {
        "symbol": lambda value: value.__setitem__("symbol", "MSFT"),
        "adjustment_policy": lambda value: value.__setitem__("adjustment_policy", "split_adjusted"),
        "raw_adjustment_basis": lambda value: value.__setitem__("raw_adjustment_basis", "split"),
        "cutover_at": lambda value: value.__setitem__("cutover_at", "not-a-timestamp"),
        "latest_alias": lambda value: value["latest_frozen_second_ref"].__setitem__("close", "999"),
    }
    for label, mutate in mutations.items():
        candidate = view.to_dict()
        mutate(candidate)
        _rehash_view(candidate)
        if validate_shared_market_view(candidate).valid:
            accepted.append(label)
    assert not accepted, f"validator accepted forged fields: {accepted}"


def test_current_price_reference_is_not_self_attested_by_a_matching_ref_pair():
    history = _history()
    _aggregator, events = _events()
    candidate = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z").to_dict()
    previous_id = candidate["latest_second"]["event_id"]
    fake_id = "sec_" + "a" * 64
    fake_hash = "b" * 64
    candidate["latest_second"].update({"event_id": fake_id, "event_hash": fake_hash})
    candidate["latest_frozen_second_ref"] = copy.deepcopy(candidate["latest_second"])
    for ref in candidate["quality"]["source_revisions"]:
        if ref["event_id"] == previous_id:
            ref.update({"event_id": fake_id, "event_hash": fake_hash})
    _rehash_view(candidate)
    assert not validate_shared_market_view(candidate).valid


def test_one_hour_selected_timeframe_has_exact_live_ohlcv():
    history = _synthetic_history(
        "1Hour",
        "2026-03-09T13:30:00Z",
        "2026-03-09T14:30:00Z",
        [{"t": "2026-03-09T13:30:00Z", "o": "100", "h": "105", "l": "99", "c": "104", "v": 10}],
    )
    aggregator = LocalSecondAggregator("AAPL")
    _trade(aggregator, 1, "110", "2026-03-09T14:30:00.1Z", "2026-03-09T14:30:01Z")
    _trade(aggregator, 2, "115", "2026-03-09T15:29:59.1Z", "2026-03-09T15:30:00Z")
    events = aggregator.freeze(now="2026-03-09T15:31:00Z")

    view = build_market_view(history, events, cutover_at="2026-03-09T14:30:00Z", now_at="2026-03-09T15:31:00Z")
    assert view.closed_snapshot["bars"][-1] == {
        **view.closed_snapshot["bars"][-1],
        "start_at": "2026-03-09T14:30:00Z",
        "end_at": "2026-03-09T15:30:00Z",
        "open": "110",
        "high": "115",
        "low": "110",
        "close": "115",
        "volume": "2",
        "is_closed": True,
    }


def test_as_of_view_cannot_include_execution_seconds_from_the_future():
    history = _history()
    _aggregator, events = _events(prices=("10", "11"))

    view = build_market_view(
        history,
        events,
        cutover_at="2026-03-09T13:33:00Z",
        now_at="2026-03-09T13:33:05Z",
    )
    assert view.forming_candle is not None
    assert view.forming_candle["open"] == "10"
    assert view.forming_candle["close"] == "10"
    assert view.latest_second is not None and view.latest_second["close"] == "10"
    assert all(event["execution_watermark"] <= "2026-03-09T13:33:05Z" for event in view.quality["source_events"])


def test_closed_candle_crossing_confirmed_gap_is_not_closed_evidence():
    history = _history()
    _aggregator, events = _events()
    gap = {"start_at": "2026-03-09T13:33:00Z", "end_at": "2026-03-09T13:33:20Z"}

    view = build_market_view(
        history,
        events,
        cutover_at="2026-03-09T13:33:00Z",
        now_at="2026-03-09T13:34:03Z",
        connection_gaps=[gap],
    )
    assert not view.executable
    assert view.readiness.connection_gap and view.readiness.partial
    assert all(bar["end_at"] <= "2026-03-09T13:33:00Z" for bar in view.closed_snapshot["bars"])
    assert validate_shared_market_view(view).valid


def test_serialized_partial_quality_cannot_be_rehashed_as_executable():
    history = _history()
    _aggregator, events = _events()
    candidate = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z").to_dict()
    candidate["quality"]["status"] = "partial"
    candidate["quality"]["codes"] = ["partial"]
    _rehash_view(candidate)
    assert not validate_shared_market_view(candidate).valid


def test_latest_second_must_be_the_latest_eligible_frozen_source_event():
    history = _history()
    _aggregator, events = _events(prices=("10", "11"))
    candidate = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z").to_dict()
    older = min(candidate["quality"]["source_events"], key=lambda event: event["end_at"])
    candidate["latest_second"] = {
        "event_id": older["event_id"],
        "event_hash": older["event_hash"],
        "end_at": older["end_at"],
        "close": older["close"],
    }
    candidate["latest_frozen_second_ref"] = copy.deepcopy(candidate["latest_second"])
    _rehash_view(candidate)
    assert not validate_shared_market_view(candidate).valid


def test_embedded_source_events_must_match_symbol_cutover_and_as_of():
    history = _history()
    _aggregator, events = _events()
    candidate = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z").to_dict()

    foreign = LocalSecondAggregator("MSFT")
    foreign.add_trade(
        parse_trade(
            {"T": "t", "S": "MSFT", "i": 99, "x": "V", "p": "200", "s": 1, "c": ["@"], "t": "2026-03-09T13:33:20.1Z", "z": "C"},
            received_at="2026-03-09T13:33:21Z",
        )
    )
    foreign_event = foreign.freeze(now="2026-03-09T13:34:00Z")[0].to_dict()
    candidate["quality"]["source_events"].append(foreign_event)
    candidate["quality"]["source_revisions"].append(
        {"event_id": foreign_event["event_id"], "event_hash": foreign_event["event_hash"], "revision": foreign_event["revision_sequence"]}
    )
    _rehash_view(candidate)
    assert not validate_shared_market_view(candidate).valid


def test_day_timeframe_cutover_uses_g019_local_calendar_boundary():
    history = _synthetic_history(
        "1Day",
        "2026-03-09T04:00:00Z",
        "2026-03-10T04:00:00Z",
        [{"t": "2026-03-09T04:00:00Z", "o": "100", "h": "101", "l": "99", "c": "100", "v": 1}],
    )
    aggregator = LocalSecondAggregator("AAPL")
    _trade(aggregator, 1, "101", "2026-03-10T13:30:00.1Z", "2026-03-10T13:30:01Z")
    events = aggregator.freeze(now="2026-03-10T13:31:00Z")

    view = build_market_view(
        history,
        events,
        cutover_at="2026-03-10T04:00:00Z",
        now_at="2026-03-10T13:31:00Z",
    )
    assert view.forming_candle is not None
    assert view.forming_candle["start_at"] == "2026-03-10T04:00:00Z"
    assert view.forming_candle["end_at"] == "2026-03-11T04:00:00Z"
    assert validate_shared_market_view(view).valid


def test_session_fixture_records_the_actual_spring_dst_offset():
    fixture = json.loads((FIXTURE_ROOT / "session_edges.json").read_text())
    spring = next(edge for edge in fixture["edges"] if edge["name"] == "spring_dst")
    assert spring["open_at"] == "2026-03-09T13:30:00Z"
    assert spring["utc_offset"] == "-04:00"


def test_public_shared_view_constructor_cannot_self_attest_a_fake_current_price():
    history = _history()
    _aggregator, events = _events()
    source = build_market_view(
        history,
        events,
        cutover_at="2026-03-09T13:33:00Z",
        now_at="2026-03-09T13:34:00Z",
    )
    fake_id = "sec_" + "a" * 64
    fake_hash = "b" * 64
    quality = source.quality
    quality["source_revisions"] = [{"event_id": fake_id, "event_hash": fake_hash, "revision": 0}]
    quality["source_events"] = [{"event_id": fake_id, "event_hash": fake_hash}]

    with pytest.raises(MarketViewError):
        SharedMarketView(
            source.symbol,
            source.timeframe,
            source.session,
            ALPACA_IEX_FEED,
            "unadjusted",
            source.cutover_at,
            source.closed_snapshot,
            source.observation_frame,
            source.forming_candle,
            {"event_id": fake_id, "event_hash": fake_hash, "end_at": "2026-03-09T13:33:01Z", "close": "999"},
            quality,
            source.readiness,
            0,
        )
