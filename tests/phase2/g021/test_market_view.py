from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from pa_engine.market_sources.alpaca.history import AlpacaHistoryAdapter, RecordedTransport, TransportResponse
from pa_engine.market_sources.alpaca.stream import LocalSecondAggregator, parse_correction, parse_trade
from pa_engine.market_view import (
    InvalidJoinError,
    SharedMarketViewBuilder,
    SharedMarketViewPublisher,
    build_market_view,
    validate_shared_market_view,
)


ROOT = Path(__file__).parents[2]


def _history(*, timeframe="1Min", end="2026-03-09T13:33:00Z"):
    payload = json.loads((ROOT / "fixtures" / "phase2" / "g019" / "iex_history_success.json").read_text())
    result = AlpacaHistoryAdapter(key_id="runtime", secret_key="runtime", transport=RecordedTransport([TransportResponse(200, payload)])).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at=end, timeframe=timeframe
    )
    assert result.success and result.snapshot is not None
    return result.snapshot


def _events(*, prices=("10", "11"), start="2026-03-09T13:33:00.100Z"):
    aggregator = LocalSecondAggregator("AAPL")
    base = __import__("pa_engine.market_sources.alpaca.stream.parser", fromlist=["timestamp_ns"]).timestamp_ns(start)
    for index, price in enumerate(prices, 1):
        second = __import__("datetime").datetime.fromtimestamp(base / 1_000_000_000 + (index - 1) * 10, tz=__import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + ".100Z"
        aggregator.add_trade(parse_trade({"T": "t", "S": "AAPL", "i": index, "x": "V", "p": price, "s": 1, "c": ["@"], "t": second, "z": "C"}, received_at="2026-03-09T13:33:01Z"))
    return aggregator, aggregator.freeze(now="2026-03-09T13:34:00Z")


def _synthetic_history(timeframe: str, start: str, end: str, bars: list[dict]):
    payload = {"bars": {"AAPL": bars}}
    result = AlpacaHistoryAdapter(key_id="runtime", secret_key="runtime", transport=RecordedTransport([TransportResponse(200, payload)])).fetch_history(
        symbol="AAPL", start_at=start, end_at=end, timeframe=timeframe
    )
    assert result.success and result.snapshot is not None
    return result.snapshot


def test_exact_boundary_and_current_price_lineage_are_half_open():
    history = _history()
    _aggregator, events = _events()
    view = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z")
    assert [bar["end_at"] for bar in view.closed_snapshot["bars"]] == ["2026-03-09T13:31:00Z", "2026-03-09T13:32:00Z", "2026-03-09T13:33:00Z"]
    assert view.forming_candle is not None and view.forming_candle["is_closed"] is False
    assert view.latest_second is not None
    assert view.latest_second["event_id"].startswith("sec_")
    assert view.latest_second["close"] == "11"
    assert validate_shared_market_view(view).valid


def test_feed_and_raw_mismatches_fail_closed():
    history = copy.deepcopy(_history())
    history["extensions"]["market_feed"]["feed"] = "sip"
    with pytest.raises(InvalidJoinError):
        build_market_view(history, (), cutover_at="2026-03-09T13:33:00Z")
    history = _history()
    _aggregator, events = _events()
    forged = copy.deepcopy(events[0].to_dict())
    forged["extensions"]["market_feed"]["feed"] = "sip"
    with pytest.raises(InvalidJoinError):
        build_market_view(history, [forged], cutover_at="2026-03-09T13:33:00Z")


def test_late_revision_is_a_new_view_and_old_view_is_unchanged():
    history = _history()
    aggregator, events = _events(prices=("10",))
    # Keep the scheduling instant fixed so the identity change below is from
    # the explicit G020 correction lineage, not a forming->closed transition.
    first = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:35:00Z")
    old_hash = first.view_hash
    correction = {"T": "c", "S": "AAPL", "x": "V", "oi": 1, "op": "10", "os": 1, "oc": ["@"], "ci": 2, "cp": "12", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:34:00Z"}
    update = aggregator.apply_correction(parse_correction(correction, received_at="2026-03-09T13:34:01Z"))
    aggregator.freeze(now="2026-03-09T13:35:00Z")
    revised = build_market_view(history, [*aggregator.frozen_events.values(), update.event], cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:35:00Z")
    assert first.view_hash == old_hash and revised.view_hash != old_hash
    assert first.current_price == "10"


def test_publisher_builds_once_for_batch():
    history = _history()
    _aggregator, events = _events()
    builder = SharedMarketViewBuilder("AAPL", {"unit": "minute", "multiplier": 1})
    publisher = SharedMarketViewPublisher(builder)
    received = []
    publisher.publish_from(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z", subscribers=[received.append, received.append])
    assert publisher.source_subscription_count == 1
    assert len(received) == 2 and received[0].view_id == received[1].view_id and received[0].view_hash == received[1].view_hash


def test_connection_gap_is_non_executable_until_reseed():
    history = _history()
    _aggregator, events = _events()
    gap = {"start_at": "2026-03-09T13:33:00Z", "end_at": "2026-03-09T13:33:20Z"}
    gapped = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z", connection_gaps=[gap])
    assert not gapped.executable and gapped.readiness.connection_gap
    # A truthy recovery flag cannot clear the same unresolved source.  A new
    # reseeded build has no unresolved gap and therefore becomes eligible.
    claimed = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z", connection_gaps=[gap], recovered=True, recovery_generation=1)
    assert claimed.readiness.recovery_generation == 1 and claimed.readiness.connection_gap is True and not claimed.executable
    recovered = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z", recovered=True, recovery_generation=2)
    assert recovered.readiness.recovery_generation == 2 and recovered.readiness.connection_gap is False


def test_five_minute_and_hour_candles_are_exact_and_use_local_session_alignment():
    five = _synthetic_history(
        "1Min", "2026-03-09T13:30:00Z", "2026-03-09T13:35:00Z",
        [{"t": f"2026-03-09T13:{30 + index:02d}:00Z", "o": str(100 + index), "h": str(101 + index), "l": str(99 + index), "c": str(100 + index), "v": 1} for index in range(5)],
    )
    five["timeframe"] = {"unit": "minute", "multiplier": 5}
    base = five["bars"]
    five["bars"] = [{"bar_id": __import__("pa_engine.contracts", fromlist=["bar_id_for"]).bar_id_for(five["instrument"], five["timeframe"], five["session"], base[0]["start_at"], base[-1]["end_at"]), "start_at": base[0]["start_at"], "end_at": base[-1]["end_at"], "open": base[0]["open"], "high": "105", "low": "99", "close": base[-1]["close"], "volume": "5", "is_closed": True}]
    five["coverage"]["expected"] = five["coverage"]["actual"] = {"start_at": base[0]["start_at"], "end_at": base[-1]["end_at"]}
    five["analysis_as_of"] = five["bars"][0]["end_at"]
    five = __import__("pa_engine.contracts", fromlist=["finalize_artifact"]).finalize_artifact(five, "market_snapshot")
    aggregator = LocalSecondAggregator("AAPL")
    for index, (price, tm) in enumerate((("110", "2026-03-09T13:35:00.1Z"), ("111", "2026-03-09T13:39:59.1Z")), 1):
        aggregator.add_trade(parse_trade({"T": "t", "S": "AAPL", "i": index, "x": "V", "p": price, "s": 1, "c": ["@"], "t": tm, "z": "C"}, received_at="2026-03-09T13:40:01Z"))
    aggregator.freeze(now="2026-03-09T13:41:00Z")
    view = build_market_view(five, aggregator.frozen_events.values(), cutover_at="2026-03-09T13:35:00Z", now_at="2026-03-09T13:41:00Z")
    assert view.closed_snapshot["bars"][-1]["start_at"] == "2026-03-09T13:35:00Z"
    assert view.closed_snapshot["bars"][-1]["end_at"] == "2026-03-09T13:40:00Z"
    assert view.closed_snapshot["bars"][-1]["open"] == "110" and view.closed_snapshot["bars"][-1]["close"] == "111" and view.closed_snapshot["bars"][-1]["volume"] == "2"


def test_provisional_rehashed_straddle_and_forming_leak_are_rejected():
    history = _history()
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(parse_trade({"T": "t", "S": "AAPL", "i": 1, "x": "V", "p": "10", "s": 1, "c": ["@"], "t": "2026-03-09T13:33:00.1Z", "z": "C"}, received_at="2026-03-09T13:33:01Z"))
    provisional = aggregator.aggregate("2026-03-09T13:33:00Z", "2026-03-09T13:33:01Z")
    assert provisional is not None
    with pytest.raises(InvalidJoinError):
        build_market_view(history, [provisional], cutover_at="2026-03-09T13:33:00Z")
    aggregator.freeze(now="2026-03-09T13:34:00Z")
    frozen = list(aggregator.frozen_events.values())[0]
    forged = copy.deepcopy(frozen.to_dict())
    forged["event_hash"] = "f" * 64
    with pytest.raises(InvalidJoinError):
        build_market_view(history, [forged], cutover_at="2026-03-09T13:33:00Z")
    with pytest.raises(InvalidJoinError) as error:
        build_market_view(history, [frozen], cutover_at="2026-03-09T13:33:00.5Z")
    assert error.value.code == "cutover_straddle"


def test_public_values_are_deep_copy_isolated_and_g018_pair_ref_is_separate():
    history = _history()
    _aggregator, events = _events()
    view = build_market_view(history, events, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:00Z")
    payload = view.to_dict()
    payload["closed_snapshot"]["bars"][0]["close"] = "999"
    payload["latest_second"]["close"] = "999"
    assert view.closed_snapshot["bars"][0]["close"] != "999" and view.current_price != "999"
    assert view.view_id.startswith("mview_") and view.shared_market_view_ref["shared_market_view_id"].startswith("smv_")


def test_dst_and_regular_session_edges_are_deterministic():
    # After the US fall-back transition, 09:30 New York is 14:30 UTC (not
    # 13:30 UTC as it is during daylight time).
    history = _synthetic_history(
        "1Min", "2026-11-02T14:30:00Z", "2026-11-02T14:32:00Z",
        [
            {"t": "2026-11-02T14:30:00Z", "o": "100", "h": "101", "l": "99", "c": "100.5", "v": 1},
            {"t": "2026-11-02T14:31:00Z", "o": "100.5", "h": "102", "l": "100", "c": "101", "v": 1},
        ],
    )
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(parse_trade({"T": "t", "S": "AAPL", "i": 1, "x": "V", "p": "101", "s": 1, "c": ["@"], "t": "2026-11-02T14:32:00.1Z", "z": "C"}, received_at="2026-11-02T14:32:01Z"))
    aggregator.freeze(now="2026-11-02T14:33:00Z")
    view = build_market_view(history, aggregator.frozen_events.values(), cutover_at="2026-11-02T14:32:00Z", now_at="2026-11-02T14:33:00Z")
    assert view.closed_snapshot["session"] == {"kind": "regular", "timezone": "America/New_York"}
    assert view.readiness.session_ready
    outside = LocalSecondAggregator("AAPL")
    outside.add_trade(parse_trade({"T": "t", "S": "AAPL", "i": 2, "x": "V", "p": "101", "s": 1, "c": ["@"], "t": "2026-11-02T21:00:00.1Z", "z": "C"}, received_at="2026-11-02T21:00:01Z"))
    outside.freeze(now="2026-11-02T21:01:00Z")
    with pytest.raises(InvalidJoinError):
        build_market_view(history, outside.frozen_events.values(), cutover_at="2026-11-02T14:32:00Z", now_at="2026-11-02T14:33:00Z")


def test_deterministic_fixture_index_covers_candles_sessions_gaps_and_revisions():
    fixture_root = ROOT / "fixtures" / "phase2" / "g021"
    index = json.loads((fixture_root / "fixture-index.json").read_text())
    declared = {entry["path"] for entry in index["entries"]}
    assert {"candle_vectors.json", "session_edges.json", "gap_revision_cases.json"}.issubset(declared)
    for path in declared:
        assert (fixture_root / path).exists()

    vectors = json.loads((fixture_root / "candle_vectors.json").read_text())
    assert {vector["timeframe"]["unit"] for vector in vectors["vectors"]} == {"minute", "hour", "day", "week"}
    for vector in vectors["vectors"]:
        aggregator = LocalSecondAggregator(vectors["symbol"])
        for trade in vector["trades"]:
            received_at = trade.get("t", vector.get("freeze_at", "2026-03-09T15:59:00Z"))
            if "." in received_at:
                received_at = received_at.split(".", 1)[0] + "Z"
            aggregator.add_trade(parse_trade(trade, received_at=received_at))
        freeze_at = vector.get("freeze_at", "2026-03-09T16:00:00Z")
        events = aggregator.freeze(now=freeze_at)
        assert len(events) == 2
        expected = vector["expected"]
        assert events[0].open == expected["open"] and events[-1].close == expected["close"]
        assert max(event.high for event in events) == expected["high"]
        assert min(event.low for event in events) == expected["low"]
        assert sum(int(event.volume) for event in events) == int(expected["volume"])
        unit = vector["timeframe"]["unit"]
        multiplier = vector["timeframe"]["multiplier"]
        timeframe_name = f"{multiplier}{'Min' if unit == 'minute' else 'Hour' if unit == 'hour' else 'Day' if unit == 'day' else 'Week'}"
        if vector["name"] == "one_minute_cutover":
            history = _synthetic_history("1Min", "2026-03-09T13:32:00Z", vector["cutover_at"], [{"t": "2026-03-09T13:32:00Z", "o": "99", "h": "99", "l": "99", "c": "99", "v": 1}])
        elif vector["name"] == "five_minute_session_bucket":
            history = _synthetic_history("5Min", "2026-03-09T13:30:00Z", vector["cutover_at"], [{"t": "2026-03-09T13:30:00Z", "o": "99", "h": "99", "l": "99", "c": "99", "v": 1}])
        elif vector["name"] == "one_hour_session_bucket":
            history = _synthetic_history("1Hour", "2026-03-09T13:30:00Z", vector["cutover_at"], [{"t": "2026-03-09T13:30:00Z", "o": "99", "h": "99", "l": "99", "c": "99", "v": 1}])
        else:
            history = _synthetic_history(timeframe_name, vector["history_start_at"], vector["history_end_at"], [{"t": vector["history_bar_at"], "o": "99", "h": "99", "l": "99", "c": "99", "v": 1}])
        view = build_market_view(history, events, cutover_at=vector["cutover_at"], now_at=freeze_at)
        assert view.closed_snapshot["bars"][-1]["start_at"] == expected["start_at"]
        assert view.closed_snapshot["bars"][-1]["end_at"] == expected["end_at"]
        for field in ("open", "high", "low", "close", "volume"):
            assert view.closed_snapshot["bars"][-1][field] == expected[field]

    edges = json.loads((fixture_root / "session_edges.json").read_text())
    assert edges["regular_session"] == {"local_open": "09:30:00", "local_close": "16:00:00"}
    assert any(item["name"] == "half_open_cutover" for item in edges["edges"])
    ny = ZoneInfo(edges["timezone"])
    for edge in edges["edges"]:
        if edge["name"] not in {"spring_dst", "fall_dst"}:
            continue
        local_date = datetime.fromisoformat(edge["date"]).date()
        local_open = datetime.combine(local_date, datetime.strptime(edges["regular_session"]["local_open"], "%H:%M:%S").time(), tzinfo=ny)
        local_close = datetime.combine(local_date, datetime.strptime(edges["regular_session"]["local_close"], "%H:%M:%S").time(), tzinfo=ny)
        assert local_open.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") == edge["open_at"]
        assert local_close.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") == edge["close_at"]
        offset = local_open.strftime("%z")
        assert edge["utc_offset"] == offset[:3] + ":" + offset[3:]
    cases = json.loads((fixture_root / "gap_revision_cases.json").read_text())["cases"]
    assert {item["evidence"] for item in cases if "evidence" in item} == {"connection_gap", "none"}
    gap_case = next(item for item in cases if item["name"] == "confirmed_connection_gap")
    gapped = build_market_view(_history(), _events()[1], cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:34:03Z", connection_gaps=[gap_case])
    assert not gapped.executable and gapped.readiness.connection_gap
    sparse_case = next(item for item in cases if item["name"] == "no_trade_sparse")
    sparse_history = _history(end=sparse_case["start_at"])
    sparse_aggregator = LocalSecondAggregator("AAPL")
    sparse_aggregator.add_trade(parse_trade({"T": "t", "S": "AAPL", "i": 901, "x": "V", "p": "10", "s": 1, "c": ["@"], "t": f"{sparse_case['end_at'].replace('Z', '.100Z')}", "z": "C"}, received_at=sparse_case["end_at"]))
    sparse_events = sparse_aggregator.freeze(now="2026-03-09T13:34:10Z")
    sparse_view = build_market_view(sparse_history, sparse_events, cutover_at=sparse_case["start_at"], now_at="2026-03-09T13:34:10Z")
    assert sparse_view.readiness.non_gapped and not sparse_view.readiness.partial
    revision_case = next(item for item in cases if item["name"] == "late_reconciliation")
    assert revision_case["old_view_immutable"] is True and revision_case["status"] == "reconciliation" and revision_case["revision_sequence"] == 1
    revision_history = _history()
    revision_aggregator, revision_frozen = _events(prices=("10",))
    old_view = build_market_view(revision_history, revision_frozen, cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:35:00Z")
    correction = {"T": "c", "S": "AAPL", "x": "V", "oi": 1, "op": "10", "os": 1, "oc": ["@"], "ci": 2, "cp": "12", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:34:00Z"}
    correction_result = revision_aggregator.apply_correction(parse_correction(correction, received_at="2026-03-09T13:34:01Z"))
    assert correction_result.event is not None and correction_result.event.status == revision_case["status"] and correction_result.event.revision_sequence == revision_case["revision_sequence"]
    revised_view = build_market_view(revision_history, [*revision_frozen, correction_result.event], cutover_at="2026-03-09T13:33:00Z", now_at="2026-03-09T13:35:00Z")
    assert old_view.view_id != revised_view.view_id and old_view.current_price == "10"
