from __future__ import annotations

import copy
import json
from pathlib import Path

from pa_engine.market import ALPACA_IEX_FEED
from pa_engine.market_sources.alpaca.stream import (
    LocalSecondAggregator,
    floor_second,
    parse_cancel_error,
    parse_correction,
    parse_trade,
    validate_second_event,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "phase2" / "g020"


def _trade(i, price, size, tm, *, conditions=("@",), receipt="2026-03-09T13:30:01Z"):
    return parse_trade(
        {"T": "t", "S": "AAPL", "i": i, "x": "V", "p": price, "s": size, "c": list(conditions), "t": tm, "z": "C"},
        received_at=receipt,
    )


def test_condition_filter_extrema_tie_break_volume_only_audit():
    agg = LocalSecondAggregator("AAPL")
    agg.add_trade(_trade(102, "99.9", 5, "2026-03-09T13:30:00.100000001Z"))
    agg.add_trade(_trade(101, "100.1", 10, "2026-03-09T13:30:00.100000001Z"))
    # Odd-lot/derivatively priced survives volume, not OHLC.
    update = agg.add_trade(_trade(103, "100.5", 7, "2026-03-09T13:30:00.400000000Z", conditions=("@", "4", "I")))
    assert update.audit is not None
    event = agg.aggregate("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    assert event is not None
    assert (event.open, event.high, event.low, event.close, event.volume) == ("100.1", "100.1", "99.9", "99.9", "22")
    assert event.contributor_ids == ("101", "102", "103")


def test_duplicate_and_out_of_order_arrival_are_deterministic():
    first = _trade(10, "10", 1, "2026-03-09T13:30:00.2Z")
    second = _trade(11, "11", 1, "2026-03-09T13:30:00.1Z")
    a, b = LocalSecondAggregator("AAPL"), LocalSecondAggregator("AAPL")
    a.add_trade(first)
    a.add_trade(second)
    b.add_trade(second)
    b.add_trade(first)
    ea = a.aggregate("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    eb = b.aggregate("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    assert ea is not None and eb is not None
    assert ea.to_dict() == eb.to_dict()
    duplicate = a.add_trade(first)
    assert duplicate.duplicate and not duplicate.accepted


def test_multiple_pre_freeze_trades_share_initial_publication_revision():
    agg = LocalSecondAggregator("AAPL")
    first = _trade(201, "10", 2, "2026-03-09T13:30:00.100000001Z")
    second = _trade(202, "11", 3, "2026-03-09T13:30:00.200000001Z")

    first_update = agg.add_trade(first)
    second_update = agg.add_trade(second)
    candidate = agg.aggregate("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")

    assert first_update.event is not None and first_update.event.revision_sequence == 0
    assert second_update.event is not None and second_update.event.revision_sequence == 0
    assert candidate is not None and candidate.revision_sequence == 0 and candidate.parent_revision_id is None
    assert candidate.contributor_ids == ("201", "202")

    frozen = agg.freeze(now="2026-03-09T13:30:04Z")[0]
    assert frozen.revision_sequence == 0 and frozen.parent_revision_id is None
    assert frozen.contributor_ids == ("201", "202")
    assert validate_second_event(frozen.to_dict()).valid


def test_correction_and_cancel_use_original_bucket_not_delivery_time():
    agg = LocalSecondAggregator("AAPL")
    agg.add_trade(_trade(1, "1", 10, "2026-03-09T13:30:00.2Z", receipt="2026-03-09T13:30:01Z"))
    agg.add_trade(_trade(2, "2", 10, "2026-03-09T13:30:00.3Z", receipt="2026-03-09T13:30:01Z"))
    correction = parse_correction({"T":"c","S":"AAPL","x":"V","oi":1,"op":"1","os":10,"oc":["@"],"ci":3,"cp":"3","cs":5,"cc":["@"],"z":"C","t":"2026-03-09T13:31:00Z"}, received_at="2026-03-09T13:31:01Z")
    result = agg.apply_correction(correction)
    assert result.event is not None
    assert result.event.start_at == "2026-03-09T13:30:00Z"
    assert agg.aggregate("2026-03-09T13:31:00Z", "2026-03-09T13:31:01Z") is None
    cancel = parse_cancel_error({"T":"x","S":"AAPL","i":2,"x":"V","p":"2","s":10,"a":"C","t":"2026-03-09T13:31:02Z","z":"C"}, received_at="2026-03-09T13:31:03Z")
    result = agg.apply_cancel(cancel)
    assert result.event is not None
    assert result.event.start_at == "2026-03-09T13:30:00Z"


def test_freeze_watermark_and_post_freeze_reconciliation_keep_issued_event_immutable():
    agg = LocalSecondAggregator("AAPL")
    agg.add_trade(_trade(1, "1", 1, "2026-03-09T13:30:00.1Z", receipt="2026-03-09T13:30:00.2Z"))
    issued = agg.freeze(now="2026-03-09T13:30:04Z")[0]
    assert issued.status == "frozen"
    assert issued.execution_watermark == "2026-03-09T13:30:04Z"
    correction = parse_correction({"T":"c","S":"AAPL","x":"V","oi":1,"op":"1","os":1,"oc":["@"],"ci":2,"cp":"2","cs":1,"cc":["@"],"z":"C","t":"2026-03-09T13:31:00Z"}, received_at="2026-03-09T13:31:01Z")
    result = agg.apply_correction(correction)
    assert result.reconciliation
    assert agg.frozen_events[(issued.start_at, issued.end_at)].to_dict() == issued.to_dict()
    reconciliation = agg.reconciliation_events[(issued.start_at, issued.end_at)]
    assert reconciliation.status == "reconciliation"
    assert reconciliation.revision_id != issued.revision_id


def test_second_event_identity_feed_and_decimal_validation():
    agg = LocalSecondAggregator("AAPL")
    agg.add_trade(_trade(1, "1.0000", 1, "2026-03-09T13:30:00.1Z"))
    event = agg.aggregate("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    assert event is not None
    assert validate_second_event(event.to_dict())
    assert event.market_feed == ALPACA_IEX_FEED
    tampered = copy.deepcopy(event.to_dict())
    tampered["open"] = "1.0"
    assert not validate_second_event(tampered)
    tampered = copy.deepcopy(event.to_dict())
    tampered["market_feed"]["feed"] = "sip"
    assert not validate_second_event(tampered)
    assert floor_second("2026-03-09T13:30:00.999999999Z") == ("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")


def test_recorded_second_event_fixture_is_hash_bound():
    fixture = json.loads((FIXTURES / "second_event.json").read_text())
    assert validate_second_event(fixture)


def test_multi_trade_frozen_fixture_preserves_initial_lineage():
    fixture = json.loads((FIXTURES / "multi_trade_frozen_second.json").read_text())
    assert validate_second_event(fixture)
    assert fixture["contributors"]["ids"] == ["201", "202"]
    assert fixture["revision_sequence"] == 0 and fixture["parent_revision_id"] is None
