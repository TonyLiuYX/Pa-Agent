from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from pa_engine.market_sources.alpaca.stream import (
    MalformedPayloadError,
    combine_rules,
    condition_rule,
    parse_message,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "phase2" / "g020"


def test_alpaca_frames_are_arrays_and_prices_start_as_decimal():
    values = json.loads((FIXTURES / "trade_messages.json").read_text())
    events = parse_message(json.dumps([values[0]]))
    assert events[0].price == Decimal("100.10")
    assert isinstance(events[0].price, Decimal)
    assert events[0].event_time == "2026-03-09T13:30:00.100000001Z"
    with pytest.raises(MalformedPayloadError):
        parse_message(json.dumps(values[0]))


def test_predecoded_binary_float_price_is_rejected():
    values = json.loads((FIXTURES / "trade_messages.json").read_text())
    with pytest.raises(MalformedPayloadError):
        parse_message([values[0]])


def test_complete_minute_condition_table_and_strictest_per_field():
    # Regular trade + odd lot + derivatively priced: only volume survives.
    strict = combine_rules(["@", "4", "I"], "C")
    assert strict.open_close == "red"
    assert strict.high_low is False
    assert strict.volume is True
    assert condition_rule("T", "O").updates_high_low
    assert not condition_rule("M", "A").updates_volume
    # Tape-specific rows cannot silently cross tapes.
    with pytest.raises(ValueError):
        condition_rule("A", "A")


def test_status_and_control_errors_are_typed_without_provider_text():
    statuses = json.loads((FIXTURES / "status_messages.json").read_text())
    events = parse_message(json.dumps(statuses))
    assert events[0].message_type == "success"
    assert events[1].message_type == "subscription"
    assert events[2].code == 401
    assert events[2].message == "provider_status"
