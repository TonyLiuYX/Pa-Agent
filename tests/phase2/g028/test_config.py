"""Focused G028 runtime configuration tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pa_engine.market.feed import ALPACA_IEX_FEED
from pa_engine.runtime.config import RuntimeConfig, RuntimeConfigError


def test_defaults_are_valid_and_frozen():
    config = RuntimeConfig(symbol="AAPL")
    assert config.reasoning_mode == "enabled"
    assert config.timeframe == {"unit": "minute", "multiplier": 1}
    assert config.database_path == "runtime.db"
    assert config.model == "deepseek-v4-flash"
    assert config.market_data_usage == "internal_prototype"
    assert config.session == {"kind": "regular", "timezone": "America/New_York"}
    assert config.adjustment_policy == "unadjusted"
    assert config.minimum_closed_bars == 3
    assert config.stale_after_seconds == 60
    assert config.queue_capacity == 200
    assert config.max_in_flight == 40
    assert config.mode == "automatic"
    assert config.no_live_orders is True
    assert config.market_feed == ALPACA_IEX_FEED
    assert config.feed_key == "alpaca:iex:single_exchange"


def test_from_kwargs_applies_defaults_and_validates():
    config = RuntimeConfig.from_kwargs(symbol="MSFT")
    assert config.symbol == "MSFT"
    assert config.reasoning_mode == "enabled"
    assert config.timeframe_text() == "1Min"
    assert config.validate() is config


def test_from_kwargs_accepts_path_like_database():
    config = RuntimeConfig.from_kwargs(symbol="AAPL", database_path=Path("tmp/runtime.db"))
    assert config.database_path == Path("tmp/runtime.db")
    assert config.to_dict()["database_path"] == "tmp/runtime.db"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"symbol": "aapl"},
        {"symbol": "1AAPL"},
        {"symbol": ""},
        {"symbol": "A!B"},
    ],
)
def test_invalid_symbols_are_rejected(kwargs):
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(**kwargs)
    assert error.value.code == "invalid_symbol"


@pytest.mark.parametrize(
    "timeframe",
    [
        "1sec",
        "10min",
        "45min",
        "3hour",
        "8hour",
        "month",
        "1Minute",
        "",
        {"unit": "minute", "multiplier": 60},
        {"unit": "minute", "multiplier": 0},
        {"unit": "hour", "multiplier": 24},
        {"unit": "day", "multiplier": 2},
        {"unit": "week", "multiplier": 2},
        {"unit": "minute"},
        {"unit": "month", "multiplier": 1},
        {"unit": "minute", "multiplier": 1.5},
        {"unit": "minute", "multiplier": True},
        "2min",
        "3hour",
    ],
)
def test_invalid_timeframes_are_rejected(timeframe):
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", timeframe=timeframe)
    assert error.value.code == "invalid_timeframe"


def test_timeframe_mapping_and_text_are_canonical():
    five = RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="5min")
    assert five.timeframe_mapping() == {"unit": "minute", "multiplier": 5}
    assert five.timeframe_text() == "5Min"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="5MIN").timeframe_mapping() == {"unit": "minute", "multiplier": 5}
    hour = RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="1hour")
    assert hour.timeframe_mapping() == {"unit": "hour", "multiplier": 1}
    assert hour.timeframe_text() == "1Hour"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="1HOUR").timeframe_text() == "1Hour"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="4hour").timeframe_text() == "4Hour"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="day").timeframe_text() == "1Day"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="week").timeframe_text() == "1Week"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="30min").timeframe_text() == "30Min"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="15min").timeframe_mapping() == {"unit": "minute", "multiplier": 15}
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe={"unit": "hour", "multiplier": 2}).timeframe_text() == "2Hour"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe={"unit": "minute", "multiplier": 59}).timeframe_text() == "59Min"
    assert RuntimeConfig.from_kwargs(symbol="AAPL", timeframe={"unit": "hour", "multiplier": 23}).timeframe_text() == "23Hour"


def test_reasoning_mode_must_be_enabled_or_disabled():
    assert RuntimeConfig.from_kwargs(symbol="AAPL", reasoning_mode="disabled").reasoning_mode == "disabled"
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", reasoning_mode="always")
    assert error.value.code == "invalid_reasoning_mode"


def test_model_is_fixed_to_deepseek_v4_flash():
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", model="another-provider-model")
    assert error.value.code == "invalid_model"


def test_session_override_is_rejected():
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", session={"kind": "extended", "timezone": "America/New_York"})
    assert error.value.code == "invalid_session"
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", session={"kind": "regular", "timezone": "UTC"})
    assert error.value.code == "invalid_session"


def test_adjustment_policy_must_be_unadjusted():
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", adjustment_policy="adjusted")
    assert error.value.code == "invalid_adjustment_policy"


def test_market_data_usage_must_be_internal_prototype():
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", market_data_usage="public")
    assert error.value.code == "invalid_market_data_usage"


def test_mode_must_be_automatic():
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", mode="replay")
    assert error.value.code == "invalid_mode"


def test_no_live_orders_cannot_be_disabled():
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", no_live_orders=False)
    assert error.value.code == "live_orders_forbidden"


@pytest.mark.parametrize(
    "field,value",
    [
        ("minimum_closed_bars", 0),
        ("minimum_closed_bars", -1),
        ("stale_after_seconds", 0),
        ("stale_after_seconds", -5),
        ("queue_capacity", 0),
        ("max_in_flight", 0),
        ("max_in_flight", -3),
        ("minimum_closed_bars", True),
    ],
)
def test_non_positive_parameters_are_rejected(field, value):
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig.from_kwargs(symbol="AAPL", **{field: value})
    assert error.value.code == "non_positive_parameter"


def test_paper_only_label_marks_the_boundary():
    label = RuntimeConfig.from_kwargs(symbol="AAPL").paper_only_label()
    assert "PAPER TRADING ONLY" in label
    assert "internal_prototype" in label
    assert "no live orders" in label
    assert "Alpaca Basic IEX" in label


def test_to_dict_round_trip_is_json_native_and_identical():
    original = RuntimeConfig.from_kwargs(symbol="AAPL", timeframe="5min", reasoning_mode="disabled")
    projection = original.to_dict()
    assert json.dumps(projection)  # JSON-native
    restored = RuntimeConfig.from_kwargs(**projection)
    assert restored == original
    assert restored.timeframe_text() == "5Min"
    assert restored.to_dict() == projection


def test_feed_identity_is_fixed():
    config = RuntimeConfig(symbol="AAPL")
    assert config.market_feed.to_dict() == {
        "schema_version": "0.1.0",
        "provider": "alpaca",
        "feed": "iex",
        "coverage": "single_exchange",
    }
