from __future__ import annotations

from pa_engine.web.presentation import MAX_CLOSED_BARS, build_presentation_snapshot

from .conftest import FixtureRuntime
from pa_engine.runtime.config import RuntimeConfig


def test_presentation_is_bounded_immutable_and_contains_paper_states():
    runtime = FixtureRuntime(RuntimeConfig(symbol="AAPL", timeframe="1min"))
    runtime.started = True
    snapshot = build_presentation_snapshot(runtime, lifecycle="running", secret_values=("G032_MODEL_CANARY_7f3c", "G032_ALPACA_SECRET_CANARY_4b8e"))
    value = snapshot.to_dict()
    assert value["paper_only"] is True
    assert value["iex_only"] is True
    assert value["market_data_usage"] == "internal_prototype"
    assert value["no_live_orders"] is True
    assert len(value["bars"]["closed"]) == 63
    assert len(value["bars"]["closed"]) <= MAX_CLOSED_BARS
    assert [bar["start_at"] for bar in value["bars"]["closed"]] == sorted(bar["start_at"] for bar in value["bars"]["closed"])
    assert value["bars"]["forming"]["is_closed"] is False
    assert value["market"]["latest_price"] == "109.90"
    states = {item["state"] for item in value["strategies"]}
    assert {"analysis_due", "analyzing", "sleeping", "waiting_entry", "open_position", "market_wait", "blocked"} <= states
    assert value["pending_entries"][0]["entry_price"] == "109.00"
    assert value["open_trades"][0]["quantity"] == "9.09090909"
    strategies = {item["strategy_id"]: item for item in value["strategies"]}
    assert strategies["pa:sleep"]["timers"][0]["kind"] == "analysis"
    assert strategies["pa:pending"]["latest_decision"]["stop_price"] == "108.00"
    assert strategies["pa:pending"]["latest_decision"]["target_price"] == "111.00"
    assert strategies["pa:open"]["open_trade"]["entry_price"] == "109.00"
    assert {item["exit_reason"] for item in value["closed_trades"]} == {"take_profit", "stop_loss"}
    assert {item["paper_pnl"] for item in value["closed_trades"]} == {"14.93", "-10.00"}
    assert all("envelope" not in item and "payload" not in item for item in value["events"])
    serialized = str(value)
    assert "G032_MODEL_CANARY" not in serialized
    assert "G032_ALPACA_SECRET" not in serialized
    value["bars"]["closed"].clear()
    assert len(snapshot.to_dict()["bars"]["closed"]) == 63


def test_projection_does_not_serialize_raw_provider_or_trade_payloads():
    runtime = FixtureRuntime(RuntimeConfig(symbol="AAPL", timeframe="1min"))
    snapshot = build_presentation_snapshot(runtime, lifecycle="blocked")
    value = snapshot.to_dict()
    forbidden = ("provider_raw_payload", "trade_json", "settlement_json", "state_json", "envelope")
    flattened = str(value)
    assert all(key not in flattened for key in forbidden)
