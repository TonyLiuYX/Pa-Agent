"""Offline G019 adapter vectors."""

from __future__ import annotations

import json
import os
from pathlib import Path
from collections.abc import Mapping

import pytest

from pa_engine.market import ALPACA_IEX_FEED, validate_market_snapshot
from pa_engine.market_sources.alpaca.history import (
    AdapterError,
    AdapterOutcome,
    AlpacaHistoryAdapter,
    HistoryRequest,
    OutcomeCode,
    RecoveryBar,
    RecordedTransport,
    TransportResponse,
    recovery_bar_content_hash,
    validate_recovery_bar,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "phase2" / "g019"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_fixture_index_is_closed_and_recorded_payloads_are_json():
    index = fixture("fixture-index.json")
    declared = {item["path"] for item in index["entries"]}
    actual = {path.name for path in FIXTURES.glob("*.json") if path.name != "fixture-index.json"}
    assert declared == actual


def adapter(responses, *, key="demo-key", secret="demo-secret"):
    return AlpacaHistoryAdapter(key_id=key, secret_key=secret, transport=RecordedTransport(responses))


def test_success_builds_valid_unadjusted_iex_snapshot_and_half_open_query():
    result = adapter([TransportResponse(200, fixture("iex_history_success.json"))]).fetch_history(
        symbol="AAPL",
        start_at="2026-03-09T13:30:00Z",
        end_at="2026-03-09T13:33:00Z",
        timeframe="1Min",
    )
    assert result.success
    assert len(result.bars) == 3
    assert result.snapshot is not None
    assert result.snapshot["adjustment_policy"] == "unadjusted"
    assert result.snapshot["extensions"]["market_feed"] == ALPACA_IEX_FEED.to_dict()
    assert validate_market_snapshot(result.snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted").valid


def test_query_is_explicit_and_auth_headers_are_runtime_only():
    transport = RecordedTransport([TransportResponse(200, fixture("iex_history_success.json"))])
    result = AlpacaHistoryAdapter(key_id="secret-id", secret_key="secret-value", transport=transport).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe={"unit": "minute", "multiplier": 1}
    )
    assert result.success
    request = transport.requests[0]
    assert request.params == {
        "feed": "iex",
        "adjustment": "raw",
        "sort": "asc",
        "timeframe": "1Min",
        "start": "2026-03-09T13:30:00Z",
        "end": "2026-03-09T13:31:00Z",
        "limit": "1000",
    }
    assert request.headers["APCA-API-KEY-ID"] == "secret-id"
    assert request.headers["APCA-API-SECRET-KEY"] == "secret-value"
    encoded = json.dumps(result.to_dict(), sort_keys=True)
    assert "secret-id" not in encoded and "secret-value" not in encoded
    assert "secret-id" not in json.dumps(request.to_dict(), sort_keys=True)


def test_transport_request_log_surfaces_redact_page_token_and_auth_values():
    transport = RecordedTransport([TransportResponse(200, fixture("iex_history_page_1.json")), TransportResponse(200, fixture("iex_history_page_2.json"))])
    AlpacaHistoryAdapter(key_id="runtime-id", secret_key="runtime-secret", transport=transport).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:32:00Z", timeframe="1Min"
    )
    request = transport.requests[-1]
    assert "page-2" not in repr(request)
    assert "page-2" not in json.dumps(request.to_dict(), sort_keys=True)
    assert "runtime-id" not in repr(request)
    assert "runtime-secret" not in repr(request)


def test_end_boundary_is_exclusive_even_when_vendor_end_is_inclusive():
    payload = {"bars": {"AAPL": [
        {"t": "2026-03-09T13:30:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 1},
        {"t": "2026-03-09T13:31:00Z", "o": 2, "h": 3, "l": 2, "c": 3, "v": 1},
    ]}}
    result = adapter([TransportResponse(200, payload)]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.success and [bar["start_at"] for bar in result.bars] == ["2026-03-09T13:30:00Z"]


def test_pagination_and_deterministic_request_hash():
    first = fixture("iex_history_page_1.json")
    second = fixture("iex_history_page_2.json")
    a = adapter([TransportResponse(200, first), TransportResponse(200, second)])
    b = adapter([TransportResponse(200, first), TransportResponse(200, second)])
    kwargs = {"symbol": "AAPL", "start_at": "2026-03-09T13:30:00Z", "end_at": "2026-03-09T13:32:00Z", "timeframe": "1Min"}
    ra, rb = a.fetch_history(**kwargs), b.fetch_history(**kwargs)
    assert ra.success and rb.success and ra.snapshot == rb.snapshot
    assert len(a.transport.requests) == 2
    assert a.transport.requests[1].params["page_token"] == "page-2"
    assert "page_token" not in ra.request.to_dict()
    assert "page-2" not in repr(a.transport.requests[1])
    assert "page-2" not in json.dumps(a.transport.requests[1].to_dict(), sort_keys=True)


def test_history_request_timeframe_is_immutable_and_forged_object_is_typed_invalid():
    adapter_instance = AlpacaHistoryAdapter(key_id="k", secret_key="s", transport=RecordedTransport())
    request = adapter_instance.build_request("AAPL", "2026-03-09T13:30:00Z", "2026-03-09T13:31:00Z", "1Min")
    with pytest.raises(TypeError):
        request.timeframe["unit"] = "second"  # type: ignore[index]

    forged = object.__new__(HistoryRequest)
    object.__setattr__(forged, "symbol", "AAPL")
    object.__setattr__(forged, "start_at", "2026-03-09T13:30:00Z")
    object.__setattr__(forged, "end_at", "2026-03-09T13:31:00Z")
    object.__setattr__(forged, "timeframe", {"unit": "second", "multiplier": 1})
    object.__setattr__(forged, "limit", 1000)
    object.__setattr__(forged, "feed", "iex")
    object.__setattr__(forged, "adjustment", "raw")
    object.__setattr__(forged, "sort", "asc")
    object.__setattr__(forged, "recovery", False)
    result = adapter_instance.fetch_history(forged)
    assert result.outcome.code == OutcomeCode.INVALID_REQUEST.value
    with pytest.raises(AdapterError) as request_error:
        adapter_instance.request_spec(forged)
    assert request_error.value.outcome.code == OutcomeCode.INVALID_REQUEST.value
    with pytest.raises(AdapterError) as raise_error:
        adapter_instance.fetch_or_raise(forged)
    assert raise_error.value.outcome.code == OutcomeCode.INVALID_REQUEST.value


@pytest.mark.parametrize(
    "timeframe",
    ["60Min", "24Hour", "2Day", "2Week", {"unit": "minute", "multiplier": 60}, {"unit": "hour", "multiplier": 24}, {"unit": "day", "multiplier": 2}, {"unit": "week", "multiplier": 2}],
)
def test_official_timeframe_bounds_fail_closed_at_all_request_boundaries(timeframe):
    adapter_instance = AlpacaHistoryAdapter(key_id="k", secret_key="s", transport=RecordedTransport())
    with pytest.raises(ValueError):
        adapter_instance.build_request("AAPL", "2026-03-09T13:30:00Z", "2026-03-09T13:31:00Z", timeframe)
    result = adapter_instance.fetch_history(
        request={"symbol": "AAPL", "start_at": "2026-03-09T13:30:00Z", "end_at": "2026-03-09T13:31:00Z", "timeframe": timeframe}
    )
    assert result.outcome.code == OutcomeCode.INVALID_REQUEST.value


def test_empty_window_is_typed_and_does_not_construct_empty_g014_snapshot():
    result = adapter([TransportResponse(200, {"bars": {"AAPL": []}})]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == OutcomeCode.EMPTY_WINDOW.value
    assert result.snapshot is None


@pytest.mark.parametrize(
    "status,code,retryable,auth_terminal",
    [(401, "authentication_error", False, True), (403, "authentication_error", False, True), (429, "rate_limit", True, False), (503, "server_error", True, False)],
)
def test_http_failures_are_stable_typed_outcomes(status, code, retryable, auth_terminal):
    result = adapter([TransportResponse(status, {"secret": "secret-value"})]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == code
    assert result.outcome.retryable is retryable
    assert result.outcome.authentication_terminal is auth_terminal
    assert "secret-value" not in result.outcome.message


@pytest.mark.parametrize("status", ["401", None, True, False, 0, 700])
def test_malformed_transport_status_is_typed_without_traceback_or_secrets(status):
    result = adapter([TransportResponse(status, {"secret": "SECRET-IN-STATUS"})]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == OutcomeCode.MALFORMED_RESPONSE.value
    assert not result.outcome.retryable
    encoded = json.dumps(result.to_dict(), sort_keys=True)
    assert "SECRET-IN-STATUS" not in encoded


class _ExplodingMapping(Mapping):
    def __init__(self, secret="SECRET-IN-MAPPING"):
        self.secret = secret

    def __getitem__(self, key):
        raise RuntimeError(self.secret)

    def __iter__(self):
        raise RuntimeError(self.secret)

    def __len__(self):
        raise RuntimeError(self.secret)


def test_custom_mapping_response_body_is_non_throwing_and_secret_safe():
    result = adapter([TransportResponse(200, _ExplodingMapping())]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == OutcomeCode.MALFORMED_RESPONSE.value
    assert "SECRET-IN-MAPPING" not in result.outcome.message
    assert "SECRET-IN-MAPPING" not in json.dumps(result.to_dict(), sort_keys=True)


def test_custom_mapping_bar_item_is_non_throwing_and_secret_safe():
    payload = {"bars": {"AAPL": [_ExplodingMapping()]}}
    result = adapter([TransportResponse(200, payload)]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == OutcomeCode.MALFORMED_RESPONSE.value
    assert "SECRET-IN-MAPPING" not in result.outcome.message
    assert "SECRET-IN-MAPPING" not in json.dumps(result.to_dict(), sort_keys=True)


def test_adapter_outcome_terminal_tri_state_semantics():
    success = AdapterOutcome("success", "success", False, False, "ok")
    retryable_failure = AdapterOutcome("rate_limit", "rate_limit", True, False, "retry")
    terminal_failure = AdapterOutcome("malformed_response", "malformed_response", False, False, "bad")
    assert not success.terminal
    assert not retryable_failure.terminal
    assert terminal_failure.terminal


def test_transport_and_malformed_body_fail_closed():
    timeout = adapter([TimeoutError("secret-value in URL")]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert timeout.outcome.code == "timeout"
    malformed = adapter([TransportResponse(200, "not-json")]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert malformed.outcome.code == "malformed_response"
    malformed_numeric = adapter([TransportResponse(200, {"bars": {"AAPL": [{"t": "2026-03-09T13:30:00Z", "o": "NaN", "h": "2", "l": "1", "c": "2", "v": "1"}]}})]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert malformed_numeric.outcome.code == "malformed_response"
    missing_bars = adapter([TransportResponse(200, {})]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert missing_bars.outcome.code == "malformed_response"
    null_bars = adapter([TransportResponse(200, {"bars": None})]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert null_bars.outcome.code == "malformed_response"


def test_pagination_cycle_is_terminal_and_token_not_exposed():
    payload = {"bars": {"AAPL": [{"t": "2026-03-09T13:30:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 1}]}, "next_page_token": "cycle-token"}
    result = adapter([TransportResponse(200, payload), TransportResponse(200, payload)]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == "pagination_cycle"
    assert "cycle-token" not in result.outcome.message
    assert "cycle-token" not in json.dumps(result.to_dict())


def test_regular_session_filter_and_dst():
    payload = {"bars": {"AAPL": [
        {"t": "2026-03-09T13:29:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 1},
        {"t": "2026-03-09T13:30:00Z", "o": 2, "h": 3, "l": 2, "c": 3, "v": 1},
        {"t": "2026-03-09T20:00:00Z", "o": 3, "h": 4, "l": 3, "c": 4, "v": 1},
    ]}}
    result = adapter([TransportResponse(200, payload)]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:29:00Z", end_at="2026-03-09T20:01:00Z", timeframe="1Min"
    )
    assert result.success and [bar["start_at"] for bar in result.bars] == ["2026-03-09T13:30:00Z"]
    assert result.snapshot["session"] == {"kind": "regular", "timezone": "America/New_York"}


@pytest.mark.parametrize(
    "timeframe,payload,start_at,end_at,expected_end",
    [
        (
            "1Day",
            {"bars": {"AAPL": [
                {"t": "2026-03-06T05:00:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 1},
                {"t": "2026-03-09T04:00:00Z", "o": 2, "h": 3, "l": 2, "c": 3, "v": 1},
            ]}},
            "2026-03-06T05:00:00Z",
            "2026-03-10T04:00:00Z",
            ["2026-03-07T05:00:00Z", "2026-03-10T04:00:00Z"],
        ),
        (
            "1Week",
            {"bars": {"AAPL": [
                {"t": "2026-03-02T05:00:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 1},
                {"t": "2026-03-09T04:00:00Z", "o": 2, "h": 3, "l": 2, "c": 3, "v": 1},
            ]}},
            "2026-03-02T05:00:00Z",
            "2026-03-16T04:00:00Z",
            ["2026-03-09T04:00:00Z", "2026-03-16T04:00:00Z"],
        ),
    ],
)
def test_day_week_bars_use_local_calendar_boundaries_across_dst(timeframe, payload, start_at, end_at, expected_end):
    result = adapter([TransportResponse(200, payload)]).fetch_history(
        symbol="AAPL", start_at=start_at, end_at=end_at, timeframe=timeframe
    )
    assert result.success
    assert [bar["end_at"] for bar in result.bars] == expected_end
    assert all(result.bars[index]["end_at"] <= result.bars[index + 1]["start_at"] for index in range(len(result.bars) - 1))
    again = adapter([TransportResponse(200, payload)]).fetch_history(
        symbol="AAPL", start_at=start_at, end_at=end_at, timeframe=timeframe
    )
    assert result.snapshot == again.snapshot


@pytest.mark.parametrize("payload", [{"symbol": "MSFT", "bars": []}, {"symbol": 123, "bars": []}])
def test_single_symbol_payload_symbol_mismatch_or_non_string_is_malformed(payload):
    result = adapter([TransportResponse(200, payload)]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == OutcomeCode.MALFORMED_RESPONSE.value


def test_single_symbol_bars_mapping_rejects_cross_symbol_payload():
    bar = {"t": "2026-03-09T13:30:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 1}
    result = adapter([TransportResponse(200, {"bars": {"AAPL": [bar], "MSFT": [bar]}})]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == OutcomeCode.MALFORMED_RESPONSE.value


def test_single_symbol_bars_mapping_empty_is_typed_empty():
    result = adapter([TransportResponse(200, {"bars": {}})]).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == OutcomeCode.EMPTY_WINDOW.value


def test_non_official_base_url_is_rejected_before_credentials_can_be_sent():
    with pytest.raises(ValueError):
        AlpacaHistoryAdapter(key_id="real-id", secret_key="real-secret", base_url="https://evil.example", transport=RecordedTransport())


def test_recovery_request_is_fixed_bounded_and_recovery_bar_identity_is_strict():
    result = adapter([TransportResponse(200, fixture("iex_history_page_2.json"))]).fetch_recovery(
        symbol="AAPL", start_at="2026-03-09T13:31:00Z", end_at="2026-03-09T13:32:00Z"
    )
    assert result.success and result.request.timeframe == {"unit": "minute", "multiplier": 1}
    bar = result.recovery_bars[0]
    assert bar.path_precision == "minute_bar_no_path"
    assert bar.is_trade_trigger_eligible()
    wire = bar.to_dict()
    assert validate_recovery_bar(wire).valid
    tampered = dict(wire)
    tampered["close"] = "999"
    assert not validate_recovery_bar(tampered).valid
    sip = dict(wire)
    sip["market_feed"] = {**sip["market_feed"], "feed": "sip"}
    assert not validate_recovery_bar(sip).valid
    # Recomputed identities must not bypass semantic identity checks.
    venue_tampered = dict(wire)
    venue_tampered["instrument"] = {**venue_tampered["instrument"], "venue": "SIP"}
    venue_tampered["content_hash"] = recovery_bar_content_hash(venue_tampered)
    venue_tampered["recovery_bar_id"] = "rbar_" + venue_tampered["content_hash"]
    assert not validate_recovery_bar(venue_tampered).valid
    cursor_tampered = dict(wire)
    cursor_tampered["recovery_cursor"] = "garbage"
    cursor_tampered["content_hash"] = recovery_bar_content_hash(cursor_tampered)
    cursor_tampered["recovery_bar_id"] = "rbar_" + cursor_tampered["content_hash"]
    assert not validate_recovery_bar(cursor_tampered).valid
    cursor_mismatch = dict(wire)
    cursor_mismatch["recovery_cursor"] = "2026-03-09T13:33:00Z"
    cursor_mismatch["content_hash"] = recovery_bar_content_hash(cursor_mismatch)
    cursor_mismatch["recovery_bar_id"] = "rbar_" + cursor_mismatch["content_hash"]
    assert not validate_recovery_bar(cursor_mismatch).valid
    malformed_instrument = dict(wire)
    malformed_instrument["instrument"] = {**malformed_instrument["instrument"], "venue": object()}
    assert not validate_recovery_bar(malformed_instrument).valid
    assert not validate_recovery_bar(wire, expected_source_request_hash="f" * 64).valid
    class ExplodingMapping(Mapping):
        def __getitem__(self, key):
            raise RuntimeError("mapping exploded")

        def __iter__(self):
            raise RuntimeError("mapping exploded")

        def __len__(self):
            raise RuntimeError("mapping exploded")

    assert not validate_recovery_bar(ExplodingMapping()).valid
    missing = dict(wire)
    missing.pop("recovery_cursor")
    assert not validate_recovery_bar(missing).valid
    unknown = dict(wire)
    unknown["unexpected"] = True
    assert not validate_recovery_bar(unknown).valid
    incomplete = RecoveryBar(
        instrument=bar.instrument, timeframe=bar.timeframe, session=bar.session,
        start_at=bar.start_at, end_at=bar.end_at, open=bar.open, high=bar.high,
        low=bar.low, close=bar.close, volume=bar.volume, complete=False,
        coverage_state="partial", source_request_hash=bar.source_request_hash,
        recovery_cursor=bar.recovery_cursor, market_feed=bar.market_feed,
    )
    assert validate_recovery_bar(incomplete.to_dict()).valid
    assert not incomplete.is_trade_trigger_eligible()


def test_recovery_window_is_minute_aligned_and_bounded():
    a = AlpacaHistoryAdapter(key_id="k", secret_key="s", transport=RecordedTransport())
    with pytest.raises(Exception):
        a.build_recovery_request("AAPL", "2026-03-09T13:30:01Z", "2026-03-09T13:31:00Z")
    with pytest.raises(Exception):
        a.build_recovery_request("AAPL", "2026-03-01T13:30:00Z", "2026-03-09T13:31:00Z")
    result = a.fetch_recovery(request={"symbol": "AAPL", "start_at": "2026-03-09T13:30:00Z", "end_at": "2026-03-09T13:31:00Z", "timeframe": "5Min"})
    assert result.outcome.code == "invalid_request"


def test_missing_credentials_and_strict_raise_are_safe():
    result = AlpacaHistoryAdapter(transport=RecordedTransport()).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min"
    )
    assert result.outcome.code == "missing_credentials"
    with pytest.raises(AdapterError) as exc:
        AlpacaHistoryAdapter(transport=RecordedTransport()).fetch_history(
            symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:31:00Z", timeframe="1Min", strict=True
        )
    assert "APCA" not in str(exc.value)


def test_import_closure_excludes_model_strategy_and_paper_modules():
    import pa_engine.market_sources.alpaca.history as history

    names = set(history.__dict__)
    assert not any(token in " ".join(names).lower() for token in ("deepseek", "strategy", "paper_account", "openai"))
