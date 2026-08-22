"""Focused G028 prompt/request builder tests with real upstream artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pa_engine.contracts import canonical_json_bytes
from pa_engine.market_sources.alpaca.history import AlpacaHistoryAdapter, RecordedTransport, TransportResponse
from pa_engine.market_sources.alpaca.stream import LocalSecondAggregator, parse_trade
from pa_engine.market_view import build_market_view
from pa_engine.providers import validate_normalized_request
from pa_engine.runtime.boundary import ModelBoundaryError, scan_model_boundary
from pa_engine.runtime.prompt import (
    PROMPT_TEMPLATE_VERSION,
    PROMPT_VERSION,
    PROVISIONAL_NOTE,
    build_analysis_prompt,
    build_analysis_request,
)
from pa_engine.strategies.knowledge import load_default_registry

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "phase2"
PROMPT_FIXTURES = FIXTURES / "g028" / "prompt"


def _history():
    payload = json.loads((FIXTURES / "g019" / "iex_history_success.json").read_text())
    result = AlpacaHistoryAdapter(key_id="runtime", secret_key="runtime", transport=RecordedTransport([TransportResponse(200, payload)])).fetch_history(
        symbol="AAPL", start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:33:00Z", timeframe="1Min"
    )
    assert result.success and result.snapshot is not None
    return result.snapshot


def _events():
    aggregator = LocalSecondAggregator("AAPL")
    for index, price in enumerate(("10", "11"), 1):
        aggregator.add_trade(
            parse_trade(
                {"T": "t", "S": "AAPL", "i": index, "x": "V", "p": price, "s": 1, "c": ["@"], "t": f"2026-03-09T13:33:{index * 10 - 10:02d}.100Z", "z": "C"},
                received_at="2026-03-09T13:33:01Z",
            )
        )
    return aggregator.freeze(now="2026-03-09T13:34:00Z")


def _view():
    return build_market_view(
        _history(),
        _events(),
        cutover_at="2026-03-09T13:33:00Z",
        now_at="2026-03-09T13:34:00Z",
        minimum_closed_bars=3,
    )


def _packet():
    registry = load_default_registry(ROOT / "knowledge")
    strategy_ids = registry.strategy_ids
    assert strategy_ids
    outcome = registry.load_packet(strategy_ids[0])
    assert outcome.ok
    return outcome.packet, outcome.profile_ref, outcome.evidence_package


@pytest.fixture(scope="module")
def inputs():
    packet, profile_ref, evidence_package = _packet()
    view = _view()
    return packet, profile_ref, evidence_package, view


def _request(inputs, *, reasoning_mode="enabled", **kwargs):
    packet, profile_ref, evidence_package, view = inputs
    return build_analysis_request(
        strategy_id=packet["strategy_id"],
        packet=packet,
        profile_ref=profile_ref,
        market_view=view,
        reasoning_mode=reasoning_mode,
        evidence_package=evidence_package,
        **kwargs,
    )


def test_request_is_deterministic_and_canonical(inputs):
    first = _request(inputs)
    second = _request(inputs)
    assert first == second
    assert json.loads(canonical_json_bytes(first).decode("utf-8")) == first
    expected_hash = hashlib.sha256(
        canonical_json_bytes(
            {
                "prompt_template_version": PROMPT_TEMPLATE_VERSION,
                "instructions": first["instructions"],
                "context_text": first["context_text"],
            }
        )
    ).hexdigest()
    assert first["prompt_hash"] == expected_hash


def test_request_validates_against_the_frozen_schema(inputs):
    request = _request(inputs)
    result = validate_normalized_request(request)
    assert result.valid, [issue.to_dict() for issue in result.issues]
    assert request["schema_version"] == "0.1.0"
    assert request["artifact_kind"] == "normalized_model_request"
    assert request["provider"] == "deepseek"
    assert request["model"] == "deepseek-v4-flash"
    assert request["requested_capabilities"] == ["structured_json", "reasoning_mode"]
    assert request["prompt_template_version"] == PROMPT_TEMPLATE_VERSION
    assert request["response_schema_version"] == "0.1.0"


def test_extensions_carry_the_runtime_boundary_fields(inputs):
    packet, profile_ref, evidence_package, view = inputs
    enabled = _request(inputs, reasoning_mode="enabled")
    extensions = enabled["extensions"]
    assert extensions["reasoning_mode"] == "enabled"
    assert extensions["strategy_profile_ref"] == profile_ref
    assert extensions["market_data_usage"] == "internal_prototype"
    assert extensions["no_live_orders"] is True
    disabled = _request(inputs, reasoning_mode="disabled")
    assert disabled["extensions"]["reasoning_mode"] == "disabled"
    assert disabled["parameters"] == {"temperature": "0", "max_output_tokens": 4096}


def test_request_has_zero_boundary_issues(inputs):
    request = _request(inputs)
    assert scan_model_boundary(request) == []


def test_context_contains_claim_text_and_provenance(inputs):
    packet, profile_ref, evidence_package, view = inputs
    request = _request(inputs)
    context = request["context_text"]
    first_claim = next(iter(packet["claims"].values()))[0]
    assert first_claim["statement"] in context
    assert f"strategy_id={packet['strategy_id']}" in context
    assert f"profile_ref_id={profile_ref['profile_ref_id']}" in context
    assert f"profile_ref_hash={profile_ref['profile_ref_hash']}" in context
    assert "INTERNAL PROVENANCE" in context
    chunk = packet["chunk_excerpts"][0]
    assert chunk["chunk_id"] in context


def test_context_contains_bar_lines_and_latest_second(inputs):
    packet, profile_ref, evidence_package, view = inputs
    request = _request(inputs)
    context = request["context_text"]
    assert "start_at=2026-03-09T13:30:00Z end_at=2026-03-09T13:31:00Z O=100 H=101.25 L=99.5 C=100.75 V=1000" in context
    assert "start_at=2026-03-09T13:32:00Z end_at=2026-03-09T13:33:00Z" in context
    latest = view.latest_second
    assert latest is not None
    assert f"event_id={latest['event_id']}" in context
    assert f"event_hash={latest['event_hash']}" in context
    assert "close=11 (reference_price)" in context
    assert "execution_watermark=" in context
    assert "revision=" in context
    assert f"cutover_at={view.cutover_at}" in context
    assert "readiness:" in context and "frozen_second=True" in context


def test_context_refs_and_extension_refs_are_bound_to_the_view(inputs):
    packet, profile_ref, evidence_package, view = inputs
    request = _request(inputs)
    snapshot = view.closed_snapshot
    frame = view.observation_frame
    assert request["context_refs"]["market_snapshot_id"] == snapshot["market_snapshot_id"]
    assert request["context_refs"]["market_snapshot_hash"] == snapshot["content_hash"]
    assert request["context_refs"]["observation_frame_id"] == frame["observation_frame_id"]
    assert request["context_refs"]["observation_frame_hash"] == frame["content_hash"]
    assert request["context_refs"]["evidence_package_id"] == evidence_package["evidence_package_id"]
    assert request["context_refs"]["evidence_package_hash"] == evidence_package["content_hash"]
    extension_refs = request["extensions"]["context_refs"]
    assert extension_refs["shared_market_view_id"] == view.shared_market_view_id
    assert extension_refs["shared_market_view_hash"] == view.shared_market_view_hash
    assert extension_refs["latest_second_event_id"] == view.latest_second["event_id"]
    assert extension_refs["latest_second_event_hash"] == view.latest_second["event_hash"]
    assert extension_refs["packet_id"] == packet["packet_id"]
    assert extension_refs["packet_hash"] == packet["packet_hash"]


def test_forming_candle_is_labeled_provisional(inputs):
    packet, profile_ref, evidence_package, view = inputs
    assert view.forming_candle is not None
    request = _request(inputs)
    context = request["context_text"]
    assert "provisional_context:" in context
    assert PROVISIONAL_NOTE in context
    forming = view.forming_candle
    assert forming["start_at"] in context


def test_instructions_quote_all_three_branches(inputs):
    request = _request(inputs)
    instructions = request["instructions"]
    assert "You are a price-action paper-trading decision agent." in instructions
    assert "You are a price-action analysis model. You never receive, compute, or output any account balance, position quantity, notional, R multiple or P&L." in instructions
    for marker in (
        '"no_entry"',
        '"enter_immediately"',
        '"wait_for_price"',
        "1 to 86400",
        "second_event_ref",
        "canonical positive decimal string",
        "240 codepoints",
        "low\" | \"medium\" | \"high\"",
        "No account, balance, position quantity, notional, R multiple, P&L or credential field is permitted anywhere in the output",
        "no_live_orders: true",
    ):
        assert marker in instructions, marker


def test_build_analysis_prompt_matches_the_request(inputs):
    packet, profile_ref, evidence_package, view = inputs
    instructions, context_text = build_analysis_prompt(
        strategy_id=packet["strategy_id"],
        packet=packet,
        profile_ref=profile_ref,
        market_view=view,
        reasoning_mode="enabled",
        evidence_package=evidence_package,
    )
    request = _request(inputs)
    assert instructions == request["instructions"]
    assert context_text == request["context_text"]
    assert "OUTPUT DIRECTIVE" in context_text
    assert "Return exactly one JSON object matching the contract above and nothing else." in context_text


def test_optional_view_parts_are_omitted_gracefully(inputs):
    packet, profile_ref, evidence_package, view = inputs
    minimal = {
        "symbol": view.symbol,
        "timeframe": dict(view.timeframe),
        "session": dict(view.session),
        "cutover_at": view.cutover_at,
        "closed_snapshot": dict(view.closed_snapshot),
        "observation_frame": None,
        "forming_candle": None,
        "latest_second": None,
        "quality": dict(view.quality),
        "readiness": dict(view.readiness.to_dict()),
    }
    request = build_analysis_request(
        strategy_id=packet["strategy_id"],
        packet=packet,
        profile_ref=profile_ref,
        market_view=minimal,
        reasoning_mode="disabled",
    )
    assert validate_normalized_request(request).valid
    assert request["context_refs"]["observation_frame_id"].startswith("obs_")
    assert request["context_refs"]["evidence_package_id"].startswith("evp_")
    assert request["extensions"]["context_refs"] == {
        "packet_id": packet["packet_id"],
        "packet_hash": packet["packet_hash"],
    }
    assert "provisional_context:" not in request["context_text"]
    assert "LATEST FROZEN SECOND" not in request["context_text"]


def test_invalid_reasoning_mode_and_tokens_are_rejected(inputs):
    packet, profile_ref, evidence_package, view = inputs
    with pytest.raises(ValueError, match="reasoning_mode"):
        build_analysis_request(
            strategy_id=packet["strategy_id"], packet=packet, profile_ref=profile_ref, market_view=view, reasoning_mode="always"
        )
    with pytest.raises(ValueError, match="max_output_tokens"):
        build_analysis_request(
            strategy_id=packet["strategy_id"], packet=packet, profile_ref=profile_ref, market_view=view, reasoning_mode="enabled", max_output_tokens=0
        )


def test_recorded_view_fixture_matches_a_fresh_build(inputs):
    fixture_path = PROMPT_FIXTURES / "view_snapshot.json"
    assert fixture_path.exists()
    recorded = json.loads(fixture_path.read_text())
    packet, profile_ref, evidence_package, view = inputs
    assert recorded == view.to_dict()
    request_from_fixture = build_analysis_request(
        strategy_id=packet["strategy_id"],
        packet=packet,
        profile_ref=profile_ref,
        market_view=recorded,
        reasoning_mode="enabled",
        evidence_package=evidence_package,
    )
    assert request_from_fixture == _request(inputs)
    assert PROMPT_VERSION in request_from_fixture["context_text"]


def test_profile_ref_mismatch_stops_the_builder(inputs, monkeypatch):
    packet, profile_ref, evidence_package, view = inputs
    import pa_engine.runtime.prompt as prompt_module

    monkeypatch.setattr(prompt_module, "profile_ref_intact", lambda value, expected: False)
    with pytest.raises(ModelBoundaryError) as error:
        build_analysis_request(
            strategy_id=packet["strategy_id"],
            packet=packet,
            profile_ref=profile_ref,
            market_view=view,
            reasoning_mode="enabled",
            evidence_package=evidence_package,
        )
    assert error.value.code in {"profile_ref_missing", "profile_ref_mismatch"}
