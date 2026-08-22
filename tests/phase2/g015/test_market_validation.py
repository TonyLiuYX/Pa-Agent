"""G015 market/request validation and frozen policy outcomes."""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

pytest_plugins = ("g015_fixtures",)

from pa_engine.contracts import bar_id_for, finalize_artifact, validate_artifact, verify_identity
from pa_engine.market import (
    ALPACA_IEX_FEED,
    MarketAssessment,
    MarketDataError,
    PolicyOutcome,
    assess_market,
    validate_analysis_policy,
    validate_analysis_request,
    validate_closed_bar_window,
    validate_market_snapshot,
)

from g015_fixtures import FIXTURES, build_frame, finalize_snapshot, issue_codes, load_fixture, scenario_snapshot


def _codes(result: Any) -> set[str]:
    return issue_codes(result)


def _assessment(snapshot: dict[str, Any], policy: dict[str, Any]) -> MarketAssessment:
    return assess_market(
        snapshot,
        policy,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
        expected_adjustment_policy="unadjusted",
    )


def test_valid_snapshot_policy_request_round_trip_and_g014_identity(valid_snapshot, valid_policy, valid_request):
    for artifact, kind in (
        (valid_snapshot, "market_snapshot"),
        (valid_policy, "analysis_policy"),
        (valid_request, "analysis_request"),
    ):
        assert verify_identity(artifact), kind
        assert validate_artifact(artifact, expected_kind=kind).valid

    assert validate_market_snapshot(valid_snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted").valid
    assert validate_analysis_policy(valid_policy).valid
    request_result = validate_analysis_request(
        valid_request,
        snapshot=valid_snapshot,
        policy=valid_policy,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
    )
    assert request_result.valid, request_result
    closed = validate_closed_bar_window(valid_snapshot)
    assert closed.valid, closed


@pytest.mark.parametrize(
    "field,value,expected_valid",
    [
        ("low", "-1", False),
        ("low", "0", False),
        ("low", "-0", False),
        ("volume", "-0", False),
        ("volume", "0", True),
        ("close", "1e0", False),
        ("close", "101.0", False),
    ],
)
def test_closed_bar_window_decimal_boundaries_are_typed_and_traceback_free(valid_snapshot, field, value, expected_valid):
    bars = copy.deepcopy(valid_snapshot["bars"])
    bars[0][field] = value
    result = validate_closed_bar_window(bars)
    assert result.valid is expected_valid, (field, value, result)
    if not expected_valid:
        assert _codes(result).intersection({"malformed_ohlcv", "ohlc_invariant"}), (field, value, result)


def test_closed_bar_window_valid_bare_bars_remain_accepted(valid_snapshot):
    result = validate_closed_bar_window(copy.deepcopy(valid_snapshot["bars"]))
    assert result.valid, result


def test_closed_bar_window_full_context_accepts_valid_bars_and_checks_ids(valid_snapshot):
    result = validate_closed_bar_window(
        copy.deepcopy(valid_snapshot["bars"]),
        analysis_as_of=valid_snapshot["analysis_as_of"],
        timeframe=copy.deepcopy(valid_snapshot["timeframe"]),
        instrument=copy.deepcopy(valid_snapshot["instrument"]),
        session=copy.deepcopy(valid_snapshot["session"]),
    )
    assert result.valid, result


@pytest.mark.parametrize("bar_id", ["x", "bar_not_hash"])
def test_closed_bar_window_rejects_bad_bar_ids_with_full_context(valid_snapshot, bar_id):
    bars = copy.deepcopy(valid_snapshot["bars"])
    bars[0]["bar_id"] = bar_id
    result = validate_closed_bar_window(
        bars,
        analysis_as_of=valid_snapshot["analysis_as_of"],
        timeframe=copy.deepcopy(valid_snapshot["timeframe"]),
        instrument=copy.deepcopy(valid_snapshot["instrument"]),
        session=copy.deepcopy(valid_snapshot["session"]),
    )
    assert not result.valid, (bar_id, result)
    assert _codes(result).intersection({"schema_violation", "malformed_ohlcv", "invalid_market_data"}), (bar_id, result)


def test_closed_bar_window_rejects_extra_bar_fields(valid_snapshot):
    bars = copy.deepcopy(valid_snapshot["bars"])
    bars[0]["unexpected"] = "not-permitted"
    result = validate_closed_bar_window(
        bars,
        analysis_as_of=valid_snapshot["analysis_as_of"],
        timeframe=copy.deepcopy(valid_snapshot["timeframe"]),
        instrument=copy.deepcopy(valid_snapshot["instrument"]),
        session=copy.deepcopy(valid_snapshot["session"]),
    )
    assert not result.valid, result
    assert _codes(result).intersection({"schema_violation", "malformed_ohlcv", "invalid_market_data"}), result


@pytest.mark.parametrize("analysis_as_of", ["2026-02-30T14:43:00Z", "not-a-utc-timestamp"])
def test_closed_bar_window_rejects_invalid_explicit_analysis_as_of(valid_snapshot, analysis_as_of):
    result = validate_closed_bar_window(
        copy.deepcopy(valid_snapshot["bars"]),
        analysis_as_of=analysis_as_of,
        timeframe=copy.deepcopy(valid_snapshot["timeframe"]),
        instrument=copy.deepcopy(valid_snapshot["instrument"]),
        session=copy.deepcopy(valid_snapshot["session"]),
    )
    assert not result.valid, (analysis_as_of, result)
    assert _codes(result).intersection({"invalid_timestamp", "malformed_ohlcv", "invalid_market_data"}), (analysis_as_of, result)


@pytest.mark.parametrize(
    "instrument",
    [
        {"asset_class": "us_equity", "symbol": "aapl", "venue": "NASDAQ", "currency": "USD"},
        {"asset_class": "crypto", "symbol": "AAPL", "venue": "NASDAQ", "currency": "USD"},
    ],
)
def test_closed_bar_window_rejects_lowercase_or_bad_instrument(valid_snapshot, instrument):
    result = validate_closed_bar_window(
        copy.deepcopy(valid_snapshot["bars"]),
        analysis_as_of=valid_snapshot["analysis_as_of"],
        timeframe=copy.deepcopy(valid_snapshot["timeframe"]),
        instrument=instrument,
        session=copy.deepcopy(valid_snapshot["session"]),
    )
    assert not result.valid, (instrument, result)
    assert _codes(result).intersection({"schema_violation", "malformed_ohlcv", "invalid_market_data"}), (instrument, result)


@pytest.mark.parametrize(
    "session",
    [
        {"kind": "regular", "timezone": "UTC+05:30"},
        {"kind": "regular", "timezone": "Etc/GMT+5"},
        {"kind": "overnight", "timezone": "America/New_York"},
    ],
)
def test_closed_bar_window_rejects_invalid_session_context(valid_snapshot, session):
    result = validate_closed_bar_window(
        copy.deepcopy(valid_snapshot["bars"]),
        analysis_as_of=valid_snapshot["analysis_as_of"],
        timeframe=copy.deepcopy(valid_snapshot["timeframe"]),
        instrument=copy.deepcopy(valid_snapshot["instrument"]),
        session=session,
    )
    assert not result.valid, (session, result)
    assert _codes(result).intersection({"schema_violation", "invalid_timezone", "malformed_ohlcv", "invalid_market_data"}), (session, result)


def test_isolated_fixture_index_covers_every_json_and_static_ids_are_valid(valid_snapshot, valid_policy, valid_request):
    manifest = load_fixture("fixture-index.json")
    declared = {entry["path"] for entry in manifest["entries"]}
    actual = {
        path.relative_to(FIXTURES).as_posix()
        for path in FIXTURES.rglob("*.json")
        if path.name != "fixture-index.json"
    }
    assert declared == actual
    assert manifest["formula_version"] == "g015-objective-0.1.0"
    static = {
        "valid/market_snapshot.json": (valid_snapshot, "market_snapshot", {}),
        "valid/analysis_policy.json": (valid_policy, "analysis_policy", {}),
        "valid/analysis_request.json": (valid_request, "analysis_request", {"market_snapshot": valid_snapshot, "analysis_policy": valid_policy}),
        "valid/market_observation_frame.json": (load_fixture("valid/market_observation_frame.json"), "market_observation_frame", {"market_snapshot": valid_snapshot}),
    }
    for path, (artifact, kind, related) in static.items():
        assert verify_identity(artifact), path
        assert validate_artifact(artifact, expected_kind=kind, related_artifacts=related).valid, path


@pytest.mark.parametrize(
    "name,issue_code,condition",
    [
        ("bad_ohlc", "ohlc_invariant", "ohlc_invariant"),
        ("duplicate_bars", "duplicate_bars", "duplicate_bars"),
        ("out_of_order_bars", "out_of_order_bars", "out_of_order_bars"),
        ("future_bar", "future_bars", "future_bars"),
        ("future_nanosecond", "future_bars", "future_bars"),
        ("invalid_utc", "non_utc_timestamp", "malformed_ohlcv"),
        ("direct_second", "malformed_ohlcv", "malformed_ohlcv"),
        ("semantic_duplicate_fraction", "duplicate_bars", "duplicate_bars"),
        ("duration_mismatch", "out_of_order_bars", "out_of_order_bars"),
        ("overlap_bars", "out_of_order_bars", "out_of_order_bars"),
    ],
)
def test_structural_market_failures_are_terminal_and_never_model_callable(valid_policy, name, issue_code, condition):
    snapshot = scenario_snapshot(name)
    result = validate_market_snapshot(snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True)
    assert not result.valid, (name, result)
    assert issue_code in _codes(result), (name, result)
    assessment = _assessment(snapshot, valid_policy)
    assert isinstance(assessment, MarketAssessment)
    assert assessment.model_call_allowed is False
    assert assessment.accepted is False
    assert assessment.terminal_outcome is not None
    assert assessment.terminal_outcome.condition == condition
    assert assessment.terminal_outcome.code == "invalid_market_data"
    assert isinstance(assessment.error, MarketDataError)


def test_partial_stale_and_incomplete_policy_rows_follow_g014_exactly(valid_snapshot, valid_policy):
    partial = scenario_snapshot("partial_coverage")
    denied = _assessment(partial, valid_policy)
    assert denied.terminal_outcome == PolicyOutcome(
        "partial_coverage_disallowed", "market", "terminal", "abstain", "partial_data", False, (), "high", False
    )
    allow = copy.deepcopy(valid_policy)
    allow["allow_partial_data"] = True
    allow = finalize_artifact(allow, "analysis_policy")
    allowed = _assessment(partial, allow)
    assert allowed.accepted and allowed.model_call_allowed
    assert [outcome.condition for outcome in allowed.outcomes] == ["partial_coverage_allowed"]
    assert allowed.warnings == ("partial_data",)

    stale = _assessment(scenario_snapshot("stale_data"), valid_policy)
    assert stale.terminal_outcome is not None
    assert stale.terminal_outcome.condition == "stale_beyond_policy"
    assert stale.terminal_outcome.code == "stale_data"
    assert stale.model_call_allowed is False

    incomplete = _assessment(scenario_snapshot("incomplete_final_bar"), valid_policy)
    assert incomplete.accepted and incomplete.model_call_allowed
    assert incomplete.terminal_outcome is None
    assert [outcome.condition for outcome in incomplete.outcomes] == ["incomplete_final_bar_sufficient"]
    assert incomplete.warnings == ("incomplete_last_bar_excluded",)
    assert incomplete.closed_bars[-1]["is_closed"] is True

    insufficient = _assessment(scenario_snapshot("incomplete_insufficient"), valid_policy)
    assert insufficient.terminal_outcome is not None
    assert insufficient.terminal_outcome.condition == "incomplete_final_bar_insufficient"
    assert insufficient.terminal_outcome.code == "insufficient_history"
    assert insufficient.warnings == ("incomplete_last_bar_excluded",)


def test_policy_combines_continue_rows_but_any_terminal_blocks_call(valid_snapshot, valid_policy):
    allow = copy.deepcopy(valid_policy)
    allow["allow_partial_data"] = True
    allow = finalize_artifact(allow, "analysis_policy")

    partial_stale = scenario_snapshot("partial_coverage")
    partial_stale["analysis_as_of"] = "2026-08-10T15:00:00Z"
    partial_stale = finalize_snapshot(partial_stale)
    result = _assessment(partial_stale, allow)
    assert [outcome.condition for outcome in result.outcomes] == ["partial_coverage_allowed", "stale_beyond_policy"]
    assert result.terminal_outcome is not None and result.terminal_outcome.condition == "stale_beyond_policy"
    assert result.model_call_allowed is False
    assert result.warnings == ("partial_data",)

    partial_incomplete = scenario_snapshot("partial_coverage")
    partial_incomplete["bars"][-1]["is_closed"] = False
    partial_incomplete = finalize_snapshot(partial_incomplete)
    result = _assessment(partial_incomplete, allow)
    assert [outcome.condition for outcome in result.outcomes] == ["partial_coverage_allowed", "incomplete_final_bar_sufficient"]
    assert result.accepted and result.model_call_allowed
    assert result.warnings == ("partial_data", "incomplete_last_bar_excluded")


def test_ordinary_insufficient_history_is_abstain_high_without_incomplete_bar(valid_policy):
    snapshot = scenario_snapshot("insufficient_history")
    assert all(bar["is_closed"] is True for bar in snapshot["bars"])
    assessment = _assessment(snapshot, valid_policy)
    assert assessment.terminal_outcome is not None
    assert assessment.terminal_outcome.condition == "insufficient_closed_history"
    assert assessment.terminal_outcome.expected_status == "abstain"
    assert assessment.terminal_outcome.code == "insufficient_history"
    assert assessment.terminal_outcome.uncertainty == "high"
    assert assessment.model_call_allowed is False
    assert assessment.accepted is False


@pytest.mark.parametrize("name", ["coverage_complete_subset", "coverage_partial_wrong_edge", "coverage_partial_overlap_ns"])
def test_nanosecond_coverage_honesty_failures_are_malformed_and_terminal(valid_policy, name):
    snapshot = scenario_snapshot(name)
    result = validate_market_snapshot(snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True)
    assert not result.valid
    assert "malformed_ohlcv" in _codes(result) or "invalid_market_data" in _codes(result)
    assessment = _assessment(snapshot, valid_policy)
    assert assessment.terminal_outcome is not None
    assert assessment.terminal_outcome.condition == "malformed_ohlcv"
    assert assessment.terminal_outcome.code == "invalid_market_data"
    assert assessment.accepted is False
    assert assessment.model_call_allowed is False


def test_partial_coverage_fixture_is_g014_valid_and_policy_controls_call(valid_policy):
    partial = scenario_snapshot("partial_coverage")
    assert partial["coverage"]["status"] == "partial"
    assert partial["coverage"]["missing_intervals"]
    assert partial["quality"]["status"] == "partial"
    assert partial["quality"]["codes"] == ["partial_data"]
    assert validate_market_snapshot(partial, expected_feed=ALPACA_IEX_FEED, require_feed=True).valid
    denied = _assessment(partial, valid_policy)
    assert denied.terminal_outcome is not None and denied.terminal_outcome.condition == "partial_coverage_disallowed"
    allow = copy.deepcopy(valid_policy)
    allow["allow_partial_data"] = True
    allow = finalize_artifact(allow, "analysis_policy")
    allowed = _assessment(partial, allow)
    assert allowed.accepted and allowed.model_call_allowed


@pytest.mark.parametrize("name", ["lowercase_symbol", "fixed_offset_timezone"])
def test_g014_context_schema_failures_are_not_relaxed_by_market_wrapper(valid_policy, name):
    snapshot = scenario_snapshot(name)
    result = validate_market_snapshot(snapshot)
    assert not result.valid
    assert _codes(result).intersection({"schema_violation", "invalid_timezone"})
    assessment = _assessment(snapshot, valid_policy)
    assert assessment.terminal_outcome is not None
    assert assessment.terminal_outcome.condition == "malformed_ohlcv"
    assert assessment.terminal_outcome.code == "invalid_market_data"


@pytest.mark.parametrize("name", ["quality_invalid", "complete_partial_quality", "incomplete_warning_without_bar"])
def test_contradictory_quality_metadata_is_rejected_before_policy_or_frame(valid_policy, name):
    snapshot = scenario_snapshot(name)
    result = validate_market_snapshot(snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True)
    assert not result.valid, (name, result)
    assert "malformed_ohlcv" in _codes(result), (name, result)
    assessment = _assessment(snapshot, valid_policy)
    assert assessment.terminal_outcome is not None, (name, assessment)
    assert assessment.terminal_outcome.condition == "malformed_ohlcv"
    assert assessment.terminal_outcome.code == "invalid_market_data"
    assert assessment.accepted is False
    assert assessment.model_call_allowed is False


@pytest.mark.parametrize("name", ["fresh_stale_quality", "fresh_insufficient_quality"])
def test_quality_claims_that_do_not_match_fresh_sufficient_history_block_frames(valid_policy, name):
    snapshot = scenario_snapshot(name)
    assessment = _assessment(snapshot, valid_policy)
    assert assessment.terminal_outcome is not None, (name, assessment)
    assert assessment.terminal_outcome.condition == "malformed_ohlcv"
    assert assessment.terminal_outcome.code == "invalid_market_data"
    assert assessment.accepted is False
    assert assessment.model_call_allowed is False
    with pytest.raises(MarketDataError):
        build_frame(snapshot)


def test_request_market_feed_is_optional_but_typed_when_present(valid_snapshot, valid_policy, valid_request):
    omitted = copy.deepcopy(valid_request)
    omitted["extensions"] = {}
    omitted = finalize_artifact(omitted, "analysis_request")
    omitted_result = validate_analysis_request(
        omitted,
        snapshot=valid_snapshot,
        policy=valid_policy,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
    )
    assert omitted_result.valid, omitted_result

    malformed = copy.deepcopy(valid_request)
    malformed["extensions"]["market_feed"] = {"provider": "alpaca"}
    malformed = finalize_artifact(malformed, "analysis_request")
    malformed_result = validate_analysis_request(
        malformed,
        snapshot=valid_snapshot,
        policy=valid_policy,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
    )
    assert not malformed_result.valid
    assert _codes(malformed_result).intersection({"missing_field", "malformed_market_feed"})
    malformed_assessment = assess_market(
        valid_snapshot,
        valid_policy,
        request=malformed,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
        expected_adjustment_policy="unadjusted",
    )
    assert malformed_assessment.terminal_outcome is not None
    assert malformed_assessment.terminal_outcome.condition == "invalid_market_data"
    assert malformed_assessment.accepted is False
    assert malformed_assessment.model_call_allowed is False

    mismatch = copy.deepcopy(valid_request)
    mismatch["extensions"]["market_feed"]["feed"] = "sip"
    mismatch = finalize_artifact(mismatch, "analysis_request")
    mismatch_result = validate_analysis_request(
        mismatch,
        snapshot=valid_snapshot,
        policy=valid_policy,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
    )
    assert not mismatch_result.valid
    assert "market_feed_mismatch" in _codes(mismatch_result)
    mismatch_assessment = assess_market(
        valid_snapshot,
        valid_policy,
        request=mismatch,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
        expected_adjustment_policy="unadjusted",
    )
    assert mismatch_assessment.terminal_outcome is not None
    assert mismatch_assessment.terminal_outcome.condition == "invalid_market_data"
    assert mismatch_assessment.accepted is False
    assert mismatch_assessment.model_call_allowed is False


def test_analysis_policy_feed_extension_is_optional_and_provider_neutral(valid_policy):
    neutral = copy.deepcopy(valid_policy)
    neutral["extensions"]["market_feed"] = {
        "schema_version": "0.1.0",
        "provider": "other_provider",
        "feed": "other_feed",
        "coverage": "consolidated",
    }
    neutral = finalize_artifact(neutral, "analysis_policy")
    assert validate_analysis_policy(neutral).valid

    omitted = copy.deepcopy(valid_policy)
    omitted["extensions"] = {}
    omitted = finalize_artifact(omitted, "analysis_policy")
    assert validate_analysis_policy(omitted).valid


@pytest.mark.parametrize("path,value", [
    ("timeframe.multiplier", 1.5),
    ("instrument.symbol", 1.5),
    ("bars[0]", 1.5),
    ("bars[0].bar_id", {}),
    ("quality.codes", [{}]),
])
def test_raw_non_json_native_snapshot_inputs_never_leak_tracebacks(valid_snapshot, valid_policy, path, value):
    snapshot = copy.deepcopy(valid_snapshot)
    snapshot.pop("market_snapshot_id", None)
    snapshot.pop("content_hash", None)
    parts = path.replace("]", "").replace("[", ".").split(".")
    current: Any = snapshot
    for part in parts[:-1]:
        current = current[int(part)] if isinstance(current, list) else current[part]
    leaf = parts[-1]
    current[int(leaf) if isinstance(current, list) else leaf] = value

    result = validate_market_snapshot(snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True)
    assert not result.valid, (path, result)
    assert _codes(result).intersection({"malformed_ohlcv", "schema_violation", "hash_mismatch"}), (path, result)
    assessment = _assessment(snapshot, valid_policy)
    assert assessment.terminal_outcome is not None, (path, assessment)
    assert assessment.terminal_outcome.condition in {"malformed_ohlcv", "invalid_market_data"}
    assert assessment.terminal_outcome.code == "invalid_market_data"
    assert assessment.accepted is False
    assert assessment.model_call_allowed is False


def test_re_finalized_unknown_request_field_is_invalid_and_blocks_assessment(valid_snapshot, valid_policy, valid_request):
    bad_request = copy.deepcopy(valid_request)
    bad_request["unexpected_core_field"] = "not-permitted"
    bad_request = finalize_artifact(bad_request, "analysis_request")
    result = validate_analysis_request(bad_request, snapshot=valid_snapshot, policy=valid_policy, expected_feed=ALPACA_IEX_FEED, require_feed=True)
    assert not result.valid
    assert _codes(result).intersection({"schema_violation", "unknown_field"})
    assessment = assess_market(
        valid_snapshot,
        valid_policy,
        request=bad_request,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
        expected_adjustment_policy="unadjusted",
    )
    assert assessment.terminal_outcome is not None
    assert assessment.terminal_outcome.condition == "invalid_market_data"
    assert assessment.terminal_outcome.code == "invalid_market_data"
    assert assessment.accepted is False
    assert assessment.model_call_allowed is False


def test_stale_boundary_is_strictly_greater_than_policy_window(valid_snapshot, valid_policy):
    exact = copy.deepcopy(valid_snapshot)
    exact["analysis_as_of"] = "2026-08-10T14:53:00Z"  # latest bar ends 14:43:00Z
    exact = finalize_snapshot(exact)
    assessment = _assessment(exact, valid_policy)
    assert all(outcome.condition != "stale_beyond_policy" for outcome in assessment.outcomes)

    beyond = copy.deepcopy(valid_snapshot)
    beyond["analysis_as_of"] = "2026-08-10T14:53:00.0000001Z"
    beyond = finalize_snapshot(beyond)
    assessment = _assessment(beyond, valid_policy)
    assert assessment.terminal_outcome is not None
    assert assessment.terminal_outcome.condition == "stale_beyond_policy"

    ns_policy = copy.deepcopy(valid_policy)
    ns_policy["max_staleness_seconds"] = 0
    ns_policy = finalize_artifact(ns_policy, "analysis_policy")
    ns = scenario_snapshot("stale_nanosecond")
    ns_assessment = _assessment(ns, ns_policy)
    assert ns_assessment.terminal_outcome is not None
    assert ns_assessment.terminal_outcome.condition == "stale_beyond_policy"


def test_adjustment_policy_is_provider_neutral_but_mismatch_is_observable(valid_snapshot):
    accepted = validate_market_snapshot(valid_snapshot, expected_adjustment_policy="unadjusted")
    assert accepted.valid
    mismatch = validate_market_snapshot(valid_snapshot, expected_adjustment_policy="split_adjusted")
    assert not mismatch.valid
    assert "adjustment_policy_mismatch" in _codes(mismatch)


@pytest.mark.parametrize("unit", ["minute", "hour", "day", "week"])
def test_minute_and_larger_timeframes_are_accepted(unit, valid_snapshot):
    snapshot = copy.deepcopy(valid_snapshot)
    durations = {"minute": timedelta(minutes=1), "hour": timedelta(hours=1), "day": timedelta(days=1), "week": timedelta(days=7)}
    start = datetime(2026, 8, 3, tzinfo=timezone.utc)
    duration = durations[unit]
    snapshot["timeframe"] = {"unit": unit, "multiplier": 1}
    snapshot["bars"] = []
    for i in range(3):
        begin = start + i * duration
        end = begin + duration
        begin_text = begin.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_text = end.strftime("%Y-%m-%dT%H:%M:%SZ")
        snapshot["bars"].append({
            "bar_id": "pending",
            "start_at": begin_text,
            "end_at": end_text,
            "open": str(100 + i),
            "high": str(102 + i),
            "low": str(99 + i),
            "close": str(101 + i),
            "volume": str(1000 + i),
            "is_closed": True,
        })
    snapshot["analysis_as_of"] = snapshot["bars"][-1]["end_at"]
    snapshot["coverage"]["expected"] = {"start_at": snapshot["bars"][0]["start_at"], "end_at": snapshot["bars"][-1]["end_at"]}
    snapshot["coverage"]["actual"] = copy.deepcopy(snapshot["coverage"]["expected"])
    snapshot = finalize_snapshot(snapshot)
    result = validate_market_snapshot(snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True)
    assert result.valid, (unit, result)


def test_direct_second_analysis_timeframe_is_rejected(valid_snapshot):
    snapshot = copy.deepcopy(valid_snapshot)
    snapshot["timeframe"] = {"unit": "second", "multiplier": 1}
    snapshot = finalize_snapshot(snapshot)
    result = validate_market_snapshot(snapshot)
    assert not result.valid
    assert "malformed_ohlcv" in _codes(result)


@pytest.mark.parametrize("field,value", [
    ("instrument", {"asset_class": "us_equity", "symbol": "MSFT", "venue": "NASDAQ", "currency": "USD"}),
    ("timeframe", {"unit": "hour", "multiplier": 1}),
    ("session", {"kind": "extended", "timezone": "America/New_York"}),
])
def test_request_context_mismatch_is_rejected(valid_snapshot, valid_policy, valid_request, field, value):
    request = copy.deepcopy(valid_request)
    request[field] = value
    request = finalize_artifact(request, "analysis_request")
    result = validate_analysis_request(request, snapshot=valid_snapshot, policy=valid_policy, expected_feed=ALPACA_IEX_FEED, require_feed=True)
    assert not result.valid
    assert "cross_lineage_mismatch" in _codes(result)
