"""Exact Decimal objective observation goldens and continuity behavior."""
from __future__ import annotations

import copy
import re
from decimal import Decimal
from typing import Any

pytest_plugins = ("g015_fixtures",)

from pa_engine.contracts import finalize_artifact, validate_artifact
from pa_engine.market import ALPACA_IEX_FEED, assess_market, build_observation_frame

from g015_fixtures import build_frame, finalize_snapshot


DECIMAL_TEXT = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")


def _selected(observation: dict[str, Any]) -> dict[str, Any]:
    return {key: observation[key] for key in (
        "observation_id", "bar_id", "direction", "range", "body", "upper_wick", "lower_wick",
        "range_relation", "gap_relation", "rolling_windows",
    )}


def _walk(value: Any):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key, item
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def test_expected_frame_matches_exact_decimal_fixture(valid_snapshot, expected_frame):
    assert validate_artifact(expected_frame, expected_kind="market_observation_frame", related_artifacts={"market_snapshot": valid_snapshot}).valid
    actual = build_frame(valid_snapshot)
    assert actual["formula_version"] == "g015-objective-0.1.0"
    assert actual["rounding_mode"] == "exact_decimal"
    assert actual["market_snapshot_id"] == valid_snapshot["market_snapshot_id"]
    assert actual["market_snapshot_hash"] == valid_snapshot["content_hash"]
    assert actual["observation_frame_id"] == expected_frame["observation_frame_id"]
    assert actual["content_hash"] == expected_frame["content_hash"]
    assert [_selected(item) for item in actual["observations"]] == [_selected(item) for item in expected_frame["observations"]]
    assert actual["quality"]["status"] == "ok"


def test_all_direction_edges_relations_gaps_and_zero_wicks_are_covered(expected_frame):
    observations = expected_frame["observations"]
    assert {item["direction"] for item in observations} == {"bullish", "bearish", "doji"}
    assert {item["range_relation"] for item in observations} == {"none", "inside", "outside", "overlap"}
    assert {item["gap_relation"] for item in observations} == {"none", "up", "down"}
    assert any(item["upper_wick"] == "0" and item["lower_wick"] == "0" for item in observations)
    assert any(item["body"] == "0" for item in observations)
    # Equal and partial intersections both use the contract's overlap value;
    # disjoint ranges use none even when the gap relation is up/down.
    assert observations[9]["range_relation"] == "none" and observations[9]["gap_relation"] == "up"
    assert observations[10]["range_relation"] == "overlap"
    assert observations[11]["range_relation"] == "overlap"
    assert observations[12]["range_relation"] == "none" and observations[12]["gap_relation"] == "down"


def test_rolling_windows_use_preceding_contiguous_history_and_exclude_current(expected_frame):
    observations = expected_frame["observations"]
    assert all(item["rolling_windows"][0]["status"] == "insufficient_history" for item in observations[:3])
    assert observations[3]["rolling_windows"][0] == {"lookback": 3, "status": "available", "high": "104", "low": "99"}
    # Current bar 3 has high 106; it must not leak into its own preceding high.
    assert observations[3]["rolling_windows"][0]["high"] != "106"


def test_incomplete_final_bar_is_excluded_before_observations(valid_snapshot):
    snapshot = copy.deepcopy(valid_snapshot)
    final_bar_id = snapshot["bars"][-1]["bar_id"]
    snapshot["bars"][-1]["is_closed"] = False
    snapshot = finalize_snapshot(snapshot)
    frame = build_frame(snapshot)
    ids = [item["bar_id"] for item in frame["observations"]]
    assert final_bar_id not in ids
    assert len(ids) == len(snapshot["bars"]) - 1


def test_declared_gap_resets_relation_and_rolling_history(valid_snapshot):
    snapshot = copy.deepcopy(valid_snapshot)
    dropped = snapshot["bars"].pop(4)
    snapshot["coverage"]["status"] = "partial"
    snapshot["coverage"]["missing_intervals"] = [{"start_at": dropped["start_at"], "end_at": dropped["end_at"]}]
    snapshot["quality"]["status"] = "partial"
    snapshot["quality"]["codes"] = ["partial_data"]
    snapshot = finalize_snapshot(snapshot)
    frame = build_frame(snapshot)
    first_after_gap = next(item for item in frame["observations"] if item["bar_id"] == snapshot["bars"][4]["bar_id"])
    assert first_after_gap["range_relation"] == "none"
    assert first_after_gap["rolling_windows"][0]["status"] == "insufficient_history"
    assert first_after_gap["rolling_windows"][0]["high"] is None
    assert first_after_gap["rolling_windows"][0]["low"] is None


def test_generated_at_is_volatile_for_frame_identity(valid_snapshot):
    policy = {
        "schema_version": "0.1.0",
        "artifact_kind": "analysis_policy",
        "extensions": {},
        "minimum_closed_bars": 3,
        "max_staleness_seconds": 600,
        "allow_partial_data": False,
        "exclude_incomplete_last_bar": True,
        "required_capabilities": ["price_action"],
        "policy_version": "0.1.0",
    }
    policy = finalize_artifact(policy, "analysis_policy")
    assessment = assess_market(valid_snapshot, policy, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted")
    first = build_observation_frame(valid_snapshot, assessment, lookbacks=(3,), generated_at="2026-08-10T15:00:00Z")
    second = build_observation_frame(valid_snapshot, assessment, lookbacks=(3,), generated_at="2026-08-10T15:00:01Z")
    assert first["generated_at"] != second["generated_at"]
    assert first["observation_frame_id"] == second["observation_frame_id"]
    assert first["content_hash"] == second["content_hash"]

    changed = copy.deepcopy(valid_snapshot)
    changed["generated_at"] = "2026-08-10T15:59:59Z"
    changed = finalize_snapshot(changed)
    assert changed["content_hash"] == valid_snapshot["content_hash"]
    changed_frame = build_frame(changed)
    assert changed_frame["observation_frame_id"] == first["observation_frame_id"]
    assert changed_frame["content_hash"] == first["content_hash"]


def test_huge_high_precision_decimal_values_remain_exact_and_canonical(valid_snapshot):
    snapshot = copy.deepcopy(valid_snapshot)
    bar = snapshot["bars"][0]
    bar["open"] = "100000000000000000000.1234567890123456789"
    bar["close"] = "100000000000000000001.1234567890123456789"
    bar["high"] = "100000000000000000002.1234567890123456789"
    bar["low"] = "100000000000000000000.0000000000000000001"
    snapshot = finalize_snapshot(snapshot)
    frame = build_frame(snapshot)
    first = frame["observations"][0]
    assert first["range"] == "2.1234567890123456788"
    assert first["body"] == "1"
    assert first["upper_wick"] == "1"
    assert first["lower_wick"] == "0.1234567890123456788"
    decimal_fields = {"range", "body", "upper_wick", "lower_wick", "high", "low"}
    for key, value in _walk(frame):
        if key in decimal_fields:
            if value is not None:
                assert isinstance(value, str) and DECIMAL_TEXT.fullmatch(value), (key, value)
                assert "e" not in value.lower() and value != "-0"
    assert not any(isinstance(value, float) for _key, value in _walk(frame))
