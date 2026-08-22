"""Shared G015 fixture loaders and small semantic-snapshot builders."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from pa_engine.contracts import bar_id_for, finalize_artifact
from pa_engine.market import ALPACA_IEX_FEED, assess_market, build_observation_frame


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "phase2" / "g015"


def load_fixture(relative_path: str) -> dict[str, Any]:
    value = json.loads((FIXTURES / relative_path).read_text(encoding="utf-8"))
    assert isinstance(value, dict), relative_path
    return value


def as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        assert isinstance(result, Mapping)
        return dict(result)
    raise TypeError(f"expected mapping-like public result, got {type(value)!r}")


def issue_codes(result: Any) -> set[str]:
    codes: set[str] = set()
    for issue in getattr(result, "issues", ()):
        if hasattr(issue, "code"):
            codes.add(issue.code)
        elif isinstance(issue, Mapping):
            codes.add(str(issue["code"]))
    return codes


def recalc_bar_ids(snapshot: dict[str, Any]) -> None:
    for bar in snapshot.get("bars", []):
        bar["bar_id"] = bar_id_for(
            snapshot["instrument"],
            snapshot["timeframe"],
            snapshot["session"],
            bar["start_at"],
            bar["end_at"],
        )


def finalize_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    recalc_bar_ids(snapshot)
    return finalize_artifact(snapshot, "market_snapshot")


def scenario_snapshot(name: str) -> dict[str, Any]:
    spec = load_fixture(f"invalid/{name}.json")
    snapshot = copy.deepcopy(load_fixture(spec["base_fixture"]))
    for mutation in spec.get("mutations", []):
        operation = mutation.get("operation")
        if operation == "duplicate":
            index = int(mutation["index"])
            snapshot["bars"].insert(index + 1, copy.deepcopy(snapshot["bars"][index]))
            continue
        if operation == "swap":
            left, right = int(mutation["left"]), int(mutation["right"])
            snapshot["bars"][left], snapshot["bars"][right] = snapshot["bars"][right], snapshot["bars"][left]
            continue
        if operation == "truncate":
            snapshot["bars"] = snapshot["bars"][: int(mutation["length"])]
            continue
        if operation == "drop":
            snapshot["bars"].pop(int(mutation["index"]))
            continue
        if operation == "shift_all_fraction":
            suffix = "." + str(mutation["fraction"]) + "Z"
            for bar in snapshot["bars"]:
                for key in ("start_at", "end_at"):
                    bar[key] = bar[key].removesuffix("Z") + suffix
            for scope in (snapshot["coverage"].get("expected"), snapshot["coverage"].get("actual")):
                if isinstance(scope, Mapping):
                    for key in ("start_at", "end_at"):
                        scope[key] = scope[key].removesuffix("Z") + suffix
            for interval in snapshot["coverage"].get("missing_intervals", []):
                if isinstance(interval, Mapping):
                    for key in ("start_at", "end_at"):
                        interval[key] = interval[key].removesuffix("Z") + suffix
            continue
        path = mutation["path"]
        current: Any = snapshot
        parts = path.replace("]", "").replace("[", ".").split(".")
        for part in parts[:-1]:
            current = current[int(part)] if isinstance(current, list) else current[part]
        leaf = parts[-1]
        current[int(leaf) if isinstance(current, list) else leaf] = mutation["value"]
    return finalize_snapshot(snapshot)


def build_frame(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Call the public builder with the fixture's explicit lookback contract."""

    policy = load_fixture("valid/analysis_policy.json")
    if isinstance(snapshot.get("coverage"), Mapping) and snapshot["coverage"].get("status") == "partial":
        policy["allow_partial_data"] = True
        policy = finalize_artifact(policy, "analysis_policy")
    assessment = assess_market(
        snapshot,
        policy,
        expected_feed=ALPACA_IEX_FEED,
        require_feed=True,
        expected_adjustment_policy="unadjusted",
    )
    result = build_observation_frame(snapshot, assessment, lookbacks=(3,))
    return as_dict(result)


@pytest.fixture
def valid_snapshot() -> dict[str, Any]:
    return load_fixture("valid/market_snapshot.json")


@pytest.fixture
def valid_policy() -> dict[str, Any]:
    return load_fixture("valid/analysis_policy.json")


@pytest.fixture
def valid_request() -> dict[str, Any]:
    return load_fixture("valid/analysis_request.json")


@pytest.fixture
def expected_frame() -> dict[str, Any]:
    return load_fixture("valid/market_observation_frame.json")


@pytest.fixture
def expected_feed() -> Any:
    return ALPACA_IEX_FEED
