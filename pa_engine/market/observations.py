"""Deterministic objective OHLCV observations for G015."""

from __future__ import annotations

import copy
from decimal import Decimal, localcontext
from typing import Any, Iterable, Mapping

from pa_engine.contracts import (
    ValidationIssue,
    finalize_artifact,
    observation_id_for,
    validate_artifact,
)

from .validation import (
    MarketAssessment,
    MarketDataError,
    _instant,
    _parse_utc_exact,
    _decimal,
    validate_market_snapshot,
)


FORMULA_VERSION = "g015-objective-0.1.0"


def _format_decimal(value: Decimal) -> str:
    """Canonical non-exponent decimal string used by G014 observations."""

    if value == 0:
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"
    return text


def _contiguous(previous: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    previous_end = _instant(previous.get("end_at"))
    current_start = _instant(current.get("start_at"))
    return previous_end is not None and current_start is not None and previous_end == current_start


def _lookbacks(value: Iterable[int]) -> tuple[int, ...]:
    if isinstance(value, (str, bytes)):
        raise ValueError("lookbacks must be positive integer values")
    try:
        values = list(value)
    except TypeError as exc:
        raise ValueError("lookbacks must be positive integer values") from exc
    if any(not isinstance(item, int) or isinstance(item, bool) or item < 1 for item in values):
        raise ValueError("lookbacks must be positive integer values")
    return tuple(sorted(set(values)))


def _decimal_precision(bars: Iterable[Mapping[str, Any]]) -> int:
    total = 64
    for bar in bars:
        for name in ("open", "high", "low", "close", "volume"):
            value = bar.get(name)
            if isinstance(value, str):
                total += len(value.replace("-", "").replace(".", "")) + 4
    return max(128, total)


def _quality(snapshot: Mapping[str, Any], assessment: MarketAssessment) -> tuple[dict[str, Any], dict[str, Any]]:
    source = snapshot.get("quality") if isinstance(snapshot.get("quality"), Mapping) else {}
    quality_codes: list[str] = []
    warnings: list[str] = []
    for value in source.get("codes", []) if isinstance(source.get("codes"), list) else []:
        if value in {"invalid_market_data", "insufficient_history", "partial_data", "stale_data", "incomplete_last_bar_excluded"} and value not in quality_codes:
            quality_codes.append(value)
    for value in source.get("warnings", []) if isinstance(source.get("warnings"), list) else []:
        if value in {"partial_data", "stale_data", "incomplete_last_bar_excluded"} and value not in warnings:
            warnings.append(value)
    for outcome in assessment.outcomes:
        if outcome.code in {"invalid_market_data", "insufficient_history", "partial_data", "stale_data", "incomplete_last_bar_excluded"} and outcome.code not in quality_codes:
            quality_codes.append(outcome.code)
        for warning in outcome.warning_codes:
            if warning not in warnings:
                warnings.append(warning)
    # Allowed partial/incomplete outcomes remain usable but are represented as
    # quality metadata on the frame.  A snapshot's own status is preserved.
    status = "partial" if source.get("status") == "partial" or "partial_data" in quality_codes or "partial_data" in warnings else "ok"
    details = source.get("details") if isinstance(source.get("details"), str) else None
    quality = {"status": status, "codes": quality_codes, "warnings": warnings, "details": details}
    uncertainty_codes = [code for code in warnings if code in {"partial_data", "stale_data", "incomplete_last_bar_excluded"}]
    if assessment.uncertainty == "high":
        level = "high"
    elif assessment.uncertainty == "medium":
        level = "medium"
    else:
        level = "low"
    uncertainty = {
        "level": level,
        "codes": uncertainty_codes,
        "details": "deterministic market assessment" if uncertainty_codes else "deterministic observations",
    }
    return quality, uncertainty


def _range_relation(previous: Mapping[str, Any] | None, current: Mapping[str, Any]) -> str:
    if previous is None or not _contiguous(previous, current):
        return "none"
    previous_high, previous_low = _decimal(previous.get("high")), _decimal(previous.get("low"))
    current_high, current_low = _decimal(current.get("high")), _decimal(current.get("low"))
    if None in (previous_high, previous_low, current_high, current_low):
        return "none"
    assert previous_high is not None and previous_low is not None and current_high is not None and current_low is not None
    if current_low >= previous_low and current_high <= previous_high and (current_low > previous_low or current_high < previous_high):
        return "inside"
    if current_low <= previous_low and current_high >= previous_high and (current_low < previous_low or current_high > previous_high):
        return "outside"
    if current_low > previous_high or current_high < previous_low:
        return "none"
    return "overlap"


def _gap_relation(previous: Mapping[str, Any] | None, current: Mapping[str, Any]) -> str:
    if previous is None or not _contiguous(previous, current):
        return "none"
    previous_high, previous_low = _decimal(previous.get("high")), _decimal(previous.get("low"))
    current_high, current_low = _decimal(current.get("high")), _decimal(current.get("low"))
    if None in (previous_high, previous_low, current_high, current_low):
        return "none"
    assert previous_high is not None and previous_low is not None and current_high is not None and current_low is not None
    if current_low > previous_high:
        return "up"
    if current_high < previous_low:
        return "down"
    return "none"


def _rolling(closed: list[Mapping[str, Any]], index: int, lookback: int) -> dict[str, Any]:
    preceding: list[Mapping[str, Any]] = []
    cursor = index - 1
    while cursor >= 0:
        if not _contiguous(closed[cursor], closed[cursor + 1]):
            break
        preceding.append(closed[cursor])
        cursor -= 1
        if len(preceding) == lookback:
            break
    if len(preceding) < lookback:
        return {"lookback": lookback, "status": "insufficient_history", "high": None, "low": None}
    highs = [_decimal(bar.get("high")) for bar in preceding]
    lows = [_decimal(bar.get("low")) for bar in preceding]
    if any(value is None for value in highs + lows):
        return {"lookback": lookback, "status": "insufficient_history", "high": None, "low": None}
    assert all(value is not None for value in highs + lows)
    return {
        "lookback": lookback,
        "status": "available",
        "high": _format_decimal(max(highs)),
        "low": _format_decimal(min(lows)),
    }


def build_observation_frame(
    snapshot: Mapping[str, Any],
    assessment: MarketAssessment,
    *,
    lookbacks: Iterable[int] = (2,),
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a canonical G014 observation frame from an accepted assessment."""

    if not isinstance(snapshot, Mapping):
        raise MarketDataError("malformed_ohlcv", "market snapshot must be an object")
    if not isinstance(assessment, MarketAssessment):
        raise TypeError("assessment must be a MarketAssessment")
    if assessment.terminal_outcome is not None or not assessment.accepted or not assessment.model_call_allowed:
        condition = assessment.terminal_outcome.condition if assessment.terminal_outcome else "invalid_market_data"
        raise MarketDataError(condition, "terminal market assessment cannot produce observations", issues=assessment.issues)
    if assessment.snapshot_id != snapshot.get("market_snapshot_id") or assessment.snapshot_hash != snapshot.get("content_hash"):
        raise MarketDataError("unresolvable_lineage", "assessment does not match market snapshot identity", issues=assessment.issues)
    snapshot_result = validate_market_snapshot(snapshot)
    if not snapshot_result.valid:
        raise MarketDataError("malformed_ohlcv", "market snapshot failed validation", issues=tuple(snapshot_result.issues))
    if generated_at is not None and _parse_utc_exact(generated_at) is None:
        raise ValueError("generated_at must be an exact UTC RFC3339 timestamp")
    normalized_lookbacks = _lookbacks(lookbacks)
    bars = snapshot.get("bars") if isinstance(snapshot.get("bars"), list) else []
    # Exclude incomplete final bar before every objective formula.
    closed = [bar for bar in bars if isinstance(bar, Mapping) and bar.get("is_closed") is True]
    if not closed:
        raise MarketDataError("insufficient_closed_history", "no closed bars are available", issues=assessment.issues)
    if tuple(bar.get("bar_id") for bar in closed) != tuple(bar.get("bar_id") for bar in assessment.closed_bars):
        raise MarketDataError("unresolvable_lineage", "assessment closed-bar set does not match snapshot", issues=assessment.issues)

    quality, uncertainty = _quality(snapshot, assessment)
    observations: list[dict[str, Any]] = []
    with localcontext() as context:
        context.prec = _decimal_precision(closed)
        for index, bar in enumerate(closed):
            open_value = _decimal(bar.get("open"))
            high_value = _decimal(bar.get("high"))
            low_value = _decimal(bar.get("low"))
            close_value = _decimal(bar.get("close"))
            if None in (open_value, high_value, low_value, close_value):
                raise MarketDataError("malformed_ohlcv", "closed bar contains invalid decimal values", path=f"$.bars[{index}]")
            assert open_value is not None and high_value is not None and low_value is not None and close_value is not None
            direction = "bullish" if close_value > open_value else "bearish" if close_value < open_value else "doji"
            range_value = high_value - low_value
            body_value = abs(close_value - open_value)
            upper_value = high_value - max(open_value, close_value)
            lower_value = min(open_value, close_value) - low_value
            previous = closed[index - 1] if index > 0 else None
            rolling_windows = [_rolling(closed, index, lookback) for lookback in normalized_lookbacks]
            observation = {
                "observation_id": observation_id_for(snapshot.get("market_snapshot_id"), bar.get("bar_id"), FORMULA_VERSION),
                "bar_id": bar.get("bar_id"),
                "direction": direction,
                "range": _format_decimal(range_value),
                "body": _format_decimal(body_value),
                "upper_wick": _format_decimal(upper_value),
                "lower_wick": _format_decimal(lower_value),
                "range_relation": _range_relation(previous, bar),
                "gap_relation": _gap_relation(previous, bar),
                "rolling_windows": rolling_windows,
                "quality": copy.deepcopy(quality),
                "uncertainty": copy.deepcopy(uncertainty),
            }
            observations.append(observation)

    extensions: dict[str, Any] = {}
    source_extensions = snapshot.get("extensions")
    if isinstance(source_extensions, Mapping) and "market_feed" in source_extensions:
        extensions["market_feed"] = copy.deepcopy(source_extensions["market_feed"])
    frame: dict[str, Any] = {
        "schema_version": "0.1.0",
        "artifact_kind": "market_observation_frame",
        "market_snapshot_id": snapshot.get("market_snapshot_id"),
        "market_snapshot_hash": snapshot.get("content_hash"),
        "formula_version": FORMULA_VERSION,
        "rounding_mode": "exact_decimal",
        "observations": observations,
        "quality": quality,
        "uncertainty": uncertainty,
        "extensions": extensions,
    }
    if generated_at is not None:
        frame["generated_at"] = generated_at
    finalized = finalize_artifact(frame, "market_observation_frame")
    related_result = validate_artifact(finalized, expected_kind="market_observation_frame", related_artifacts={"market_snapshot": snapshot})
    if not related_result.valid:
        raise MarketDataError("unresolvable_lineage", "generated observation frame failed related-artifact validation", issues=tuple(related_result.issues))
    return finalized


__all__ = ["FORMULA_VERSION", "build_observation_frame"]
