"""Exact market validation and frozen policy assessment for G015.

The G014 contract validator remains the first schema/identity pass.  This
module adds market-specific checks that need nanosecond timestamp comparison,
provider-feed expectations and the deterministic frozen policy matrix.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pa_engine.contracts import (
    CanonicalizationError,
    REQUIRED_POLICY_OUTCOMES,
    ValidationIssue,
    ValidationResult,
    bar_id_for,
    validate_artifact,
)

from .feed import MarketFeedRef, extract_market_feed


UTC_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$")
DECIMAL_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
BAR_FIELDS = {"bar_id", "start_at", "end_at", "open", "high", "low", "close", "volume", "is_closed"}
INSTRUMENT_FIELDS = {"asset_class", "symbol", "venue", "currency"}
TIMEFRAME_FIELDS = {"unit", "multiplier"}
SESSION_FIELDS = {"kind", "timezone"}
SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,11}$")
BAR_ID_RE = re.compile(r"^bar_[0-9a-f]{64}$")
IANA_TIMEZONE_RE = re.compile(r"^(?!Etc/GMT[+-])(?:UTC|GMT|[A-Za-z][A-Za-z0-9._+-]*(?:/[A-Za-z0-9._+-]+)+)$")
_NS_PER_SECOND = 1_000_000_000
_NS_PER_MINUTE = 60 * _NS_PER_SECOND
_NS_PER_HOUR = 60 * _NS_PER_MINUTE
_NS_PER_DAY = 24 * _NS_PER_HOUR
_NS_PER_WEEK = 7 * _NS_PER_DAY


class _FrozenTuple(tuple):
    """Immutable sequence whose equality is friendly to JSON-style lists."""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (tuple, list)):
            return tuple(self) == tuple(other)
        return False

    __hash__ = tuple.__hash__


@dataclass(frozen=True, slots=True)
class _UtcInstant:
    text: str
    nanoseconds: int


def _parse_utc_exact(value: Any) -> _UtcInstant | None:
    """Parse G014's UTC grammar without truncating fractional nanoseconds."""

    if not isinstance(value, str):
        return None
    match = UTC_RE.fullmatch(value)
    if match is None:
        return None
    year, month, day, hour, minute, second = (int(match.group(i)) for i in range(1, 7))
    fraction = (match.group(7) or "").ljust(9, "0")
    try:
        # Construct only whole-second UTC time; fractional digits are added as
        # an integer so no datetime or float microsecond truncation occurs.
        stamp = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = stamp - epoch
    except (TypeError, ValueError, OverflowError):
        return None
    whole_seconds = delta.days * 86_400 + delta.seconds
    return _UtcInstant(value, whole_seconds * _NS_PER_SECOND + int(fraction or "0"))


def _instant(value: Any) -> int | None:
    parsed = _parse_utc_exact(value)
    return parsed.nanoseconds if parsed else None


def _issue(code: str, path: str, message: str) -> ValidationIssue:
    return ValidationIssue(code, path or "$", message)


def _dedupe(issues: Iterable[ValidationIssue]) -> tuple[ValidationIssue, ...]:
    seen: set[tuple[str, str, str]] = set()
    result: list[ValidationIssue] = []
    for issue in issues:
        key = (issue.code, issue.path, issue.message)
        if key not in seen:
            seen.add(key)
            result.append(issue)
    return tuple(result)


def _result(kind: str, issues: Iterable[ValidationIssue]) -> ValidationResult:
    unique = list(_dedupe(issues))
    return ValidationResult(not unique, kind, unique)


def _decimal(value: Any) -> Decimal | None:
    if not isinstance(value, str) or value == "-0" or not DECIMAL_RE.fullmatch(value):
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _feed_issue_result(
    document: Mapping[str, Any],
    *,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None,
    require_feed: bool,
) -> tuple[MarketFeedRef | None, tuple[ValidationIssue, ...]]:
    _feed, feed_result = extract_market_feed(document, expected=expected_feed, require_feed=require_feed)
    return _feed, tuple(feed_result.issues)


def _timeframe_duration_ns(timeframe: Any) -> int | None:
    if not isinstance(timeframe, Mapping):
        return None
    unit, multiplier = timeframe.get("unit"), timeframe.get("multiplier")
    if not isinstance(multiplier, int) or isinstance(multiplier, bool) or multiplier < 1:
        return None
    return {"minute": _NS_PER_MINUTE, "hour": _NS_PER_HOUR}.get(unit, 0) * multiplier or None


def _coverage_semantic_issues(document: Mapping[str, Any]) -> list[ValidationIssue]:
    """Validate coverage using exact nanosecond interval arithmetic.

    G014's generic semantic pass is intentionally reused, but its datetime
    helper only has microsecond precision.  These checks therefore repeat the
    coverage invariants with integer nanoseconds and reject an unreported gap,
    fabricated missing interval or interval outside the declared expected
    window before policy assessment.
    """

    coverage = document.get("coverage")
    if not isinstance(coverage, Mapping):
        return [_issue("malformed_ohlcv", "$.coverage", "coverage must be an object")]
    expected = coverage.get("expected")
    actual = coverage.get("actual")
    status = coverage.get("status")
    missing_values = coverage.get("missing_intervals")
    issues: list[ValidationIssue] = []

    def interval(value: Any, path: str) -> tuple[int, int] | None:
        if not isinstance(value, Mapping):
            issues.append(_issue("malformed_ohlcv", path, "coverage interval must be an object"))
            return None
        start, end = _instant(value.get("start_at")), _instant(value.get("end_at"))
        if start is None or end is None:
            issues.append(_issue("malformed_ohlcv", path, "coverage interval timestamps must be exact UTC values"))
            return None
        if end <= start:
            issues.append(_issue("malformed_ohlcv", path, "coverage interval must have positive duration"))
            return None
        return start, end

    expected_interval = interval(expected, "$.coverage.expected")
    actual_interval = interval(actual, "$.coverage.actual")
    if not isinstance(missing_values, list):
        issues.append(_issue("malformed_ohlcv", "$.coverage.missing_intervals", "missing_intervals must be an array"))
        missing_values = []
    missing: list[tuple[int, int, int]] = []
    for index, value in enumerate(missing_values):
        parsed = interval(value, f"$.coverage.missing_intervals[{index}]")
        if parsed is not None:
            missing.append((parsed[0], parsed[1], index))

    bars = document.get("bars")
    bar_intervals: list[tuple[int, int, int]] = []
    if not isinstance(bars, list) or not bars:
        issues.append(_issue("malformed_ohlcv", "$.bars", "coverage requires at least one bar"))
    else:
        for index, bar in enumerate(bars):
            if not isinstance(bar, Mapping):
                continue
            start, end = _instant(bar.get("start_at")), _instant(bar.get("end_at"))
            if start is not None and end is not None and end > start:
                bar_intervals.append((start, end, index))

    if expected_interval is not None and actual_interval is not None:
        expected_start, expected_end = expected_interval
        actual_start, actual_end = actual_interval
        if bar_intervals and (actual_start != bar_intervals[0][0] or actual_end != bar_intervals[-1][1]):
            issues.append(_issue("malformed_ohlcv", "$.coverage.actual", "actual coverage must equal first/last bar boundaries"))
        if actual_start < expected_start or actual_end > expected_end:
            issues.append(_issue("malformed_ohlcv", "$.coverage.actual", "actual coverage must lie within expected coverage"))

        # Every bar must itself lie in the expected interval.  This also keeps
        # bars from being hidden outside an otherwise plausible first/last
        # boundary pair.
        for start, end, index in bar_intervals:
            if start < expected_start or end > expected_end:
                issues.append(_issue("malformed_ohlcv", f"$.bars[{index}]", "bar interval lies outside expected coverage"))

        previous_missing_end: int | None = None
        for start, end, index in missing:
            if start < expected_start or end > expected_end:
                issues.append(_issue("malformed_ohlcv", f"$.coverage.missing_intervals[{index}]", "missing interval lies outside expected coverage"))
            if previous_missing_end is not None and start < previous_missing_end:
                issues.append(_issue("malformed_ohlcv", f"$.coverage.missing_intervals[{index}]", "missing intervals overlap or are out of order"))
            previous_missing_end = end
            if any(start < bar_end and end > bar_start for bar_start, bar_end, _ in bar_intervals):
                issues.append(_issue("malformed_ohlcv", f"$.coverage.missing_intervals[{index}]", "missing interval overlaps a bar"))

        # Build the exact union.  Bars are already required to be ordered by
        # the market pass; still sort only for a deterministic coverage check.
        union = [(start, end, "bar", index) for start, end, index in bar_intervals]
        union.extend((start, end, "missing", index) for start, end, index in missing)
        union.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        if union:
            cursor = expected_start
            for start, end, source, index in union:
                if start != cursor:
                    issues.append(_issue("malformed_ohlcv", "$.coverage", "coverage union has an unexplained gap or overlap"))
                    break
                cursor = end
            if cursor != expected_end:
                issues.append(_issue("malformed_ohlcv", "$.coverage", "coverage union does not reach expected end"))
        else:
            issues.append(_issue("malformed_ohlcv", "$.coverage", "coverage union is empty"))

        if status == "complete" and missing:
            issues.append(_issue("malformed_ohlcv", "$.coverage.missing_intervals", "complete coverage must not declare missing intervals"))
        if status == "partial" and not missing:
            issues.append(_issue("malformed_ohlcv", "$.coverage.missing_intervals", "partial coverage must declare missing intervals"))
    return issues


def _quality_consistency_issues(document: Mapping[str, Any]) -> list[ValidationIssue]:
    """Reject contradictory producer quality metadata before policy use."""

    quality = document.get("quality")
    if not isinstance(quality, Mapping):
        return []
    codes = {value for value in quality.get("codes", []) if isinstance(value, str)} if isinstance(quality.get("codes"), list) else set()
    warnings = {value for value in quality.get("warnings", []) if isinstance(value, str)} if isinstance(quality.get("warnings"), list) else set()
    issues: list[ValidationIssue] = []
    if quality.get("status") == "invalid" or "invalid_market_data" in codes:
        issues.append(_issue("malformed_ohlcv", "$.quality", "invalid market quality cannot enter policy assessment"))
    coverage = document.get("coverage")
    coverage_status = coverage.get("status") if isinstance(coverage, Mapping) else None
    if ("partial_data" in codes or "partial_data" in warnings) and coverage_status != "partial":
        issues.append(_issue("malformed_ohlcv", "$.quality", "partial_data quality requires partial coverage"))
    if "incomplete_last_bar_excluded" in codes or "incomplete_last_bar_excluded" in warnings:
        bars = document.get("bars")
        final_incomplete = bool(isinstance(bars, list) and bars and isinstance(bars[-1], Mapping) and bars[-1].get("is_closed") is False)
        if not final_incomplete:
            issues.append(_issue("malformed_ohlcv", "$.quality", "incomplete_last_bar_excluded requires an incomplete final bar"))
    return issues


def _snapshot_semantic_issues(
    document: Mapping[str, Any],
    *,
    check_ids: bool = True,
    check_timeframe: bool = True,
    check_coverage: bool = True,
) -> tuple[list[ValidationIssue], set[str]]:
    """Run exact timestamp, duplicate, ordering, duration and OHLC checks."""

    issues: list[ValidationIssue] = []
    conditions: set[str] = set()
    bars = document.get("bars")
    if not isinstance(bars, list):
        return [_issue("malformed_ohlcv", "$.bars", "bars must be an array")], {"malformed_ohlcv"}
    if not bars:
        return [_issue("malformed_ohlcv", "$.bars", "bars must contain at least one bar")], {"malformed_ohlcv"}

    as_of = _instant(document.get("analysis_as_of"))
    duration = _timeframe_duration_ns(document.get("timeframe"))
    timeframe = document.get("timeframe")
    if isinstance(timeframe, Mapping) and timeframe.get("unit") == "second":
        issues.append(_issue("malformed_ohlcv", "$.timeframe.unit", "direct second timeframe is not in G014"))
        conditions.add("malformed_ohlcv")
    # A malformed timeframe (including a zero/non-integer multiplier) is
    # structural market input failure even when JSON schema reports it too.
    timeframe_valid = (
        isinstance(timeframe, Mapping)
        and timeframe.get("unit") in {"minute", "hour", "day", "week"}
        and isinstance(timeframe.get("multiplier"), int)
        and not isinstance(timeframe.get("multiplier"), bool)
        and timeframe.get("multiplier", 0) >= 1
    )
    if check_timeframe and not timeframe_valid:
        issues.append(_issue("malformed_ohlcv", "$.timeframe", "timeframe must be a G014 minute/hour/day/week multiplier"))
        conditions.add("malformed_ohlcv")

    seen_ids: dict[Any, int] = {}
    seen_intervals: dict[tuple[int, int], int] = {}
    previous_start: int | None = None
    previous_end: int | None = None
    for index, bar in enumerate(bars):
        path = f"$.bars[{index}]"
        if not isinstance(bar, Mapping):
            issues.append(_issue("malformed_ohlcv", path, "bar must be an object"))
            conditions.add("malformed_ohlcv")
            continue
        start = _parse_utc_exact(bar.get("start_at"))
        end = _parse_utc_exact(bar.get("end_at"))
        if start is None or end is None:
            issues.append(_issue("malformed_ohlcv", path, "bar timestamps must be real UTC RFC3339 values"))
            conditions.add("malformed_ohlcv")
            continue
        start_ns, end_ns = start.nanoseconds, end.nanoseconds
        if end_ns <= start_ns:
            issues.append(_issue("malformed_ohlcv", path, "bar interval must have positive duration"))
            conditions.add("malformed_ohlcv")
        if duration is not None and end_ns - start_ns != duration:
            issues.append(_issue("out_of_order_bars", path + ".end_at", "bar duration does not match timeframe"))
            conditions.add("out_of_order_bars")
        if previous_start is not None and start_ns <= previous_start:
            code = "duplicate_bars" if start_ns == previous_start else "out_of_order_bars"
            issues.append(_issue(code, path + ".start_at", "bars must have strictly increasing instant starts"))
            conditions.add(code)
        if previous_end is not None and start_ns < previous_end:
            issues.append(_issue("out_of_order_bars", path + ".start_at", "bar intervals overlap"))
            conditions.add("out_of_order_bars")
        previous_start, previous_end = start_ns, end_ns
        interval = (start_ns, end_ns)
        if interval in seen_intervals:
            issues.append(_issue("duplicate_bars", path, "duplicate bar interval"))
            conditions.add("duplicate_bars")
        seen_intervals[interval] = index
        bar_id = bar.get("bar_id")
        if not isinstance(bar_id, str):
            issues.append(_issue("malformed_ohlcv", path + ".bar_id", "bar_id must be a canonical string"))
            conditions.add("malformed_ohlcv")
        else:
            if bar_id in seen_ids:
                issues.append(_issue("duplicate_bars", path + ".bar_id", "duplicate bar identity"))
                conditions.add("duplicate_bars")
            seen_ids[bar_id] = index
        values = {name: _decimal(bar.get(name)) for name in ("open", "high", "low", "close", "volume")}
        if any(value is None for value in values.values()):
            issues.append(_issue("malformed_ohlcv", path, "OHLCV values must be canonical finite decimals"))
            conditions.add("malformed_ohlcv")
        else:
            assert all(value is not None for value in values.values())
            if (
                values["open"] <= 0
                or values["high"] <= 0
                or values["low"] <= 0
                or values["close"] <= 0
                or values["volume"] < 0
                or values["high"] < max(values["open"], values["close"])
                or values["low"] > min(values["open"], values["close"])
                or values["high"] < values["low"]
            ):
                issues.append(_issue("ohlc_invariant", path, "OHLC invariant or non-negative volume violated"))
                conditions.add("ohlc_invariant")
        if as_of is not None and end_ns > as_of:
            issues.append(_issue("future_bars", path + ".end_at", "bar ends after analysis_as_of"))
            conditions.add("future_bars")
        try:
            expected_bar_id = bar_id_for(document.get("instrument"), document.get("timeframe"), document.get("session"), bar.get("start_at"), bar.get("end_at"))
        except (CanonicalizationError, TypeError, ValueError):
            expected_bar_id = None
        if check_ids and expected_bar_id is None:
            issues.append(_issue("malformed_ohlcv", path + ".bar_id", "bar identity context is not canonical JSON"))
            conditions.add("malformed_ohlcv")
        elif check_ids and bar.get("bar_id") != expected_bar_id:
            # Identity mismatch is structural malformed input, not an OHLC
            # quality code.  It must terminate before any provider call.
            issues.append(_issue("malformed_ohlcv", path + ".bar_id", "bar_id does not match canonical helper identity"))
            conditions.add("malformed_ohlcv")

    # Duplicate semantic instants may use lexically different UTC strings;
    # compare their parsed nanoseconds explicitly.
    starts: dict[int, int] = {}
    for index, bar in enumerate(bars):
        if not isinstance(bar, Mapping):
            continue
        start = _instant(bar.get("start_at"))
        if start is not None:
            if start in starts:
                issues.append(_issue("duplicate_bars", f"$.bars[{index}].start_at", "duplicate bar start instant"))
                conditions.add("duplicate_bars")
            starts[start] = index
    coverage_issues = _coverage_semantic_issues(document) if check_coverage else []
    issues.extend(coverage_issues)
    # A duplicate/overlap/order/OHLC/future bar is classified by its exact
    # market structural outcome.  Coverage's union check may also observe the
    # same overlap; do not let that secondary diagnostic mask the primary row.
    primary_bar_failure = conditions.intersection({"ohlc_invariant", "duplicate_bars", "out_of_order_bars", "future_bars"})
    if coverage_issues and not primary_bar_failure:
        conditions.add("malformed_ohlcv")
    quality_issues = _quality_consistency_issues(document)
    issues.extend(quality_issues)
    if quality_issues:
        conditions.add("malformed_ohlcv")
    return issues, conditions


def validate_market_snapshot(
    snapshot: Any,
    *,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_ref: MarketFeedRef | Mapping[str, Any] | None = None,
    require_feed: bool = False,
    expected_adjustment_policy: str | None = None,
    expected_adjustment: str | None = None,
) -> ValidationResult:
    """Validate a G014 market snapshot plus exact market/feed semantics."""

    if expected_feed is None:
        expected_feed = expected_ref
    if expected_adjustment_policy is None:
        expected_adjustment_policy = expected_adjustment
    try:
        base = validate_artifact(snapshot, expected_kind="market_snapshot")
    except (CanonicalizationError, TypeError, ValueError, KeyError, AttributeError, IndexError):
        # External producer mappings must never leak canonicalization tracebacks
        # through the market validator.
        base = ValidationResult(False, "market_snapshot", [_issue("malformed_ohlcv", "$", "market snapshot cannot be canonically validated")])
    issues: list[ValidationIssue] = list(base.issues)
    if not isinstance(snapshot, Mapping):
        return _result("market_snapshot", issues + [_issue("malformed_ohlcv", "$", "market snapshot must be an object")])
    _feed, feed_issues = _feed_issue_result(snapshot, expected_feed=expected_feed, require_feed=require_feed)
    issues.extend(feed_issues)
    if expected_adjustment_policy is not None and snapshot.get("adjustment_policy") != expected_adjustment_policy:
        issues.append(_issue("adjustment_policy_mismatch", "$.adjustment_policy", "snapshot adjustment policy does not match expectation"))
    semantic, _conditions = _snapshot_semantic_issues(snapshot)
    issues.extend(semantic)
    return _result("market_snapshot", issues)


def validate_analysis_policy(
    policy: Any,
    *,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None = None,
) -> ValidationResult:
    """Validate a G014 analysis policy without requiring feed metadata.

    A policy may omit ``extensions.market_feed``.  If it supplies one, the
    value is still strict and can be checked against an optional expectation.
    """

    result = validate_artifact(policy, expected_kind="analysis_policy")
    issues: list[ValidationIssue] = list(result.issues)
    if isinstance(policy, Mapping):
        extensions = policy.get("extensions")
        if isinstance(extensions, Mapping) and "market_feed" in extensions:
            _feed, feed_result = extract_market_feed(policy, expected=expected_feed, require_feed=True)
            issues.extend(feed_result.issues)
    return _result("analysis_policy", issues)


def validate_analysis_request(
    request: Any,
    *,
    snapshot: Mapping[str, Any] | None = None,
    policy: Mapping[str, Any] | None = None,
    market_snapshot: Mapping[str, Any] | None = None,
    analysis_policy: Mapping[str, Any] | None = None,
    related_artifacts: Any = None,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None = None,
    require_feed: bool = False,
) -> ValidationResult:
    """Validate a request and optional cross-lineage snapshot/policy pins."""

    related: dict[str, Mapping[str, Any]] = {}
    snapshot = snapshot or market_snapshot
    policy = policy or analysis_policy
    if isinstance(related_artifacts, Mapping):
        snapshot = snapshot or related_artifacts.get("market_snapshot")
        policy = policy or related_artifacts.get("analysis_policy")
    if snapshot is not None:
        related["market_snapshot"] = snapshot
    if policy is not None:
        related["analysis_policy"] = policy
    base = validate_artifact(request, expected_kind="analysis_request", related_artifacts=related)
    issues: list[ValidationIssue] = list(base.issues)
    if snapshot is not None and isinstance(request, Mapping):
        for field_name in ("market_snapshot_id", "analysis_as_of"):
            if request.get(field_name) != snapshot.get(field_name):
                issues.append(_issue("cross_lineage_mismatch", f"$.{field_name}", "request does not match market snapshot"))
        for field_name in ("instrument", "timeframe", "session"):
            if field_name in request and request.get(field_name) != snapshot.get(field_name):
                issues.append(_issue("cross_lineage_mismatch", f"$.{field_name}", "request context does not match market snapshot"))
        feed_ref, feed_issues = _feed_issue_result(snapshot, expected_feed=expected_feed, require_feed=require_feed)
        issues.extend(feed_issues)
        if expected_feed is not None and feed_ref is None:
            issues.append(_issue("cross_lineage_mismatch", "$.market_snapshot_id", "market snapshot feed expectation is unresolved"))
    if policy is not None and isinstance(request, Mapping) and request.get("analysis_policy_id") != policy.get("analysis_policy_id"):
        issues.append(_issue("cross_lineage_mismatch", "$.analysis_policy_id", "request does not match analysis policy"))
    # Request feed metadata is optional, but when supplied it is a typed
    # hash-bound assertion and must match the snapshot (and any expectation).
    if isinstance(request, Mapping):
        extensions = request.get("extensions")
        if isinstance(extensions, Mapping) and "market_feed" in extensions:
            request_feed, request_feed_result = extract_market_feed(request, expected=expected_feed, require_feed=True)
            issues.extend(request_feed_result.issues)
            if snapshot is not None:
                snapshot_feed, snapshot_feed_result = extract_market_feed(snapshot, require_feed=False)
                if not snapshot_feed_result.valid:
                    issues.extend(snapshot_feed_result.issues)
                elif snapshot_feed is None or request_feed is None or request_feed != snapshot_feed:
                    issues.append(_issue("market_feed_mismatch", "$.extensions.market_feed", "request feed does not match market snapshot feed"))
    return _result("analysis_request", issues)


def _closed_window_context_issues(
    bars: list[Any],
    *,
    analysis_as_of: str | None,
    timeframe: Mapping[str, Any] | None,
    instrument: Mapping[str, Any] | None,
    session: Mapping[str, Any] | None,
) -> list[ValidationIssue]:
    """Validate standalone list shape/context without a full snapshot shell."""

    issues: list[ValidationIssue] = []
    if analysis_as_of is not None and _parse_utc_exact(analysis_as_of) is None:
        issues.append(_issue("malformed_ohlcv", "$.analysis_as_of", "analysis_as_of must be an exact UTC timestamp"))

    def exact_object(value: Any, fields: set[str], path: str, label: str) -> Mapping[str, Any] | None:
        if not isinstance(value, Mapping):
            issues.append(_issue("malformed_ohlcv", path, f"{label} must be an object"))
            return None
        if set(value) != fields:
            issues.append(_issue("malformed_ohlcv", path, f"{label} must contain exactly its G014 fields"))
        return value

    if instrument is not None:
        value = exact_object(instrument, INSTRUMENT_FIELDS, "$.instrument", "instrument")
        if value is not None:
            symbol = value.get("symbol")
            if value.get("asset_class") != "us_equity" or not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol) or not isinstance(value.get("venue"), str) or not value.get("venue") or value.get("currency") != "USD":
                issues.append(_issue("malformed_ohlcv", "$.instrument", "instrument is not a valid G014 us_equity value"))

    if timeframe is not None:
        value = exact_object(timeframe, TIMEFRAME_FIELDS, "$.timeframe", "timeframe")
        if value is not None:
            unit, multiplier = value.get("unit"), value.get("multiplier")
            if unit not in {"minute", "hour", "day", "week"} or not isinstance(multiplier, int) or isinstance(multiplier, bool) or multiplier < 1:
                issues.append(_issue("malformed_ohlcv", "$.timeframe", "timeframe is not a valid G014 value"))

    if session is not None:
        value = exact_object(session, SESSION_FIELDS, "$.session", "session")
        if value is not None:
            timezone_name = value.get("timezone")
            timezone_valid = False
            if isinstance(timezone_name, str):
                try:
                    ZoneInfo(timezone_name)
                    timezone_valid = IANA_TIMEZONE_RE.fullmatch(timezone_name) is not None
                except (ZoneInfoNotFoundError, ValueError):
                    timezone_valid = False
            if value.get("kind") not in {"regular", "extended", "all"} or not timezone_valid:
                issues.append(_issue("malformed_ohlcv", "$.session", "session is not a valid G014 value"))

    for index, bar in enumerate(bars):
        path = f"$.bars[{index}]"
        if not isinstance(bar, Mapping):
            continue
        if set(bar) != BAR_FIELDS:
            issues.append(_issue("malformed_ohlcv", path, "bar must contain exactly the G014 bar fields"))
        bar_id = bar.get("bar_id")
        if not isinstance(bar_id, str) or not BAR_ID_RE.fullmatch(bar_id):
            issues.append(_issue("malformed_ohlcv", path + ".bar_id", "bar_id must match the frozen bar identity pattern"))
        if not isinstance(bar.get("is_closed"), bool):
            issues.append(_issue("malformed_ohlcv", path + ".is_closed", "is_closed must be boolean"))
    return issues


def validate_closed_bar_window(
    snapshot_or_bars: Any,
    *,
    analysis_as_of: str | None = None,
    timeframe: Mapping[str, Any] | None = None,
    instrument: Mapping[str, Any] | None = None,
    session: Mapping[str, Any] | None = None,
) -> ValidationResult:
    """Validate a closed-bar sequence using G014's exact bar semantics."""

    if isinstance(snapshot_or_bars, Mapping):
        result = validate_market_snapshot(snapshot_or_bars)
        extra: list[ValidationIssue] = list(result.issues)
        bars = snapshot_or_bars.get("bars") if isinstance(snapshot_or_bars.get("bars"), list) else []
        for index, bar in enumerate(bars):
            if isinstance(bar, Mapping) and bar.get("is_closed") is not True:
                extra.append(_issue("incomplete_final_bar", f"$.bars[{index}].is_closed", "closed-bar window cannot contain an incomplete bar"))
        return _result("market_snapshot", extra)
    bars = snapshot_or_bars
    if not isinstance(bars, list):
        return _result("market_snapshot", [_issue("malformed_ohlcv", "$.bars", "closed-bar window must be an array")])
    context_issues = _closed_window_context_issues(
        bars,
        analysis_as_of=analysis_as_of,
        timeframe=timeframe,
        instrument=instrument,
        session=session,
    )
    # Build a minimal semantic shell; no identity is claimed for this helper
    # unless all three optional G014 contexts are supplied and valid-looking.
    shell: dict[str, Any] = {
        "artifact_kind": "market_snapshot",
        "schema_version": "0.1.0",
        "instrument": instrument or {},
        # A standalone bar list may omit timeframe context; in that case do
        # not invent a one-minute duration.  When supplied, exact minute/hour
        # duration checks still apply.
        "timeframe": timeframe if timeframe is not None else {},
        "session": session or {},
        "analysis_as_of": analysis_as_of,
        "bars": bars,
    }
    semantic, _conditions = _snapshot_semantic_issues(
        shell,
        check_ids=instrument is not None and timeframe is not None and session is not None,
        check_timeframe=timeframe is not None,
        check_coverage=False,
    )
    semantic = context_issues + semantic
    for index, bar in enumerate(bars):
        if isinstance(bar, Mapping) and bar.get("is_closed") is not True:
            semantic.append(_issue("incomplete_final_bar", f"$.bars[{index}].is_closed", "closed-bar window cannot contain an incomplete bar"))
    return _result("market_snapshot", semantic)


def _outcome(condition: str) -> "PolicyOutcome":
    raw = REQUIRED_POLICY_OUTCOMES[condition]
    uncertainty = raw.get("uncertainty")
    return PolicyOutcome(
        condition=condition,
        stage=raw["stage"],
        disposition=raw["disposition"],
        expected_status=raw["status"],
        code=raw["code"],
        model_call_allowed=bool(raw["model_call_allowed"]),
        warning_codes=_FrozenTuple(raw.get("warning_codes", ())),
        uncertainty=uncertainty,
        exclude_incomplete_last_bar=bool(raw["exclude_incomplete_last_bar"]),
    )


def _invalid_market_outcome() -> "PolicyOutcome":
    """Internal pre-provider outcome for invalid policy/feed metadata.

    G014 intentionally has no persisted ``invalid_policy`` or ``missing_feed``
    quality code.  Keep the existing ``invalid_market_data`` contract code and
    expose the concrete typed issues alongside it.
    """

    return PolicyOutcome(
        condition="invalid_market_data",
        stage="market",
        disposition="terminal",
        expected_status="error",
        code="invalid_market_data",
        model_call_allowed=False,
        warning_codes=_FrozenTuple(),
        uncertainty=None,
        exclude_incomplete_last_bar=False,
    )


@dataclass(frozen=True, slots=True)
class PolicyOutcome:
    """One exact row from :data:`REQUIRED_POLICY_OUTCOMES`."""

    condition: str
    stage: str
    disposition: str
    expected_status: str | None
    code: str
    model_call_allowed: bool
    warning_codes: tuple[str, ...] = field(default_factory=_FrozenTuple)
    uncertainty: str | None = None
    exclude_incomplete_last_bar: bool = False

    @property
    def terminal(self) -> bool:
        return self.disposition == "terminal"

    @property
    def status(self) -> str | None:
        return self.expected_status

    @property
    def warnings(self) -> tuple[str, ...]:
        return self.warning_codes

    @property
    def uncertainty_level(self) -> str | None:
        return self.uncertainty

    def to_dict(self) -> dict[str, Any]:
        uncertainty: dict[str, Any] | None
        if self.uncertainty is None:
            uncertainty = None
        else:
            uncertainty = {"level": self.uncertainty, "codes": [], "details": "frozen policy outcome"}
        return {
            "condition": self.condition,
            "stage": self.stage,
            "disposition": self.disposition,
            "expected_status": self.expected_status,
            "code": self.code,
            "model_call_allowed": self.model_call_allowed,
            "warning_codes": list(self.warning_codes),
            "uncertainty": uncertainty,
            "exclude_incomplete_last_bar": self.exclude_incomplete_last_bar,
        }


@dataclass(frozen=True, slots=True)
class MarketDataError(ValueError):
    """Frozen typed error descriptor for callers that need an exception form."""

    condition: str
    message: str
    path: str = "$"
    issues: tuple[ValidationIssue, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class MarketAssessment:
    """Deterministic policy assessment exposed to observations and adapters."""

    snapshot_id: str | None
    snapshot_hash: str | None
    closed_bars: tuple[Mapping[str, Any], ...]
    issues: tuple[ValidationIssue, ...]
    outcomes: tuple[PolicyOutcome, ...]
    accepted: bool
    model_call_allowed: bool
    terminal_outcome: PolicyOutcome | None
    warnings: tuple[str, ...]
    uncertainty: str
    incomplete_final_bar: bool = False

    @property
    def outcome(self) -> PolicyOutcome | None:
        return self.terminal_outcome

    @property
    def terminal(self) -> PolicyOutcome | None:
        return self.terminal_outcome

    @property
    def warning_codes(self) -> tuple[str, ...]:
        return self.warnings

    @property
    def uncertainty_level(self) -> str:
        return self.uncertainty

    @property
    def error(self) -> MarketDataError | None:
        if self.terminal_outcome is None:
            return None
        first = self.issues[0] if self.issues else None
        return MarketDataError(self.terminal_outcome.condition, self.terminal_outcome.code, first.path if first else "$", self.issues)

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_hash": self.snapshot_hash,
            "closed_bars": [dict(bar) for bar in self.closed_bars],
            "issues": [issue.to_dict() for issue in self.issues],
            "outcomes": [outcome.to_dict() for outcome in self.outcomes],
            "accepted": self.accepted,
            "model_call_allowed": self.model_call_allowed,
            "terminal_outcome": self.terminal_outcome.to_dict() if self.terminal_outcome else None,
            "warnings": list(self.warnings),
            "uncertainty": self.uncertainty,
            "incomplete_final_bar": self.incomplete_final_bar,
        }


def _structural_condition(conditions: set[str]) -> str | None:
    # Matrix precedence is intentionally preserved.  Duplicate and ordering
    # checks are distinct from OHLC invariant and malformed shape checks.
    for condition in ("malformed_ohlcv", "ohlc_invariant", "duplicate_bars", "out_of_order_bars", "future_bars"):
        if condition in conditions:
            return condition
    return None


def assess_market(
    snapshot: Any,
    policy: Any,
    request: Mapping[str, Any] | None = None,
    *,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_ref: MarketFeedRef | Mapping[str, Any] | None = None,
    require_feed: bool = False,
    expected_adjustment_policy: str | None = None,
) -> MarketAssessment:
    """Apply the frozen market policy and expose all deterministic outcomes.

    The primary calling convention is ``assess_market(snapshot, policy,
    request=None)``.  When a request is supplied it is always validated before
    a model-call decision; use :func:`assess_market_bundle` when the explicit
    ``(snapshot, request, policy)`` ordering is preferred.
    """

    if expected_feed is None:
        expected_feed = expected_ref

    snapshot_result = validate_market_snapshot(
        snapshot,
        expected_feed=expected_feed,
        require_feed=require_feed,
        expected_adjustment_policy=expected_adjustment_policy,
    )
    policy_result = validate_analysis_policy(policy, expected_feed=expected_feed)
    issues: list[ValidationIssue] = list(snapshot_result.issues) + list(policy_result.issues)
    request_result: ValidationResult | None = None
    if request is not None:
        request_result = validate_analysis_request(
            request,
            snapshot=snapshot if isinstance(snapshot, Mapping) else None,
            policy=policy if isinstance(policy, Mapping) else None,
            expected_feed=expected_feed,
            require_feed=require_feed,
        )
        issues.extend(request_result.issues)
    snapshot_map = snapshot if isinstance(snapshot, Mapping) else {}
    policy_map = policy if isinstance(policy, Mapping) else {}
    snapshot_id = snapshot_map.get("market_snapshot_id")
    snapshot_hash = snapshot_map.get("content_hash")
    bars = snapshot_map.get("bars") if isinstance(snapshot_map.get("bars"), list) else []
    closed_bars = tuple(bar for bar in bars if isinstance(bar, Mapping) and bar.get("is_closed") is True)
    incomplete_final = bool(bars and isinstance(bars[-1], Mapping) and bars[-1].get("is_closed") is False)
    semantic_issues, structural_conditions = _snapshot_semantic_issues(snapshot_map) if snapshot_map else ([_issue("malformed_ohlcv", "$", "market snapshot must be an object")], {"malformed_ohlcv"})

    # Invalid policy/request/feed/cross-lineage errors must use the existing
    # pre-provider market code rather than inventing a persisted quality code.
    feed_internal = any(
        issue.path.startswith("$.extensions")
        and issue.code in {"missing_market_feed", "market_feed_mismatch", "invalid_expected_feed", "unknown_field", "secret_field", "version_mismatch", "invalid_slug", "malformed_market_feed"}
        for issue in snapshot_result.issues
    )
    metadata_invalid = any(issue.code == "adjustment_policy_mismatch" for issue in snapshot_result.issues)
    policy_extensions = policy_map.get("extensions") if isinstance(policy_map.get("extensions"), Mapping) else None
    policy_has_feed = isinstance(policy_extensions, Mapping) and "market_feed" in policy_extensions
    policy_feed_mismatch = False
    if policy_has_feed and policy_result.valid and isinstance(snapshot, Mapping):
        policy_feed, _policy_feed_result = extract_market_feed(policy, expected=expected_feed, require_feed=True)
        snapshot_feed, snapshot_feed_result = extract_market_feed(snapshot, require_feed=False)
        if snapshot_feed_result.valid and (snapshot_feed is None or policy_feed is None or policy_feed != snapshot_feed):
            issues.append(_issue("market_feed_mismatch", "$.extensions.market_feed", "policy feed does not match market snapshot feed"))
            policy_feed_mismatch = True
    elif policy_has_feed and policy_result.valid and not isinstance(snapshot, Mapping):
        issues.append(_issue("market_feed_mismatch", "$.extensions.market_feed", "policy feed cannot be bound without a snapshot"))
        policy_feed_mismatch = True
    request_invalid = request_result is not None and not request_result.valid
    policy_invalid = not policy_result.valid or policy_feed_mismatch or any(issue.path.startswith("$") and issue.code == "cross_lineage_mismatch" for issue in issues)
    structural = _structural_condition(structural_conditions)
    outcomes: list[PolicyOutcome] = []
    feed_issue_codes = {"missing_market_feed", "market_feed_mismatch", "invalid_expected_feed", "unknown_field", "secret_field", "version_mismatch", "invalid_slug", "malformed_market_feed"}
    snapshot_residual = tuple(
        issue
        for issue in snapshot_result.issues
        if not (issue.path.startswith("$.extensions") and issue.code in feed_issue_codes)
        and issue.code != "adjustment_policy_mismatch"
    )
    # Frozen rows 1–5 always outrank request/policy/feed metadata failures.
    if structural is not None:
        outcomes.append(_outcome(structural))
        return MarketAssessment(snapshot_id, snapshot_hash, _FrozenTuple(closed_bars), _FrozenTuple(_dedupe(issues)), _FrozenTuple(outcomes), False, False, outcomes[0], _FrozenTuple(), outcomes[0].uncertainty or "high", incomplete_final)
    # A residual snapshot contract failure (for example a forged top-level
    # hash or unknown core field) is malformed market input.  Feed and
    # adjustment expectation issues are intentionally excluded and handled as
    # internal pre-provider metadata below.
    if snapshot_residual:
        outcomes.append(_outcome("malformed_ohlcv"))
        return MarketAssessment(snapshot_id, snapshot_hash, _FrozenTuple(closed_bars), _FrozenTuple(_dedupe(issues)), _FrozenTuple(outcomes), False, False, outcomes[0], _FrozenTuple(), "high", incomplete_final)
    # stale_data/insufficient_history claims are accepted only when the same
    # condition is objectively produced by this policy assessment.  This
    # prevents producer labels from advertising a condition that the engine
    # would otherwise silently ignore; accepted frames never carry a stale or
    # insufficient claim detached from their computed outcome.
    if policy_result.valid:
        quality = snapshot_map.get("quality") if isinstance(snapshot_map.get("quality"), Mapping) else {}
        quality_claims = (
            {value for value in quality.get("codes", []) if isinstance(value, str)}
            | {value for value in quality.get("warnings", []) if isinstance(value, str)}
            if isinstance(quality, Mapping)
            else set()
        )
        minimum_claim = policy_map.get("minimum_closed_bars")
        minimum_valid = isinstance(minimum_claim, int) and not isinstance(minimum_claim, bool) and minimum_claim >= 1
        latest_claim_end = _instant(closed_bars[-1].get("end_at")) if closed_bars else None
        as_of_claim = _instant(snapshot_map.get("analysis_as_of"))
        max_claim = policy_map.get("max_staleness_seconds")
        stale_computed = (
            isinstance(max_claim, int)
            and not isinstance(max_claim, bool)
            and latest_claim_end is not None
            and as_of_claim is not None
            and as_of_claim - latest_claim_end > max_claim * _NS_PER_SECOND
        )
        insufficient_computed = minimum_valid and len(closed_bars) < minimum_claim
        quality_claim_issue = None
        if "stale_data" in quality_claims and not stale_computed:
            quality_claim_issue = "stale_data quality is not supported by exact staleness policy"
        elif "insufficient_history" in quality_claims and not insufficient_computed:
            quality_claim_issue = "insufficient_history quality is not supported by closed-bar policy"
        if quality_claim_issue is not None:
            issues.append(_issue("malformed_ohlcv", "$.quality", quality_claim_issue))
            outcomes.append(_outcome("malformed_ohlcv"))
            return MarketAssessment(snapshot_id, snapshot_hash, _FrozenTuple(closed_bars), _FrozenTuple(_dedupe(issues)), _FrozenTuple(outcomes), False, False, outcomes[0], _FrozenTuple(), "high", incomplete_final)
    if request_invalid or policy_invalid or feed_internal or metadata_invalid:
        # Internal assessment-only condition; no frozen quality outcome is
        # fabricated for request/policy/feed metadata failures.
        terminal = _invalid_market_outcome()
        accepted = False
        model_allowed = False
        uncertainty = "high"
        outcomes.append(terminal)
        return MarketAssessment(snapshot_id, snapshot_hash, _FrozenTuple(closed_bars), _FrozenTuple(_dedupe(issues)), _FrozenTuple(outcomes), accepted, model_allowed, terminal, _FrozenTuple(), uncertainty, incomplete_final)
    if not snapshot_result.valid:
        # Any residual contract/schema issue is malformed market input.  Keep
        # the exact frozen outcome while preserving typed validator issues.
        outcomes.append(_outcome("malformed_ohlcv"))
        return MarketAssessment(snapshot_id, snapshot_hash, _FrozenTuple(closed_bars), _FrozenTuple(_dedupe(issues)), _FrozenTuple(outcomes), False, False, outcomes[0], _FrozenTuple(), "high", incomplete_final)

    minimum = policy_map.get("minimum_closed_bars")
    if not isinstance(minimum, int) or isinstance(minimum, bool) or minimum < 1:
        outcomes.append(_outcome("malformed_ohlcv"))
        return MarketAssessment(snapshot_id, snapshot_hash, _FrozenTuple(closed_bars), _FrozenTuple(_dedupe(issues)), _FrozenTuple(outcomes), False, False, outcomes[0], _FrozenTuple(), "high", incomplete_final)

    def finish() -> MarketAssessment:
        """Finalize rows 6–11 with deterministic unions and first terminal."""

        warning_union: list[str] = []
        uncertainty_levels: list[str] = []
        for outcome in outcomes:
            for warning in outcome.warning_codes:
                if warning not in warning_union:
                    warning_union.append(warning)
            if outcome.uncertainty and outcome.uncertainty not in uncertainty_levels:
                uncertainty_levels.append(outcome.uncertainty)
        terminal = next((outcome for outcome in outcomes if outcome.terminal), None)
        model_allowed = terminal is None and all(outcome.model_call_allowed for outcome in outcomes)
        accepted = model_allowed and snapshot_result.valid and policy_result.valid
        return MarketAssessment(
            snapshot_id,
            snapshot_hash,
            _FrozenTuple(closed_bars),
            _FrozenTuple(_dedupe(issues)),
            _FrozenTuple(outcomes),
            accepted,
            model_allowed,
            terminal,
            _FrozenTuple(warning_union),
            "high" if "high" in uncertainty_levels else "medium" if "medium" in uncertainty_levels else "low",
            incomplete_final,
        )

    # Frozen market rows 6–11 are evaluated in order.  A terminal row stops
    # evaluation of all lower-precedence rows, while continue rows accumulate.
    # Ordinary insufficiency is row 6 only when no incomplete final bar exists.
    if not incomplete_final and len(closed_bars) < minimum:
        outcomes.append(_outcome("insufficient_closed_history"))
        return finish()

    coverage = snapshot_map.get("coverage")
    is_partial = isinstance(coverage, Mapping) and coverage.get("status") == "partial"
    if is_partial:
        if policy_map.get("allow_partial_data") is True:
            outcomes.append(_outcome("partial_coverage_allowed"))
        else:
            outcomes.append(_outcome("partial_coverage_disallowed"))
            return finish()

    max_staleness = policy_map.get("max_staleness_seconds")
    latest_end = _instant(closed_bars[-1].get("end_at")) if closed_bars else None
    as_of = _instant(snapshot_map.get("analysis_as_of"))
    if isinstance(max_staleness, int) and not isinstance(max_staleness, bool) and latest_end is not None and as_of is not None and as_of - latest_end > max_staleness * _NS_PER_SECOND:
        outcomes.append(_outcome("stale_beyond_policy"))
        return finish()

    if incomplete_final:
        if len(closed_bars) < minimum:
            outcomes.append(_outcome("incomplete_final_bar_insufficient"))
            return finish()
        outcomes.append(_outcome("incomplete_final_bar_sufficient"))
    return finish()


def assess_market_bundle(
    snapshot: Any,
    request: Mapping[str, Any],
    policy: Any,
    **kwargs: Any,
) -> MarketAssessment:
    """Unambiguous ``(snapshot, request, policy)`` assessment convenience."""

    return assess_market(snapshot, policy, request=request, **kwargs)


__all__ = [
    "MarketAssessment",
    "MarketDataError",
    "PolicyOutcome",
    "assess_market",
    "assess_market_bundle",
    "validate_analysis_policy",
    "validate_analysis_request",
    "validate_closed_bar_window",
    "validate_market_snapshot",
]
