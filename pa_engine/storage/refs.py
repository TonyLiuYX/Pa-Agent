"""Typed market references for the G027 runtime event envelope lineage.

The market-ref union is deliberately closed with exactly two kinds:
``second_event`` (one G020 frozen second aggregate) and ``recovery_bar``
(one G019 minute recovery bar).  A ref carries immutable id/hash identity,
the symbol and the canonical UTC interval only; OHLC values are absent.

Validation is deterministic, exception-safe and reports one stable code per
violation so tampering (bad hash/id, cross-kind substitution, non-canonical
timestamps, wrong spans) is rejected explicitly rather than silently
adopted.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any, Mapping

from pa_engine.contracts import ValidationIssue, ValidationResult
from pa_engine.storage.schema import FEED_KEY
from pa_engine.storage.times import instant_from_text, is_canonical_instant

MARKET_REF_KINDS = ("second_event", "recovery_bar")
PATH_PRECISIONS = ("minute", "no_path")

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ONE_SECOND = timedelta(seconds=1)
_ONE_MINUTE = timedelta(minutes=1)
_ONE_DAY = timedelta(hours=24)
_FEED_REF = {
    "schema_version": "0.1.0",
    "provider": "alpaca",
    "feed": "iex",
    "coverage": "single_exchange",
}

_SECOND_KEYS = frozenset(
    {
        "kind",
        "symbol",
        "second_event_id",
        "second_event_hash",
        "start_at",
        "end_at",
    }
)
_BAR_KEYS = frozenset(
    {
        "kind",
        "symbol",
        "recovery_bar_id",
        "recovery_bar_hash",
        "path_precision",
        "start_at",
        "end_at",
    }
)


def _issue(code: str, path: str, message: str) -> ValidationIssue:
    return ValidationIssue(code, path, message)


def _native_issues(value: Any, path: str, issues: list[ValidationIssue]) -> None:
    if type(value) not in (type(None), bool, int, str, list, dict):
        issues.append(_issue("invalid_market_ref", path, "value is not strict JSON-native data"))
        return
    if isinstance(value, dict):
        for key, child in value.items():
            if type(key) is not str:
                issues.append(_issue("invalid_market_ref", path, "object keys must be strings"))
                continue
            _native_issues(child, f"{path}.{key}", issues)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _native_issues(child, f"{path}[{index}]", issues)


def _validate_market_ref(value: Any) -> ValidationResult:
    issues: list[ValidationIssue] = []
    _native_issues(value, "$", issues)
    if issues:
        return ValidationResult(False, "market_ref", issues)
    if not isinstance(value, Mapping):
        return ValidationResult(
            False, "market_ref", [_issue("invalid_market_ref", "$", "market ref must be a JSON object")]
        )
    kind = value.get("kind")
    if kind not in MARKET_REF_KINDS:
        return ValidationResult(
            False,
            "market_ref",
            [_issue("unknown_market_ref_kind", "$.kind", "market ref kind is not in the closed union")],
        )
    if "second_event_id" in value and "recovery_bar_id" in value:
        return ValidationResult(
            False,
            "market_ref",
            [_issue("cross_kind_reference", "$", "both market ref kinds are referenced at once")],
        )
    expected = _SECOND_KEYS if kind == "second_event" else _BAR_KEYS
    if set(value) != expected:
        missing = sorted(expected - set(value))
        extra = sorted(set(value) - expected)
        detail = []
        if missing:
            detail.append("missing: " + ",".join(missing))
        if extra:
            detail.append("unexpected: " + ",".join(extra))
        return ValidationResult(
            False,
            "market_ref",
            [_issue("invalid_market_ref", "$", "market ref has an unexpected shape (" + "; ".join(detail) + ")")],
        )
    symbol = value["symbol"]
    if (
        not isinstance(symbol, str)
        or not symbol
        or symbol != symbol.upper()
        or any(character.isspace() for character in symbol)
    ):
        return ValidationResult(
            False,
            "market_ref",
            [_issue("invalid_symbol", "$.symbol", "symbol must be a non-empty upper-case value without whitespace")],
        )
    if not is_canonical_instant(value["start_at"]) or not is_canonical_instant(value["end_at"]):
        return ValidationResult(
            False,
            "market_ref",
            [_issue("non_utc_timestamp", "$", "market ref instants must be canonical UTC")],
        )
    start = instant_from_text(value["start_at"])
    end = instant_from_text(value["end_at"])
    if end <= start:
        return ValidationResult(
            False,
            "market_ref",
            [_issue("interval_order", "$", "market ref interval must have start before end")],
        )
    span = end - start
    if kind == "second_event":
        if (
            not isinstance(value["second_event_id"], str)
            or not re.fullmatch(r"sec_[0-9a-f]{64}", value["second_event_id"])
        ):
            return ValidationResult(
                False,
                "market_ref",
                [_issue("invalid_second_id", "$.second_event_id", "second_event_id is malformed")],
            )
        if not isinstance(value["second_event_hash"], str) or not _SHA256_RE.fullmatch(value["second_event_hash"]):
            return ValidationResult(
                False,
                "market_ref",
                [_issue("invalid_second_hash", "$.second_event_hash", "second_event_hash must be 64 lowercase hex chars")],
            )
        if span != _ONE_SECOND:
            return ValidationResult(
                False,
                "market_ref",
                [_issue("second_interval_mismatch", "$", "second event interval must be exactly 1.0 second")],
            )
    else:
        if (
            not isinstance(value["recovery_bar_id"], str)
            or not re.fullmatch(r"rbar_[0-9a-f]{64}", value["recovery_bar_id"])
        ):
            return ValidationResult(
                False,
                "market_ref",
                [_issue("invalid_bar_id", "$.recovery_bar_id", "recovery_bar_id is malformed")],
            )
        if not isinstance(value["recovery_bar_hash"], str) or not _SHA256_RE.fullmatch(value["recovery_bar_hash"]):
            return ValidationResult(
                False,
                "market_ref",
                [_issue("invalid_bar_hash", "$.recovery_bar_hash", "recovery_bar_hash must be 64 lowercase hex chars")],
            )
        if value["recovery_bar_id"] != "rbar_" + value["recovery_bar_hash"]:
            return ValidationResult(
                False,
                "market_ref",
                [_issue("identity_mismatch", "$.recovery_bar_id", "recovery_bar_id must bind to recovery_bar_hash")],
            )
        precision = value["path_precision"]
        if precision not in PATH_PRECISIONS:
            return ValidationResult(
                False,
                "market_ref",
                [_issue("invalid_path_precision", "$.path_precision", "path_precision is not in the closed vocabulary")],
            )
        if precision == "minute":
            if span != _ONE_MINUTE:
                return ValidationResult(
                    False,
                    "market_ref",
                    [_issue("bar_interval_mismatch", "$", "minute recovery bar interval must be exactly 60.0 seconds")],
                )
        elif span > _ONE_DAY:
            return ValidationResult(
                False,
                "market_ref",
                [_issue("bar_interval_mismatch", "$", "no_path recovery bar interval must not exceed 24 hours")],
            )
    return ValidationResult(True, "market_ref", [])


def validate_market_ref(value: Any) -> ValidationResult:
    """Validate one market ref and return a result; never raises."""

    try:
        return _validate_market_ref(value)
    except Exception:
        return ValidationResult(
            False,
            "market_ref",
            [_issue("invalid_market_ref", "$", "market ref is malformed or non-JSON")],
        )


def make_second_ref(second: Mapping) -> dict[str, Any]:
    """Extract a ``second_event`` ref from a G020 aggregate-like mapping."""

    if not isinstance(second, Mapping):
        raise TypeError("second must be a mapping")
    _validate_source_metadata(second, symbol=second.get("symbol"), require_recovery=False)
    return {
        "kind": "second_event",
        "symbol": second.get("symbol"),
        "second_event_id": second.get("second_event_id") or second.get("event_id"),
        "second_event_hash": (
            second.get("second_event_hash") or second.get("event_hash") or second.get("content_hash")
        ),
        "start_at": second.get("start_at"),
        "end_at": second.get("end_at"),
    }


def make_recovery_bar_ref(bar: Mapping) -> dict[str, Any]:
    """Extract a ``recovery_bar`` ref from a G019 RecoveryBar-like mapping.

    The G019 ``minute_bar_no_path`` precision alias is normalized to
    ``minute``; when the source omits ``path_precision`` it is derived from
    the interval (exactly 60 seconds means ``minute``, otherwise ``no_path``).
    """

    if not isinstance(bar, Mapping):
        raise TypeError("bar must be a mapping")
    instrument = bar.get("instrument")
    symbol = bar.get("symbol")
    if symbol is None and isinstance(instrument, Mapping):
        symbol = instrument.get("symbol")
    if isinstance(instrument, Mapping) and instrument.get("symbol") not in (None, symbol):
        raise ValueError("bar instrument symbol does not match bar symbol")
    _validate_source_metadata(bar, symbol=symbol, require_recovery=True)
    precision = bar.get("path_precision")
    if precision == "minute_bar_no_path":
        precision = "minute"
    if precision is None:
        start, end = bar.get("start_at"), bar.get("end_at")
        if (
            isinstance(start, str)
            and isinstance(end, str)
            and is_canonical_instant(start)
            and is_canonical_instant(end)
        ):
            span = instant_from_text(end) - instant_from_text(start)
            precision = "minute" if span == _ONE_MINUTE else "no_path"
        else:
            precision = "no_path"
    return {
        "kind": "recovery_bar",
        "symbol": symbol,
        "recovery_bar_id": bar.get("recovery_bar_id") or bar.get("bar_id"),
        "recovery_bar_hash": bar.get("recovery_bar_hash") or bar.get("content_hash"),
        "path_precision": precision,
        "start_at": bar.get("start_at"),
        "end_at": bar.get("end_at"),
    }


def _validate_source_metadata(
    source: Mapping[str, Any], *, symbol: str | None, require_recovery: bool
) -> None:
    """Validate source-only lineage before projecting it into the closed ref union."""

    feed = source.get("market_feed")
    if feed is not None:
        if not isinstance(feed, Mapping) or dict(feed) not in (_FEED_REF, {"feed_key": FEED_KEY}):
            raise ValueError("source market_feed is not the frozen Alpaca IEX feed")
    feed_key = source.get("feed_key")
    if feed_key is not None and feed_key != FEED_KEY:
        raise ValueError("source feed_key is not the frozen Alpaca IEX feed")
    request_hash = source.get("source_request_hash")
    if request_hash is not None and (
        not isinstance(request_hash, str) or not _SHA256_RE.fullmatch(request_hash)
    ):
        raise ValueError("source_request_hash must be lowercase SHA-256")
    recovery_cursor = source.get("recovery_cursor")
    if recovery_cursor is not None and not is_canonical_instant(recovery_cursor):
        raise ValueError("recovery_cursor must be canonical UTC text")
    if not require_recovery:
        return
    complete = source.get("complete")
    coverage = source.get("coverage_state")
    eligible = source.get("trigger_eligible")
    if complete is not None and type(complete) is not bool:
        raise ValueError("complete must be boolean")
    if coverage is not None and coverage not in ("complete", "partial"):
        raise ValueError("coverage_state is not in the closed vocabulary")
    if complete is not None and coverage is not None:
        if complete is True and coverage != "complete":
            raise ValueError("complete bar must have complete coverage")
        if complete is False and coverage != "partial":
            raise ValueError("incomplete bar must have partial coverage")
    if eligible is not None:
        if type(eligible) is not bool:
            raise ValueError("trigger_eligible must be boolean")
        expected = complete is True and coverage == "complete"
        if complete is not None and coverage is not None and eligible != expected:
            raise ValueError("trigger_eligible does not match recovery completeness")


def is_second_ref(value: Any) -> bool:
    """Return whether *value* is a valid ``second_event`` market ref."""

    return validate_market_ref(value).valid and isinstance(value, Mapping) and value.get("kind") == "second_event"


def is_recovery_bar_ref(value: Any) -> bool:
    """Return whether *value* is a valid ``recovery_bar`` market ref."""

    return validate_market_ref(value).valid and isinstance(value, Mapping) and value.get("kind") == "recovery_bar"


__all__ = [
    "MARKET_REF_KINDS",
    "PATH_PRECISIONS",
    "is_recovery_bar_ref",
    "is_second_ref",
    "make_recovery_bar_ref",
    "make_second_ref",
    "validate_market_ref",
]
