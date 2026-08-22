"""Strict internal ``RecoveryBar 0.1.0`` value and validator.

Recovery bars are deliberately not G014 artifacts.  They are minute-level
reconciliation inputs used after a live-stream interruption and therefore
carry an explicit no-path marker.  A complete bar may be considered by a
downstream pending-entry/exit monitor; an incomplete or partial bar remains a
valid recovery observation but can never be a trade trigger.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from pa_engine.contracts import ValidationIssue, ValidationResult, canonical_json_bytes, bar_id_for
from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef, validate_market_feed_ref


SCHEMA_VERSION = "0.1.0"
ARTIFACT_KIND = "recovery_bar"
PATH_PRECISION = "minute_bar_no_path"
_FIELDS = frozenset(
    {
        "schema_version",
        "artifact_kind",
        "recovery_bar_id",
        "content_hash",
        "instrument",
        "timeframe",
        "session",
        "start_at",
        "end_at",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "complete",
        "coverage_state",
        "source_request_hash",
        "recovery_cursor",
        "market_feed",
        "path_precision",
        "trigger_eligible",
    }
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$")
_DECIMAL_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
_POSITIVE_RE = re.compile(r"^(?:[1-9][0-9]*(?:\.[0-9]*[1-9])?|0\.[0-9]*[1-9])$")
_NONNEGATIVE_RE = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
_NS_PER_MINUTE = 60 * 1_000_000_000


def _issue(code: str, path: str, message: str) -> ValidationIssue:
    return ValidationIssue(code, path, message)


def _instant(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    match = _UTC_RE.fullmatch(value)
    if match is None:
        return None
    fraction = (match.group(7) or "").ljust(9, "0")
    try:
        stamp = datetime(
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3)),
            int(match.group(4)),
            int(match.group(5)),
            int(match.group(6)),
            tzinfo=timezone.utc,
        )
    except (TypeError, ValueError, OverflowError):
        return None
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = stamp - epoch
    whole_seconds = delta.days * 86_400 + delta.seconds
    return whole_seconds * 1_000_000_000 + int(fraction)


def _decimal(value: Any, *, positive: bool = False, nonnegative: bool = False) -> Decimal | None:
    if not isinstance(value, str) or value == "-0" or not _DECIMAL_RE.fullmatch(value):
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    if positive and (parsed <= 0 or not _POSITIVE_RE.fullmatch(value)):
        return None
    if nonnegative and (parsed < 0 or not _NONNEGATIVE_RE.fullmatch(value)):
        return None
    return parsed


def _canonical_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value[key] for key in sorted(_FIELDS - {"recovery_bar_id", "content_hash"})}


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        mapped = to_dict()
        if isinstance(mapped, Mapping):
            return dict(mapped)
    raise TypeError("value must be a mapping")


def recovery_bar_content_hash(value: Mapping[str, Any]) -> str:
    """Return the semantic hash of a recovery-bar mapping."""

    if not isinstance(value, Mapping):
        raise TypeError("recovery bar identity requires a mapping")
    try:
        projection = _canonical_projection(value)
        return hashlib.sha256(canonical_json_bytes(projection)).hexdigest()
    except Exception:
        # Identity helpers expose a stable value-error boundary to callers;
        # the parser/validator remains the non-throwing API for untrusted wire
        # values.
        raise ValueError("recovery bar identity requires JSON-native fields") from None


def recovery_bar_id_for(value: Mapping[str, Any], *args: Any, **kwargs: Any) -> str:
    """Return a stable recovery ID.

    The canonical call accepts the wire mapping.  For convenience an
    already-constructed :class:`RecoveryBar` (or any mapping with its fields)
    is accepted as well; extra positional arguments are intentionally rejected
    rather than inventing a second identity grammar.
    """

    if args or kwargs:
        raise TypeError("recovery_bar_id_for accepts one recovery-bar mapping")
    if isinstance(value, RecoveryBar):
        try:
            value = value.to_dict(include_identity=False)
        except Exception:
            raise ValueError("recovery bar identity requires JSON-native fields") from None
    if not isinstance(value, Mapping):
        raise TypeError("recovery bar identity requires a mapping")
    return "rbar_" + recovery_bar_content_hash(value)


recovery_bar_hash = recovery_bar_content_hash
recovery_bar_id = recovery_bar_id_for


def _identity_issues(value: Mapping[str, Any], issues: list[ValidationIssue]) -> None:
    expected_hash = recovery_bar_content_hash(value)
    if value.get("content_hash") != expected_hash:
        issues.append(_issue("identity_mismatch", "$.content_hash", "recovery bar content hash does not match payload"))
    if value.get("recovery_bar_id") != "rbar_" + expected_hash:
        issues.append(_issue("identity_mismatch", "$.recovery_bar_id", "recovery bar ID does not match payload hash"))


def _validate_recovery_bar_impl(
    value: Any,
    *,
    expected_market_feed: MarketFeedRef | Mapping[str, Any] = ALPACA_IEX_FEED,
    expected_source_request_hash: str | None = None,
) -> ValidationResult:
    """Strictly validate a recovery-bar mapping.

    The validator accepts both complete and incomplete recovery state, but
    rejects any state that claims an incomplete/partial interval is trigger
    eligible.  Unknown or omitted fields are errors, making parsing safe at
    the G025 boundary.
    """

    issues: list[ValidationIssue] = []
    if not isinstance(value, Mapping):
        return ValidationResult(False, ARTIFACT_KIND, [_issue("malformed_recovery_bar", "$", "recovery bar must be an object")])
    keys = set(value)
    for key in sorted(keys - _FIELDS, key=str):
        issues.append(_issue("unknown_field", f"$.{key}", "unknown recovery bar field"))
    for key in sorted(_FIELDS - keys):
        issues.append(_issue("missing_field", f"$.{key}", "required recovery bar field is missing"))
    if issues:
        return ValidationResult(False, ARTIFACT_KIND, issues)

    if value.get("schema_version") != SCHEMA_VERSION:
        issues.append(_issue("version_mismatch", "$.schema_version", "recovery bar schema_version must be 0.1.0"))
    if value.get("artifact_kind") != ARTIFACT_KIND:
        issues.append(_issue("malformed_recovery_bar", "$.artifact_kind", "artifact_kind must be recovery_bar"))
    if value.get("path_precision") != PATH_PRECISION:
        issues.append(_issue("path_precision_mismatch", "$.path_precision", "recovery bars require minute_bar_no_path"))

    instrument = value.get("instrument")
    if not isinstance(instrument, Mapping) or set(instrument) != {"asset_class", "symbol", "venue", "currency"}:
        issues.append(_issue("malformed_identity", "$.instrument", "instrument must be the exact four-field US-equity object"))
    else:
        if instrument.get("asset_class") != "us_equity" or instrument.get("currency") != "USD" or instrument.get("venue") != "IEX":
            issues.append(_issue("malformed_identity", "$.instrument", "recovery bars require us_equity/USD on IEX"))
        if not isinstance(instrument.get("symbol"), str) or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,11}", instrument["symbol"]):
            issues.append(_issue("malformed_identity", "$.instrument.symbol", "symbol is not canonical"))

    timeframe = value.get("timeframe")
    if timeframe != {"unit": "minute", "multiplier": 1}:
        issues.append(_issue("timeframe_unsupported", "$.timeframe", "recovery bars require one-minute timeframe"))
    session = value.get("session")
    if session != {"kind": "regular", "timezone": "America/New_York"}:
        issues.append(_issue("session_unsupported", "$.session", "recovery bars require regular America/New_York session"))

    start_ns, end_ns = _instant(value.get("start_at")), _instant(value.get("end_at"))
    if start_ns is None:
        issues.append(_issue("invalid_timestamp", "$.start_at", "start_at must be exact UTC"))
    if end_ns is None:
        issues.append(_issue("invalid_timestamp", "$.end_at", "end_at must be exact UTC"))
    if start_ns is not None and end_ns is not None:
        if end_ns - start_ns != _NS_PER_MINUTE or start_ns % _NS_PER_MINUTE != 0:
            issues.append(_issue("minute_boundary", "$.start_at", "recovery interval must be an exact UTC minute"))

    for field in ("open", "high", "low", "close"):
        if _decimal(value.get(field), positive=True) is None:
            issues.append(_issue("invalid_decimal", f"$.{field}", "OHLC must be canonical positive decimals"))
    volume = _decimal(value.get("volume"), nonnegative=True)
    if volume is None:
        issues.append(_issue("invalid_decimal", "$.volume", "volume must be a canonical nonnegative decimal"))
    o, h, l, c = (_decimal(value.get(field), positive=True) for field in ("open", "high", "low", "close"))
    if None not in (o, h, l, c) and not (l <= o <= h and l <= c <= h):
        issues.append(_issue("ohlc_invariant", "$.high", "OHLC values violate low <= open/close <= high"))

    if not isinstance(value.get("complete"), bool):
        issues.append(_issue("malformed_coverage", "$.complete", "complete must be boolean"))
    if value.get("coverage_state") not in {"complete", "partial"}:
        issues.append(_issue("malformed_coverage", "$.coverage_state", "coverage_state must be complete or partial"))
    if value.get("complete") is True and value.get("coverage_state") != "complete":
        issues.append(_issue("malformed_coverage", "$.coverage_state", "complete bars require complete coverage_state"))
    if value.get("complete") is False and value.get("coverage_state") != "partial":
        issues.append(_issue("malformed_coverage", "$.coverage_state", "incomplete bars require partial coverage_state"))

    source_hash = value.get("source_request_hash")
    if not isinstance(source_hash, str) or _SHA256_RE.fullmatch(source_hash) is None:
        issues.append(_issue("invalid_source_request_hash", "$.source_request_hash", "source_request_hash must be lowercase SHA-256"))
    elif expected_source_request_hash is not None and source_hash != expected_source_request_hash:
        issues.append(_issue("source_request_mismatch", "$.source_request_hash", "source request hash does not match expected request"))
    cursor = value.get("recovery_cursor")
    if (
        not isinstance(cursor, str)
        or _instant(cursor) is None
        or cursor != value.get("end_at")
        or len(cursor) > 256
        or "page_token" in cursor.lower()
        or "token" in cursor.lower()
    ):
        issues.append(_issue("invalid_recovery_cursor", "$.recovery_cursor", "recovery_cursor must be exact UTC end_at"))

    feed_result = validate_market_feed_ref(value.get("market_feed"), expected=expected_market_feed, path="$.market_feed")
    issues.extend(feed_result.issues)

    eligible = value.get("complete") is True and value.get("coverage_state") == "complete"
    if value.get("trigger_eligible") is not eligible:
        issues.append(_issue("trigger_eligibility_mismatch", "$.trigger_eligible", "trigger eligibility must match complete coverage"))

    if not issues:
        _identity_issues(value, issues)
    return ValidationResult(not issues, ARTIFACT_KIND, issues)


def validate_recovery_bar(
    value: Any,
    *,
    expected_market_feed: MarketFeedRef | Mapping[str, Any] = ALPACA_IEX_FEED,
    expected_source_request_hash: str | None = None,
) -> ValidationResult:
    """Return a typed invalid result for every malformed/non-JSON mapping.

    Canonicalization and custom Mapping implementations can raise arbitrary
    exceptions while identity or feed checks are evaluated.  Recovery is a
    parser boundary, so those exceptions are normalized instead of leaking a
    traceback into a caller or log.
    """

    try:
        return _validate_recovery_bar_impl(
            value,
            expected_market_feed=expected_market_feed,
            expected_source_request_hash=expected_source_request_hash,
        )
    except Exception:
        return ValidationResult(
            False,
            ARTIFACT_KIND,
            [_issue("malformed_recovery_bar", "$", "recovery bar contains malformed or non-JSON values")],
        )


@dataclass(frozen=True, slots=True)
class RecoveryBar:
    """Immutable, hash-bound one-minute raw recovery bar."""

    instrument: Mapping[str, Any]
    timeframe: Mapping[str, Any]
    session: Mapping[str, Any]
    start_at: str
    end_at: str
    open: str
    high: str
    low: str
    close: str
    volume: str
    complete: bool
    coverage_state: str
    source_request_hash: str
    recovery_cursor: str
    market_feed: Mapping[str, Any]
    path_precision: str = PATH_PRECISION

    @property
    def trigger_eligible(self) -> bool:
        return self.complete and self.coverage_state == "complete"

    @property
    def eligible(self) -> bool:
        return self.trigger_eligible

    @property
    def is_complete(self) -> bool:
        return self.complete

    @property
    def content_hash(self) -> str:
        return recovery_bar_content_hash(self.to_dict(include_identity=False))

    @property
    def recovery_bar_hash(self) -> str:
        return self.content_hash

    hash = recovery_bar_hash

    @property
    def recovery_bar_id(self) -> str:
        return "rbar_" + self.content_hash

    @property
    def bar_id(self) -> str:
        """G015-compatible deterministic minute ID for lineage convenience."""

        return bar_id_for(self.instrument, self.timeframe, self.session, self.start_at, self.end_at)

    def to_dict(self, *, include_identity: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "instrument": dict(self.instrument),
            "timeframe": dict(self.timeframe),
            "session": dict(self.session),
            "start_at": self.start_at,
            "end_at": self.end_at,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "complete": self.complete,
            "coverage_state": self.coverage_state,
            "source_request_hash": self.source_request_hash,
            "recovery_cursor": self.recovery_cursor,
            "market_feed": _mapping(self.market_feed),
            "path_precision": self.path_precision,
            "trigger_eligible": self.trigger_eligible,
        }
        if include_identity:
            digest = recovery_bar_content_hash(payload)
            payload["content_hash"] = digest
            payload["recovery_bar_id"] = "rbar_" + digest
        return payload

    as_dict = to_dict

    def validate(self) -> ValidationResult:
        try:
            return validate_recovery_bar(self.to_dict())
        except Exception:
            # A caller may construct a value with a non-JSON-native nested
            # object before asking for validation; keep that boundary typed.
            return ValidationResult(
                False,
                ARTIFACT_KIND,
                [_issue("malformed_recovery_bar", "$", "recovery bar contains malformed or non-JSON values")],
            )

    def is_trade_trigger_eligible(self) -> bool:
        return self.trigger_eligible and self.validate().valid

    can_trigger = is_trade_trigger_eligible
    eligible_for_trade = is_trade_trigger_eligible

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RecoveryBar":
        result = validate_recovery_bar(value)
        if not result.valid:
            raise ValueError("recovery bar validation failed: " + ",".join(issue.code for issue in result.issues))
        return cls(
            instrument=dict(value["instrument"]),
            timeframe=dict(value["timeframe"]),
            session=dict(value["session"]),
            start_at=value["start_at"],
            end_at=value["end_at"],
            open=value["open"],
            high=value["high"],
            low=value["low"],
            close=value["close"],
            volume=value["volume"],
            complete=value["complete"],
            coverage_state=value["coverage_state"],
            source_request_hash=value["source_request_hash"],
            recovery_cursor=value["recovery_cursor"],
            market_feed=dict(value["market_feed"]),
            path_precision=value["path_precision"],
        )

    parse = from_mapping
    from_dict = from_mapping


def parse_recovery_bar(value: Mapping[str, Any]) -> RecoveryBar:
    return RecoveryBar.from_mapping(value)


RecoveryBarValidationError = ValueError
RecoveryParserError = ValueError


__all__ = [
    "ARTIFACT_KIND",
    "PATH_PRECISION",
    "RecoveryBar",
    "RecoveryBarValidationError",
    "RecoveryParserError",
    "SCHEMA_VERSION",
    "parse_recovery_bar",
    "recovery_bar_hash",
    "recovery_bar_id",
    "recovery_bar_content_hash",
    "recovery_bar_id_for",
    "validate_recovery_bar",
]
