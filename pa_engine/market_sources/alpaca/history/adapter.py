"""Alpaca Basic IEX historical-bars adapter (G019).

This module owns only historical REST data and one-minute recovery.  It keeps
the transport replaceable for recorded tests, requires the fixed IEX/raw
request parameters, maps vendor bars into the frozen G014 market snapshot,
and exposes a separate strict :class:`RecoveryBar` value for restart
reconciliation.  Credentials are runtime-only and are never copied into a
result, request hash, log, or exception message.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import time
import urllib.error
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Sequence
from types import MappingProxyType
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pa_engine.contracts import ValidationIssue, ValidationResult, bar_id_for, canonical_json_bytes, finalize_artifact
from pa_engine.market import ALPACA_IEX_FEED, validate_market_snapshot

from .recovery import RecoveryBar, validate_recovery_bar
from .transport import (
    HistoryTransport,
    MalformedTransportResponse,
    RecordedTransport,
    TransportIOError,
    TransportRequest,
    TransportResponse,
    UrllibTransport,
    coerce_transport_response,
)


SCHEMA_VERSION = "0.1.0"
ADAPTER_VERSION = "g019-alpaca-iex-history-0.1.0"
BASE_URL = "https://data.alpaca.markets/v2"
MARKET_TIMEZONE = "America/New_York"
SESSION_KIND = "regular"
FEED = "iex"
ADJUSTMENT = "raw"
SORT = "asc"
MAX_RECOVERY_WINDOW = timedelta(days=7)
MAX_PAGES = 1000
DEFAULT_LIMIT = 1000
MAX_LIMIT = 10_000
DEFAULT_TIMEOUT = 10.0
_UTC_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DECIMAL_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
_PRICE_RE = re.compile(r"^(?:[1-9][0-9]*(?:\.[0-9]*[1-9])?|0\.[0-9]*[1-9])$")
_VOLUME_RE = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
_NS_SECOND = 1_000_000_000
_NS_MINUTE = 60 * _NS_SECOND


class OutcomeCode(str, Enum):
    SUCCESS = "success"
    EMPTY_WINDOW = "empty_window"
    INVALID_REQUEST = "invalid_request"
    MISSING_CREDENTIALS = "missing_credentials"
    AUTHENTICATION = "authentication_error"
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
    TIMEOUT = "timeout"
    TRANSPORT_ERROR = "transport_error"
    MALFORMED_RESPONSE = "malformed_response"
    PAGINATION_CYCLE = "pagination_cycle"
    PAGINATION_LIMIT = "pagination_limit"
    INVALID_MARKET_DATA = "invalid_market_data"
    RECOVERY_INVALID = "recovery_invalid"


@dataclass(frozen=True, slots=True)
class AdapterOutcome:
    """Stable, sanitized result for a provider or conversion failure."""

    code: str
    classification: str
    retryable: bool
    authentication_terminal: bool
    message: str
    status_code: int | None = None
    pages: int = 0

    @property
    def terminal(self) -> bool:
        # Success and retryable failures both leave the caller able to
        # continue.  Only a non-success, non-retryable outcome is terminal.
        return not self.success and not self.retryable

    @property
    def auth_terminal(self) -> bool:
        return self.authentication_terminal

    @property
    def kind(self) -> str:
        return self.classification

    @property
    def error_code(self) -> str:
        return self.code

    @property
    def is_retryable(self) -> bool:
        return self.retryable

    @property
    def is_auth_terminal(self) -> bool:
        return self.authentication_terminal

    @property
    def requires_reauthentication(self) -> bool:
        return self.authentication_terminal

    @property
    def success(self) -> bool:
        return self.code == OutcomeCode.SUCCESS.value

    @property
    def status(self) -> str:
        return self.code

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "classification": self.classification,
            "retryable": self.retryable,
            "authentication_terminal": self.authentication_terminal,
            "message": self.message,
            "status_code": self.status_code,
            "pages": self.pages,
        }

    as_dict = to_dict


class AdapterError(RuntimeError):
    """Exception carrying an :class:`AdapterOutcome` but no sensitive text."""

    def __init__(self, outcome: AdapterOutcome) -> None:
        self.outcome = outcome
        self.code = outcome.code
        self.classification = outcome.classification
        self.retryable = outcome.retryable
        self.authentication_terminal = outcome.authentication_terminal
        super().__init__(outcome.message)


class RequestValidationError(AdapterError):
    pass


class CredentialError(AdapterError):
    pass


class HistoryResponseError(AdapterError):
    pass


@dataclass(frozen=True, slots=True)
class CoverageBoundary:
    """Explicit half-open history/recovery boundary."""

    start_at: str
    end_at: str
    boundary: str = "half_open"

    @property
    def half_open(self) -> bool:
        return self.boundary == "half_open"

    @property
    def start_inclusive(self) -> bool:
        return True

    @property
    def end_exclusive(self) -> bool:
        return True

    @property
    def interval(self) -> dict[str, str]:
        return {"start_at": self.start_at, "end_at": self.end_at}

    def to_dict(self) -> dict[str, str]:
        return {"start_at": self.start_at, "end_at": self.end_at, "boundary": self.boundary}

    as_dict = to_dict


@dataclass(frozen=True, slots=True)
class HistoryRequest:
    """Canonical request independent of any page token or credential."""

    symbol: str
    start_at: str
    end_at: str
    timeframe: Mapping[str, Any]
    limit: int = DEFAULT_LIMIT
    feed: str = FEED
    adjustment: str = ADJUSTMENT
    sort: str = SORT
    recovery: bool = False

    def __post_init__(self) -> None:
        symbol = _validate_symbol(self.symbol)
        start = _canonical_timestamp(self.start_at)
        end = _canonical_timestamp(self.end_at)
        timeframe = _normalize_timeframe(self.timeframe)
        if _instant_ns(start) >= _instant_ns(end):
            raise ValueError("history request requires start_at < end_at")
        if self.feed != FEED or self.adjustment != ADJUSTMENT or self.sort != SORT:
            raise ValueError("G019 request feed, adjustment and sort are fixed")
        if isinstance(self.limit, bool) or not isinstance(self.limit, int) or not 1 <= self.limit <= MAX_LIMIT:
            raise ValueError("history request limit must be between 1 and 10000")
        if self.recovery:
            if timeframe != {"unit": "minute", "multiplier": 1}:
                raise ValueError("recovery timeframe is fixed to one minute")
            if _instant_ns(start) % _NS_MINUTE != 0 or _instant_ns(end) % _NS_MINUTE != 0:
                raise ValueError("recovery start_at/end_at must align to UTC minute boundaries")
            if _instant_ns(end) - _instant_ns(start) > _duration_ns(MAX_RECOVERY_WINDOW):
                raise ValueError("recovery window exceeds bounded maximum")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "start_at", start)
        object.__setattr__(self, "end_at", end)
        # Freeze the nested mapping as well as the dataclass fields.  A plain
        # ``frozen=True`` dataclass would still permit callers to mutate the
        # timeframe dict after construction and bypass the request boundary.
        object.__setattr__(self, "timeframe", MappingProxyType(dict(timeframe)))

    @property
    def coverage(self) -> CoverageBoundary:
        return CoverageBoundary(self.start_at, self.end_at)

    @property
    def timeframe_param(self) -> str:
        unit = self.timeframe["unit"]
        multiplier = self.timeframe["multiplier"]
        return {"minute": "Min", "hour": "Hour", "day": "Day", "week": "Week"}[unit].join((str(multiplier), ""))

    @property
    def request_identity(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "adapter_version": ADAPTER_VERSION,
            "symbol": self.symbol,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "timeframe": dict(self.timeframe),
            "feed": self.feed,
            "adjustment": self.adjustment,
            "sort": self.sort,
            "limit": self.limit,
            "recovery": self.recovery,
            "boundary": "half_open",
        }

    @property
    def request_hash(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.request_identity)).hexdigest()

    @property
    def source_request_hash(self) -> str:
        return self.request_hash

    def to_params(self, *, page_token: str | None = None) -> dict[str, str]:
        params = {
            "feed": FEED,
            "adjustment": ADJUSTMENT,
            "sort": SORT,
            "timeframe": self.timeframe_param,
            "start": self.start_at,
            "end": self.end_at,
            "limit": str(self.limit),
        }
        if page_token:
            params["page_token"] = page_token
        return params

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "timeframe": dict(self.timeframe),
            "feed": self.feed,
            "adjustment": self.adjustment,
            "sort": self.sort,
            "limit": self.limit,
            "recovery": self.recovery,
            "boundary": "half_open",
            "source_request_hash": self.request_hash,
        }

    as_dict = to_dict


@dataclass(frozen=True, slots=True)
class HistoryResult:
    """Success/empty/error result returned by :class:`AlpacaHistoryAdapter`."""

    request: HistoryRequest
    outcome: AdapterOutcome
    bars: tuple[Mapping[str, Any], ...] = ()
    snapshot: Mapping[str, Any] | None = None
    recovery_bars: tuple[RecoveryBar, ...] = ()
    coverage: CoverageBoundary | None = None

    @property
    def success(self) -> bool:
        return self.outcome.success

    @property
    def ok(self) -> bool:
        return self.success

    @property
    def status(self) -> str:
        return self.outcome.code

    @property
    def empty(self) -> bool:
        return self.outcome.code == OutcomeCode.EMPTY_WINDOW.value

    @property
    def error(self) -> AdapterOutcome | None:
        return None if self.success else self.outcome

    @property
    def recovery(self) -> tuple[RecoveryBar, ...]:
        return self.recovery_bars

    @property
    def market_snapshot(self) -> Mapping[str, Any] | None:
        return self.snapshot

    @property
    def source_request_hash(self) -> str:
        return self.request.source_request_hash

    @property
    def coverage_metadata(self) -> dict[str, str] | None:
        return self.coverage.to_dict() if self.coverage is not None else None

    @property
    def recovery_bar_dicts(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(bar.to_dict() for bar in self.recovery_bars)

    def raise_for_error(self) -> "HistoryResult":
        if not self.success:
            raise AdapterError(self.outcome)
        return self

    def to_dict(self) -> dict[str, Any]:
        result = {
            "outcome": self.outcome.to_dict(),
            "request": self.request.to_dict(),
            "bars": [dict(bar) for bar in self.bars],
            "snapshot": copy.deepcopy(self.snapshot) if self.snapshot is not None else None,
            "recovery_bars": [bar.to_dict() for bar in self.recovery_bars],
            "coverage": self.coverage.to_dict() if self.coverage is not None else None,
        }
        return result

    as_dict = to_dict


def _duration_ns(value: timedelta) -> int:
    return value.days * 86_400 * _NS_SECOND + value.seconds * _NS_SECOND + value.microseconds * 1_000


def _validate_symbol(symbol: Any) -> str:
    if not isinstance(symbol, str) or re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,11}", symbol) is None:
        raise ValueError("symbol must be an upper-case US-equity symbol")
    return symbol


def _instant_ns(value: str) -> int:
    match = _UTC_RE.fullmatch(value)
    if match is None:
        raise ValueError("timestamp must be exact UTC")
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
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("timestamp must be exact UTC") from exc
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = stamp - epoch
    return (delta.days * 86_400 + delta.seconds) * _NS_SECOND + int(fraction)


def _canonical_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include UTC timezone")
        value = value.astimezone(timezone.utc)
        fraction = value.microsecond * 1000
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = value - epoch
        whole_seconds = delta.days * 86_400 + delta.seconds
        return _format_ns(whole_seconds * _NS_SECOND + fraction)
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        return value.strftime("%Y-%m-%dT00:00:00Z")
    if not isinstance(value, str):
        raise ValueError("timestamp must be an exact UTC string")
    match = _UTC_RE.fullmatch(value)
    if match is None:
        raise ValueError("timestamp must be an exact UTC string")
    return _format_ns(_instant_ns(value))


def _format_ns(value: int) -> str:
    seconds, nanos = divmod(value, _NS_SECOND)
    stamp = datetime.fromtimestamp(seconds, tz=timezone.utc)
    text = stamp.strftime("%Y-%m-%dT%H:%M:%S")
    if nanos:
        fraction = f"{nanos:09d}".rstrip("0")
        text += "." + fraction
    return text + "Z"


def _normalize_timeframe(value: Any) -> dict[str, int | str]:
    if isinstance(value, str):
        match = re.fullmatch(r"(\d+)\s*(min|minute|hour|hr|day|week)s?", value, re.IGNORECASE)
        if match is None:
            # Also accept Alpaca's canonical 1Min/1Hour spellings.
            match = re.fullmatch(r"(\d+)(Min|Hour|Day|Week)", value)
        if match is None:
            raise ValueError("unsupported timeframe")
        raw_unit = match.group(2).lower()
        unit = "minute" if raw_unit in {"min", "minute"} else "hour" if raw_unit in {"hour", "hr"} else raw_unit
        value = {"unit": unit, "multiplier": int(match.group(1))}
    if not isinstance(value, Mapping) or set(value) != {"unit", "multiplier"}:
        raise ValueError("timeframe must contain unit and multiplier")
    unit, multiplier = value.get("unit"), value.get("multiplier")
    if unit not in {"minute", "hour", "day", "week"} or isinstance(multiplier, bool) or not isinstance(multiplier, int) or multiplier < 1:
        raise ValueError("unsupported timeframe")
    # Alpaca's single-symbol historical-bars grammar is intentionally narrow:
    # minute 1..59, hour 1..23, and only one day/week.  G014 excludes months,
    # and the adapter fails closed instead of inventing an unsupported unit.
    bounds = {"minute": 59, "hour": 23, "day": 1, "week": 1}
    if multiplier > bounds[unit]:
        raise ValueError("unsupported timeframe multiplier")
    return {"unit": unit, "multiplier": multiplier}


def _timeframe_duration(value: Mapping[str, Any]) -> timedelta:
    unit, multiplier = value["unit"], int(value["multiplier"])
    return {"minute": timedelta(minutes=multiplier), "hour": timedelta(hours=multiplier), "day": timedelta(days=multiplier), "week": timedelta(weeks=multiplier)}[unit]


def _calendar_bar_end_ns(start_ns: int, timeframe: Mapping[str, Any]) -> int:
    """Return a bar end using fixed duration or local calendar boundaries.

    Minute/hour bars are elapsed durations.  Day/week aggregates use the
    America/New_York calendar so a DST transition changes the UTC interval
    length without creating an overlap with the following bar.
    """

    unit = timeframe["unit"]
    if unit in {"minute", "hour"}:
        return start_ns + _duration_ns(_timeframe_duration(timeframe))
    start_seconds, start_nanos = divmod(start_ns, _NS_SECOND)
    local = (datetime.fromtimestamp(start_seconds, tz=timezone.utc) + timedelta(microseconds=start_nanos // 1000)).astimezone(ZoneInfo(MARKET_TIMEZONE))
    if unit == "day":
        target_date = local.date() + timedelta(days=1)
    elif unit == "week":
        days_until_next_monday = 7 - local.weekday()
        target_date = local.date() + timedelta(days=days_until_next_monday)
    else:  # pragma: no cover - _normalize_timeframe rejects unknown units
        raise ValueError("unsupported timeframe")
    local_end = datetime.combine(target_date, dt_time.min, tzinfo=ZoneInfo(MARKET_TIMEZONE))
    end_utc = local_end.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = end_utc - epoch
    return (delta.days * 86_400 + delta.seconds) * _NS_SECOND + end_utc.microsecond * 1_000


def _decimal_text(value: Any, *, price: bool = False, volume: bool = False) -> str | None:
    # JSON decoders in recorded tests may already produce Decimal/int/float;
    # use str only after rejecting non-finite floats and booleans.
    if isinstance(value, bool) or value is None:
        return None
    source_is_string = isinstance(value, str)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        value = Decimal(repr(value))
    elif isinstance(value, (int, Decimal)):
        value = Decimal(value)
    if source_is_string and not isinstance(value, str):
        return None
    if not isinstance(value, (str, Decimal)):
        return None
    if isinstance(value, str) and re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value) is None:
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed == 0 and parsed.as_tuple().sign:
        return None
    if isinstance(value, str) and "e" in value.lower():
        # Vendor JSON numbers are decoded as exact Decimal by this adapter;
        # an exponent in a direct fake payload is still rejected to keep the
        # wire contract canonical and deterministic.
        return None
    # Canonicalize JSON numeric values (e.g. 100.0 -> "100") without using a
    # binary float in the resulting artifact.  ``format(..., 'f')`` never
    # emits an exponent; remove only insignificant trailing zeroes.
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return None
    if price:
        if parsed <= 0 or _PRICE_RE.fullmatch(text) is None:
            return None
    elif volume:
        if parsed < 0 or _VOLUME_RE.fullmatch(text) is None:
            return None
    elif _DECIMAL_RE.fullmatch(text) is None:
        return None
    return text


def _parse_vendor_bar_impl(
    raw: Any,
    *,
    request: HistoryRequest,
) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    timestamp = raw.get("t")
    try:
        start = _canonical_timestamp(timestamp)
        start_ns = _instant_ns(start)
    except (TypeError, ValueError):
        return None
    try:
        end_ns = _calendar_bar_end_ns(start_ns, request.timeframe)
        end = _format_ns(end_ns)
    except (OverflowError, OSError, ValueError):
        return None
    start_request_ns, end_request_ns = _instant_ns(request.start_at), _instant_ns(request.end_at)
    # Apply the local half-open filter before interpreting values.  Alpaca may
    # legitimately include the inclusive ``end`` bar, and it must not make a
    # malformed numeric value outside our requested window poison an otherwise
    # valid response.
    if start_ns < start_request_ns or start_ns >= end_request_ns or end_ns > end_request_ns:
        return {"_out_of_range": True}
    values = {
        "open": _decimal_text(raw.get("o"), price=True),
        "high": _decimal_text(raw.get("h"), price=True),
        "low": _decimal_text(raw.get("l"), price=True),
        "close": _decimal_text(raw.get("c"), price=True),
        "volume": _decimal_text(raw.get("v"), volume=True),
    }
    if any(value is None for value in values.values()):
        return None
    o, h, l, c = (Decimal(values[key]) for key in ("open", "high", "low", "close"))
    if not (l <= o <= h and l <= c <= h):
        return None
    instrument = {"asset_class": "us_equity", "symbol": request.symbol, "venue": "IEX", "currency": "USD"}
    timeframe = dict(request.timeframe)
    session = {"kind": SESSION_KIND, "timezone": MARKET_TIMEZONE}
    return {
        "bar_id": bar_id_for(instrument, timeframe, session, start, end),
        "start_at": start,
        "end_at": end,
        "open": values["open"],
        "high": values["high"],
        "low": values["low"],
        "close": values["close"],
        "volume": values["volume"],
        "is_closed": True,
        "_start_ns": start_ns,
        "_end_ns": end_ns,
    }


def _parse_vendor_bar(
    raw: Any,
    *,
    request: HistoryRequest,
) -> dict[str, Any] | None:
    """Parse one untrusted vendor item without leaking mapping exceptions."""

    try:
        return _parse_vendor_bar_impl(raw, request=request)
    except Exception:
        return None


def _is_regular_bar(start_ns: int, end_ns: int, timeframe: Mapping[str, Any]) -> bool:
    zone = ZoneInfo(MARKET_TIMEZONE)
    start_seconds, start_nanos = divmod(start_ns, _NS_SECOND)
    end_seconds, end_nanos = divmod(end_ns, _NS_SECOND)
    start = (datetime.fromtimestamp(start_seconds, tz=timezone.utc) + timedelta(microseconds=start_nanos // 1000)).astimezone(zone)
    end = (datetime.fromtimestamp(end_seconds, tz=timezone.utc) + timedelta(microseconds=end_nanos // 1000)).astimezone(zone)
    if start.weekday() >= 5:
        return False
    if timeframe["unit"] in {"day", "week"}:
        # Alpaca day/week bars represent regular-session aggregates.  Their
        # UTC interval can span calendar time, so session filtering is based
        # on the local start date rather than a minute-level end clock.
        return True
    if end.date() != start.date() or end.weekday() >= 5:
        return False
    regular_start = dt_time(9, 30)
    regular_end = dt_time(16, 0)
    return start.time().replace(tzinfo=None) >= regular_start and end.time().replace(tzinfo=None) <= regular_end


def _missing_intervals(start_ns: int, end_ns: int, bars: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    missing: list[dict[str, str]] = []
    cursor = start_ns
    for bar in bars:
        bstart, bend = int(bar["_start_ns"]), int(bar["_end_ns"])
        if bstart > cursor:
            missing.append({"start_at": _format_ns(cursor), "end_at": _format_ns(bstart)})
        cursor = max(cursor, bend)
    if cursor < end_ns:
        missing.append({"start_at": _format_ns(cursor), "end_at": _format_ns(end_ns)})
    return missing


def _strip_internal(bar: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in bar.items() if not key.startswith("_")}


def _safe_message(message: str, secrets: Iterable[str] = ()) -> str:
    # Do not retain provider text.  Replace known credentials first, then
    # avoid common API-key-looking fragments if an injected transport raises
    # an exception containing one.
    cleaned = str(message)
    for secret in secrets:
        if secret:
            # Very short test/dev credentials must still redact when they are
            # the whole token, but replacing one-character strings globally
            # would corrupt every ordinary outcome message (for example the
            # ``s`` in ``response``).
            if len(secret) >= 4:
                cleaned = cleaned.replace(secret, "<redacted>")
            elif cleaned == secret:
                cleaned = "<redacted>"
            else:
                cleaned = re.sub(rf"(?<![A-Za-z0-9]){re.escape(secret)}(?![A-Za-z0-9])", "<redacted>", cleaned)
    cleaned = re.sub(r"(?i)(api[_-]?key|secret|token|authorization)[=:][^\s,;]+", r"\1=<redacted>", cleaned)
    return cleaned if cleaned else "adapter error"


def _error(
    code: str | OutcomeCode,
    classification: str,
    *,
    retryable: bool,
    authentication_terminal: bool = False,
    message: str,
    status_code: int | None = None,
    pages: int = 0,
    secrets: Iterable[str] = (),
) -> AdapterOutcome:
    return AdapterOutcome(
        code.value if isinstance(code, OutcomeCode) else str(code),
        classification,
        bool(retryable),
        bool(authentication_terminal),
        _safe_message(message, secrets),
        status_code,
        pages,
    )


def _success(*, pages: int, message: str = "historical bars fetched") -> AdapterOutcome:
    return _error(OutcomeCode.SUCCESS, "success", retryable=False, message=message, pages=pages)


class AlpacaHistoryAdapter:
    """Fetch and validate Alpaca Basic IEX bars for one symbol."""

    def __init__(
        self,
        *,
        key_id: str | None = None,
        secret_key: str | None = None,
        api_key_id: str | None = None,
        api_secret_key: str | None = None,
        credentials: Mapping[str, Any] | None = None,
        transport: HistoryTransport | Callable[[TransportRequest], Any] | None = None,
        base_url: str = BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        max_pages: int = MAX_PAGES,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        if credentials is not None:
            if not isinstance(credentials, Mapping):
                raise ValueError("credentials must be a mapping")
            key_id = key_id or api_key_id or credentials.get("key_id") or credentials.get("api_key_id") or credentials.get("APCA-API-KEY-ID")
            secret_key = secret_key or api_secret_key or credentials.get("secret_key") or credentials.get("api_secret_key") or credentials.get("APCA-API-SECRET-KEY")
        candidate_key = key_id or api_key_id
        candidate_secret = secret_key or api_secret_key
        self._key_id = candidate_key if isinstance(candidate_key, str) and candidate_key else None
        self._secret_key = candidate_secret if isinstance(candidate_secret, str) and candidate_secret else None
        self._secrets = tuple(value for value in (self._key_id, self._secret_key) if isinstance(value, str) and value)
        if not isinstance(base_url, str) or base_url.rstrip("/") != BASE_URL:
            # This is a fixed Alpaca adapter.  Rejecting alternate hosts at
            # construction time guarantees a default UrllibTransport can
            # never send credentials to a caller-supplied endpoint.
            raise ValueError("base_url must be the official Alpaca data endpoint")
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
            raise ValueError("timeout must be positive")
        if not isinstance(max_pages, int) or isinstance(max_pages, bool) or not 1 <= max_pages <= MAX_PAGES:
            raise ValueError("max_pages out of bounds")
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.max_pages = max_pages
        self.transport = transport or UrllibTransport()
        self._sleep = sleep or time.sleep

    @classmethod
    def from_env(
        cls,
        *,
        transport: HistoryTransport | Callable[[TransportRequest], Any] | None = None,
        environ: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> "AlpacaHistoryAdapter":
        env = environ if environ is not None else os.environ
        return cls(key_id=env.get("ALPACA_API_KEY_ID"), secret_key=env.get("ALPACA_API_SECRET_KEY"), transport=transport, **kwargs)

    @property
    def feed(self) -> Mapping[str, Any]:
        return ALPACA_IEX_FEED.to_dict()

    @property
    def market_feed(self) -> Mapping[str, Any]:
        return self.feed

    @property
    def feed_ref(self):
        return ALPACA_IEX_FEED

    market_feed_ref = feed_ref

    @property
    def has_credentials(self) -> bool:
        return bool(self._key_id and self._secret_key)

    def build_request(
        self,
        symbol: str,
        start_at: str | datetime | date,
        end_at: str | datetime | date,
        timeframe: Mapping[str, Any] | str,
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> HistoryRequest:
        return HistoryRequest(symbol, _canonical_timestamp(start_at), _canonical_timestamp(end_at), _normalize_timeframe(timeframe), limit=limit)

    make_request = build_request

    def build_recovery_request(
        self,
        symbol: str,
        start_at: str | datetime | date,
        end_at: str | datetime | date,
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> HistoryRequest:
        return HistoryRequest(
            symbol,
            _canonical_timestamp(start_at),
            _canonical_timestamp(end_at),
            {"unit": "minute", "multiplier": 1},
            limit=limit,
            recovery=True,
        )

    make_recovery_request = build_recovery_request

    def request_spec(self, request: HistoryRequest, *, page_token: str | None = None) -> TransportRequest:
        if not isinstance(request, HistoryRequest):
            raise TypeError("request must be HistoryRequest")
        try:
            # Re-validate nested values at this public transport boundary too;
            # a forged object must not reach ``timeframe_param`` and leak a
            # mapping KeyError or send an unintended query.
            request = HistoryRequest(
                request.symbol,
                request.start_at,
                request.end_at,
                request.timeframe,
                limit=request.limit,
                feed=request.feed,
                adjustment=request.adjustment,
                sort=request.sort,
                recovery=request.recovery,
            )
        except Exception as exc:
            outcome = _error(OutcomeCode.INVALID_REQUEST, "invalid_request", retryable=False, message="history request is invalid")
            raise RequestValidationError(outcome) from None
        endpoint = f"{self.base_url}/stocks/{quote(request.symbol, safe='')}/bars"
        headers = {
            "APCA-API-KEY-ID": self._key_id or "",
            "APCA-API-SECRET-KEY": self._secret_key or "",
            "Accept": "application/json",
        }
        return TransportRequest("GET", endpoint, request.to_params(page_token=page_token), headers, self.timeout)

    build_transport_request = request_spec

    def fetch_history(
        self,
        request: HistoryRequest | Mapping[str, Any] | None = None,
        *,
        symbol: str | None = None,
        start_at: str | datetime | date | None = None,
        end_at: str | datetime | date | None = None,
        timeframe: Mapping[str, Any] | str | None = None,
        limit: int = DEFAULT_LIMIT,
        strict: bool = False,
    ) -> HistoryResult:
        request, early = self._coerce_request(request, symbol=symbol, start_at=start_at, end_at=end_at, timeframe=timeframe, limit=limit, recovery=False)
        if early is not None:
            return self._finish(HistoryResult(request, early), strict)
        return self._fetch(request, strict=strict, recovery=False)

    fetch_startup = fetch_history
    fetch_bars = fetch_history
    get_history = fetch_history
    startup = fetch_history
    fetch = fetch_history
    get_bars = fetch_history
    history = fetch_history

    def fetch_recovery(
        self,
        request: HistoryRequest | Mapping[str, Any] | None = None,
        *,
        symbol: str | None = None,
        start_at: str | datetime | date | None = None,
        end_at: str | datetime | date | None = None,
        limit: int = DEFAULT_LIMIT,
        strict: bool = False,
    ) -> HistoryResult:
        request, early = self._coerce_request(request, symbol=symbol, start_at=start_at, end_at=end_at, timeframe=None, limit=limit, recovery=True)
        if early is not None:
            return self._finish(HistoryResult(request, early), strict)
        return self._fetch(request, strict=strict, recovery=True)

    fetch_recovery_bars = fetch_recovery
    recover = fetch_recovery
    get_recovery = fetch_recovery

    def fetch_or_raise(self, request: HistoryRequest, *, recovery: bool = False) -> HistoryResult:
        canonical, early = self._coerce_request(request, symbol=None, start_at=None, end_at=None, timeframe=None, limit=DEFAULT_LIMIT, recovery=recovery)
        if early is not None:
            return self._finish(HistoryResult(canonical, early, coverage=canonical.coverage), strict=True)
        return self._fetch(canonical, strict=True, recovery=recovery)

    def _coerce_request(
        self,
        request: HistoryRequest | Mapping[str, Any] | None,
        *,
        symbol: str | None,
        start_at: str | datetime | date | None,
        end_at: str | datetime | date | None,
        timeframe: Mapping[str, Any] | str | None,
        limit: int,
        recovery: bool,
    ) -> tuple[HistoryRequest, AdapterOutcome | None]:
        try:
            if request is None:
                if symbol is None or start_at is None or end_at is None or (timeframe is None and not recovery):
                    raise ValueError("symbol, start_at, end_at and timeframe are required")
                request = self.build_recovery_request(symbol, start_at, end_at, limit=limit) if recovery else self.build_request(symbol, start_at, end_at, timeframe, limit=limit)
            elif isinstance(request, Mapping):
                if any(request.get(name, expected) != expected for name, expected in (("feed", FEED), ("adjustment", ADJUSTMENT), ("sort", SORT))):
                    raise ValueError("G019 request feed, adjustment and sort are fixed")
                if "recovery" in request and bool(request.get("recovery")) is not recovery:
                    raise ValueError("request recovery mode does not match operation")
                if recovery:
                    if "timeframe" in request and _normalize_timeframe(request.get("timeframe")) != {"unit": "minute", "multiplier": 1}:
                        raise ValueError("recovery timeframe is fixed to one minute")
                    request = self.build_recovery_request(request.get("symbol"), request.get("start_at"), request.get("end_at"), limit=request.get("limit", limit))
                else:
                    request = self.build_request(request.get("symbol"), request.get("start_at"), request.get("end_at"), request.get("timeframe"), limit=request.get("limit", limit))
            elif not isinstance(request, HistoryRequest):
                raise TypeError("request must be HistoryRequest or mapping")
            else:
                # Reconstruct even an apparently-canonical object.  This
                # catches objects forged with ``object.__new__`` or mutated
                # nested fields before they can reach timeframe mapping or
                # transport code, and returns a typed invalid_request below.
                request = HistoryRequest(
                    request.symbol,
                    request.start_at,
                    request.end_at,
                    request.timeframe,
                    limit=request.limit,
                    feed=request.feed,
                    adjustment=request.adjustment,
                    sort=request.sort,
                    recovery=request.recovery,
                )
            if request.recovery is not recovery:
                raise ValueError("request recovery mode does not match operation")
            return request, None
        except Exception:
            # Construct a harmless placeholder request for a typed result.
            # It is never sent, and contains no caller text or credentials.
            try:
                placeholder = HistoryRequest("A", "1970-01-01T00:00:00Z", "1970-01-01T00:01:00Z", {"unit": "minute", "multiplier": 1}, recovery=recovery)
            except Exception:  # pragma: no cover - impossible constant
                raise
            return placeholder, _error(OutcomeCode.INVALID_REQUEST, "invalid_request", retryable=False, message="history request is invalid")

    def _finish(self, result: HistoryResult, strict: bool) -> HistoryResult:
        if strict and not result.success:
            if result.outcome.code == OutcomeCode.MISSING_CREDENTIALS.value:
                raise CredentialError(result.outcome)
            if result.outcome.code in {OutcomeCode.INVALID_REQUEST.value}:
                raise RequestValidationError(result.outcome)
            if result.outcome.code in {OutcomeCode.MALFORMED_RESPONSE.value, OutcomeCode.INVALID_MARKET_DATA.value}:
                raise HistoryResponseError(result.outcome)
            raise AdapterError(result.outcome)
        return result

    def _fetch(self, request: HistoryRequest, *, strict: bool, recovery: bool) -> HistoryResult:
        if not self.has_credentials:
            outcome = _error(OutcomeCode.MISSING_CREDENTIALS, "authentication", retryable=False, authentication_terminal=True, message="Alpaca credentials are not configured")
            return self._finish(HistoryResult(request, outcome, coverage=request.coverage), strict)

        raw_bars: list[Mapping[str, Any]] = []
        seen_tokens: set[str] = set()
        token: str | None = None
        pages = 0
        while True:
            pages += 1
            if pages > self.max_pages:
                outcome = _error(OutcomeCode.PAGINATION_LIMIT, "pagination", retryable=False, message="pagination page limit exceeded", pages=pages - 1)
                return self._finish(HistoryResult(request, outcome, coverage=request.coverage), strict)
            if token is not None:
                if token in seen_tokens:
                    outcome = _error(OutcomeCode.PAGINATION_CYCLE, "pagination", retryable=False, message="pagination token cycle detected", pages=pages - 1)
                    return self._finish(HistoryResult(request, outcome, coverage=request.coverage), strict)
                seen_tokens.add(token)
            try:
                response = self._send(request, page_token=token)
            except CredentialError as exc:
                return self._finish(HistoryResult(request, exc.outcome, coverage=request.coverage), strict)
            except AdapterError as exc:
                return self._finish(HistoryResult(request, exc.outcome, coverage=request.coverage), strict)
            token, page_bars, response_error = self._parse_page(response, request, pages=pages)
            if response_error is not None:
                return self._finish(HistoryResult(request, response_error, coverage=request.coverage), strict)
            raw_bars.extend(page_bars)
            if token is None:
                break

        converted: list[dict[str, Any]] = []
        for raw in raw_bars:
            parsed = _parse_vendor_bar(raw, request=request)
            if parsed is None:
                outcome = _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca bar payload is malformed", pages=pages)
                return self._finish(HistoryResult(request, outcome, coverage=request.coverage), strict)
            if parsed.get("_out_of_range"):
                continue
            if not _is_regular_bar(parsed["_start_ns"], parsed["_end_ns"], request.timeframe):
                continue
            converted.append(parsed)
        # The request explicitly asks Alpaca for ascending order.  Do not sort
        # a malformed provider response into validity: preserving the received
        # order lets us reject out-of-order/duplicate bars deterministically.
        # Duplicates/overlaps are not silently de-duplicated because a page
        # boundary defect must not alter a hash-bound snapshot.
        previous_start: int | None = None
        previous_end: int | None = None
        for bar in converted:
            start_ns, end_ns = int(bar["_start_ns"]), int(bar["_end_ns"])
            if previous_start is not None and start_ns <= previous_start:
                outcome = _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca bars overlap or duplicate", pages=pages)
                return self._finish(HistoryResult(request, outcome, coverage=request.coverage), strict)
            if previous_end is not None and start_ns < previous_end:
                outcome = _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca bars overlap or duplicate", pages=pages)
                return self._finish(HistoryResult(request, outcome, coverage=request.coverage), strict)
            previous_start = start_ns
            previous_end = end_ns

        if not converted:
            outcome = _error(OutcomeCode.EMPTY_WINDOW, "empty_window", retryable=False, message="no regular-session bars in requested half-open window", pages=pages)
            return self._finish(HistoryResult(request, outcome, coverage=request.coverage), strict)

        start_ns, end_ns = _instant_ns(request.start_at), _instant_ns(request.end_at)
        missing = _missing_intervals(start_ns, end_ns, converted)
        bars = tuple(_strip_internal(bar) for bar in converted)
        snapshot = self._build_snapshot(request, bars, missing)
        snapshot_result = validate_market_snapshot(snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted")
        if not snapshot_result.valid:
            outcome = _error(OutcomeCode.INVALID_MARKET_DATA, "invalid_market_data", retryable=False, message="converted Alpaca bars failed market validation", pages=pages)
            return self._finish(HistoryResult(request, outcome, bars=bars, coverage=request.coverage), strict)
        if recovery:
            recovery_bars = tuple(self._build_recovery_bars(request, bars, missing))
            if len(recovery_bars) != len(bars):
                outcome = _error(OutcomeCode.RECOVERY_INVALID, "recovery_invalid", retryable=False, message="converted recovery bars failed strict validation", pages=pages)
                return self._finish(HistoryResult(request, outcome, bars=bars, coverage=request.coverage), strict)
            outcome = _success(pages=pages, message="recovery bars fetched")
            return self._finish(HistoryResult(request, outcome, bars=bars, recovery_bars=recovery_bars, coverage=request.coverage), strict)
        return self._finish(HistoryResult(request, _success(pages=pages), bars=bars, snapshot=snapshot, coverage=request.coverage), strict)

    def _send(self, request: HistoryRequest, *, page_token: str | None) -> TransportResponse:
        if not self.has_credentials:
            raise CredentialError(_error(OutcomeCode.MISSING_CREDENTIALS, "authentication", retryable=False, authentication_terminal=True, message="Alpaca credentials are not configured"))
        spec = self.request_spec(request, page_token=page_token)
        try:
            if hasattr(self.transport, "send"):
                sender = self.transport.send  # type: ignore[attr-defined]
            elif hasattr(self.transport, "request"):
                sender = self.transport.request  # type: ignore[attr-defined]
            elif callable(self.transport):
                sender = self.transport
            else:
                raise TypeError("transport does not implement send")
            try:
                raw = sender(spec)
            except TypeError as first_error:
                # A tiny compatibility path for transports exposing a
                # conventional request(method, url, **kwargs) signature.
                try:
                    raw = sender(spec.method, spec.url, params=dict(spec.params), headers=dict(spec.headers), timeout=spec.timeout)
                except TypeError:
                    raise first_error
            response = coerce_transport_response(raw)
        except CredentialError:
            raise
        except TransportIOError as exc:
            code = OutcomeCode.TIMEOUT if getattr(exc, "is_timeout", False) else OutcomeCode.TRANSPORT_ERROR
            classification = "timeout" if code == OutcomeCode.TIMEOUT else "transport"
            raise AdapterError(_error(code, classification, retryable=True, message="Alpaca request timed out" if code == OutcomeCode.TIMEOUT else "Alpaca transport failed", secrets=self._secrets))
        except TimeoutError:
            raise AdapterError(_error(OutcomeCode.TIMEOUT, "timeout", retryable=True, message="Alpaca request timed out", secrets=self._secrets))
        except MalformedTransportResponse:
            raise AdapterError(
                _error(
                    OutcomeCode.MALFORMED_RESPONSE,
                    "malformed_response",
                    retryable=False,
                    message="Alpaca transport response is malformed",
                    secrets=self._secrets,
                )
            ) from None
        except urllib.error.HTTPError as exc:
            # Recorded transports may raise HTTPError directly; normalize it
            # exactly as the default urllib transport's response path.
            response = TransportResponse(int(exc.code), b"")
        except (OSError, ConnectionError):
            raise AdapterError(_error(OutcomeCode.TRANSPORT_ERROR, "transport", retryable=True, message="Alpaca transport failed", secrets=self._secrets))
        except Exception:
            raise AdapterError(_error(OutcomeCode.TRANSPORT_ERROR, "transport", retryable=True, message="Alpaca transport failed", secrets=self._secrets))

        status = response.status_code
        # ``coerce_transport_response`` validates this boundary.  Keep the
        # guard local as well for custom transports returning a response-like
        # object that bypasses the normalizer in a future implementation.
        if isinstance(status, bool) or not isinstance(status, int) or not 100 <= status <= 599:
            raise AdapterError(
                _error(
                    OutcomeCode.MALFORMED_RESPONSE,
                    "malformed_response",
                    retryable=False,
                    message="Alpaca transport response is malformed",
                    secrets=self._secrets,
                )
            )
        if status in {401, 403}:
            raise AdapterError(_error(OutcomeCode.AUTHENTICATION, "authentication", retryable=False, authentication_terminal=True, message="Alpaca authentication failed", status_code=status, secrets=self._secrets))
        if status == 429:
            raise AdapterError(_error(OutcomeCode.RATE_LIMIT, "rate_limit", retryable=True, message="Alpaca rate limit", status_code=status, secrets=self._secrets))
        if 500 <= status <= 599:
            raise AdapterError(_error(OutcomeCode.SERVER_ERROR, "server", retryable=True, message="Alpaca server failure", status_code=status, secrets=self._secrets))
        if 400 <= status <= 499:
            raise AdapterError(_error(OutcomeCode.INVALID_REQUEST, "provider_request", retryable=False, message="Alpaca request rejected", status_code=status, secrets=self._secrets))
        if status < 200 or status >= 300:
            raise AdapterError(_error(OutcomeCode.TRANSPORT_ERROR, "transport", retryable=True, message="unexpected Alpaca response status", status_code=status, secrets=self._secrets))
        return response

    def _parse_page_impl(
        self,
        response: TransportResponse,
        request: HistoryRequest,
        *,
        pages: int,
    ) -> tuple[str | None, list[Mapping[str, Any]], AdapterOutcome | None]:
        try:
            payload = response.json()
        except Exception:
            return None, [], _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca response body is not valid JSON", pages=pages, secrets=self._secrets)
        if not isinstance(payload, Mapping):
            return None, [], _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca response root must be an object", pages=pages, secrets=self._secrets)
        if "symbol" in payload:
            response_symbol = payload.get("symbol")
            if not isinstance(response_symbol, str) or response_symbol != request.symbol:
                return None, [], _error(
                    OutcomeCode.MALFORMED_RESPONSE,
                    "malformed_response",
                    retryable=False,
                    message="Alpaca response symbol does not match requested symbol",
                    pages=pages,
                    secrets=self._secrets,
                )
        if "bars" not in payload:
            return None, [], _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca response is missing bars", pages=pages, secrets=self._secrets)
        raw_bars = payload.get("bars")
        nested_next_token: Any = None
        if isinstance(raw_bars, Mapping):
            keys = set(raw_bars)
            # A legacy recorded fixture nests ``next_page_token`` alongside
            # the symbol inside ``bars``.  Keep that metadata compatibility,
            # while rejecting every other cross-symbol key.
            if keys - {request.symbol, "next_page_token"}:
                return None, [], _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca response does not contain requested symbol", pages=pages, secrets=self._secrets)
            if "next_page_token" in keys:
                nested_next_token = raw_bars.get("next_page_token")
            if request.symbol not in keys:
                raw_bars = []
            else:
                selected = raw_bars.get(request.symbol)
                if selected is None:
                    return None, [], _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca bars symbol value is malformed", pages=pages, secrets=self._secrets)
                raw_bars = selected
        elif raw_bars is None:
            return None, [], _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca bars field is malformed", pages=pages, secrets=self._secrets)
        if raw_bars is None:
            raw_bars = []
        if not isinstance(raw_bars, list):
            return None, [], _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca bars field is malformed", pages=pages, secrets=self._secrets)
        next_token = payload.get("next_page_token") if "next_page_token" in payload else nested_next_token
        if next_token in (None, ""):
            next_token = None
        elif not isinstance(next_token, str) or len(next_token) > 4096:
            return None, [], _error(OutcomeCode.MALFORMED_RESPONSE, "malformed_response", retryable=False, message="Alpaca pagination token is malformed", pages=pages, secrets=self._secrets)
        # Preserve raw objects only in memory.  Token never enters an artifact
        # or exception and is used exclusively for the next transport call.
        if any(not isinstance(item, Mapping) for item in raw_bars):
            return None, [], _error(
                OutcomeCode.MALFORMED_RESPONSE,
                "malformed_response",
                retryable=False,
                message="Alpaca bars array contains a malformed item",
                pages=pages,
                secrets=self._secrets,
            )
        return next_token, list(raw_bars), None

    def _parse_page(
        self,
        response: TransportResponse,
        request: HistoryRequest,
        *,
        pages: int,
    ) -> tuple[str | None, list[Mapping[str, Any]], AdapterOutcome | None]:
        """Parse an untrusted response through a non-throwing boundary."""

        try:
            return self._parse_page_impl(response, request, pages=pages)
        except Exception:
            return None, [], _error(
                OutcomeCode.MALFORMED_RESPONSE,
                "malformed_response",
                retryable=False,
                message="Alpaca response body is malformed",
                pages=pages,
                secrets=self._secrets,
            )

    def _build_snapshot(
        self,
        request: HistoryRequest,
        bars: Sequence[Mapping[str, Any]],
        missing: Sequence[Mapping[str, str]],
    ) -> dict[str, Any]:
        instrument = {"asset_class": "us_equity", "symbol": request.symbol, "venue": "IEX", "currency": "USD"}
        timeframe = dict(request.timeframe)
        session = {"kind": SESSION_KIND, "timezone": MARKET_TIMEZONE}
        bars_clean = [dict(bar) for bar in bars]
        actual = {"start_at": bars_clean[0]["start_at"], "end_at": bars_clean[-1]["end_at"]}
        coverage = {
            "expected": request.coverage.interval,
            "actual": actual,
            "status": "partial" if missing else "complete",
            "missing_intervals": [dict(interval) for interval in missing],
        }
        quality = {
            "status": "partial" if missing else "ok",
            "codes": ["partial_data"] if missing else [],
            "warnings": ["partial_data"] if missing else [],
            "details": None,
        }
        # ``analysis_as_of`` is deterministic and at the request's exclusive
        # end boundary.  generated_at is intentionally excluded by G014's
        # canonical hash projection.
        snapshot = {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": "market_snapshot",
            "extensions": {
                "market_feed": self.feed,
                "alpaca_history": {
                    "coverage": request.coverage.to_dict(),
                },
            },
            "input_mode": "ohlcv",
            "instrument": instrument,
            "timeframe": timeframe,
            "session": session,
            "adjustment_policy": "unadjusted",
            "analysis_as_of": request.end_at,
            "coverage": coverage,
            "bars": bars_clean,
            "quality": quality,
            "generated_at": request.end_at,
        }
        return finalize_artifact(snapshot, "market_snapshot")

    def _build_recovery_bars(
        self,
        request: HistoryRequest,
        bars: Sequence[Mapping[str, Any]],
        missing: Sequence[Mapping[str, str]],
    ) -> list[RecoveryBar]:
        missing_starts = {(item["start_at"], item["end_at"]) for item in missing}
        result: list[RecoveryBar] = []
        for bar in bars:
            interval = (bar["start_at"], bar["end_at"])
            complete = interval not in missing_starts
            recovery = RecoveryBar(
                instrument={"asset_class": "us_equity", "symbol": request.symbol, "venue": "IEX", "currency": "USD"},
                timeframe={"unit": "minute", "multiplier": 1},
                session={"kind": SESSION_KIND, "timezone": MARKET_TIMEZONE},
                start_at=bar["start_at"],
                end_at=bar["end_at"],
                open=bar["open"],
                high=bar["high"],
                low=bar["low"],
                close=bar["close"],
                volume=bar["volume"],
                complete=complete,
                coverage_state="complete" if complete else "partial",
                source_request_hash=request.source_request_hash,
                recovery_cursor=bar["end_at"],
                market_feed=self.feed,
            )
            if not validate_recovery_bar(recovery.to_dict()).valid:
                # This should be unreachable after the G015 snapshot pass;
                # leave no partially constructed object in a success result.
                continue
            result.append(recovery)
        return result


# Public aliases make the provider-specific name obvious without forcing
# callers to know the implementation class spelling.
AlpacaIEXHistoryAdapter = AlpacaHistoryAdapter
AlpacaHistoricalBarsAdapter = AlpacaHistoryAdapter
AlpacaIexHistoryAdapter = AlpacaHistoryAdapter
AlpacaIexHistoricalBarsAdapter = AlpacaHistoryAdapter
AlpacaHistoricalBars = AlpacaHistoryAdapter
HistoryAdapter = AlpacaHistoryAdapter
Transport = HistoryTransport


def build_history_request(
    symbol: str,
    start_at: str | datetime | date,
    end_at: str | datetime | date,
    timeframe: Mapping[str, Any] | str,
    *,
    limit: int = DEFAULT_LIMIT,
) -> HistoryRequest:
    return AlpacaHistoryAdapter(key_id="placeholder", secret_key="placeholder").build_request(symbol, start_at, end_at, timeframe, limit=limit)


def build_recovery_request(
    symbol: str,
    start_at: str | datetime | date,
    end_at: str | datetime | date,
    *,
    limit: int = DEFAULT_LIMIT,
) -> HistoryRequest:
    return AlpacaHistoryAdapter(key_id="placeholder", secret_key="placeholder").build_recovery_request(symbol, start_at, end_at, limit=limit)


__all__ = [
    "ADAPTER_VERSION",
    "ADJUSTMENT",
    "AdapterError",
    "AdapterOutcome",
    "AlpacaHistoricalBarsAdapter",
    "AlpacaHistoricalBars",
    "AlpacaHistoryAdapter",
    "AlpacaIexHistoryAdapter",
    "AlpacaIexHistoricalBarsAdapter",
    "AlpacaIEXHistoryAdapter",
    "BASE_URL",
    "CoverageBoundary",
    "CredentialError",
    "DEFAULT_LIMIT",
    "FEED",
    "HistoryAdapter",
    "HistoryRequest",
    "HistoryResponseError",
    "HistoryResult",
    "MAX_LIMIT",
    "MAX_PAGES",
    "MAX_RECOVERY_WINDOW",
    "MARKET_TIMEZONE",
    "OutcomeCode",
    "RequestValidationError",
    "SESSION_KIND",
    "SORT",
    "Transport",
    "build_history_request",
    "build_recovery_request",
]
