"""Strict parsing for Alpaca stock stream message arrays.

Alpaca sends every WebSocket frame as a JSON array.  The parser keeps prices
as :class:`~decimal.Decimal` from the first JSON decode and rejects mappings
which already contain a binary ``float``.  This is important for deterministic
hashes and one-second extrema: accepting a float here would silently change a
provider price before the reducer sees it.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .errors import MalformedPayloadError


_TIMESTAMP_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?Z$"
)
_TAPES = frozenset({"A", "B", "C", "O"})
_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,11}$")


def _safe_malformed() -> MalformedPayloadError:
    return MalformedPayloadError()


def _parse_timestamp(value: Any) -> tuple[str, int]:
    """Return canonical UTC ``Z`` text and nanoseconds since Unix epoch.

    Alpaca timestamps have up to nine fractional digits.  ``datetime`` only
    stores microseconds, so ordering uses the accompanying integer nanosecond
    value while the exact canonical text remains available for provenance.
    """

    if not isinstance(value, str):
        raise _safe_malformed()
    match = _TIMESTAMP_RE.fullmatch(value)
    if not match:
        raise _safe_malformed()
    text = match.group("date")
    fraction = (match.group("fraction") or "").ljust(9, "0")
    canonical = f"{text}.{fraction.rstrip('0')}Z" if fraction.rstrip("0") else f"{text}Z"
    try:
        parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        raise _safe_malformed() from None
    epoch_ns = int(parsed.timestamp()) * 1_000_000_000 + int(fraction)
    return canonical, epoch_ns


def canonical_timestamp(value: Any) -> str:
    """Validate and normalize a provider timestamp to UTC ``Z`` text."""

    return _parse_timestamp(value)[0]


def timestamp_ns(value: Any) -> int:
    """Return an integer nanosecond ordering key for a UTC timestamp."""

    return _parse_timestamp(value)[1]


def _parse_decimal(value: Any, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    # A Python float has already lost information and is never silently
    # accepted.  JSON text is decoded with parse_float=Decimal below.
    if isinstance(value, float):
        raise _safe_malformed()
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, str)):
        raise _safe_malformed()
    if isinstance(value, str) and value.strip() != value:
        raise _safe_malformed()
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise _safe_malformed() from None
    if not result.is_finite() or (positive and result <= 0) or (nonnegative and result < 0):
        raise _safe_malformed()
    return result


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _parse_id(value: Any, *, field_name: str) -> int | str:
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(value, (int, str)):
        raise _safe_malformed()
    if isinstance(value, int):
        if value < 0:
            raise _safe_malformed()
        return value
    if not value or value.strip() != value:
        raise _safe_malformed()
    # Alpaca's stock IDs are decimal integers.  Retain an opaque non-empty
    # string as a forward-compatible provider identity, but reject secrets and
    # control characters that should never be part of a trade ID.
    if any(ord(char) < 0x20 for char in value):
        raise _safe_malformed()
    return value


def _parse_size(value: Any) -> int:
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(value, int) or value <= 0:
        raise _safe_malformed()
    return value


def _parse_conditions(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or any(not isinstance(code, str) or len(code) != 1 for code in value):
        raise _safe_malformed()
    # A blank condition is represented by a single space in the official
    # table.  Keep order and duplicates for provenance, but the rule reducer
    # de-duplicates when selecting the strictest behavior.
    return tuple(value)


def _parse_tape(value: Any) -> str:
    if not isinstance(value, str) or value not in _TAPES:
        raise _safe_malformed()
    return value


def _parse_symbol(value: Any) -> str:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise _safe_malformed()
    return value


def _parse_exchange(value: Any) -> str:
    if not isinstance(value, str) or len(value) != 1 or not value.isprintable():
        raise _safe_malformed()
    return value


def _decode_payload(payload: Any) -> Any:
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = bytes(payload).decode("utf-8")
        except UnicodeDecodeError:
            raise _safe_malformed() from None
    if isinstance(payload, str):
        try:
            # parse_float is the non-negotiable exact-price boundary.
            payload = json.loads(payload, parse_float=Decimal, parse_int=int)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError):
            raise _safe_malformed() from None
    if not isinstance(payload, list):
        raise _safe_malformed()
    return payload


def _received_text(received_at: Any) -> str | None:
    if received_at is None:
        return None
    if isinstance(received_at, datetime):
        if received_at.tzinfo is None or received_at.utcoffset() != timezone.utc.utcoffset(received_at):
            raise _safe_malformed()
        received_at = received_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return canonical_timestamp(received_at)


@dataclass(frozen=True, slots=True)
class TradeEvent:
    """Normalized Alpaca ``T=t`` trade."""

    symbol: str
    trade_id: int | str
    exchange: str
    price: Decimal
    size: int
    conditions: tuple[str, ...]
    event_time: str
    tape: str
    received_at: str | None = None
    event_time_ns: int = field(init=False, repr=False)
    message_type: str = field(default="t", init=False)

    def __post_init__(self) -> None:
        if _SYMBOL_RE.fullmatch(self.symbol) is None or not isinstance(self.trade_id, (int, str)) or isinstance(self.trade_id, bool):
            raise _safe_malformed()
        if not isinstance(self.price, Decimal) or not self.price.is_finite() or self.price <= 0 or isinstance(self.size, bool) or not isinstance(self.size, int) or self.size <= 0:
            raise _safe_malformed()
        if not isinstance(self.conditions, tuple) or not self.conditions or any(not isinstance(code, str) or len(code) != 1 for code in self.conditions):
            raise _safe_malformed()
        object.__setattr__(self, "event_time_ns", timestamp_ns(self.event_time))

    @property
    def provider_trade_id(self) -> int | str:
        return self.trade_id

    @property
    def original_event_time(self) -> str:
        return self.event_time

    @property
    def source(self) -> str:
        return "alpaca_iex"

    @property
    def adjustment(self) -> str:
        return "raw"

    @property
    def condition_codes(self) -> tuple[str, ...]:
        return self.conditions

    @property
    def event_datetime(self) -> datetime:
        base = self.event_time.removesuffix("Z")
        return datetime.fromisoformat(base).replace(tzinfo=timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        return {
            "T": "t",
            "S": self.symbol,
            "i": self.trade_id,
            "x": self.exchange,
            "p": _decimal_text(self.price),
            "s": self.size,
            "c": list(self.conditions),
            "t": self.event_time,
            "z": self.tape,
            "source": self.source,
            "adjustment": self.adjustment,
        }


@dataclass(frozen=True, slots=True)
class CorrectionEvent:
    """Normalized Alpaca ``T=c`` trade correction."""

    symbol: str
    original_trade_id: int | str
    original_price: Decimal
    original_size: int
    original_conditions: tuple[str, ...]
    corrected_trade_id: int | str
    corrected_price: Decimal
    corrected_size: int
    corrected_conditions: tuple[str, ...]
    exchange: str
    tape: str
    event_time: str
    received_at: str | None = None
    event_time_ns: int = field(init=False, repr=False)
    message_type: str = field(default="c", init=False)

    def __post_init__(self) -> None:
        if _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise _safe_malformed()
        for price, size, conditions in ((self.original_price, self.original_size, self.original_conditions), (self.corrected_price, self.corrected_size, self.corrected_conditions)):
            if not isinstance(price, Decimal) or not price.is_finite() or price <= 0 or isinstance(size, bool) or not isinstance(size, int) or size <= 0 or not isinstance(conditions, tuple) or not conditions:
                raise _safe_malformed()
        object.__setattr__(self, "event_time_ns", timestamp_ns(self.event_time))

    @property
    def original_event_id(self) -> int | str:
        return self.original_trade_id

    @property
    def corrected_event_id(self) -> int | str:
        return self.corrected_trade_id

    @property
    def source(self) -> str:
        return "alpaca_iex"

    @property
    def adjustment(self) -> str:
        return "raw"

    def to_dict(self) -> dict[str, Any]:
        return {
            "T": "c",
            "S": self.symbol,
            "x": self.exchange,
            "oi": self.original_trade_id,
            "op": _decimal_text(self.original_price),
            "os": self.original_size,
            "oc": list(self.original_conditions),
            "ci": self.corrected_trade_id,
            "cp": _decimal_text(self.corrected_price),
            "cs": self.corrected_size,
            "cc": list(self.corrected_conditions),
            "z": self.tape,
            "t": self.event_time,
            "source": self.source,
            "adjustment": self.adjustment,
        }


@dataclass(frozen=True, slots=True)
class CancelErrorEvent:
    """Normalized Alpaca ``T=x`` cancel/error event."""

    symbol: str
    trade_id: int | str
    exchange: str
    price: Decimal
    size: int
    action: str
    event_time: str
    tape: str
    received_at: str | None = None
    event_time_ns: int = field(init=False, repr=False)
    message_type: str = field(default="x", init=False)

    def __post_init__(self) -> None:
        if _SYMBOL_RE.fullmatch(self.symbol) is None or not isinstance(self.price, Decimal) or not self.price.is_finite() or self.price <= 0 or isinstance(self.size, bool) or not isinstance(self.size, int) or self.size <= 0 or not isinstance(self.action, str) or self.action not in {"C", "E"}:
            raise _safe_malformed()
        object.__setattr__(self, "event_time_ns", timestamp_ns(self.event_time))

    @property
    def provider_trade_id(self) -> int | str:
        return self.trade_id

    @property
    def source(self) -> str:
        return "alpaca_iex"

    @property
    def adjustment(self) -> str:
        return "raw"

    def to_dict(self) -> dict[str, Any]:
        return {
            "T": "x",
            "S": self.symbol,
            "i": self.trade_id,
            "x": self.exchange,
            "p": _decimal_text(self.price),
            "s": self.size,
            "a": self.action,
            "t": self.event_time,
            "z": self.tape,
            "source": self.source,
            "adjustment": self.adjustment,
        }


@dataclass(frozen=True, slots=True)
class StatusEvent:
    """Connection/status/control message (including typed provider errors)."""

    message_type: str
    message: str | None = None
    code: int | None = None
    # ``semantic`` is a closed, provider-independent control state.  The raw
    # ``msg`` is intentionally never retained; callers can distinguish the
    # initial transport handshake (``connected``) from authentication.
    semantic: str = "unknown"
    trades: tuple[str, ...] = ()
    corrections: tuple[str, ...] = ()
    cancel_errors: tuple[str, ...] = ()
    quotes: tuple[str, ...] = ()
    bars: tuple[str, ...] = ()
    payload: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @property
    def kind(self) -> str:
        return self.message_type

    @property
    def control_semantic(self) -> str:
        """Alias used by lifecycle consumers for the safe semantic enum."""

        return self.semantic

    @property
    def subscription_symbols(self) -> dict[str, tuple[str, ...]]:
        return {
            "trades": self.trades,
            "corrections": self.corrections,
            "cancelErrors": self.cancel_errors,
            "quotes": self.quotes,
            "bars": self.bars,
        }

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"T": self.message_type}
        if self.code is not None:
            result["code"] = self.code
        result["semantic"] = self.semantic
        # Keep the normalized subscription acknowledgement auditable without
        # retaining provider text or arbitrary payload fields.
        if self.message_type in {"subscription", "subscribed"} or any((self.trades, self.corrections, self.cancel_errors, self.quotes, self.bars)):
            result.update({
                "trades": list(self.trades),
                "corrections": list(self.corrections),
                "cancelErrors": list(self.cancel_errors),
                "quotes": list(self.quotes),
                "bars": list(self.bars),
            })
        return result


StreamEvent = TradeEvent | CorrectionEvent | CancelErrorEvent | StatusEvent


def parse_trade(value: Mapping[str, Any], *, received_at: Any = None) -> TradeEvent:
    if not isinstance(value, Mapping) or value.get("T") != "t":
        raise _safe_malformed()
    allowed = {"T", "S", "i", "x", "p", "s", "c", "t", "z"}
    if set(value) - allowed:
        extras = set(value) - allowed
        if extras != {"source", "adjustment"} or value.get("source") != "alpaca_iex" or value.get("adjustment") != "raw":
            raise _safe_malformed()
    if not set(value).issuperset(allowed):
        raise _safe_malformed()
    try:
        symbol = _parse_symbol(value["S"])
        trade_id = _parse_id(value["i"], field_name="i")
        exchange = _parse_exchange(value["x"])
        price = _parse_decimal(value["p"], positive=True)
        size = _parse_size(value["s"])
        conditions = _parse_conditions(value["c"])
        event_time = canonical_timestamp(value["t"])
        tape = _parse_tape(value["z"])
        receipt = _received_text(received_at)
    except (KeyError, TypeError, MalformedPayloadError):
        raise _safe_malformed() from None
    return TradeEvent(symbol, trade_id, exchange, price, size, conditions, event_time, tape, receipt)


def parse_correction(value: Mapping[str, Any], *, received_at: Any = None) -> CorrectionEvent:
    if not isinstance(value, Mapping) or value.get("T") != "c":
        raise _safe_malformed()
    allowed = {"T", "S", "x", "oi", "op", "os", "oc", "ci", "cp", "cs", "cc", "z", "t"}
    if set(value) - allowed:
        extras = set(value) - allowed
        if extras != {"source", "adjustment"} or value.get("source") != "alpaca_iex" or value.get("adjustment") != "raw":
            raise _safe_malformed()
    if not set(value).issuperset(allowed):
        raise _safe_malformed()
    try:
        symbol = _parse_symbol(value["S"])
        exchange = _parse_exchange(value["x"])
        original_id = _parse_id(value["oi"], field_name="oi")
        original_price = _parse_decimal(value["op"], positive=True)
        original_size = _parse_size(value["os"])
        original_conditions = _parse_conditions(value["oc"])
        corrected_id = _parse_id(value["ci"], field_name="ci")
        corrected_price = _parse_decimal(value["cp"], positive=True)
        corrected_size = _parse_size(value["cs"])
        corrected_conditions = _parse_conditions(value["cc"])
        tape = _parse_tape(value["z"])
        event_time = canonical_timestamp(value["t"])
        receipt = _received_text(received_at)
    except (KeyError, TypeError, MalformedPayloadError):
        raise _safe_malformed() from None
    return CorrectionEvent(
        symbol,
        original_id,
        original_price,
        original_size,
        original_conditions,
        corrected_id,
        corrected_price,
        corrected_size,
        corrected_conditions,
        exchange,
        tape,
        event_time,
        receipt,
    )


def parse_cancel_error(value: Mapping[str, Any], *, received_at: Any = None) -> CancelErrorEvent:
    if not isinstance(value, Mapping) or value.get("T") != "x":
        raise _safe_malformed()
    allowed = {"T", "S", "i", "x", "p", "s", "a", "t", "z"}
    if set(value) - allowed:
        extras = set(value) - allowed
        if extras != {"source", "adjustment"} or value.get("source") != "alpaca_iex" or value.get("adjustment") != "raw":
            raise _safe_malformed()
    if not set(value).issuperset(allowed):
        raise _safe_malformed()
    try:
        symbol = _parse_symbol(value["S"])
        trade_id = _parse_id(value["i"], field_name="i")
        exchange = _parse_exchange(value["x"])
        price = _parse_decimal(value["p"], positive=True)
        size = _parse_size(value["s"])
        action = value["a"]
        # Alpaca uses ``C`` for a cancellation and ``E`` for a cancel/error
        # notification.  Accepting arbitrary text here would make replay and
        # revision identity ambiguous.
        if action not in {"C", "E"}:
            raise _safe_malformed()
        event_time = canonical_timestamp(value["t"])
        tape = _parse_tape(value["z"])
        receipt = _received_text(received_at)
    except (KeyError, TypeError, MalformedPayloadError):
        raise _safe_malformed() from None
    return CancelErrorEvent(symbol, trade_id, exchange, price, size, action, event_time, tape, receipt)


def parse_status(value: Mapping[str, Any]) -> StatusEvent:
    if not isinstance(value, Mapping):
        raise _safe_malformed()
    message_type = value.get("T")
    if not isinstance(message_type, str) or message_type not in {"success", "subscription", "error", "listening", "authenticated", "subscribed", "auth"}:
        raise _safe_malformed()
    code = value.get("code")
    if code is not None and (isinstance(code, bool) or not isinstance(code, int)):
        raise _safe_malformed()
    message = value.get("msg")
    # Do not retain provider text in the normalized object.  The stable status
    # marker is enough for authentication/subscription state transitions.
    if message is not None and not isinstance(message, str):
        raise _safe_malformed()
    lower_message = message.strip().lower() if isinstance(message, str) else ""
    if message_type == "error":
        semantic = "error"
    elif message_type in {"subscription", "subscribed"}:
        semantic = "subscription_ack"
    elif message_type in {"authenticated", "auth"}:
        semantic = "authenticated"
    elif message_type == "listening":
        semantic = "listening"
    elif message_type == "success" and lower_message == "authenticated":
        semantic = "authenticated"
    elif message_type == "success" and lower_message == "connected":
        semantic = "connected"
    elif message_type == "success":
        semantic = "success"
    else:
        semantic = "unknown"

    def _symbols(key: str, alternate: str | None = None) -> tuple[str, ...]:
        raw = value.get(key)
        if raw is None and alternate is not None:
            raw = value.get(alternate)
        if raw is None:
            return ()
        if not isinstance(raw, list):
            raise _safe_malformed()
        try:
            return tuple(_parse_symbol(symbol) for symbol in raw)
        except MalformedPayloadError:
            raise _safe_malformed() from None

    trades = _symbols("trades")
    corrections = _symbols("corrections")
    cancel_errors = _symbols("cancelErrors", "cancel_errors")
    quotes = _symbols("quotes")
    bars = _symbols("bars")
    # Retain only a fixed marker for compatibility with the existing status
    # API; provider text itself is deliberately omitted from serialization.
    safe_message = None if message is None else "provider_status"
    safe_payload: dict[str, Any] = {"T": message_type, "semantic": semantic}
    if code is not None:
        safe_payload["code"] = code
    if any((trades, corrections, cancel_errors, quotes, bars)) or message_type in {"subscription", "subscribed"}:
        safe_payload.update({"trades": list(trades), "corrections": list(corrections), "cancelErrors": list(cancel_errors), "quotes": list(quotes), "bars": list(bars)})
    return StatusEvent(message_type, safe_message, code, semantic, trades, corrections, cancel_errors, quotes, bars, safe_payload)


def parse_message(payload: Any, *, received_at: Any = None) -> list[StreamEvent]:
    """Parse one Alpaca WebSocket frame, which must be a JSON array."""

    values = _decode_payload(payload)
    events: list[StreamEvent] = []
    receipt = _received_text(received_at)
    for value in values:
        if not isinstance(value, Mapping):
            raise _safe_malformed()
        message_type = value.get("T")
        if message_type == "t":
            events.append(parse_trade(value, received_at=receipt))
        elif message_type == "c":
            events.append(parse_correction(value, received_at=receipt))
        elif message_type == "x":
            events.append(parse_cancel_error(value, received_at=receipt))
        else:
            events.append(parse_status(value))
    return events


parse_stream_message = parse_message
parse_alpaca_message = parse_message
parse_trade_message = parse_trade
parse_correction_message = parse_correction
parse_cancel_message = parse_cancel_error
normalize_trade = parse_trade
normalize_correction = parse_correction
normalize_cancel = parse_cancel_error
parse_trade_event = parse_trade
parse_correction_event = parse_correction
parse_cancel_error_event = parse_cancel_error


class AlpacaTradeParser:
    """Small namespace-style parser facade for dependency injection."""

    parse_message = staticmethod(parse_message)
    parse_trade = staticmethod(parse_trade)
    parse_correction = staticmethod(parse_correction)
    parse_cancel_error = staticmethod(parse_cancel_error)


TradeParser = AlpacaTradeParser


__all__ = [
    "CancelErrorEvent",
    "CorrectionEvent",
    "StatusEvent",
    "StreamEvent",
    "TradeEvent",
    "canonical_timestamp",
    "parse_alpaca_message",
    "parse_cancel_error",
    "parse_cancel_message",
    "parse_correction",
    "parse_correction_message",
    "parse_message",
    "parse_status",
    "parse_stream_message",
    "parse_trade",
    "parse_trade_message",
    "normalize_trade",
    "normalize_correction",
    "normalize_cancel",
    "parse_trade_event",
    "parse_correction_event",
    "parse_cancel_error_event",
    "AlpacaTradeParser",
    "TradeParser",
    "timestamp_ns",
]
