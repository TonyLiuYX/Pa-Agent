"""Strict adapters for the frozen G020/G019 values consumed by G026.

The exit reducer deliberately accepts the upstream values at a small mapping
boundary.  A malformed or forged mapping is represented as a typed rejection
by the reducer; it is never allowed to escape as an arbitrary traceback.
"""

from __future__ import annotations

import copy
import hashlib
import inspect
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Callable

from pa_engine.contracts import canonical_json_bytes
from pa_engine.market import ALPACA_IEX_FEED
from pa_engine.market_sources.alpaca.history import validate_recovery_bar
from pa_engine.market_sources.alpaca.stream import canonical_timestamp as _stream_timestamp
from pa_engine.market_sources.alpaca.stream import validate_second_event
from pa_engine.paper.core import PaperTrade


UTC = timezone.utc
PATH_PRECISION = "minute_bar_no_path"
RESOLUTION_GRACE_SECONDS = 3


def freeze(value: Any) -> Any:
    """Recursively freeze JSON-like values for public result boundaries."""

    if isinstance(value, Mapping):
        return _FrozenMap({str(key): freeze(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(child) for child in value)
    if isinstance(value, set):
        return tuple(sorted((freeze(child) for child in value), key=repr))
    return value


class _FrozenMap(Mapping):
    __slots__ = ("_data",)

    def __init__(self, value: Mapping[str, Any]) -> None:
        object.__setattr__(self, "_data", MappingProxyType(dict(value)))

    def __getitem__(self, key: Any) -> Any:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise TypeError("immutable mapping")

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_FrozenMap":
        return self


def thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: thaw(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [thaw(child) for child in value]
    return copy.deepcopy(value)


def _json_projection(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return canonical_timestamp(value)
    if isinstance(value, Mapping):
        return {str(key): _json_projection(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_projection(child) for child in value]
    return value


def identity_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(_json_projection(value))).hexdigest()


content_hash = identity_hash


def utc_instant(value: datetime | str, *, default: datetime | None = None) -> datetime:
    if value is None:
        if default is not None:
            return default.astimezone(UTC)
        raise ValueError("UTC timestamp is required")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC)
    if not isinstance(value, str):
        raise ValueError("timestamp must be a datetime or canonical UTC string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise ValueError("timestamp must be valid UTC") from None
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def canonical_timestamp(value: datetime | str) -> str:
    if isinstance(value, str):
        return _stream_timestamp(value)
    parsed = utc_instant(value)
    # G020/G019 use exact UTC, without a ``+00:00`` suffix.
    return parsed.isoformat().replace("+00:00", "Z")


def _field(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def exact_decimal(value: Any, field: str = "value", *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError(f"{field} must be an exact Decimal value")
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, (str, int)):
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise ValueError(f"{field} must be an exact Decimal value") from None
    else:
        raise ValueError(f"{field} must be an exact Decimal value")
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError(f"{field} must be finite and positive")
    return parsed


def _as_mapping(value: Any, name: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return copy.deepcopy(dict(value))
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        mapped = to_dict()
        if isinstance(mapped, Mapping):
            return copy.deepcopy(dict(mapped))
    raise ValueError(f"{name} must be a mapping or typed upstream value")


def trade_mapping(value: Any) -> dict[str, Any]:
    """Return a strict, copied projection of a G024 ``PaperTrade``."""

    if isinstance(value, PaperTrade):
        try:
            mapped = value.to_dict()
        except Exception:
            raise ValueError("trade mapping is malformed") from None
    else:
        try:
            mapped = _as_mapping(value, "trade")
        except Exception:
            raise ValueError("trade mapping is malformed") from None
    required = {
        "strategy_id",
        "trade_id",
        "symbol",
        "direction",
        "entry_price",
        "stop_price",
        "target_price",
        "quantity",
        "planned_r",
    }
    if not required.issubset(mapped):
        missing = ", ".join(sorted(required - set(mapped)))
        raise ValueError(f"trade is missing required fields: {missing}")
    sid = mapped.get("strategy_id")
    trade_id = mapped.get("trade_id")
    symbol = mapped.get("symbol")
    direction = mapped.get("direction")
    if not isinstance(sid, str) or not sid.strip() or not isinstance(trade_id, str) or not trade_id.strip():
        raise ValueError("trade identity is malformed")
    if mapped.get("opened") is False:
        raise ValueError("trade is not open")
    if not isinstance(symbol, str) or not symbol or symbol != symbol.upper() or any(ch.isspace() for ch in symbol):
        raise ValueError("trade symbol is malformed")
    if direction not in {"long", "short"}:
        raise ValueError("trade direction is malformed")
    for field in ("entry_price", "stop_price", "target_price", "quantity", "planned_r"):
        mapped[field] = exact_decimal(mapped[field], field, positive=field != "quantity")
    if mapped["quantity"] < 0:
        raise ValueError("trade quantity must not be negative")
    if direction == "long" and not (mapped["stop_price"] < mapped["entry_price"] < mapped["target_price"]):
        raise ValueError("long trade levels are malformed")
    if direction == "short" and not (mapped["target_price"] < mapped["entry_price"] < mapped["stop_price"]):
        raise ValueError("short trade levels are malformed")
    return mapped


def event_mapping(value: Any) -> dict[str, Any]:
    """Validate a G020 event and require the immutable frozen boundary."""

    mapped = _as_mapping(value, "second event")
    result = validate_second_event(mapped)
    if not getattr(result, "valid", False):
        raise ValueError("second event failed strict G020 validation")
    if mapped.get("status") != "frozen":
        raise ValueError("only execution-frozen second events are eligible")
    feed = mapped.get("market_feed")
    if hasattr(feed, "to_dict"):
        feed = feed.to_dict()
    if dict(feed or {}) != ALPACA_IEX_FEED.to_dict():
        raise ValueError("second event feed identity differs from Alpaca IEX")
    return mapped


def recovery_mapping(value: Any, *, expected_source_request_hash: str | None = None) -> dict[str, Any]:
    """Validate a G019 RecoveryBar and return its copied wire mapping."""

    mapped = _as_mapping(value, "recovery bar")
    result = validate_recovery_bar(mapped, expected_source_request_hash=expected_source_request_hash)
    if not getattr(result, "valid", False):
        raise ValueError("recovery bar failed strict G019 validation")
    feed = mapped.get("market_feed")
    if hasattr(feed, "to_dict"):
        mapped["market_feed"] = feed.to_dict()
    return mapped


def fill_interval(value: Any, _depth: int = 0) -> tuple[str, str] | None:
    """Extract the exact filled interval from a G020 entry reference."""

    if value is None or _depth > 8:
        return None
    try:
        mapped = _as_mapping(value, "entry event")
    except Exception:
        return None
    start = mapped.get("start_at") or mapped.get("interval_start")
    end = mapped.get("end_at") or mapped.get("interval_end")
    if not isinstance(start, str) or not isinstance(end, str):
        interval = mapped.get("interval")
        if isinstance(interval, (list, tuple)) and len(interval) == 2:
            start, end = interval
    if not isinstance(start, str) or not isinstance(end, str):
        for nested_key in ("source", "event_reference", "second_event_ref", "entry_event_ref", "lineage", "payload"):
            nested = mapped.get(nested_key)
            nested_interval = fill_interval(nested, _depth + 1) if nested is not None else None
            if nested_interval is not None:
                return nested_interval
    if not isinstance(start, str) or not isinstance(end, str):
        return None
    try:
        return canonical_timestamp(start), canonical_timestamp(end)
    except Exception:
        return None


def fill_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        try:
            return utc_instant(value)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            return utc_instant(value)
        except Exception:
            return None
    return None


def invoke_compatible(callback: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Invoke a small typed callback without leaking signature errors."""

    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args, **kwargs)
    accepted = {
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
    }
    attempts = [(args, kwargs), (args, {key: value for key, value in kwargs.items() if key in accepted}), (args, {}), ((args[0],), {}), ((), {})]
    for positional, keywords in attempts:
        try:
            signature.bind_partial(*positional, **keywords)
        except TypeError:
            continue
        return callback(*positional, **keywords)
    return callback(*args, **kwargs)


__all__ = [
    "ALPACA_IEX_FEED",
    "PATH_PRECISION",
    "RESOLUTION_GRACE_SECONDS",
    "canonical_timestamp",
    "content_hash",
    "event_mapping",
    "exact_decimal",
    "fill_interval",
    "fill_timestamp",
    "freeze",
    "identity_hash",
    "invoke_compatible",
    "recovery_mapping",
    "thaw",
    "trade_mapping",
    "utc_instant",
]
