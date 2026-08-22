"""G025 input boundary helpers.

This module is intentionally provider/account agnostic apart from consuming
the accepted G018/G019/G020 values.  It validates the wait-for-price branch,
normalizes exact UTC/Decimal values and constructs the immutable pending state.
"""

from __future__ import annotations

import copy
import hashlib
import inspect
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping

from pa_engine.contracts import canonical_json_bytes
from pa_engine.market import ALPACA_IEX_FEED
from pa_engine.market_sources.alpaca.history import RecoveryBar, validate_recovery_bar
from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent, validate_second_event

from .models import EntryLineage, PendingEntryState, freeze


UTC = timezone.utc
RESOLUTION_GRACE_SECONDS = 3


def utc_instant(value: datetime | str, *, default: datetime | None = None) -> datetime:
    if value is None:
        if default is None:
            raise ValueError("UTC instant is required")
        value = default
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        text = value
        fraction = re.search(r"\.(\d+)(?:Z|[+-]\d\d:?\d\d)$", text)
        if fraction is not None and len(fraction.group(1)) > 6:
            raise ValueError("timestamp precision exceeds the in-memory UTC boundary")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(text)
        except ValueError:
            raise ValueError("timestamp must be an ISO UTC instant") from None
    else:
        raise ValueError("timestamp must be an ISO UTC instant")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timestamp must include UTC offset")
    return result.astimezone(UTC)


def canonical_timestamp(value: datetime | str) -> str:
    instant = utc_instant(value)
    # Public upstream contracts permit at most nanosecond precision.  Python
    # datetime has microsecond precision; reject silently lossy values rather
    # than inventing a precision extension.
    if instant.microsecond:
        text = instant.isoformat(timespec="microseconds").replace("+00:00", "Z")
        head, _z = text[:-1], text[-1]
        return head.rstrip("0").rstrip(".") + "Z"
    return instant.isoformat(timespec="seconds").replace("+00:00", "Z")


def exact_decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError(f"{field} must be an exact Decimal, not float/bool")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, str):
        try:
            result = Decimal(value)
        except (InvalidOperation, ValueError):
            raise ValueError(f"{field} must be a finite Decimal") from None
    else:
        raise ValueError(f"{field} must be an exact Decimal")
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{field} must be positive and finite")
    return result


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


def content_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(_json_projection(value))).hexdigest()


def _value(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def _profile_lineage(instruction: Mapping[str, Any]) -> EntryLineage:
    return EntryLineage(
        instruction_id=instruction.get("instruction_id"),
        instruction_hash=instruction.get("instruction_hash"),
        result_id=instruction.get("result_id"),
        result_hash=instruction.get("result_hash"),
        evidence_package_id=instruction.get("evidence_package_id"),
        evidence_package_hash=instruction.get("evidence_package_hash"),
        profile_ref_id=instruction.get("profile_ref_id"),
        profile_ref_hash=instruction.get("profile_ref_hash"),
        market_view_ref=instruction.get("market_view_ref"),
        instruction=copy.deepcopy(dict(instruction)),
    )


def validate_wait_instruction(
    instruction: Mapping[str, Any],
    *,
    strategy_id: str | None = None,
    expected_profile_ref: Mapping[str, Any] | None = None,
    validator: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Validate a complete G018 wait-for-price instruction.

    The authoritative G018 verifier is called by default.  Tests and small
    integrations may inject a compatible verifier, but the local branch and
    exact-level checks always remain active.
    """

    if not isinstance(instruction, Mapping):
        raise ValueError("instruction must be an object")
    raw = copy.deepcopy(dict(instruction))
    value = copy.deepcopy(raw)
    if value.get("branch") != "wait_for_price":
        raise ValueError("only the G018 wait_for_price branch is accepted")
    sid = value.get("strategy_id")
    if not isinstance(sid, str) or not sid.strip() or (strategy_id is not None and sid != strategy_id):
        raise ValueError("strategy_id is missing or does not match the pending monitor")
    if value.get("direction") not in {"long", "short"}:
        raise ValueError("direction must be long or short")
    for field in ("entry_price", "stop_price", "target_price"):
        value[field] = exact_decimal(value.get(field), field)
    x = value.get("entry_wait_seconds")
    if type(x) is not int or not 1 <= x <= 86_400:
        raise ValueError("entry_wait_seconds must be an integer from 1 through 86400")
    if value["direction"] == "long" and not (value["stop_price"] < value["entry_price"] < value["target_price"]):
        raise ValueError("long levels must satisfy stop < entry < target")
    if value["direction"] == "short" and not (value["target_price"] < value["entry_price"] < value["stop_price"]):
        raise ValueError("short levels must satisfy target < entry < stop")
    if validator is None:
        try:
            from pa_engine.strategy.decision import verify_instruction

            validator = verify_instruction
        except Exception:
            validator = None
    if validator is False:
        validator = None
    if validator is not None:
        kwargs: dict[str, Any] = {}
        if expected_profile_ref is not None:
            kwargs["expected_profile_ref"] = expected_profile_ref
        try:
            # The authoritative G018 boundary intentionally accepts only its
            # canonical JSON wire values (price strings), so validate the raw
            # copy before replacing prices with exact Decimal values locally.
            valid = validator(raw, **kwargs)
        except TypeError:
            valid = validator(raw)
        except Exception as exc:
            raise ValueError("authoritative G018 instruction validation failed") from exc
        if getattr(valid, "valid", valid) is not True:
            raise ValueError("instruction is not a complete, bound G018 instruction")
    return value


def symbol_from_instruction(instruction: Mapping[str, Any], symbol: str | None = None) -> str:
    value = symbol or instruction.get("symbol")
    if value is None:
        view = instruction.get("market_view_ref")
        if isinstance(view, Mapping):
            value = view.get("symbol")
            instrument = view.get("instrument")
            if value is None and isinstance(instrument, Mapping):
                value = instrument.get("symbol")
    if not isinstance(value, str) or not value or value != value.upper() or any(ch.isspace() for ch in value):
        raise ValueError("a canonical symbol is required at the entry boundary")
    return value


def feed_from_instruction(instruction: Mapping[str, Any], market_feed: Mapping[str, Any] | Any | None = None) -> dict[str, Any]:
    value = market_feed
    if value is None:
        value = instruction.get("market_feed") or instruction.get("feed")
    if value is None:
        value = ALPACA_IEX_FEED
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise ValueError("market_feed must be a mapping")
    result = dict(value)
    expected = ALPACA_IEX_FEED.to_dict()
    if result != expected:
        raise ValueError("G025 only accepts the frozen Alpaca IEX feed identity")
    return result


def make_pending_state(
    instruction: Mapping[str, Any],
    *,
    symbol: str | None = None,
    decision_accepted_at: datetime | str | None = None,
    submitted_at: datetime | str | None = None,
    generation: int = 0,
    pending_id: str | None = None,
    market_feed: Mapping[str, Any] | Any | None = None,
    expected_profile_ref: Mapping[str, Any] | None = None,
    validator: Callable[..., Any] | None = None,
) -> PendingEntryState:
    raw_instruction = copy.deepcopy(dict(instruction))
    value = validate_wait_instruction(raw_instruction, strategy_id=instruction.get("strategy_id"), expected_profile_ref=expected_profile_ref, validator=validator)
    accepted = utc_instant(decision_accepted_at or raw_instruction.get("decision_accepted_at"))
    submitted = utc_instant(submitted_at or raw_instruction.get("submitted_at"), default=accepted)
    x = int(value["entry_wait_seconds"])
    expires = accepted + timedelta(seconds=x)
    resolution = expires + timedelta(seconds=RESOLUTION_GRACE_SECONDS)
    feed = feed_from_instruction(value, market_feed)
    sid = value["strategy_id"]
    lineage = _profile_lineage(raw_instruction)
    instruction_hash = value.get("instruction_hash") or content_hash(_json_projection(value))
    expected_pending_id = "pend_" + hashlib.sha256(f"{sid}:{generation}:{instruction_hash}".encode("utf-8")).hexdigest()[:32]
    if pending_id is None:
        pending_id = expected_pending_id
    elif pending_id != expected_pending_id:
        raise ValueError("pending_id must be the content-bound pending identity")
    if not isinstance(pending_id, str) or not pending_id.strip():
        raise ValueError("pending_id must be a non-empty stable ID")
    if type(generation) is not int or generation < 0:
        raise ValueError("generation must be a non-negative integer")
    return PendingEntryState(
        strategy_id=sid,
        pending_id=pending_id,
        symbol=symbol_from_instruction(value, symbol),
        direction=value["direction"],
        entry_price=value["entry_price"],
        stop_price=value["stop_price"],
        target_price=value["target_price"],
        decision_lineage=lineage,
        decision_accepted_at=accepted,
        submitted_at=submitted,
        expires_at=expires,
        resolution_at=resolution,
        generation=generation,
        market_feed=feed,
    )


def event_mapping(value: Any) -> dict[str, Any]:
    """Validate and copy a frozen G020 ``SecondAggregateEvent``."""

    if isinstance(value, SecondAggregateEvent):
        event = value
        mapped = event.to_dict()
    elif isinstance(value, Mapping):
        mapped = copy.deepcopy(dict(value))
    else:
        to_dict = getattr(value, "to_dict", None)
        if not callable(to_dict):
            raise ValueError("second event must be a G020 event or mapping")
        mapped = copy.deepcopy(dict(to_dict()))
    if hasattr(mapped.get("market_feed"), "to_dict"):
        mapped["market_feed"] = mapped["market_feed"].to_dict()
    result = validate_second_event(mapped)
    if not result.valid:
        raise ValueError("second event failed strict G020 validation")
    if mapped.get("status") != "frozen":
        raise ValueError("only execution-frozen second events are eligible")
    return mapped


def recovery_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, RecoveryBar):
        mapped = value.to_dict()
    elif isinstance(value, Mapping):
        mapped = copy.deepcopy(dict(value))
    else:
        to_dict = getattr(value, "to_dict", None)
        if not callable(to_dict):
            raise ValueError("recovery value must be a G019 RecoveryBar or mapping")
        mapped = copy.deepcopy(dict(to_dict()))
    if hasattr(mapped.get("market_feed"), "to_dict"):
        mapped["market_feed"] = mapped["market_feed"].to_dict()
    result = validate_recovery_bar(mapped)
    if not result.valid:
        raise ValueError("recovery bar failed strict G019 validation")
    return mapped


def invoke_compatible(callback: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call a boundary callback with a compatible typed argument shape."""

    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args, **kwargs)
    attempts = [(args, kwargs)]
    accepted_names = {name for name, parameter in signature.parameters.items() if parameter.kind in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}}
    if kwargs:
        attempts.append((args, {key: value for key, value in kwargs.items() if key in accepted_names}))
    attempts.extend(((args, {}), ((args[0],), {}), ((), {})))
    for positional, keywords in attempts:
        try:
            signature.bind_partial(*positional, **keywords)
        except TypeError:
            continue
        return callback(*positional, **keywords)
    return callback(*args, **kwargs)


__all__ = [
    "RESOLUTION_GRACE_SECONDS",
    "canonical_timestamp",
    "content_hash",
    "event_mapping",
    "exact_decimal",
    "feed_from_instruction",
    "invoke_compatible",
    "make_pending_state",
    "recovery_mapping",
    "symbol_from_instruction",
    "utc_instant",
    "validate_wait_instruction",
]
