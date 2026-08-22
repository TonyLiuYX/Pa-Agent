"""Exact Decimal calculations used by the paper-account core.

No state, clock, storage, feed, or provider is involved here.  Prices are
never converted through ``float`` and remain as supplied Decimal values.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP
from typing import Any

from .errors import InvalidDecimal, InvalidLevelOrder, InvalidPaperInput


PAPER_MATH_VERSION = "0.1.0"
INITIAL_BALANCE = Decimal("1000.00")
ZERO = Decimal("0")
ONE = Decimal("1")
SHARE_QUANTUM = Decimal("0.00000001")
MONEY_QUANTUM = Decimal("0.01")
R_QUANTUM = Decimal("0.0001")
ZERO_FEES = Decimal("0.00")
ZERO_SLIPPAGE = Decimal("0.00")


def decimal_input(value: Any, field: str = "value", *, allow_zero: bool = True) -> Decimal:
    """Return an exact finite Decimal, rejecting bools and all floats.

    Strings and integers are accepted as exact convenience inputs.  A Decimal
    supplied by a caller is returned unchanged, preserving its exponent and
    value until a policy quantization is explicitly requested.
    """

    if isinstance(value, bool):
        raise InvalidDecimal(f"{field} must not be bool", code="bool_not_allowed", path=f"$.{field}")
    if isinstance(value, float):
        raise InvalidDecimal(f"{field} must not be float", code="float_not_allowed", path=f"$.{field}")
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, int):
        parsed = Decimal(value)
    elif isinstance(value, str):
        if not value.strip():
            raise InvalidDecimal(f"{field} must be a finite Decimal", path=f"$.{field}")
        try:
            parsed = Decimal(value)
        except (InvalidOperation, ValueError):
            raise InvalidDecimal(f"{field} must be a finite Decimal", path=f"$.{field}") from None
    else:
        raise InvalidDecimal(f"{field} must be a Decimal-compatible exact value", path=f"$.{field}")
    if not parsed.is_finite():
        raise InvalidDecimal(f"{field} must be finite", code="non_finite_decimal", path=f"$.{field}")
    if not allow_zero and parsed <= ZERO:
        raise InvalidDecimal(f"{field} must be positive", path=f"$.{field}")
    return parsed


def positive_decimal(value: Any, field: str = "value") -> Decimal:
    return decimal_input(value, field, allow_zero=False)


def money(value: Any) -> Decimal:
    """Quantize a money/balance value to USD cents half-up."""

    exact = decimal_input(value, "money")
    if exact < ZERO:
        raise InvalidDecimal("money/balance must not be negative", code="negative_balance", path="$.money")
    result = exact.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    if result < ZERO:
        raise InvalidDecimal("money/balance must not be negative", code="negative_balance", path="$.money")
    return result


def quantity_for_balance(balance: Any, entry_price: Any) -> Decimal:
    """Allocate all available balance, rounded down to eight shares."""

    funds = money(balance)
    price = positive_decimal(entry_price, "entry_price")
    if funds <= ZERO:
        return ZERO.quantize(SHARE_QUANTUM)
    return (funds / price).quantize(SHARE_QUANTUM, rounding=ROUND_DOWN)


def validate_direction(direction: Any) -> str:
    if type(direction) is not str or direction not in {"long", "short"}:
        raise InvalidPaperInput("direction must be exactly 'long' or 'short'", code="invalid_direction", path="$.direction")
    return direction


def validate_levels(direction: Any, entry_price: Any, stop_price: Any, target_price: Any) -> tuple[str, Decimal, Decimal, Decimal]:
    """Validate directional ordering and non-zero risk."""

    side = validate_direction(direction)
    entry = positive_decimal(entry_price, "entry_price")
    stop = positive_decimal(stop_price, "stop_price")
    target = positive_decimal(target_price, "target_price")
    if entry == stop:
        raise InvalidLevelOrder("entry and stop must have non-zero risk", code="zero_risk", path="$.entry_price")
    if side == "long" and not (stop < entry < target):
        raise InvalidLevelOrder("long levels must satisfy stop < entry < target", path="$")
    if side == "short" and not (target < entry < stop):
        raise InvalidLevelOrder("short levels must satisfy target < entry < stop", path="$")
    return side, entry, stop, target


def planned_r(entry_price: Any, stop_price: Any, target_price: Any) -> Decimal:
    """Compute exact planned reward/risk then display at four decimals."""

    entry = positive_decimal(entry_price, "entry_price")
    stop = positive_decimal(stop_price, "stop_price")
    target = positive_decimal(target_price, "target_price")
    risk = abs(entry - stop)
    if risk == ZERO:
        raise InvalidLevelOrder("entry and stop must have non-zero risk", code="zero_risk", path="$.entry_price")
    return (abs(target - entry) / risk).quantize(R_QUANTUM, rounding=ROUND_HALF_UP)


def gross_pnl(direction: Any, entry_price: Any, exit_price: Any, quantity: Any) -> Decimal:
    """Compute unquantized arithmetic P&L from exact prices and 8-decimal qty."""

    side = validate_direction(direction)
    entry = positive_decimal(entry_price, "entry_price")
    exit_ = positive_decimal(exit_price, "exit_price")
    qty = decimal_input(quantity, "quantity")
    if qty < ZERO:
        raise InvalidDecimal("quantity must not be negative", path="$.quantity")
    if side == "long":
        return (exit_ - entry) * qty
    return (entry - exit_) * qty


def apply_pnl(balance_before: Any, gross: Any) -> tuple[Decimal, Decimal, Decimal, bool]:
    """Apply the frozen cap -> quantize -> re-clamp -> balance order.

    Returns ``(applied_pnl, closing_balance, capped_gross, exhausted)``.  The
    arithmetic gross value itself is never altered by this helper.
    """

    before = money(balance_before)
    arithmetic = decimal_input(gross, "gross_pnl")
    if before == ZERO:
        # An exhausted account cannot manufacture reusable capital from a
        # stray settlement callback; no position should exist at this point.
        return ZERO.quantize(MONEY_QUANTUM), ZERO.quantize(MONEY_QUANTUM), arithmetic, True
    capped = max(arithmetic, -before)
    applied = capped.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    # A positive half-up rounding edge can theoretically cross the loss cap;
    # clamp once again exactly as frozen by the policy.
    applied = max(applied, -before)
    closing = max(ZERO, before + applied).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    exhausted = closing == ZERO
    return applied, closing, capped, exhausted


def calculate_trade(direction: Any, balance_before: Any, entry_price: Any, stop_price: Any, target_price: Any) -> dict[str, Any]:
    """Return the deterministic opening calculation as a plain mapping."""

    side, entry, stop, target = validate_levels(direction, entry_price, stop_price, target_price)
    before = money(balance_before)
    quantity = quantity_for_balance(before, entry)
    return {
        "paper_math_version": PAPER_MATH_VERSION,
        "direction": side,
        "balance_before": before,
        "allocation_fraction": ONE,
        "entry_price": entry,
        "stop_price": stop,
        "target_price": target,
        "quantity": quantity,
        "planned_r": planned_r(entry, stop, target),
        "executable": quantity > ZERO,
    }


def calculate_settlement(direction: Any, balance_before: Any, entry_price: Any, exit_price: Any, quantity: Any) -> dict[str, Any]:
    """Return exact gross and policy-applied settlement values."""

    before = money(balance_before)
    qty = decimal_input(quantity, "quantity").quantize(SHARE_QUANTUM, rounding=ROUND_DOWN)
    if qty < ZERO:
        raise InvalidDecimal("quantity must not be negative", path="$.quantity")
    gross = gross_pnl(direction, entry_price, exit_price, qty)
    applied, closing, capped, exhausted = apply_pnl(before, gross)
    return {
        "paper_math_version": PAPER_MATH_VERSION,
        "balance_before": before,
        "entry_price": positive_decimal(entry_price, "entry_price"),
        "exit_price": positive_decimal(exit_price, "exit_price"),
        "quantity": qty,
        "gross_pnl": gross,
        "capped_gross_pnl": capped,
        "applied_pnl": applied,
        "net_pnl": applied,
        "closing_balance": closing,
        "balance_after": closing,
        "exhausted": exhausted,
        "fees": ZERO_FEES,
        "slippage": ZERO_SLIPPAGE,
    }


def _jsonable(value: Any) -> Any:
    """Canonical JSON projection with explicit Decimal text values."""

    if isinstance(value, Decimal):
        return {"__decimal__": str(value)}
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(value[key]) for key in sorted(value, key=lambda item: str(item))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if value is None or type(value) in {bool, int, str}:
        return value
    # Avoid silently accepting floats/non-native objects in identity inputs.
    if isinstance(value, float):
        raise InvalidPaperInput("float is not allowed in deterministic identity", code="float_not_allowed")
    raise InvalidPaperInput("value is not canonically serializable", code="non_json_native")


def canonical_payload(value: Any) -> bytes:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_payload(value)).hexdigest()


content_hash = stable_hash
calculate_quantity = quantity_for_balance
calculate_risk_reward = planned_r
calculate_pnl = gross_pnl
settle_pnl = calculate_settlement


__all__ = [
    "INITIAL_BALANCE",
    "MONEY_QUANTUM",
    "ONE",
    "PAPER_MATH_VERSION",
    "R_QUANTUM",
    "SHARE_QUANTUM",
    "ZERO",
    "ZERO_FEES",
    "ZERO_SLIPPAGE",
    "apply_pnl",
    "calculate_pnl",
    "calculate_quantity",
    "calculate_risk_reward",
    "calculate_settlement",
    "calculate_trade",
    "canonical_payload",
    "content_hash",
    "decimal_input",
    "gross_pnl",
    "money",
    "planned_r",
    "positive_decimal",
    "quantity_for_balance",
    "settle_pnl",
    "stable_hash",
    "validate_direction",
    "validate_levels",
]
