"""Recursive model/provider payload boundary.

Paper account money is engine-only.  This validator is intentionally small and
field-based: nested mappings and sequences are traversed, so a provider/model
fixture cannot hide balance, account, quantity, P&L or planned-R fields inside
extensions or telemetry.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from .errors import EngineFieldLeak, PayloadIssue
from .models import PayloadValidationResult


_ENGINE_FIELDS = frozenset(
    {
        "account",
        "accountid",
        "paperaccount",
        "paperaccountid",
        "balance",
        "accountbalance",
        "availablebalance",
        "currentbalance",
        "openingbalance",
        "initialbalance",
        "funds",
        "availablefunds",
        "capital",
        "cash",
        "money",
        "amount",
        "quantity",
        "qty",
        "positionquantity",
        "positionsize",
        "positionqty",
        "position",
        "openposition",
        "pendingposition",
        "trade",
        "opentrade",
        "pendingtrade",
        "notional",
        "pnl",
        "pl",
        "pandl",
        "p_and_l",
        "profitloss",
        "profit",
        "loss",
        "gain",
        "grosspnl",
        "netpnl",
        "appliedpnl",
        "realizedpnl",
        "unrealizedpnl",
        "riskr",
        "risk",
        "plannedr",
        "rewardrisk",
        "riskreward",
        "allocationfraction",
        "fees",
        "slippage",
    }
)


def _tokens(key: Any) -> tuple[str, str]:
    raw = str(key)
    split = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw)
    parts = tuple(part for part in re.split(r"[^A-Za-z0-9]+", split.lower()) if part)
    return "".join(parts), "_".join(parts)


def is_engine_field(key: Any) -> bool:
    compact, underscored = _tokens(key)
    if compact in _ENGINE_FIELDS or underscored in _ENGINE_FIELDS:
        return True
    if compact.startswith("account") or compact.startswith("balance") or compact.startswith("fund") or compact.startswith("capital"):
        return True
    if "quantity" in compact or "qty" in compact or compact.startswith("pnl") or compact.startswith("profit") or compact.startswith("loss"):
        return True
    if underscored.startswith("r_") or compact.startswith("rmultiple") or compact.startswith("riskreward") or compact.startswith("rewardrisk"):
        return True
    if compact.startswith("position") and any(token in compact for token in ("qty", "quantity", "size", "notional", "value")):
        return True
    if compact.endswith("pnl") or compact.endswith("profitloss") or compact.endswith("riskr"):
        return True
    # R is an engine-owned metric only when it is a standalone key; do not
    # reject ordinary fields such as reason or reference.
    return str(key).strip().lower() in {"r", "p&l", "p/l"}


def _find_leaks(value: Any, path: str = "$") -> list[PayloadIssue]:
    issues: list[PayloadIssue] = []
    if type(value) is float or isinstance(value, Decimal):
        return [PayloadIssue("non_exact_number", path, "provider/model payload numbers must be JSON-safe exact values")]
    if type(value) in {type(None), bool, int, str}:
        return issues
    if type(value) is not dict:
        if type(value) is list:
            for index, child in enumerate(value):
                issues.extend(_find_leaks(child, f"{path}[{index}]"))
            return issues
        return [PayloadIssue("non_json_native", path, "provider/model payload must contain only JSON-native objects, arrays and scalars")]
    if type(value) is dict:
        for key, child in value.items():
            if type(key) is not str:
                issues.append(PayloadIssue("non_string_key", path, "provider/model payload object keys must be strings"))
                continue
            child_path = f"{path}.{key}"
            if is_engine_field(key):
                issues.append(PayloadIssue("engine_field_leak", child_path, f"engine-only field '{key}' is not allowed"))
                # Still recurse so the validator gives deterministic evidence
                # for all nested leaks rather than depending on traversal stop.
            issues.extend(_find_leaks(child, child_path))
    return issues


def validate_model_payload(value: Any) -> PayloadValidationResult:
    issues = tuple(_find_leaks(value))
    if issues:
        first = issues[0]
        return PayloadValidationResult(False, first.code, first.message, first.path, issues)
    return PayloadValidationResult(True)


def validate_provider_payload(value: Any) -> PayloadValidationResult:
    return validate_model_payload(value)


def assert_model_payload_safe(value: Any) -> Any:
    result = validate_model_payload(value)
    if not result.valid:
        first = result.issues[0]
        raise EngineFieldLeak(first.message, path=first.path, details=result.issues)
    return value


def assert_provider_payload_safe(value: Any) -> Any:
    return assert_model_payload_safe(value)


def reject_engine_fields(value: Any) -> Any:
    return assert_model_payload_safe(value)


def contains_engine_fields(value: Any) -> bool:
    return not validate_model_payload(value).valid


__all__ = [
    "assert_model_payload_safe",
    "assert_provider_payload_safe",
    "contains_engine_fields",
    "is_engine_field",
    "reject_engine_fields",
    "validate_model_payload",
    "validate_provider_payload",
]
