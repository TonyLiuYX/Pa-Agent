"""Immutable versioned runtime event envelope for the G027 event stream.

``RuntimeEventEnvelope 0.1.0`` is the internal append-only event unit: one
closed event type, canonical UTC timestamp, mode, strategy/profile/decision
lineage, a validated market ref, an idempotency key and a typed JSON payload.
Identity is content-bound: ``event_hash`` is SHA-256 of the canonical bytes
of the fixed-order content projection (all keys except ``event_hash`` and
``event_id``) and ``event_id`` is ``rev_`` + hash.

Validation is deterministic and exception-safe.  Stable rejection codes
include: ``version_mismatch``, ``artifact_kind_mismatch``,
``unknown_event_type``, ``invalid_mode``, ``non_utc_timestamp``,
``invalid_idempotency_key``, ``invalid_strategy_id``, ``invalid_generation``,
``invalid_payload``, ``missing_field``, ``unknown_field``,
``invalid_state``, ``state_unchanged``, ``invalid_reason``,
``invalid_branch``, ``branch_field_mismatch``, ``invalid_timer_kind``,
``invalid_direction``, ``invalid_symbol``, ``invalid_hash``,
``invalid_instruction_id``, ``invalid_id``, ``invalid_decimal``,
``invalid_seconds``, ``instant_order``, ``invalid_feed_key``,
``invalid_bar_count``, ``invalid_market_ref``, ``invalid_profile_ref``,
``invalid_decision_lineage``, ``cross_strategy_profile``,
``cross_strategy_decision``, ``cross_symbol_market_ref``,
``lineage_mismatch``, ``identity_mismatch``, ``secret_field``,
``reasoning_field``, ``broker_field``, ``invalid_json`` and
``invalid_envelope``.

The secret/reasoning/broker vocabularies are copied from
:mod:`pa_engine.contracts.validation` so the storage layer stays
self-contained; a field named ``reason`` is intentionally never blocked.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping

from pa_engine.contracts import ValidationIssue, ValidationResult, canonical_json_bytes
from pa_engine.storage.errors import (
    EnvelopeValidationError,
    InvalidStorageInputError,
    SecretLeakError,
)
from pa_engine.storage.schema import EVENT_MODES, EVENT_TYPES, FEED_KEY, STRATEGY_STATES
from pa_engine.storage.times import instant_from_text, is_canonical_instant, is_decimal_text

from .refs import validate_market_ref

ENVELOPE_SCHEMA_VERSION = "0.1.0"
ENVELOPE_ARTIFACT_KIND = "runtime_event_envelope"

MODEL_BRANCHES = ("no_entry", "enter_immediately", "wait_for_price")
TIMER_KINDS = ("sleep", "retry", "pending_expiry")
DIRECTIONS = ("long", "short")
MIN_DELAY_SECONDS = 1
MAX_DELAY_SECONDS = 86400

_SECRET_EXACT = {
    "apikey", "apitoken", "apisecret", "accesskey", "accesstoken", "authtoken",
    "bearertoken", "refreshtoken", "clientsecret", "clientcredential",
    "authorization", "password", "passwd", "credential", "credentials",
    "secret", "token", "privatekey", "signingkey", "signingsecret", "webhooksecret",
    "oauthsecret", "oauthaccesstoken", "oauthrefreshtoken",
}
_SECRET_SUFFIXES = (
    "apikey", "apitoken", "apisecret", "accesstoken", "authtoken", "bearertoken",
    "refreshtoken", "clientsecret", "credential", "credentials", "authorization",
    "password", "passwd", "privatekey", "signingkey", "signingsecret", "webhooksecret",
)
_SECRET_SAFE_KEYS = {
    "prompttokens", "completiontokens", "totaltokens", "maxoutputtokens",
    "tokencount", "tokenusage", "tokenbudget",
}
_REASONING_NAMES = frozenset(
    {
        "reasoning", "reasoningcontent", "thinking", "thinkingcontent",
        "chainofthought", "cot", "thoughts", "thought", "innermonologue",
        "monologue",
    }
)
_BROKER_NAMES = frozenset(
    {"brokerorderid", "brokerorders", "brokersecret", "brokercredential", "realorder"}
)
_PROVIDER_RAW_NAMES = frozenset(
    {
        "providerraw",
        "providerpayload",
        "providerrawpayload",
        "providerresponse",
        "providerrawresponse",
        "rawproviderpayload",
        "rawproviderresponse",
        "rawpayload",
        "rawresponse",
        "rawbody",
    }
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_PROJECTION_KEYS = (
    "schema_version",
    "artifact_kind",
    "event_type",
    "occurred_at",
    "mode",
    "strategy_id",
    "generation",
    "profile_ref",
    "decision_lineage",
    "market_ref",
    "idempotency_key",
    "payload",
)
_ENVELOPE_KEYS = _PROJECTION_KEYS + ("event_hash", "event_id")

_PAYLOAD_REQUIRED: dict[str, frozenset[str]] = {
    "strategy_state_changed": frozenset({"from", "to", "reason", "resume_state"}),
    "model_decision_accepted": frozenset(
        {
            "branch",
            "instruction_id",
            "instruction_hash",
            "result_id",
            "result_hash",
            "evidence_package_id",
            "evidence_package_hash",
        }
    ),
    "schedule_set": frozenset({"timer_kind", "due_at", "decision_accepted_at"}),
    "pending_entry_created": frozenset(
        {
            "pending_id",
            "episode_id",
            "symbol",
            "direction",
            "entry_price",
            "stop_price",
            "target_price",
            "decision_accepted_at",
            "submitted_at",
            "expires_at",
            "resolution_at",
            "state_hash",
        }
    ),
    "pending_entry_expired": frozenset({"pending_id", "episode_id", "resolution_at"}),
    "pending_entry_cancelled": frozenset(
        {"pending_id", "episode_id", "resolution_at", "reason"}
    ),
    "paper_fill": frozenset(
        {
            "trade_id",
            "episode_id",
            "symbol",
            "direction",
            "entry_price",
            "quantity",
            "planned_r",
            "filled_at",
        }
    ),
    "paper_fill_rejected": frozenset({"reason"}),
    "paper_close": frozenset({"trade_id", "episode_id", "exit_reason", "closed_at"}),
    "account_changed": frozenset({"balance_before", "balance_after"}),
    "account_exhausted": frozenset({"balance", "reason"}),
    "market_recovery": frozenset(
        {"symbol", "feed_key", "recovery_cursor", "bars", "bar_count"}
    ),
}

_PAYLOAD_ALLOWED: dict[str, frozenset[str]] = {
    "strategy_state_changed": frozenset({"from", "to", "reason", "resume_state"}),
    "model_decision_accepted": frozenset(
        {
            "branch",
            "instruction_id",
            "instruction_hash",
            "result_id",
            "result_hash",
            "evidence_package_id",
            "evidence_package_hash",
            "next_call_after_seconds",
            "entry_wait_seconds",
            "entry_price",
        }
    ),
    "schedule_set": frozenset({"timer_kind", "due_at", "decision_accepted_at"}),
    "pending_entry_created": frozenset(
        {
            "pending_id",
            "episode_id",
            "symbol",
            "direction",
            "entry_price",
            "stop_price",
            "target_price",
            "decision_accepted_at",
            "submitted_at",
            "expires_at",
            "resolution_at",
            "state_hash",
        }
    ),
    "pending_entry_expired": frozenset({"pending_id", "episode_id", "resolution_at"}),
    "pending_entry_cancelled": frozenset(
        {"pending_id", "episode_id", "resolution_at", "reason"}
    ),
    "paper_fill": frozenset(
        {
            "trade_id",
            "episode_id",
            "symbol",
            "direction",
            "entry_price",
            "quantity",
            "planned_r",
            "filled_at",
            "pending_id",
        }
    ),
    "paper_fill_rejected": frozenset({"reason", "pending_id", "trade_id"}),
    "paper_close": frozenset(
        {"trade_id", "episode_id", "exit_reason", "closed_at", "selected_exit", "settlement"}
    ),
    "account_changed": frozenset({"balance_before", "balance_after"}),
    "account_exhausted": frozenset({"balance", "reason"}),
    "market_recovery": frozenset(
        {"symbol", "feed_key", "recovery_cursor", "bars", "bar_count"}
    ),
}

_PAYLOAD_CHECKERS: dict[str, Any] = {}


def _issue(code: str, path: str, message: str) -> ValidationIssue:
    return ValidationIssue(code, path, message)


class _FrozenMap(ABCMapping):
    """Tiny recursively immutable mapping used at envelope boundaries."""

    __slots__ = ("_data",)

    def __init__(self, value: ABCMapping) -> None:
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


def _freeze(value: Any) -> Any:
    if isinstance(value, ABCMapping):
        return _FrozenMap({key: _freeze(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(child) for child in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, ABCMapping):
        return {key: _thaw(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_thaw(child) for child in value]
    return value


def envelope_hash(value: Mapping) -> str:
    """Return SHA-256 of the canonical content projection of an envelope."""

    if not isinstance(value, ABCMapping):
        raise TypeError("envelope must be a mapping")
    projection = {key: _thaw(value[key]) for key in _PROJECTION_KEYS}
    return hashlib.sha256(canonical_json_bytes(projection)).hexdigest()


def _walk(value: Any, path: str = "$"):
    yield value, path
    if isinstance(value, ABCMapping):
        for key, child in value.items():
            if isinstance(key, str):
                yield from _walk(child, f"{path}.{key}")
            else:
                yield from _walk(child, f"{path}[{key!r}]")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            yield from _walk(child, f"{path}[{index}]")


def _native_scan(value: Any, issues: list[ValidationIssue]) -> None:
    for node, path in _walk(value):
        if type(node) not in (type(None), bool, int, str, list, dict):
            issues.append(_issue("invalid_json", path, "value is not strict JSON-native data"))
        elif isinstance(node, dict) and any(type(key) is not str for key in node):
            issues.append(_issue("invalid_json", path, "object keys must be strings"))


def _secret_scan(value: Any, issues: list[ValidationIssue]) -> None:
    for node, path in _walk(value):
        if not isinstance(node, ABCMapping):
            continue
        for key in node:
            if not isinstance(key, str):
                continue
            normalized = re.sub(r"[^a-z0-9]", "", key.lower())
            key_path = f"{path}.{key}"
            if normalized in _BROKER_NAMES:
                issues.append(_issue("broker_field", key_path, "broker-secret-like field is not allowed"))
            elif normalized in _REASONING_NAMES:
                issues.append(_issue("reasoning_field", key_path, "reasoning-like field is not allowed"))
            elif normalized in _PROVIDER_RAW_NAMES:
                issues.append(_issue("provider_raw_field", key_path, "provider raw payload is not allowed"))
            elif normalized in _SECRET_EXACT or (
                normalized not in _SECRET_SAFE_KEYS
                and any(normalized.endswith(suffix) for suffix in _SECRET_SUFFIXES)
            ):
                issues.append(_issue("secret_field", key_path, "secret-like field is not allowed"))


def assert_durable_json_safe(value: Any) -> None:
    """Reject non-native or secret-bearing values before materialized storage.

    Runtime envelopes already run these scans, but strategy/pending/trade
    snapshots are also persisted as JSON columns.  Keeping the same boundary
    on those rows prevents a caller from bypassing the envelope by placing a
    credential, reasoning transcript or provider raw response in a snapshot.
    Only field names are inspected; ordinary text in a legal ``reason`` field
    remains valid.
    """

    issues: list[ValidationIssue] = []
    _native_scan(value, issues)
    _secret_scan(value, issues)
    if not issues:
        return
    first = issues[0]
    if first.code in {"secret_field", "reasoning_field", "broker_field", "provider_raw_field"}:
        raise SecretLeakError(f"durable state rejected at {first.path}")
    raise InvalidStorageInputError(f"durable state is not JSON-native at {first.path}")


def _require_non_empty_str(payload: Mapping, key: str, issues: list[ValidationIssue]) -> None:
    if not isinstance(payload[key], str) or not payload[key]:
        issues.append(_issue("invalid_id", f"$.payload.{key}", "id must be a non-empty string"))


def _require_canonical_instant(payload: Mapping, key: str, issues: list[ValidationIssue]) -> None:
    if not is_canonical_instant(payload[key]):
        issues.append(_issue("non_utc_timestamp", f"$.payload.{key}", "instant must be canonical UTC"))


def _check_state_changed(payload: Mapping, issues: list[ValidationIssue]) -> None:
    for key in ("from", "to"):
        if payload[key] not in STRATEGY_STATES:
            issues.append(_issue("invalid_state", f"$.payload.{key}", "state is not in the closed vocabulary"))
    if payload["from"] == payload["to"]:
        issues.append(_issue("state_unchanged", "$.payload", "state transition must change state"))
    if not isinstance(payload["reason"], str) or not payload["reason"]:
        issues.append(_issue("invalid_reason", "$.payload.reason", "reason must be a non-empty string"))
    resume = payload["resume_state"]
    if resume is not None and resume not in STRATEGY_STATES:
        issues.append(_issue("invalid_state", "$.payload.resume_state", "resume_state must be None or a known state"))


def _check_model_decision(payload: Mapping, issues: list[ValidationIssue]) -> None:
    branch = payload["branch"]
    if branch not in MODEL_BRANCHES:
        issues.append(_issue("invalid_branch", "$.payload.branch", "branch is not in the closed vocabulary"))
    elif branch == "no_entry":
        n_value = payload.get("next_call_after_seconds")
        if type(n_value) is not int or not MIN_DELAY_SECONDS <= n_value <= MAX_DELAY_SECONDS:
            issues.append(
                _issue(
                    "invalid_seconds",
                    "$.payload.next_call_after_seconds",
                    "next_call_after_seconds must be an int in 1..86400",
                )
            )
        for key in ("entry_wait_seconds", "entry_price"):
            if key in payload:
                issues.append(_issue("branch_field_mismatch", "$.payload", "no_entry must not carry wait/price fields"))
    elif branch == "wait_for_price":
        x_value = payload.get("entry_wait_seconds")
        if type(x_value) is not int or not MIN_DELAY_SECONDS <= x_value <= MAX_DELAY_SECONDS:
            issues.append(
                _issue(
                    "invalid_seconds",
                    "$.payload.entry_wait_seconds",
                    "entry_wait_seconds must be an int in 1..86400",
                )
            )
        price = payload.get("entry_price")
        if not is_decimal_text(price):
            issues.append(_issue("invalid_decimal", "$.payload.entry_price", "entry_price must be canonical decimal text"))
        elif Decimal(price) <= 0:
            issues.append(_issue("invalid_decimal", "$.payload.entry_price", "entry_price must be positive"))
        if "next_call_after_seconds" in payload:
            issues.append(
                _issue("branch_field_mismatch", "$.payload", "wait_for_price must not carry next_call_after_seconds")
            )
    else:
        for key in ("next_call_after_seconds", "entry_wait_seconds", "entry_price"):
            if key in payload:
                issues.append(
                    _issue("branch_field_mismatch", "$.payload", "enter_immediately must not carry timer/price fields")
                )
    for key in ("instruction_hash", "result_hash", "evidence_package_hash"):
        if not isinstance(payload[key], str) or not _SHA256_RE.fullmatch(payload[key]):
            issues.append(_issue("invalid_hash", f"$.payload.{key}", "hash must be 64 lowercase hex chars"))
    instruction_hash = payload["instruction_hash"]
    if isinstance(instruction_hash, str) and (
        not isinstance(payload["instruction_id"], str) or payload["instruction_id"] != "sdi_" + instruction_hash
    ):
        issues.append(
            _issue("invalid_instruction_id", "$.payload.instruction_id", "instruction_id must bind to instruction_hash")
        )


def _check_schedule_set(payload: Mapping, issues: list[ValidationIssue]) -> None:
    if payload["timer_kind"] not in TIMER_KINDS:
        issues.append(_issue("invalid_timer_kind", "$.payload.timer_kind", "timer_kind is not in the closed vocabulary"))
    for key in ("due_at", "decision_accepted_at"):
        _require_canonical_instant(payload, key, issues)
    if is_canonical_instant(payload["due_at"]) and is_canonical_instant(payload["decision_accepted_at"]):
        if instant_from_text(payload["due_at"]) < instant_from_text(payload["decision_accepted_at"]):
            issues.append(_issue("instant_order", "$.payload", "due_at must not precede decision_accepted_at"))


def _check_pending_entry_created(payload: Mapping, issues: list[ValidationIssue]) -> None:
    for key in ("pending_id", "episode_id"):
        _require_non_empty_str(payload, key, issues)
    symbol = payload["symbol"]
    if (
        not isinstance(symbol, str)
        or not symbol
        or symbol != symbol.upper()
        or any(character.isspace() for character in symbol)
    ):
        issues.append(_issue("invalid_symbol", "$.payload.symbol", "symbol must be a non-empty upper-case value"))
    if payload["direction"] not in DIRECTIONS:
        issues.append(_issue("invalid_direction", "$.payload.direction", "direction is not in the closed vocabulary"))
    for key in ("entry_price", "stop_price", "target_price"):
        if not is_decimal_text(payload[key]):
            issues.append(_issue("invalid_decimal", f"$.payload.{key}", "price must be canonical decimal text"))
    if not isinstance(payload["state_hash"], str) or not _SHA256_RE.fullmatch(payload["state_hash"]):
        issues.append(_issue("invalid_hash", "$.payload.state_hash", "state_hash must be 64 lowercase hex chars"))
    parsed: dict[str, Any] = {}
    for key in ("decision_accepted_at", "submitted_at", "expires_at", "resolution_at"):
        _require_canonical_instant(payload, key, issues)
        if is_canonical_instant(payload[key]):
            parsed[key] = instant_from_text(payload[key])
    if len(parsed) == 4 and (
        parsed["submitted_at"] < parsed["decision_accepted_at"]
        or parsed["expires_at"] < parsed["decision_accepted_at"]
        or parsed["resolution_at"] < parsed["submitted_at"]
    ):
        issues.append(_issue("instant_order", "$.payload", "pending entry instants are not ordered"))


def _check_pending_entry_expired(payload: Mapping, issues: list[ValidationIssue]) -> None:
    for key in ("pending_id", "episode_id"):
        _require_non_empty_str(payload, key, issues)
    _require_canonical_instant(payload, "resolution_at", issues)


def _check_pending_entry_cancelled(payload: Mapping, issues: list[ValidationIssue]) -> None:
    for key in ("pending_id", "episode_id"):
        _require_non_empty_str(payload, key, issues)
    _require_canonical_instant(payload, "resolution_at", issues)
    if not isinstance(payload["reason"], str) or not payload["reason"]:
        issues.append(_issue("invalid_reason", "$.payload.reason", "reason must be a non-empty string"))


def _check_paper_fill(payload: Mapping, issues: list[ValidationIssue]) -> None:
    for key in ("trade_id", "episode_id"):
        _require_non_empty_str(payload, key, issues)
    if "pending_id" in payload and (not isinstance(payload["pending_id"], str) or not payload["pending_id"]):
        issues.append(_issue("invalid_id", "$.payload.pending_id", "id must be a non-empty string"))
    symbol = payload["symbol"]
    if (
        not isinstance(symbol, str)
        or not symbol
        or symbol != symbol.upper()
        or any(character.isspace() for character in symbol)
    ):
        issues.append(_issue("invalid_symbol", "$.payload.symbol", "symbol must be a non-empty upper-case value"))
    if payload["direction"] not in DIRECTIONS:
        issues.append(_issue("invalid_direction", "$.payload.direction", "direction is not in the closed vocabulary"))
    for key in ("entry_price", "quantity", "planned_r"):
        if not is_decimal_text(payload[key]):
            issues.append(_issue("invalid_decimal", f"$.payload.{key}", "value must be canonical decimal text"))
    _require_canonical_instant(payload, "filled_at", issues)


def _check_paper_fill_rejected(payload: Mapping, issues: list[ValidationIssue]) -> None:
    if not isinstance(payload["reason"], str) or not payload["reason"]:
        issues.append(_issue("invalid_reason", "$.payload.reason", "reason must be a non-empty string"))
    for key in ("pending_id", "trade_id"):
        if key in payload and (not isinstance(payload[key], str) or not payload[key]):
            issues.append(_issue("invalid_id", f"$.payload.{key}", "id must be a non-empty string"))


def _check_paper_close(payload: Mapping, issues: list[ValidationIssue]) -> None:
    for key in ("trade_id", "episode_id"):
        _require_non_empty_str(payload, key, issues)
    if not isinstance(payload["exit_reason"], str) or not payload["exit_reason"]:
        issues.append(_issue("invalid_reason", "$.payload.exit_reason", "exit_reason must be a non-empty string"))
    _require_canonical_instant(payload, "closed_at", issues)
    settlement = payload.get("settlement")
    if settlement is not None:
        if not isinstance(settlement, ABCMapping):
            issues.append(_issue("invalid_payload", "$.payload.settlement", "settlement must be a JSON object"))
        else:
            for key in ("gross_pnl", "applied_pnl", "net_pnl", "balance_before", "balance_after"):
                if key in settlement and not is_decimal_text(settlement[key]):
                    issues.append(
                        _issue("invalid_decimal", f"$.payload.settlement.{key}", "settlement value must be canonical decimal text")
                    )


def _check_account_changed(payload: Mapping, issues: list[ValidationIssue]) -> None:
    for key in ("balance_before", "balance_after"):
        value = payload[key]
        if not is_decimal_text(value):
            issues.append(_issue("invalid_decimal", f"$.payload.{key}", "balance must be canonical decimal text"))
        elif Decimal(value) < 0:
            issues.append(_issue("invalid_decimal", f"$.payload.{key}", "balance must be non-negative"))
    if is_decimal_text(payload["balance_before"]) and is_decimal_text(payload["balance_after"]):
        if payload["balance_before"] == payload["balance_after"]:
            issues.append(_issue("balance_unchanged", "$.payload", "balance must change"))


def _check_account_exhausted(payload: Mapping, issues: list[ValidationIssue]) -> None:
    value = payload["balance"]
    if not is_decimal_text(value):
        issues.append(_issue("invalid_decimal", "$.payload.balance", "balance must be canonical decimal text"))
    elif Decimal(value) < 0:
        issues.append(_issue("invalid_decimal", "$.payload.balance", "balance must be non-negative"))
    if not isinstance(payload["reason"], str) or not payload["reason"]:
        issues.append(_issue("invalid_reason", "$.payload.reason", "reason must be a non-empty string"))


def _check_market_recovery(payload: Mapping, issues: list[ValidationIssue]) -> None:
    symbol = payload["symbol"]
    if (
        not isinstance(symbol, str)
        or not symbol
        or symbol != symbol.upper()
        or any(character.isspace() for character in symbol)
    ):
        issues.append(_issue("invalid_symbol", "$.payload.symbol", "symbol must be a non-empty upper-case value"))
    if payload["feed_key"] != FEED_KEY:
        issues.append(_issue("invalid_feed_key", "$.payload.feed_key", "feed_key is not the frozen feed"))
    cursor = payload["recovery_cursor"]
    if cursor is not None and not is_canonical_instant(cursor):
        issues.append(_issue("non_utc_timestamp", "$.payload.recovery_cursor", "recovery_cursor must be a canonical UTC instant or null"))
    bars = payload["bars"]
    if not isinstance(bars, list):
        issues.append(_issue("invalid_payload", "$.payload.bars", "bars must be a JSON array"))
    else:
        for index, bar in enumerate(bars):
            bar_result = validate_market_ref(bar)
            for issue in bar_result.issues:
                suffix = issue.path[1:] if issue.path.startswith("$") else ""
                issues.append(_issue(issue.code, f"$.payload.bars[{index}]{suffix}", issue.message))
    bar_count = payload["bar_count"]
    if type(bar_count) is not int or not isinstance(bars, list) or bar_count != len(bars):
        issues.append(_issue("invalid_bar_count", "$.payload.bar_count", "bar_count must equal the number of bars"))


_PAYLOAD_CHECKERS.update(
    {
        "strategy_state_changed": _check_state_changed,
        "model_decision_accepted": _check_model_decision,
        "schedule_set": _check_schedule_set,
        "pending_entry_created": _check_pending_entry_created,
        "pending_entry_expired": _check_pending_entry_expired,
        "pending_entry_cancelled": _check_pending_entry_cancelled,
        "paper_fill": _check_paper_fill,
        "paper_fill_rejected": _check_paper_fill_rejected,
        "paper_close": _check_paper_close,
        "account_changed": _check_account_changed,
        "account_exhausted": _check_account_exhausted,
        "market_recovery": _check_market_recovery,
    }
)


def _validate_payload(payload: Any, event_type: str, issues: list[ValidationIssue]) -> None:
    if not isinstance(payload, ABCMapping):
        issues.append(_issue("invalid_payload", "$.payload", "payload must be a JSON object"))
        return
    required = _PAYLOAD_REQUIRED[event_type]
    allowed = _PAYLOAD_ALLOWED[event_type]
    payload_keys = {key for key in payload if isinstance(key, str)}
    for key in sorted(required - payload_keys):
        issues.append(_issue("missing_field", f"$.payload.{key}", "payload field is required"))
    for key in sorted(payload_keys - allowed):
        issues.append(_issue("unknown_field", f"$.payload.{key}", "unknown payload field"))
    if any(key not in payload for key in required):
        return
    _PAYLOAD_CHECKERS[event_type](payload, issues)


def _check_profile_ref(profile_ref: Any, strategy_id: Any, issues: list[ValidationIssue]) -> None:
    if not isinstance(profile_ref, ABCMapping):
        issues.append(_issue("invalid_profile_ref", "$.profile_ref", "profile_ref must be a JSON object"))
        return
    allowed = {"profile_ref_id", "profile_ref_hash", "strategy_id"}
    if set(profile_ref) - allowed or {"profile_ref_id", "profile_ref_hash"} - set(profile_ref):
        issues.append(_issue("invalid_profile_ref", "$.profile_ref", "profile_ref has an unexpected shape"))
        return
    hash_value = profile_ref["profile_ref_hash"]
    if not isinstance(hash_value, str) or not _SHA256_RE.fullmatch(hash_value):
        issues.append(_issue("invalid_hash", "$.profile_ref.profile_ref_hash", "profile_ref_hash must be 64 lowercase hex chars"))
    if isinstance(hash_value, str) and profile_ref["profile_ref_id"] != "spr_" + hash_value:
        issues.append(
            _issue("invalid_profile_ref", "$.profile_ref.profile_ref_id", "profile_ref_id must bind to profile_ref_hash")
        )
    nested = profile_ref.get("strategy_id")
    if nested is not None and nested != strategy_id:
        issues.append(
            _issue("cross_strategy_profile", "$.profile_ref.strategy_id", "profile strategy_id differs from envelope strategy_id")
        )


def _check_decision_lineage(
    decision_lineage: Any,
    strategy_id: Any,
    event_type: str,
    payload: Any,
    issues: list[ValidationIssue],
) -> None:
    if not isinstance(decision_lineage, ABCMapping):
        issues.append(_issue("invalid_decision_lineage", "$.decision_lineage", "decision_lineage must be a JSON object"))
        return
    if "instruction_id" not in decision_lineage or "instruction_hash" not in decision_lineage:
        issues.append(
            _issue("invalid_decision_lineage", "$.decision_lineage", "decision_lineage must carry instruction identity")
        )
        return
    hash_value = decision_lineage["instruction_hash"]
    if not isinstance(hash_value, str) or not _SHA256_RE.fullmatch(hash_value):
        issues.append(
            _issue("invalid_hash", "$.decision_lineage.instruction_hash", "instruction_hash must be 64 lowercase hex chars")
        )
    if isinstance(hash_value, str) and decision_lineage["instruction_id"] != "sdi_" + hash_value:
        issues.append(
            _issue("invalid_decision_lineage", "$.decision_lineage.instruction_id", "instruction_id must bind to instruction_hash")
        )
    nested = decision_lineage.get("strategy_id")
    if nested is not None and nested != strategy_id:
        issues.append(
            _issue("cross_strategy_decision", "$.decision_lineage.strategy_id", "lineage strategy_id differs from envelope strategy_id")
        )
    if event_type == "model_decision_accepted":
        for key in (
            "instruction_id",
            "instruction_hash",
            "result_id",
            "result_hash",
            "evidence_package_id",
            "evidence_package_hash",
        ):
            if decision_lineage.get(key) != payload.get(key):
                issues.append(
                    _issue("lineage_mismatch", f"$.decision_lineage.{key}", "decision lineage does not match payload")
                )


def _validate_envelope(value: Any) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    _native_scan(value, issues)
    _secret_scan(value, issues)
    if not isinstance(value, ABCMapping):
        issues.append(_issue("invalid_envelope", "$", "envelope must be a JSON object"))
        return issues
    if not isinstance(value, dict) or any(type(key) is not str for key in value):
        return issues
    missing = sorted(set(_ENVELOPE_KEYS) - set(value))
    for key in missing:
        issues.append(_issue("missing_field", "$." + key, "envelope field is required"))
    extra = sorted(set(value) - set(_ENVELOPE_KEYS))
    for key in extra:
        issues.append(_issue("unknown_field", "$." + key, "unknown envelope field"))
    if missing or extra:
        return issues
    if value["schema_version"] != ENVELOPE_SCHEMA_VERSION:
        issues.append(_issue("version_mismatch", "$.schema_version", "envelope schema version is unsupported"))
    if value["artifact_kind"] != ENVELOPE_ARTIFACT_KIND:
        issues.append(_issue("artifact_kind_mismatch", "$.artifact_kind", "envelope artifact kind is unsupported"))
    event_type = value["event_type"]
    if not isinstance(event_type, str) or event_type not in EVENT_TYPES:
        issues.append(_issue("unknown_event_type", "$.event_type", "event type is not in the closed vocabulary"))
        return issues
    if value["mode"] not in EVENT_MODES:
        issues.append(_issue("invalid_mode", "$.mode", "mode is not in the closed vocabulary"))
    if not is_canonical_instant(value["occurred_at"]):
        issues.append(_issue("non_utc_timestamp", "$.occurred_at", "occurred_at must be a canonical UTC instant"))
    if not isinstance(value["idempotency_key"], str) or not value["idempotency_key"]:
        issues.append(_issue("invalid_idempotency_key", "$.idempotency_key", "idempotency_key must be a non-empty string"))
    strategy_id = value["strategy_id"]
    generation = value["generation"]
    if event_type == "market_recovery":
        if strategy_id is not None:
            issues.append(_issue("invalid_strategy_id", "$.strategy_id", "market_recovery events must not carry a strategy_id"))
        if generation is not None:
            issues.append(_issue("invalid_generation", "$.generation", "market_recovery events must not carry a generation"))
    else:
        if not isinstance(strategy_id, str) or not strategy_id:
            issues.append(_issue("invalid_strategy_id", "$.strategy_id", "strategy_id must be a non-empty string"))
        if type(generation) is not int or generation < 0:
            issues.append(_issue("invalid_generation", "$.generation", "generation must be a non-negative integer"))
    payload = value["payload"]
    if not isinstance(payload, ABCMapping):
        issues.append(_issue("invalid_payload", "$.payload", "payload must be a JSON object"))
        return issues
    _validate_payload(payload, event_type, issues)
    profile_ref = value["profile_ref"]
    if profile_ref is not None:
        _check_profile_ref(profile_ref, strategy_id, issues)
    decision_lineage = value["decision_lineage"]
    if event_type == "model_decision_accepted" and decision_lineage is None:
        issues.append(_issue("missing_field", "$.decision_lineage", "model decisions require decision lineage"))
    if decision_lineage is not None:
        _check_decision_lineage(decision_lineage, strategy_id, event_type, payload, issues)
    market_ref = value["market_ref"]
    if event_type in ("paper_fill", "paper_close") and market_ref is None:
        issues.append(_issue("missing_field", "$.market_ref", "paper events require a market ref"))
    if market_ref is not None:
        if event_type == "market_recovery":
            issues.append(_issue("invalid_market_ref", "$.market_ref", "market_recovery events must not carry a market_ref"))
        else:
            market_result = validate_market_ref(market_ref)
            if not market_result.valid:
                for issue in market_result.issues:
                    suffix = issue.path[1:] if issue.path.startswith("$") else ""
                    issues.append(_issue(issue.code, "$.market_ref" + suffix, issue.message))
            else:
                payload_symbol = payload.get("symbol") if isinstance(payload, ABCMapping) else None
                if payload_symbol is not None and market_ref.get("symbol") not in (None, payload_symbol):
                    issues.append(
                        _issue("cross_symbol_market_ref", "$.market_ref.symbol", "market ref symbol contradicts payload symbol")
                    )
    try:
        digest = envelope_hash(value)
    except Exception:
        issues.append(_issue("invalid_envelope", "$", "envelope identity cannot be computed"))
        return issues
    if value["event_hash"] != digest or value["event_id"] != "rev_" + digest:
        issues.append(_issue("identity_mismatch", "$", "event hash/id does not match content"))
    return issues


def validate_dict(value: Any) -> ValidationResult:
    """Validate one full envelope dict and return a result; never raises."""

    try:
        issues = _validate_envelope(value)
        return ValidationResult(not issues, ENVELOPE_ARTIFACT_KIND, issues)
    except Exception:
        return ValidationResult(
            False,
            ENVELOPE_ARTIFACT_KIND,
            [_issue("invalid_envelope", "$", "envelope is malformed or non-JSON")],
        )


@dataclass(frozen=True, slots=True)
class RuntimeEventEnvelope:
    """One immutable content-bound runtime event envelope."""

    event_type: str
    occurred_at: str
    mode: str
    idempotency_key: str
    payload: Mapping
    strategy_id: str | None = None
    generation: int | None = None
    profile_ref: Mapping | None = None
    decision_lineage: Mapping | None = None
    market_ref: Mapping | None = None
    event_hash: str = field(init=False)
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", _freeze(self.payload))
        object.__setattr__(
            self, "profile_ref", _freeze(self.profile_ref) if self.profile_ref is not None else None
        )
        object.__setattr__(
            self,
            "decision_lineage",
            _freeze(self.decision_lineage) if self.decision_lineage is not None else None,
        )
        object.__setattr__(self, "market_ref", _freeze(self.market_ref) if self.market_ref is not None else None)
        digest = hashlib.sha256(canonical_json_bytes(self._projection())).hexdigest()
        object.__setattr__(self, "event_hash", digest)
        object.__setattr__(self, "event_id", "rev_" + digest)

    def _projection(self) -> dict[str, Any]:
        return {
            "schema_version": ENVELOPE_SCHEMA_VERSION,
            "artifact_kind": ENVELOPE_ARTIFACT_KIND,
            "event_type": self.event_type,
            "occurred_at": self.occurred_at,
            "mode": self.mode,
            "strategy_id": self.strategy_id,
            "generation": self.generation,
            "profile_ref": _thaw(self.profile_ref),
            "decision_lineage": _thaw(self.decision_lineage),
            "market_ref": _thaw(self.market_ref),
            "idempotency_key": self.idempotency_key,
            "payload": _thaw(self.payload),
        }

    def to_dict(self) -> dict[str, Any]:
        """Return the full JSON-native envelope dict including identity."""

        return {**self._projection(), "event_hash": self.event_hash, "event_id": self.event_id}

    @classmethod
    def make(
        cls,
        event_type: str,
        *,
        strategy_id: str | None = None,
        generation: int | None = None,
        mode: str = "automatic",
        occurred_at: str,
        idempotency_key: str,
        payload: Mapping,
        profile_ref: Mapping | None = None,
        decision_lineage: Mapping | None = None,
        market_ref: Mapping | None = None,
    ) -> "RuntimeEventEnvelope":
        """Build one validated envelope, computing event hash and id.

        Raises :class:`EnvelopeValidationError` when the input is invalid.
        """

        document: dict[str, Any] = {
            "schema_version": ENVELOPE_SCHEMA_VERSION,
            "artifact_kind": ENVELOPE_ARTIFACT_KIND,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "mode": mode,
            "strategy_id": strategy_id,
            "generation": generation,
            "profile_ref": profile_ref,
            "decision_lineage": decision_lineage,
            "market_ref": market_ref,
            "idempotency_key": idempotency_key,
            "payload": payload,
        }
        try:
            digest = envelope_hash(document)
        except Exception:
            raise EnvelopeValidationError("runtime event envelope content is not canonically serializable") from None
        document["event_hash"] = digest
        document["event_id"] = "rev_" + digest
        result = validate_dict(document)
        if not result.valid:
            codes = ",".join(issue.code for issue in result.issues[:10])
            raise EnvelopeValidationError(f"runtime event envelope failed validation ({codes})")
        return cls.from_dict(document)

    @classmethod
    def from_dict(cls, value: Any) -> "RuntimeEventEnvelope":
        """Validate *value* and construct one envelope from it.

        Raises :class:`EnvelopeValidationError` when validation fails.
        """

        result = validate_dict(value)
        if not result.valid:
            codes = ",".join(issue.code for issue in result.issues[:10])
            raise EnvelopeValidationError(f"runtime event envelope failed validation ({codes})")
        return cls(
            event_type=value["event_type"],
            occurred_at=value["occurred_at"],
            mode=value["mode"],
            idempotency_key=value["idempotency_key"],
            payload=value["payload"],
            strategy_id=value["strategy_id"],
            generation=value["generation"],
            profile_ref=value["profile_ref"],
            decision_lineage=value["decision_lineage"],
            market_ref=value["market_ref"],
        )


__all__ = [
    "DIRECTIONS",
    "ENVELOPE_ARTIFACT_KIND",
    "ENVELOPE_SCHEMA_VERSION",
    "MAX_DELAY_SECONDS",
    "MIN_DELAY_SECONDS",
    "MODEL_BRANCHES",
    "RuntimeEventEnvelope",
    "TIMER_KINDS",
    "assert_durable_json_safe",
    "envelope_hash",
    "validate_dict",
]
