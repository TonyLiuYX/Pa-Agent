"""Recursive field-level model-boundary denylist for G028.

Every model-facing payload is scanned before transport.  Only object keys are
inspected; values are never searched for prose substrings, so ordinary text
like ``"the balance sheet"`` or ``"P&L stands for..."`` inside a safe field
cannot trip the boundary.  The forbidden-key logic mirrors G023's
``_scrub_model_value`` in ``pa_engine/strategy/runtime/scheduler.py``: keys
are split into normalized word tokens (camelCase, snake, kebab and space
forms) and compact forms, then checked against the frozen vocabulary below.

Rules (see :data:`BOUNDARY_RULES`):

- ``forbidden_model_field`` — the G023 account/position/money field set.
- ``account_field`` — additional explicit account-state leaks.
- ``secret_field`` — credential-like field names (G027 suffix style).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

FORBIDDEN_MODEL_KEYS = frozenset(
    {
        "account",
        "balance",
        "funds",
        "capital",
        "quantity",
        "qty",
        "pnl",
        "profit",
        "loss",
        "pl",
        "r",
        "p&l",
        "p_and_l",
        # G028 extends the G023 token set so a bare notional leaks like the
        # position-size combination it represents (the G028 acceptance list
        # requires a standalone ``notional`` key to be rejected too).
        "notional",
    }
)
_COMPACT_FORBIDDEN_KEYS = frozenset(
    {
        "accountbalance",
        "paperaccount",
        "availablefunds",
        "positionsize",
        "positionquantity",
        "positionqty",
        "appliedpnl",
        "profitloss",
        "pandl",
    }
)
_POSITION_VALUE_TOKENS = frozenset({"size", "sizes", "quantity", "qty", "notional", "value"})
_ACCOUNT_FIELD_TOKENS = frozenset(
    {
        "equity",
        "cash",
        "margin",
        "liquidation",
        "exposure",
        "buying_power",
        "buyingpower",
        "deposit",
        "withdrawal",
        "fees",
        "slippage",
    }
)
_SECRET_FIELD_TOKENS = frozenset(
    {
        "apikey",
        "secret",
        "password",
        "passwd",
        "credential",
        "credentials",
        "token",
        "passphrase",
        "privatekey",
        "bearer",
    }
)
# Mirrors G027's envelope scanner suffix set (minus the safe usage keys).
_SECRET_SUFFIXES = (
    "apikey",
    "apitoken",
    "apisecret",
    "accesskey",
    "accesstoken",
    "authtoken",
    "bearertoken",
    "refreshtoken",
    "clientsecret",
    "clientcredential",
    "credential",
    "credentials",
    "authorization",
    "password",
    "passwd",
    "secret",
    "token",
    "privatekey",
    "signingkey",
    "signingsecret",
    "webhooksecret",
)
_SECRET_SAFE_KEYS = frozenset(
    {
        "prompttokens",
        "completiontokens",
        "totaltokens",
        "maxoutputtokens",
        "tokencount",
        "tokenusage",
        "tokenbudget",
    }
)

RULE_FORBIDDEN_MODEL_FIELD = "forbidden_model_field"
RULE_ACCOUNT_FIELD = "account_field"
RULE_SECRET_FIELD = "secret_field"

BOUNDARY_RULES = {
    RULE_FORBIDDEN_MODEL_FIELD: (
        "G023 forbidden model fields: account, balance, funds, capital, quantity, qty, pnl, profit, "
        "loss, pl, r, p&l, p_and_l; compact forms accountbalance/paperaccount/availablefunds/"
        "positionsize/positionquantity/positionqty/appliedpnl/profitloss/pandl; a position token "
        "combined with size/quantity/qty/notional/value; raw p&l; or p/l tokens separated by "
        "_ - space &."
    ),
    RULE_ACCOUNT_FIELD: (
        "Additional account-state leaks: equity, cash, margin, exposure, buying power, deposit, "
        "withdrawal, fees, slippage (token or compact form)."
    ),
    RULE_SECRET_FIELD: (
        "Credential-like names: apikey, api_key, secret, password, credential(s), token, passphrase, "
        "private_key, bearer, or a normalized key suffix matching secret/password/credential/token "
        "(G027 safe usage keys such as prompttokens/tokenusage are exempt)."
    ),
}


@dataclass(frozen=True)
class BoundaryIssue:
    """One field-level boundary violation found by the recursive scan."""

    path: str
    rule: str
    key: str

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "rule": self.rule, "key": self.key}


class ModelBoundaryError(ValueError):
    """A model-facing payload crossed the account-state/secret boundary.

    ``code`` is one of ``account_state_leak``, ``profile_ref_missing`` or
    ``profile_ref_mismatch``; ``issues`` carries at most the first ten
    :class:`BoundaryIssue` values.
    """

    def __init__(self, code: str, message: str, issues: list[BoundaryIssue] | tuple[BoundaryIssue, ...] = ()) -> None:
        if code not in {"account_state_leak", "profile_ref_missing", "profile_ref_mismatch"}:
            raise ValueError("unsupported model boundary error code")
        self.code = str(code)
        self.issues = tuple(issues[:10])
        super().__init__(str(message))


def _normalize_key(raw_key: str) -> tuple[list[str], str]:
    """Split a key into word tokens and its compact form (G023 rules)."""
    split = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw_key)
    tokens = [token for token in re.split(r"[^A-Za-z0-9]+", split.lower()) if token]
    return tokens, "".join(tokens)


def _rule_for_key(raw_key: str) -> tuple[str, str] | None:
    """Return ``(rule, normalized_key)`` for a forbidden key or ``None``."""
    tokens, compact = _normalize_key(raw_key)
    lowered = raw_key.lower()
    digits_stripped = re.sub(r"[0-9]+", "", compact)
    forbidden = (
        any(token in FORBIDDEN_MODEL_KEYS for token in tokens)
        or compact in _COMPACT_FORBIDDEN_KEYS
        or digits_stripped in FORBIDDEN_MODEL_KEYS | _COMPACT_FORBIDDEN_KEYS
        or "pnl" in compact
        or ("position" in tokens and any(token in _POSITION_VALUE_TOKENS for token in tokens))
        or "p&l" in lowered
        or (
            "p" in tokens
            and "l" in tokens
            and re.search(r"[^A-Za-z0-9]", raw_key) is not None
        )
    )
    if forbidden:
        return RULE_FORBIDDEN_MODEL_FIELD, compact
    if any(item in _ACCOUNT_FIELD_TOKENS for item in (*tokens, compact, digits_stripped)):
        return RULE_ACCOUNT_FIELD, compact
    if compact in _SECRET_SAFE_KEYS:
        return None
    compound_secret = (
        "key" in tokens
        and any(token in {"api", "access", "private", "signing", "secret", "auth", "webhook"} for token in tokens)
    ) or (
        any(token in {"secret", "token", "password", "credential"} for token in tokens)
        and any(token in {"api", "access", "auth", "bearer", "webhook", "client"} for token in tokens)
    )
    if (
        any(token in _SECRET_FIELD_TOKENS for token in tokens)
        or compact in _SECRET_FIELD_TOKENS
        or digits_stripped in _SECRET_FIELD_TOKENS
        or compound_secret
    ):
        return RULE_SECRET_FIELD, compact
    if any(compact.endswith(suffix) for suffix in _SECRET_SUFFIXES):
        return RULE_SECRET_FIELD, compact
    return None


def scan_model_boundary(value: Any, *, path: str = "$") -> list[BoundaryIssue]:
    """Recursively scan *value* and return every field-level violation.

    Only mapping keys are inspected; list/tuple indexes are traversed but
    never treated as field names.  JSON-native values are expected, but the
    scan is defensive about arbitrary nested containers.
    """
    issues: list[BoundaryIssue] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            raw_key = str(key)
            child_path = f"{path}.{raw_key}" if raw_key else path
            found = _rule_for_key(raw_key)
            if found is not None:
                issues.append(BoundaryIssue(child_path, found[0], raw_key))
            issues.extend(scan_model_boundary(child, path=child_path))
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            issues.extend(scan_model_boundary(child, path=f"{path}[{index}]"))
    return issues


def assert_model_boundary_safe(value: Any, *, context: str = "model_request") -> None:
    """Raise :class:`ModelBoundaryError` if *value* leaks account state."""
    issues = scan_model_boundary(value)
    if issues:
        first = issues[0]
        raise ModelBoundaryError(
            "account_state_leak",
            f"{context} violates the account-state/secret model boundary at {first.path}",
            issues,
        )


def _find_profile_refs(value: Any) -> list[Mapping[str, Any]]:
    refs: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        if "strategy_profile_ref" in value:
            candidate = value.get("strategy_profile_ref")
            # A present-but-malformed reference is itself a tamper signal.
            # Keep a sentinel mapping so the final match check fails even if
            # another valid reference exists elsewhere in the payload.
            refs.append(candidate if isinstance(candidate, Mapping) else {})
        for child in value.values():
            refs.extend(_find_profile_refs(child))
    elif isinstance(value, (list, tuple)):
        for child in value:
            refs.extend(_find_profile_refs(child))
    return refs


def _ref_matches(ref: Mapping[str, Any], expected_id: str, expected_hash: str, expected_strategy: Any) -> bool:
    if ref.get("profile_ref_id") != expected_id or ref.get("profile_ref_hash") != expected_hash:
        return False
    actual_strategy = ref.get("strategy_id")
    if actual_strategy is not None and expected_strategy is not None and actual_strategy != expected_strategy:
        return False
    return True


def profile_ref_intact(value: Any, expected_profile_ref: Mapping[str, Any]) -> bool:
    """Return whether *value* carries the expected strategy profile reference.

    The reference may live at any nested object path.  Its
    ``profile_ref_id`` and ``profile_ref_hash`` must equal the expected
    values, and a nested ``strategy_id`` (when present on both sides) must
    match as well.  Every discovered reference must match: a forged second
    reference anywhere in the payload fails the integrity check.
    """
    if not isinstance(expected_profile_ref, Mapping):
        return False
    expected_id = expected_profile_ref.get("profile_ref_id")
    expected_hash = expected_profile_ref.get("profile_ref_hash")
    expected_strategy = expected_profile_ref.get("strategy_id")
    if not isinstance(expected_id, str) or not isinstance(expected_hash, str):
        return False
    refs = _find_profile_refs(value)
    if not refs:
        return False
    return all(_ref_matches(ref, expected_id, expected_hash, expected_strategy) for ref in refs)


__all__ = [
    "BOUNDARY_RULES",
    "BoundaryIssue",
    "ModelBoundaryError",
    "RULE_ACCOUNT_FIELD",
    "RULE_FORBIDDEN_MODEL_FIELD",
    "RULE_SECRET_FIELD",
    "assert_model_boundary_safe",
    "profile_ref_intact",
    "scan_model_boundary",
]
