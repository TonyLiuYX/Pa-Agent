"""Provider-neutral market-feed identity used by the Phase 2 market seam.

``MarketFeedRef`` deliberately contains only public identity.  It is not a
provider client and has no credential or network behaviour.  A reference is
kept in ``extensions.market_feed`` on a G014 artifact so it participates in
the artifact's normal canonical hash.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Mapping

from pa_engine.contracts import ValidationIssue, ValidationResult, canonical_json_bytes


SCHEMA_VERSION = "0.1.0"
_SLUG_RE = re.compile(r"^[a-z0-9]+(?:[_-][a-z0-9]+)*$")
_FIELDS = ("schema_version", "provider", "feed", "coverage")


class _CallableBytes(bytes):
    """Bytes value that also tolerates the ``value()`` calling style."""

    def __call__(self) -> bytes:
        return bytes(self)


class _CallableHash(str):
    """String hash value that also tolerates the ``value()`` calling style."""

    def __call__(self) -> str:
        return str(self)


def _slug(value: Any) -> bool:
    return isinstance(value, str) and bool(_SLUG_RE.fullmatch(value))


@dataclass(frozen=True, slots=True)
class MarketFeedRef:
    """Immutable market-feed identity.

    The field set is intentionally exactly ``schema_version``, ``provider``,
    ``feed`` and ``coverage``.  Construction is strict so malformed values do
    not become hash-bound references by accident; callers handling external
    mappings should use :func:`validate_market_feed_ref` first.
    """

    schema_version: str
    provider: str
    feed: str
    coverage: str

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("market feed schema_version must be 0.1.0")
        for name in ("provider", "feed", "coverage"):
            if not _slug(getattr(self, name)):
                raise ValueError(f"market feed {name} must be a lower-case slug")

    def to_dict(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "provider": self.provider,
            "feed": self.feed,
            "coverage": self.coverage,
        }

    @property
    def canonical_bytes(self) -> _CallableBytes:
        """Return frozen G014 canonical JSON bytes for this value."""

        return _CallableBytes(canonical_json_bytes(self.to_dict()))

    @property
    def content_hash(self) -> _CallableHash:
        """Return SHA-256 of :meth:`canonical_bytes`."""

        return _CallableHash(hashlib.sha256(self.canonical_bytes).hexdigest())

    # Convenient read-only aliases for consumers that treat identity values
    # as properties rather than methods.
    @property
    def sha256(self) -> str:
        return str(self.content_hash)

    @property
    def hash(self) -> str:
        return str(self.content_hash)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MarketFeedRef":
        if not isinstance(value, Mapping):
            raise TypeError("market feed reference must be an object")
        if set(value) != set(_FIELDS):
            missing = set(_FIELDS) - set(value)
            extra = set(value) - set(_FIELDS)
            detail = []
            if missing:
                detail.append("missing " + ", ".join(sorted(str(item) for item in missing)))
            if extra:
                detail.append("unknown " + ", ".join(sorted(str(item) for item in extra)))
            raise ValueError("market feed reference must contain exactly four fields (" + "; ".join(detail) + ")")
        return cls(*(value.get(field) for field in _FIELDS))


ALPACA_IEX_FEED = MarketFeedRef(SCHEMA_VERSION, "alpaca", "iex", "single_exchange")
# A descriptive alias keeps downstream callers free to use either spelling.
ALPACA_IEX_MARKET_FEED = ALPACA_IEX_FEED


def _issue(code: str, path: str, message: str) -> ValidationIssue:
    return ValidationIssue(code, path or "$", message)


def validate_market_feed_ref(
    value: Any,
    *,
    expected: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_ref: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None = None,
    path: str = "$.extensions.market_feed",
) -> ValidationResult:
    """Validate a feed mapping and optionally compare it to ``expected``.

    The result uses the frozen G014 typed issue shape.  Unknown fields,
    credential-like fields and version mismatches are reported as internal
    feed issues; they are never copied into the G014 quality code vocabulary.
    """

    if expected is None:
        expected = expected_ref
    if expected is None:
        expected = expected_feed
    issues: list[ValidationIssue] = []
    if value is None:
        issues.append(_issue("missing_market_feed", path, "market feed reference is missing"))
        return ValidationResult(False, "market_feed_ref", issues)
    if not isinstance(value, Mapping):
        issues.append(_issue("malformed_market_feed", path, "market feed reference must be an object"))
        return ValidationResult(False, "market_feed_ref", issues)

    keys = set(value)
    for key in sorted(keys - set(_FIELDS), key=lambda item: str(item)):
        # Secret names receive a distinct typed issue while ordinary extras
        # remain observable as unknown fields.
        normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
        code = "secret_field" if any(token in normalized for token in ("token", "secret", "credential", "password", "authorization", "apikey", "accesskey")) else "unknown_field"
        issues.append(_issue(code, f"{path}.{key}", "field is not permitted on a market feed reference"))
    for field in _FIELDS:
        if field not in value:
            issues.append(_issue("missing_field", f"{path}.{field}", "required market feed field is missing"))
    if value.get("schema_version") != SCHEMA_VERSION:
        issues.append(_issue("version_mismatch", f"{path}.schema_version", "market feed schema_version must be 0.1.0"))
    for field in ("provider", "feed", "coverage"):
        if field in value and not _slug(value.get(field)):
            issues.append(_issue("invalid_slug", f"{path}.{field}", "identity field must be a non-empty lower-case slug"))

    ref: MarketFeedRef | None = None
    if not issues:
        try:
            ref = MarketFeedRef.from_mapping(value)
        except (TypeError, ValueError):
            # The individual issues above should normally make this branch
            # unreachable; retain one typed issue for unusual Mapping types.
            issues.append(_issue("malformed_market_feed", path, "market feed reference cannot be constructed"))
    if ref is not None and expected is not None:
        try:
            expected_ref = expected if isinstance(expected, MarketFeedRef) else MarketFeedRef.from_mapping(expected)
        except (TypeError, ValueError):
            issues.append(_issue("invalid_expected_feed", path, "expected market feed reference is invalid"))
        else:
            if ref != expected_ref:
                issues.append(_issue("market_feed_mismatch", path, "market feed reference does not match expected feed"))
    return ValidationResult(not issues, "market_feed_ref", issues)


def extract_market_feed(
    artifact: Mapping[str, Any],
    *,
    expected: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_ref: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None = None,
    require_feed: bool = False,
) -> tuple[MarketFeedRef | None, ValidationResult]:
    """Extract and validate ``extensions.market_feed`` from a G014 artifact."""

    if expected is None:
        expected = expected_ref
    if expected is None:
        expected = expected_feed
    path = "$.extensions.market_feed"
    extensions = artifact.get("extensions") if isinstance(artifact, Mapping) else None
    must_have = require_feed or expected is not None
    if not isinstance(extensions, Mapping):
        if must_have:
            issue = _issue("missing_market_feed", "$.extensions", "extensions.market_feed is required")
            return None, ValidationResult(False, "market_feed_ref", [issue])
        return None, ValidationResult(True, "market_feed_ref", [])
    if "market_feed" not in extensions:
        if must_have:
            issue = _issue("missing_market_feed", path, "extensions.market_feed is required")
            return None, ValidationResult(False, "market_feed_ref", [issue])
        return None, ValidationResult(True, "market_feed_ref", [])
    result = validate_market_feed_ref(extensions.get("market_feed"), expected=expected, path=path)
    if not result.valid:
        return None, result
    return MarketFeedRef.from_mapping(extensions["market_feed"]), result


def feed_from_artifact(
    artifact: Mapping[str, Any],
    *,
    expected: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_ref: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None = None,
    require_feed: bool = False,
) -> tuple[MarketFeedRef | None, ValidationResult]:
    """Compatibility alias for :func:`extract_market_feed`."""

    return extract_market_feed(artifact, expected=expected, expected_ref=expected_ref, expected_feed=expected_feed, require_feed=require_feed)


def market_feed_from_extensions(
    extensions: Mapping[str, Any] | None,
    *,
    expected: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_ref: MarketFeedRef | Mapping[str, Any] | None = None,
    expected_feed: MarketFeedRef | Mapping[str, Any] | None = None,
    require_feed: bool = False,
) -> tuple[MarketFeedRef | None, ValidationResult]:
    """Extract ``market_feed`` from an extensions object (not a full artifact)."""

    if expected is None:
        expected = expected_ref
    if expected is None:
        expected = expected_feed
    must_have = require_feed or expected is not None
    if not isinstance(extensions, Mapping) or "market_feed" not in extensions:
        if must_have:
            return None, ValidationResult(False, "market_feed_ref", [_issue("missing_market_feed", "$.market_feed", "market_feed is required")])
        return None, ValidationResult(True, "market_feed_ref", [])
    result = validate_market_feed_ref(extensions.get("market_feed"), expected=expected, path="$.market_feed")
    if not result.valid:
        return None, result
    return MarketFeedRef.from_mapping(extensions["market_feed"]), result


# Short aliases used by downstream adapters and older callers.
validate_feed_ref = validate_market_feed_ref
extract_market_feed_ref = extract_market_feed
market_feed_from_artifact = extract_market_feed


__all__ = [
    "ALPACA_IEX_FEED",
    "ALPACA_IEX_MARKET_FEED",
    "MarketFeedRef",
    "SCHEMA_VERSION",
    "extract_market_feed",
    "extract_market_feed_ref",
    "feed_from_artifact",
    "market_feed_from_artifact",
    "market_feed_from_extensions",
    "validate_feed_ref",
    "validate_market_feed_ref",
]
