"""MarketFeedRef identity, extension binding and secret-safe validation."""
from __future__ import annotations

import copy
from typing import Any, Mapping

import pytest

pytest_plugins = ("g015_fixtures",)

from pa_engine.contracts import canonical_json_bytes, content_hash, finalize_artifact, validate_artifact
from pa_engine.market import (
    ALPACA_IEX_FEED,
    MarketFeedRef,
    market_feed_from_extensions,
    validate_market_feed_ref,
    validate_market_snapshot,
)



def _issue_codes(result: Any) -> set[str]:
    codes: set[str] = set()
    for issue in result.issues:
        codes.add(issue.code if hasattr(issue, "code") else issue["code"])
    return codes


def _feed_value(extensions: Mapping[str, Any]) -> Any:
    return market_feed_from_extensions(extensions)[0]


def _feed_dict(value: MarketFeedRef) -> dict[str, Any]:
    return value.to_dict()


def test_feed_fixture_has_exact_four_public_fields_and_expected_identity(expected_feed, valid_snapshot):
    feed = _feed_dict(_feed_value(valid_snapshot["extensions"]))
    assert set(feed) == {"schema_version", "provider", "feed", "coverage"}
    assert feed == expected_feed.to_dict()
    assert MarketFeedRef.from_mapping(feed) == expected_feed
    result = validate_market_feed_ref(feed, expected=expected_feed)
    assert result.valid, result


def test_feed_canonical_reordering_is_stable_and_extension_mutation_binds_snapshot_hash(valid_snapshot, expected_feed):
    feed = valid_snapshot["extensions"]["market_feed"]
    reordered = {key: feed[key] for key in reversed(tuple(feed))}
    assert canonical_json_bytes(feed) == canonical_json_bytes(reordered)

    def reverse_objects(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: reverse_objects(value[key]) for key in reversed(tuple(value))}
        if isinstance(value, list):
            return [reverse_objects(item) for item in value]
        return value

    reordered_snapshot = reverse_objects(valid_snapshot)
    assert canonical_json_bytes(valid_snapshot) == canonical_json_bytes(reordered_snapshot)
    assert content_hash(valid_snapshot) == content_hash(reordered_snapshot)
    assert reordered_snapshot["market_snapshot_id"] == valid_snapshot["market_snapshot_id"]
    assert reordered_snapshot["content_hash"] == valid_snapshot["content_hash"]

    changed = copy.deepcopy(valid_snapshot)
    changed["extensions"]["market_feed"]["coverage"] = "consolidated"
    changed = finalize_artifact(changed, "market_snapshot")
    assert changed["content_hash"] != valid_snapshot["content_hash"]
    assert changed["market_snapshot_id"] != valid_snapshot["market_snapshot_id"]
    assert validate_artifact(changed, expected_kind="market_snapshot").valid
    assert not validate_market_snapshot(changed, expected_feed=expected_feed).valid


def test_feed_unknown_secret_missing_and_version_fail_with_typed_codes(expected_feed):
    cases = [
        ({**expected_feed.to_dict(), "unexpected": "value"}, "unknown_field"),
        ({**expected_feed.to_dict(), "api_key": "not-a-credential"}, "secret_field"),
        ({"schema_version": "9.9.9", "provider": "alpaca", "feed": "iex", "coverage": "single_exchange"}, "version_mismatch"),
        ({"schema_version": "0.1.0", "provider": "Alpaca", "feed": "iex", "coverage": "single_exchange"}, "invalid_slug"),
    ]
    for value, code in cases:
        result = validate_market_feed_ref(value, expected=expected_feed)
        assert not result.valid
        assert code in _issue_codes(result), (code, result)


def test_feed_ref_constructor_rejects_missing_or_extra_fields(expected_feed):
    with pytest.raises((TypeError, ValueError)):
        MarketFeedRef.from_mapping({key: value for key, value in expected_feed.to_dict().items() if key != "coverage"})
    with pytest.raises((TypeError, ValueError)):
        MarketFeedRef.from_mapping({**expected_feed.to_dict(), "credential": "placeholder"})


def test_legacy_empty_extensions_are_only_allowed_when_feed_is_optional(valid_snapshot, expected_feed):
    legacy = copy.deepcopy(valid_snapshot)
    legacy["extensions"] = {}
    legacy = finalize_artifact(legacy, "market_snapshot")
    optional = validate_market_snapshot(legacy, require_feed=False)
    assert optional.valid, optional
    required = validate_market_snapshot(legacy, expected_feed=expected_feed, require_feed=True)
    assert not required.valid
    assert "missing_market_feed" in _issue_codes(required)


def test_feed_reference_does_not_accept_secret_like_fields_or_vendor_behavior(expected_feed):
    assert set(expected_feed.to_dict()) == {"schema_version", "provider", "feed", "coverage"}
    assert not any("key" in key or "token" in key or "secret" in key for key in expected_feed.to_dict())
